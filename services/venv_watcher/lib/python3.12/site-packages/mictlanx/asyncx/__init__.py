
from typing import List,Dict,Tuple,AsyncGenerator
import time as T
import asyncio
import httpx
import mictlanx.interfaces as InterfaceX
from mictlanx.caching import CacheFactory
import humanfriendly as HF
from mictlanx.logger import Log
from option import Ok,Some,Result, Err,NONE
# import mictlanx. as InterfaceX
from mictlanx.utils.index import Utils
from mictlanx.utils.segmentation import Chunks,Chunk
import os
import mictlanx.errors  as EX
from mictlanx.asyncx.lb import RouterLoadBalancer
from mictlanx.asyncx.utils import AsyncClientUtils
from xolo.utils.utils import Utils as XoloUtils
from mictlanx.utils.uri import MictlanXURI
from mictlanx.retry import raf,RetryPolicy
from tqdm import tqdm
from mictlanx.services import AsyncRouter
from mictlanx.types import VerifyType
# from mictlanx.ut


class AsyncClient():
    """
    The MictlanX Client is an intuitive interface designed to facilitate seamless interactions between users and the MictlanX decentralized storage system. 
    This client provides essential functionalities that enable users to securely store (put) and retrieve (get) data with ease.
    """

    def __init__(
            self,
            uri:str,
            client_id:str,
            debug:bool=True,
            max_workers:int = 12,
            log_output_path:str = os.environ.get("MICTLANX_LOG_PATH","/mictlanx/client"),
            log_when:str="m",
            log_interval:int = 30,
            # routers:List[.Router] = [],
            eviction_policy:str = "LRU",
            capacity_storage:str = "1GB",
            verify:VerifyType=False
    ):
        """
        Initializes the Client with the following parameters: 

        Args:
            client_id (str): The unique identifier for the client.
            debug (bool): Enables or disables debug mode. Defaults to True.
            max_workers (int): The maximum number of workers. Defaults to 12.
            log_output_path (str): The path for log output. Defaults to "/mictlanx/client".
            log_when (str): The log rotation interval. Defaults to "m" (minutes).
            log_interval (int): The log rotation interval value. Defaults to 30.
        
        """
        self.cache     = CacheFactory.create(eviction_policy=eviction_policy, capacity_storage=HF.parse_size(capacity_storage))
        self.client_id = client_id
        # Peers
        routers = MictlanXURI.parse(uri = uri)
        self.__routers = list(map(AsyncRouter.from_router,routers))
        self.rlb       = RouterLoadBalancer(routers=self.__routers)
        
        # Log for basic operations
        self.__log         = Log(
            name                   = self.client_id,
            console_handler_filter = lambda x: debug,
            error_log              = True,
            when                   = log_when,
            interval               = log_interval,
            path                   = log_output_path,
            output_path            = Some("{}/{}".format(log_output_path,self.client_id))
        )
        self.verify = verify
        # PeerID -> PeerStats
        # self.__peer_stats:Dict[str, InterfaceX.PeerStats] = {}
        if not os.path.exists(log_output_path):
            os.makedirs(name=log_output_path,mode=0o777,exist_ok=True)
        max_workers      = os.cpu_count() if max_workers > os.cpu_count() else max_workers

    
    async def put_chunks(self,bucket_id:str, key:str, chunks:Chunks, tags:Dict[str,str]={}, rf:int =1, timeout:int=120, max_tries:int=5, max_concurrency:int=2,max_backoff:int = 5)->Result[bool, EX.MictlanXError]:
        try:
            t1          = T.time()
            _bucket_id  = Utils.sanitize_str(bucket_id)
            _key        = Utils.sanitize_str(key)
            router      = self.rlb.get_router()
            gen_bytes   = chunks.to_generator()

            (checksum,size) = XoloUtils.sha256_stream(gen_bytes)

            num_chunks = len(chunks)
            semaphore = asyncio.Semaphore(max_concurrency)  # ✅ Limit concurrency to 10 uploads at a time
            # progress_bar = tqdm(total=len(value))
            async def upload_chunk(chunk:Chunk, attempt=1)->Tuple[Chunk, Result[InterfaceX.PeerPutChunkedResponse, EX.MictlanXError]]:
                """Uploads a chunk and retries if it fails."""
                while attempt <= max_tries:
                    try:
                        async with semaphore:  # ✅ Ensure controlled parallelism
                            res = await AsyncClientUtils.put_chunk(
                                router=router,
                                client_id=self.client_id,
                                ball_id=key,
                                bucket_id=_bucket_id,
                                key=chunk.chunk_id,
                                chunk=chunk,
                                rf=rf,
                                timeout=timeout,
                                metadata={"num_chunks": str(num_chunks), "full_checksum": checksum,**tags}
                            )
                            self.__log.debug({
                                "event":"PUT.CHUNK",
                                "bucket_id":bucket_id,
                                "key":chunk.chunk_id,
                                "ok":res.is_ok
                            })
                        if res.is_ok:
                            return (None,Ok(res.unwrap()))  # ✅ Upload success
                        else:
                            self.__log.error({
                                "error":"PUT.CHUNK.ERROR",
                                "detail":str(res.unwrap_err())
                            })
                            raise Exception(res.unwrap_err())

                    except Exception as e:
                        self.__log.error({
                            "event":"UPLOAD.CHUNK.FAILED",
                            "detail":str(e)
                        })
                        self.__log.warning(f"Chunk {chunk.chunk_id} failed on attempt {attempt}/{max_tries}. Retrying...")
                        await asyncio.sleep(min(2 ** attempt , max_backoff ))  # ✅ Exponential backoff
                        attempt += 1

                return (chunk,Err(Exception(f"Failed to upload chunk {chunk.chunk_id} after {max_tries} retries.")))

            # ✅ Execute uploads in parallel with retries
            upload_tasks = [upload_chunk(chunk) for chunk in chunks.iter()]
            results = await asyncio.gather(*upload_tasks)

            # ✅ Check if any uploads failed
            failures = [res for res in results if res[1].is_err]
            if len(failures)>0:
                raise EX.PutChunksError(message="")

            self.__log.info({
                "event": "PUT",
                "bucket_id": _bucket_id,
                "key": _key,
                "response_time": T.time() - t1
            })
            return Ok(True)
                
               

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(e)
    
    async def put_file(self,bucket_id:str, key:str, path:str, tags:Dict[str,str]={}, chunk_size:str="256kb", rf:int =1, timeout:int=120, max_tries:int=5, max_concurrency:int=2,max_backoff:int =5)->Result[bool, EX.MictlanXError]:
        try:
            t1          = T.time()
            _bucket_id  = Utils.sanitize_str(bucket_id)
            _key        = Utils.sanitize_str(key)
            router      = self.rlb.get_router()
            _chunk_size = HF.parse_size(chunk_size)
            (_,checksum,size) = XoloUtils.extract_path_sha256_size(path=path)
            op_chunks = Chunks.from_file(path=path, group_id=key, chunk_size=Some(_chunk_size))
            if op_chunks.is_none:
                raise EX.UnknownError(message=f"Failed to read the file: {path}")
            chunks = op_chunks.unwrap()

            num_chunks = len(chunks)
            semaphore = asyncio.Semaphore(max_concurrency)  # ✅ Limit concurrency to 10 uploads at a time
            # progress_bar = tqdm(total=len(value))
            async def upload_chunk(chunk:Chunk, attempt=1)->Tuple[Chunk, Result[InterfaceX.PeerPutChunkedResponse, EX.MictlanXError]]:
                """Uploads a chunk and retries if it fails."""
                while attempt <= max_tries:
                    try:
                        async with semaphore:  # ✅ Ensure controlled parallelism
                            res = await AsyncClientUtils.put_chunk(
                                router=router,
                                client_id=self.client_id,
                                ball_id=key,
                                bucket_id=_bucket_id,
                                key=chunk.chunk_id,
                                chunk=chunk,
                                rf=rf,
                                timeout=timeout,
                                metadata={"num_chunks": str(num_chunks), "full_checksum": checksum,**tags}
                            )
                        if res.is_ok:
                            return (None,Ok(res.unwrap()))  # ✅ Upload success
                        else:
                            self.__log.error({
                                "error":"PUT.CHUNK.ERROR",
                                "detail":str(res.unwrap_err())
                            })
                            raise Exception(res.unwrap_err())
                    except Exception as e:
                        self.__log.error({
                            "event":"UPLOAD.CHUNK.FAILED",
                            "detail":str(e)
                        })
                        self.__log.warning(f"Chunk {chunk.chunk_id} failed on attempt {attempt}/{max_tries}. Retrying...")
                        await asyncio.sleep(min(2 ** attempt,max_backoff))  # ✅ Exponential backoff
                        attempt += 1

                return (chunk,Err(Exception(f"Failed to upload chunk {chunk.chunk_id} after {max_tries} retries.")))

            # ✅ Execute uploads in parallel with retries
            upload_tasks = [upload_chunk(chunk) for chunk in chunks.iter()]
            results = await asyncio.gather(*upload_tasks)

            # ✅ Check if any uploads failed
            failures = [res for res in results if res[1].is_err]
            if len(failures)>0:
                raise EX.PutChunksError(message="")

            self.__log.info({
                "event": "PUT",
                "bucket_id": _bucket_id,
                "key": _key,
                "response_time": T.time() - t1
            })
            return Ok(True)
                
               

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(e)
        

    async def put(self, bucket_id: str, key: str, value: bytes, chunk_size: str = "256kb", rf: int = 1, timeout: int = 120,max_tries:int = 5,max_concurrency:int =10,max_backoff:int=5,tags:Dict[str,str]={})->Result[bool,EX.MictlanXError]:
        """Uploads chunks and retries failed ones up to `MAX_TRIES`."""
        try:
            t1         = T.time()
            _bucket_id = Utils.sanitize_str(bucket_id)
            _key       = Utils.sanitize_str(key)
            checksum   = XoloUtils.sha256(value=value)
            chunks_op  = Chunks.from_bytes(data=value, group_id=key, chunk_size=Some(HF.parse_size(chunk_size)), chunk_prefix=Some(key))
            router     = self.rlb.get_router()
            if not chunks_op.is_some:
                raise ValueError("No valid chunks to upload.")

            chunks = chunks_op.unwrap()
          
            num_chunks = len(chunks)
            semaphore = asyncio.Semaphore(max_concurrency)  # ✅ Limit concurrency to 10 uploads at a time
            progress_bar = tqdm(total=len(value))
            async def upload_chunk(chunk:Chunk, attempt=1)->Tuple[Chunk, Result[InterfaceX.PeerPutChunkedResponse, EX.MictlanXError]]:
                """Uploads a chunk and retries if it fails."""
                while attempt <= max_tries:
                    try:
                        async with semaphore:  # ✅ Ensure controlled parallelism
                            res = await AsyncClientUtils.put_chunk(
                                router=router,
                                ball_id=key,
                                client_id=self.client_id,
                                bucket_id=_bucket_id,
                                key=chunk.chunk_id,
                                chunk=chunk,
                                rf=rf,
                                timeout=timeout,
                                metadata={
                                    "num_chunks": str(num_chunks), 
                                    "full_checksum": checksum,**tags
                                },
                                chunk_size="1MB"
                            )
                        if res.is_ok:
                            progress_bar.update(chunk.size)
                            return (None,Ok(res.unwrap()))  # ✅ Upload success
                        else:
                            self.__log.error({
                                "error":"PUT.CHUNK.ERROR",
                                "detail":str(res.unwrap_err())
                            })
                            raise Exception(res.unwrap_err())
                    except Exception as e:
                        self.__log.error({
                            "event":"UPLOAD.CHUNK.FAILED",
                            "detail":str(e)
                        })
                        self.__log.warning(f"Chunk {chunk.chunk_id} failed on attempt {attempt}/{max_tries}. Retrying...")
                        await asyncio.sleep(min(2 ** attempt,max_backoff))  # ✅ Exponential backoff
                        attempt += 1

                return (chunk,Err(Exception(f"Failed to upload chunk {chunk.chunk_id} after {max_tries} retries.")))

            # ✅ Execute uploads in parallel with retries
            upload_tasks = [upload_chunk(chunk) for chunk in chunks.iter()]
            results = await asyncio.gather(*upload_tasks)
            progress_bar.close()
            # ✅ Check if any uploads failed
            failures = [res for res in results if res[1].is_err]
            if len(failures)>0:
                raise EX.PutChunksError(message="")

            self.__log.info({
                "event": "PUT",
                "bucket_id": _bucket_id,
                "key": _key,
                "response_time": T.time() - t1
            })
            return Ok(True)

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            r = await self.delete(bucket_id=bucket_id,ball_id=key, timeout=timeout, force=True)
            self.__log.error({
                "name":_e.get_name(),
                "message":_e.message,
                "is_deleted":r.is_ok,
                "status":_e.status_code, 
            })
            
            return Err(e)


    async def put_single_chunk(self, bucket_id: str, ball_id: str, chunk: Chunk, chunk_size: str = "256kb", rf: int = 1, timeout: int = 120,max_tries:int = 5,max_concurrency:int =10,max_backoff:int=5,tags:Dict[str,str]={})->Result[bool,EX.MictlanXError]:
        """Uploads chunks and retries failed ones up to `MAX_TRIES`."""
        try:
            t1           = T.time()
            _bucket_id   = Utils.sanitize_str(bucket_id)
            _ball_id         = Utils.sanitize_str(ball_id)
            router       = self.rlb.get_router()
            semaphore    = asyncio.Semaphore(max_concurrency)  # ✅ Limit concurrency to 10 uploads at a time
            progress_bar = tqdm(total=chunk.size)
            async def upload_chunk(chunk:Chunk, attempt=1)->Tuple[Chunk, Result[InterfaceX.PeerPutChunkedResponse, EX.MictlanXError]]:
                """Uploads a chunk and retries if it fails."""
                while attempt <= max_tries:
                    try:
                        async with semaphore:  # ✅ Ensure controlled parallelism
                            res = await AsyncClientUtils.put_chunk(
                                router=router,
                                ball_id=_ball_id,
                                client_id=self.client_id,
                                bucket_id=_bucket_id,
                                key=chunk.chunk_id,
                                chunk=chunk,
                                rf=rf,
                                timeout=timeout,
                                metadata={**tags},
                                chunk_size="1MB"
                            )
                        if res.is_ok:
                            progress_bar.update(chunk.size)
                            return (None,Ok(res.unwrap()))  # ✅ Upload success
                        else:
                            self.__log.error({
                                "error":"PUT.CHUNK.ERROR",
                                "detail":str(res.unwrap_err())
                            })
                            raise Exception(res.unwrap_err())
                    except Exception as e:
                        self.__log.error({
                            "event":"UPLOAD.CHUNK.FAILED",
                            "detail":str(e)
                        })
                        self.__log.warning(f"Chunk {chunk.chunk_id} failed on attempt {attempt}/{max_tries}. Retrying...")
                        await asyncio.sleep(min(2 ** attempt,max_backoff))  # ✅ Exponential backoff
                        attempt += 1

                return (chunk,Err(Exception(f"Failed to upload chunk {chunk.chunk_id} after {max_tries} retries.")))

            # ✅ Execute uploads in parallel with retries
            # upload_tasks = [upload_chunk(chunk) for chunk in chunks.iter()]
            upload_tasks = [upload_chunk(chunk=chunk)]
            results = await asyncio.gather(*upload_tasks)
            progress_bar.close()
            # ✅ Check if any uploads failed
            failures = [res for res in results if res[1].is_err]
            if len(failures)>0:
                messages = "\n".join([str(res.unwrap_err()) for _,res in failures])
                self.__log.error({
                    "event":"PUT.CHUNKS.ERROR",
                    "bucket_id":bucket_id,
                    "key":ball_id,
                    "raw":messages,
                })
                return Err(EX.PutChunksError(message=messages))

            self.__log.info({
                "event": "PUT",
                "bucket_id": _bucket_id,
                "ball_id": _ball_id,
                "key":chunk.chunk_id,
                "response_time": T.time() - t1
            })
            return Ok(True)

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            r = await self.delete(bucket_id=bucket_id,ball_id=ball_id, timeout=timeout, force=True)
            self.__log.error({
                "name":_e.get_name(),
                "message":_e.message,
                "is_deleted":r.is_ok,
                "status":_e.status_code, 
            })
            
            return Err(e)


    async def get_chunk(
        self,
        bucket_id: str,
        ball_id: str,
        index: int,
        max_parallel_gets: int = 10,
        headers: Dict[str, str] = {},
        chunk_size: str = "256kb",
        timeout: int = 120,
        http2: bool = False,
        max_retries: int = 15,
        delay: float = 1.0,
        backoff_factor: float = 0.5,
        force: bool = False,
        max_backoff:int =5,
        max_delay:int = 30,
        jitter:bool = True
    )->Result[Tuple[Chunk, InterfaceX.Metadata], EX.MictlanXError]:
        try:
            t1 = T.time()
            _bucket_id = Utils.sanitize_str(bucket_id)
            _ball_id = Utils.sanitize_str(ball_id)
            headers["Chunk-Size"] = chunk_size
            headers["Accept-Encoding"] = headers.get("Accept-Encoding", "identity")
            headers["Force-Get"] = str(headers.get("Force", str(int(force))))

            router = self.rlb.get_router()
            chunk_key = f"{_ball_id}_{index}"
            retry_policy = RetryPolicy(retries=max_retries, initial_delay=delay, backoff_factor=backoff_factor, max_delay=max_delay,jitter=jitter)

            metadata_result = await raf(
                func       = lambda: router.get_metadata(bucket_id=bucket_id, key=chunk_key),
                policy     = retry_policy,
                on_attempt = lambda i: self.__log.debug({"event":"GET.METADATA.FAILED.ATTEMPT", "attempt":i,"max_attempts":retry_policy.retries}),
                on_error   = lambda i,e: self.__log.error({"error":str(e.message), "status_code":e.status_code })
            )

            if metadata_result.is_err:
                raise EX.MictlanXError.from_exception(metadata_result.unwrap_err())
            metadata = metadata_result.unwrap()
            pbar = tqdm(total=1)

            async with httpx.AsyncClient(http2=http2, trust_env=False, timeout=timeout, verify=self.verify, headers=headers) as client:
                semaphore = asyncio.Semaphore(max_parallel_gets)

                async def fetch_chunk_with_retry():
                    attempt = 0
                    while attempt < max_retries:
                        try:
                            async with semaphore:
                                t2 = T.time()
                                # chunk_key = chunk_key
                                res = await AsyncClientUtils.get_chunk(
                                    client=client,
                                    router=router,
                                    bucket_id=_bucket_id,
                                    key=chunk_key,
                                    chunk_size=chunk_size,
                                    headers=headers
                                )
                                elapsed = T.time() - t2
                                
                                if res.is_ok:
                                    pbar.set_postfix({'chunk': 1, 'resp_time': f"{elapsed:.2f}s"})
                                    pbar.update(n=1)
                                    self.__log.info({
                                        "event":"GET.CHUNK",
                                        "bucket_id":bucket_id,
                                        "key":chunk_key,
                                        "size":chunk_size,
                                        "response_time":elapsed
                                    })
                                    return res
                                else:
                                    raise EX.GetChunkError()

                        except Exception as e:
                            attempt += 1
                            current_backoff = delay * (backoff_factor ** (attempt - 1))
                            backoff = min(current_backoff, max_backoff) if max_backoff >0 else current_backoff
                            await asyncio.sleep(backoff)
                            self.__log.warning({
                                "event": "GET.CHUNK.RETRY",
                                "chunk_index": index,
                                "attempt": attempt,
                                "error": str(e),
                                "backoff": backoff
                            })
                    return Err(EX.GetChunkError(f"Failed to fetch chunk  after {max_retries} attempts"))

                futures = [fetch_chunk_with_retry()]
                completed = 0

                for coro in asyncio.as_completed(futures):
                    result = await coro
                    if result.is_ok:
                        completed += 1
                        (metadata, data)= result.unwrap()
                        pbar.close()
                        self.__log.info({
                            "event": "GET",
                            "bucket_id": _bucket_id,
                            "key": chunk_key,
                            "response_time": T.time() - t1
                        })
                        c = Chunk.from_bytes(group_id=_ball_id, index=index,data=data,metadata={**metadata.tags}, chunk_id=Some(chunk_key))
                        return Ok((c,metadata))
                    else:
                        e = result.unwrap_err()
                        self.__log.error({
                            "event": "CHUNK.FAILURE",
                            "detail": e.message
                        })
                        return Err(e)

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name": _e.get_name(),
                "message": _e.message,
                "status": _e.status_code,
            })
            raise _e    
    async def get_chunks(self,
        bucket_id: str,
        key: str,
        max_parallel_gets: int = 10,
        headers: Dict[str, str] = {},
        chunk_size: str = "256kb",
        timeout: int = 120,
        http2: bool = False,
        max_retries: int = 15,
        delay: float = 1.0,
        backoff_factor: float = 0.5,
        force: bool = False,
        max_backoff:int =5,
        chunk_index:int = 0,
        max_delay:int =30,
        jitter:bool = True,
    ) -> AsyncGenerator[Tuple[InterfaceX.Metadata, memoryview], None]:

        try:
            t1 = T.time()
            _bucket_id = Utils.sanitize_str(bucket_id)
            _key = Utils.sanitize_str(key)
            headers["Chunk-Size"] = chunk_size
            headers["Accept-Encoding"] = headers.get("Accept-Encoding", "identity")
            headers["Force-Get"] = str(headers.get("Force", str(int(force))))

            router = self.rlb.get_router()

            initial_chunk_key = f"{_key}{'_'+str(chunk_index) if chunk_index >=0 else ''}"
            retry_policy = RetryPolicy(retries=max_retries, initial_delay=delay, backoff_factor=backoff_factor, max_delay=max_delay,jitter=jitter)

            metadata_result = await raf(
                func       = lambda: router.get_metadata(bucket_id=_bucket_id, key=initial_chunk_key),
                policy     = retry_policy,
                on_attempt = lambda i: self.__log.debug({"event":"GET.METADATA.FAILED.ATTEMPT", "attempt":i,"max_attempts":retry_policy.retries}),
                on_error   = lambda i,e: self.__log.error({"error":str(e.message), "status_code":e.status_code })
            )

            # metadata_result = await raf(
            #     func=router.get_metadata,
            #     fkwargs={"bucket_id": _bucket_id, "key": initial_chunk_key },
            #     retries=max_retries,
            #     delay=delay,
            #     backoff_factor=backoff_factor
            # )

            if metadata_result.is_err:
                raise EX.MictlanXError.from_exception(metadata_result.unwrap_err())

            metadata = metadata_result.unwrap()
            num_chunks = int(metadata.metadata.tags.get("num_chunks"))
            if num_chunks <= 0:
                raise EX.ValidationError(message=f"No valid number of chunks: {num_chunks}")

            pbar = tqdm(total=num_chunks)
            rts = []
            async with httpx.AsyncClient(http2=http2, trust_env=False, timeout=timeout, verify=self.verify, headers=headers) as client:
                semaphore = asyncio.Semaphore(max_parallel_gets)

                async def fetch_chunk_with_retry(i: int):
                    attempt = 0
                    while attempt < max_retries:
                        try:
                            async with semaphore:
                                t2 = T.time()
                                chunk_key = f"{_key}_{i}"
                                res = await AsyncClientUtils.get_chunk(
                                    client=client,
                                    router=router,
                                    bucket_id=_bucket_id,
                                    key=chunk_key,
                                    chunk_size=chunk_size,
                                    headers=headers
                                )
                                elapsed = T.time() - t2
                                rts.append(elapsed)
                                if res.is_ok:
                                    pbar.set_postfix({'chunk': i, 'resp_time': f"{elapsed:.2f}s"})
                                    pbar.update(n=1)
                                    self.__log.info({
                                        "event":"GET.CHUNK",
                                        "bucket_id":bucket_id,
                                        "key":chunk_key,
                                        "size":chunk_size,
                                        "response_time":elapsed
                                    })
                                    return res
                                else:
                                    raise EX.GetChunkError()

                        except Exception as e:
                            attempt += 1
                            current_backoff = delay * (backoff_factor ** (attempt - 1))
                            backoff = min(current_backoff, max_backoff) if max_backoff >0 else current_backoff
                            await asyncio.sleep(backoff)
                            self.__log.warning({
                                "event": "GET.CHUNK.RETRY",
                                "chunk": i,
                                "attempt": attempt,
                                "error": str(e),
                                "backoff": backoff
                            })
                    return Err(EX.GetChunkError(f"Failed to fetch chunk {i} after {max_retries} attempts"))

                futures = [fetch_chunk_with_retry(i) for i in range(num_chunks)]
                completed = 0

                for coro in asyncio.as_completed(futures):
                    result = await coro
                    if result.is_ok:
                        completed += 1
                        yield result.unwrap()
                    else:
                        self.__log.error({
                            "event": "CHUNK.FAILURE",
                            "detail": result.unwrap_err().message
                        })

                pbar.close()

                if completed != num_chunks:
                    raise EX.NotFoundError(f"Some chunks were missing: expected = {num_chunks}, chunks={completed}")
                
                if len(rts) == 0:
                    raise EX.UnknownError(message=f"{_bucket_id}@{key} not found",status_code=404)
                
                self.__log.info({
                    "event": "GET",
                    "bucket_id": _bucket_id,
                    "key": _key,
                    "response_time": max(rts)
                })

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name": _e.get_name(),
                "message": _e.message,
                "status": _e.status_code,
            })
            raise _e    


            # return 
    
    async def get(self,
        bucket_id:str,
        key:str,
        max_paralell_gets:int = 10,
        headers:Dict[str,str]={},
        chunk_size:str="256kb", 
        timeout:int = 120,
        http2:bool = False,
        max_retries:int = 5,
        delay:float = 1,
        backoff_factor:float =.5,
        force:bool = False,
        max_backoff:int =5,
        chunk_index:int = 0,
        max_delay:int =30,
        jitter:bool = True
    )->Result[InterfaceX.AsyncGetResponse, EX.MictlanXError]:
        try:
            t1                    = T.time()
            _bucket_id            = Utils.sanitize_str(bucket_id)
            _key                  = Utils.sanitize_str(key)
            headers["Chunk-Size"] = chunk_size
            headers["Accept-Encoding"] = str(headers.get("Accept-Encoding","identity"))
            headers["Force-Get"] = str(headers.get("Force",int(force)))
            router                = self.rlb.get_router()
            initial_chunk_key = f"{_key}{'_'+str(chunk_index) if chunk_index >=0 else ''}"

            retry_policy = RetryPolicy(retries=max_retries, initial_delay=delay, backoff_factor=backoff_factor, max_delay=max_delay,jitter=jitter)

            metadata_result = await raf(
                func       = lambda: router.get_metadata(bucket_id=_bucket_id, key=initial_chunk_key),
                policy     = retry_policy,
                on_attempt = lambda i: self.__log.debug({"event":"GET.METADATA.FAILED.ATTEMPT", "attempt":i,"max_attempts":retry_policy.retries}),
                on_error   = lambda i,e: self.__log.error({"error":str(e.message), "status_code":e.status_code })
            )

            # metadata_result = await raf(
            #     func    = router.get_metadata,
            #     fkwargs = {"bucket_id":_bucket_id, "key":initial_chunk_key}, 
            #     retries = max_retries,
            #     delay = delay, 
            #     backoff_factor = backoff_factor
            # )
            rts = []
            if metadata_result.is_ok:
                metadata = metadata_result.unwrap()
                num_chunks = int(metadata.metadata.tags.get("num_chunks"))
                if num_chunks <= 0:
                    raise EX.ValidationError(message=f"No valid numuber of chunks: {num_chunks}")
                
                pbar = tqdm(total=num_chunks)
                async with httpx.AsyncClient(http2=http2,trust_env=False, timeout=timeout,verify=self.verify, headers=headers) as client:
                    semaphore = asyncio.Semaphore(max_paralell_gets)  # Limit to 10 parallel requests
                    async def fetch_chunk(i: int):
                        attempt = 0
                        while attempt < max_retries:
                            try:
                                async with semaphore:
                                    t2 = T.time()
                                    chunk_key = f"{_key}_{i}"
                                    res = await AsyncClientUtils.get_chunk(
                                        client=client,
                                        router=router,
                                        bucket_id=_bucket_id,
                                        key=chunk_key,
                                        chunk_size=chunk_size,
                                        headers=headers
                                    )
                                    get_chunk_rt = T.time() - t2
                                    rts.append(get_chunk_rt)
                                    if res.is_ok:
                                        pbar.set_postfix({'chunk': i, 'resp_time': f"{get_chunk_rt:.2f}s"})
                                        pbar.update(n=1)
                                        self.__log.info({
                                            "event":"GET.CHUNK",
                                            "bucket_id":bucket_id,
                                            "key":chunk_key,
                                            "size":chunk_size,
                                            "response_time":get_chunk_rt
                                        })
                                        return res
                                    else:
                                        raise EX.GetChunkError()

                            except Exception as e:
                                attempt += 1
                                current_backoff = delay * (backoff_factor ** (attempt - 1))
                                backoff = min(current_backoff, max_backoff) if max_backoff >0 else current_backoff
                                await asyncio.sleep(backoff)
                                self.__log.warning({
                                    "event": "GET.CHUNK.RETRY",
                                    "chunk": i,
                                    "attempt": attempt,
                                    "error": str(e),
                                    "backoff": backoff
                                })
                        return Err(EX.GetChunkError(f"Failed to fetch chunk {i} after {max_retries} attempts"))


                    futures = [fetch_chunk(i) for i in range(num_chunks)]

                    results = await asyncio.gather(*futures)
                    # print("FUTURES", len(results))
                responses:List[Tuple[InterfaceX.Metadata, memoryview]] = list(map(lambda x:x.unwrap(),filter(lambda x:x.is_ok, results) ))
                # print(responses)
                if len(responses) ==0:
                    raise EX.NotFoundError("No chunks were found")
                elif len(responses) != num_chunks:
                    raise EX.NotFoundError(f"Some chunks were missing: expected = {num_chunks}, chunks={len(responses)}")
                
                pbar.close()
                remote_checksum = responses[0][0].tags.get("full_checksum","")
                x = await AsyncClientUtils.merge_chunks(chunks=responses)
                checksum = XoloUtils.sha256(x.tobytes())
                if not remote_checksum == checksum:
                    self.__log.warning({
                        "event":"INTEGRITY.CHECK.FAILED",
                        "remote_checksum":remote_checksum,
                        "local_checksum":checksum
                    })
                    raise EX.IntegrityError( message=f"Integrity check failed, remote not match with local checksum: {remote_checksum} != {checksum}")
                if len(rts) == 0:
                    return Err(EX.UnknownError(message=f"{_bucket_id}@{key} not found",status_code=404))
                
                self.__log.info({ 
                    "event":"GET",
                    "bucket_id":_bucket_id,
                    "key":_key,
                    "checksum":checksum,
                    "response_time": max(rts)
                })
                metadatas = list(map(lambda x:x[0], responses))
                return Ok(InterfaceX.AsyncGetResponse(data=x, metadatas=metadatas))
            raise EX.MictlanXError.from_exception(metadata_result.unwrap_err())
        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(EX.MictlanXError.from_exception(e))
    
    async def get_to_file(self,
        bucket_id: str,
        ball_id: str,
        output_path: str = "",
        fullname: str = "",
        max_paralell_gets: int = 10,
        headers: Dict[str, str] = {},
        chunk_size: str = "256kb", 
        timeout: int = 120,
        http2: bool = False, 
        max_retries: int = 5,
        delay: float = 1,
        backoff_factor: float = .5,
        force: bool = False,
        chunk_index:int =0,
        max_delay:int =30,
        jitter:bool = True 
    ) -> Result[str, EX.MictlanXError]:
        try:
            os.makedirs(output_path, exist_ok=True)
            t1 = T.time()
            _bucket_id = Utils.sanitize_str(bucket_id)
            _ball_id = Utils.sanitize_str(ball_id)
            headers["Chunk-Size"] = chunk_size
            headers["Accept-Encoding"] = headers.get("Accept-Encoding", "identity")
            headers["Force-Get"] = str(headers.get("Force", str(int(force))))
            router = self.rlb.get_router()

            initial_chunk_key = f"{_ball_id}{'_'+str(chunk_index) if chunk_index >=0 else ''}"
            retry_policy = RetryPolicy(retries=max_retries, initial_delay=delay, backoff_factor=backoff_factor, max_delay=max_delay,jitter=jitter)

            metadata_result = await raf(
                func       = lambda: router.get_metadata(bucket_id=_bucket_id, key=initial_chunk_key),
                policy     = retry_policy,
                on_attempt = lambda i: self.__log.debug({"event":"GET.METADATA.FAILED.ATTEMPT", "attempt":i,"max_attempts":retry_policy.retries}),
                on_error   = lambda i,e: self.__log.error({"error":str(e.message), "status_code":e.status_code })
            )

            # metadata_result = await raf(
            #     func=router.get_metadata,
            #     fkwargs={"bucket_id": _bucket_id, "key":initial_chunk_key },
            #     retries=max_retries,
            #     delay=delay,
            #     backoff_factor=backoff_factor
            # )

            if not metadata_result.is_ok:
                raise EX.MictlanXError.from_exception(metadata_result.unwrap_err())

            metadata = metadata_result.unwrap()
            tmp_ext = metadata.metadata.tags.get("extension", "")
            _fullname = fullname if fullname else f"{_ball_id}{'.'+tmp_ext if tmp_ext else ''}"
            tmp_fullname = metadata.metadata.tags.get("fullname", _fullname)
            full_checksum = metadata.metadata.tags.get("full_checksum", "")
            _path = f"{output_path}/{tmp_fullname}"

            if os.path.exists(_path):
                (local_checksum, _) = XoloUtils.sha256_file(path=_path)
                if local_checksum == full_checksum:
                    self.__log.info({
                        "event": "GET",
                        "bucket_id": _bucket_id,
                        "key": _ball_id,
                        "path": _path,
                        "hit": True,
                        "response_time": T.time() - t1
                    })
                    return Ok(_path)
                else:
                    raise EX.FileAlreadyExists(message=f"File already exists: {_path}")

            num_chunks = int(metadata.metadata.tags.get("num_chunks"))
            if num_chunks <= 0:
                raise EX.ValidationError(message=f"Invalid number of chunks: {num_chunks}")

            pbar = tqdm(total=num_chunks)
            received_chunks = {}
            received_lock = asyncio.Lock()
            next_to_write = 0
            semaphore = asyncio.Semaphore(max_paralell_gets)

            async with httpx.AsyncClient(http2=http2, trust_env=False, timeout=timeout, verify=self.verify, headers=headers) as client:
                
                async def fetch_chunk(i):
                    async with semaphore:
                        t2 = T.time()
                        res = await AsyncClientUtils.get_chunk(
                            client=client, router=router,
                            bucket_id=_bucket_id, key=f"{_ball_id}_{i}",
                            chunk_size=chunk_size, headers=headers
                        )
                        elapsed = T.time() - t2
                        if res.is_ok:
                            chunk_i_metadata, chunk_i_data = res.unwrap()
                            index = int(chunk_i_metadata.tags.get("index", i))  # default to i
                            async with received_lock:
                                received_chunks[index] = chunk_i_data.tobytes()
                            pbar.set_postfix({'chunk': index, 'resp_time': f"{elapsed:.2f}s"})
                        else:
                            raise EX.GetChunkError()

                async def writer_loop(f):
                    nonlocal next_to_write
                    while next_to_write < num_chunks:
                        async with received_lock:
                            while next_to_write in received_chunks:
                                f.write(received_chunks[next_to_write])
                                del received_chunks[next_to_write]
                                next_to_write += 1
                                pbar.update(1)
                        await asyncio.sleep(0.0001)

                # Start fetching + writing
                with open(_path, "wb") as f:
                    fetchers = [fetch_chunk(i) for i in range(num_chunks)]
                    await asyncio.gather(writer_loop(f), *fetchers)

            pbar.close()
            self.__log.info({
                "event": "GET",
                "bucket_id": _bucket_id,
                "key": _ball_id,
                "path": _path,
                "hit": False,
                "response_time": T.time() - t1
            })
            return Ok(_path)

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name": _e.get_name(),
                "message": _e.message,
                "status": _e.status_code,
            })
            return Err(_e)
        

    # async def delete_all()
    async def get_metadata_by_key(self,bucket_id:str, key:str,timeout: int = 120,headers: Dict[str, str] = {})->Result[InterfaceX.GetMetadataResponse,EX.MictlanXError]:
        try:
            t1                    = T.time()
            _bucket_id            = Utils.sanitize_str(bucket_id)
            _key              = Utils.sanitize_str(key)
            router                = self.rlb.get_router()
            x = await router.get_metadata(bucket_id=_bucket_id, key=_key,timeout=timeout, headers=headers)
            return x
        except Exception as e:
                _e = EX.MictlanXError.from_exception(e)
                self.__log.error({
                    "name":_e.get_name(),
                    "message":_e.message,
                    "status":_e.status_code, 
                })
                return Err(_e)
    async def get_metadata(self,bucket_id:str,ball_id:str,timeout: int = 120,headers: Dict[str, str] = {})->Result[InterfaceX.Ball,EX.MictlanXError]:
        try:
            t1                    = T.time()
            _bucket_id            = Utils.sanitize_str(bucket_id)
            _ball_id              = Utils.sanitize_str(ball_id)
            router                = self.rlb.get_router()
            x = await router.get_chunks_metadata(bucket_id=_bucket_id, key=_ball_id,timeout=timeout, headers=headers)
            if x.is_err:
                raise x.unwrap_err()
            bm = x.unwrap()
            b = InterfaceX.Ball(bucket_id=bucket_id,chunks=bm.chunks,checksum=bm.checksum,ball_id=ball_id)
            b.build()
            return Ok(b)
        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(_e)      
    
    async def delete(self,
        ball_id:str,
        bucket_id:str,
        timeout: int = 120,
        force:bool = True,
        headers: Dict[str, str] = {}
    )->Result[InterfaceX.DeletedByBallIdResponse, EX.MictlanXError]:
        try:
            t1                    = T.time()
            _bucket_id            = Utils.sanitize_str(bucket_id)
            _ball_id              = Utils.sanitize_str(ball_id)
            headers["Accept-Encoding"] = headers.get("Accept-Encoding","identity")
            # headers["Force"] = str(int(force))
            router                = self.rlb.get_router()
            ball_metadata_result = await router.get_chunks_metadata(bucket_id=_bucket_id, key=_ball_id,timeout=timeout, headers=headers)
            if ball_metadata_result.is_err:
                return Err(ball_metadata_result.unwrap_err())
            ball_metadata = ball_metadata_result.unwrap()
            coros = []
            for c in ball_metadata.chunks:
                coro_i = self.delete_by_key(
                    bucket_id=bucket_id,
                    key=c.key,timeout=timeout,
                    force = force,
                    headers=headers
                )
                coros.append(coro_i)
            results:List[Result[InterfaceX.DeletedByKeyResponse]] = await asyncio.gather(*coros)
            n_results = len(results)
            _results = list(map(lambda x:x.unwrap(),filter(lambda x:x.is_ok, results)))
            n_ok_results = len(_results)
            n_err_results = n_results - n_ok_results

            if n_err_results > 0:
                return Err(EX.UnknownError("Failed to delete a chunk, please try again."))
            
            res = InterfaceX.DeletedByBallIdResponse(n_deletes=0, ball_id=_ball_id)
            for r in _results:
                res.n_deletes+= r.n_deletes
            return Ok(res)
        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(_e)
    async def delete_by_key(self,
                     key: str,
                     bucket_id: str = "",
                     timeout: int = 120,
                     force:bool = True,
                     headers: Dict[str, str] = {}) -> Result[InterfaceX.DeletedByKeyResponse, EX.MictlanXError]:
        """
        Asynchronously deletes the data associated with the given key from the specified bucket.
        
        Args:
            key (str): The key for the data to be deleted.
            bucket_id (str): The ID of the bucket from which the data will be deleted. Defaults to an empty string.
            timeout (int): The timeout for the operation in seconds. Defaults to 120 seconds.
            headers (Dict[str, str]): Additional headers for the request. Defaults to an empty dictionary.
        
        Returns:
            Result[InterfaceX.DeleteByKeyResponse, Exception]: The result of the delete operation.
        """
        _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        _bucket_id = self.__bucket_id if _bucket_id == "" else _bucket_id
        try:
            failed = []
            del_res = InterfaceX.DeletedByKeyResponse(n_deletes=0, key=key)
            headers["Force"] = str(int(force))
            for router in self.__routers:
                start_time = T.time()
                # Await the async delete call on each router.
                del_result = await router.delete(bucket_id=_bucket_id, key=_key, headers=headers)
                if del_result.is_err:
                    response_time = T.time() - start_time
                    self.__log.error({
                        "event": "DELETE",
                        "bucket_id": _bucket_id,
                        "key": key,
                        "router_id": router.router_id,
                        "response_time": response_time
                    })
                    failed.append(router)
                else:
                    x_response = del_result.unwrap()
                    del_res.n_deletes += x_response.n_deletes
                    response_time = T.time() - start_time
                    self.__log.info({
                        "event": "DELETE",
                        "bucket_id": _bucket_id,
                        "key": _key,
                        "n_deletes": del_res.n_deletes,
                        "router_id": router.router_id,
                        "response_time": response_time
                    })
            return Ok(del_res)
        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(_e)    
    async def get_bucket_metadata(
        self, 
        bucket_id:str,
        timeout: int = 120,
        headers: Dict[str, str] = {}
    )-> Result[InterfaceX.Bucket,EX.MictlanXError]:
        try:
            res = await self.get_chunks_by_bucket_id(bucket_id=bucket_id)
            if res.is_err:
                raise res.unwrap_err()
            response = res.unwrap()
            # response.balls
            balls  =  await AsyncClientUtils.group_chunks(balls_list=response.balls,num_threads=4)
            bucket = InterfaceX.Bucket(bucket_id= bucket_id, balls= balls)
            return Ok(bucket)
            # print(len(bucket), bucket.size(), bucket.size_bytes())
        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(_e)
    async def get_chunks_by_bucket_id(
            self,
            bucket_id: str,
            timeout: int = 120,
            headers: Dict[str, str] = {}
        ) -> Result[InterfaceX.GetRouterBucketMetadataResponse, EX.MictlanXError]:
            """
            Asynchronously fetches the metadata for the specified bucket from the given router.

            Args:
                bucket_id (str): The ID of the bucket.
                router (InterfaceX.Router): The router to use for the request.
                timeout (int): The timeout for the request (in seconds).
                headers (Dict[str, str]): Optional HTTP headers to include in the request.

            Returns:
                Result[InterfaceX.GetRouterBucketMetadataResponse, Exception]: On success, returns an Ok-wrapped response; otherwise, returns an Err with the exception.
            """
            try:
                router = self.rlb.get_router()
                start_time = T.time()
                # Await the async call from the router.
                x = await router.get_bucket_metadata(bucket_id=bucket_id, timeout=timeout, headers=headers)
                service_time = T.time() - start_time
                if x.is_ok:
                    self.__log.info({
                        "event": "GET.BUCKET.METADATA",
                        "bucket_id": bucket_id,
                        "router_id": router.router_id,
                        "service_time": service_time
                    })
                    return x
                else:
                    self.__log.error({
                        "msg": str(x.unwrap_err()),
                        "bucket_id": bucket_id,
                        "router_id": router.router_id,
                        "service_time": service_time
                    })
                    return x

            except Exception as e:
                _e = EX.MictlanXError.from_exception(e)
                self.__log.error({
                    "name":_e.get_name(),
                    "message":_e.message,
                    "status":_e.status_code, 
                })
                return Err(_e)
    async def delete_bucket(self, bucket_id: str, headers: Dict[str, str] = {}, timeout: int = 120,force:bool = True) -> Result[InterfaceX.DeleteBucketResponse, Exception]:
        """
        Asynchronously deletes the specified bucket by fetching metadata from each router,
        then concurrently deleting each object (ball) found.

        Args:
            bucket_id (str): The ID of the bucket to delete.
            headers (Dict[str, str]): Optional HTTP headers.
            timeout (int): Request timeout in seconds.

        Returns:
            Result[InterfaceX.DeleteBucketResponse, Exception]: An Ok-wrapped response on success,
            or an Err with an Exception on failure.
        """
        try: 
            start_time = T.time()
            deleted = 0
            failed = 0
            total = 0
            keys = []  # Collect keys if needed

            deletion_tasks = []

            # For each router, schedule an asynchronous get_bucket_metadata call.
            metadata_tasks = [
                router.get_bucket_metadata(bucket_id=bucket_id, headers=headers, timeout=timeout)
                for router in self.__routers
            ]
            
            # Wait for all metadata calls to complete concurrently.
            metadata_results = await asyncio.gather(*metadata_tasks, return_exceptions=True)
            
            # Process each router's metadata result.
            for router, meta_result in zip(self.__routers, metadata_results):
                if meta_result.is_err:
                    self.__log.error({
                        "msg": str(meta_result.unwrap_err()),
                        "bucket_id": bucket_id,
                        "router_id": router.router_id
                    })
                    continue
                if meta_result.is_ok:
                    metadata = meta_result.unwrap()
                    total += len(metadata.balls)
                    # Schedule delete tasks for each ball (object) in the metadata.
                    for ball in metadata.balls:
                        deletion_tasks.append(
                            self.delete_by_key(key=ball.key, bucket_id=bucket_id, headers=headers, timeout=timeout,force=force)
                        )
                else:
                    self.__log.error({
                        "msg": str(meta_result.unwrap_err()),
                        "bucket_id": bucket_id,
                        "router_id": router.router_id
                    })
            
            # Execute all deletion tasks concurrently.
            deletion_results = await asyncio.gather(*deletion_tasks, return_exceptions=True)
            
            for result in deletion_results:
                if isinstance(result, Exception):
                    failed += 1
                else:
                    if result.is_ok:
                        deleted += 1
                    else:
                        failed += 1

            rt = T.time() - start_time
            self.__log.info({
                "event": "DELETED.BUCKET",
                "bucket_id": bucket_id,
                "deleted": deleted,
                "failed": failed,
                "total": total,
                "response_time": rt
            })
            
            return Ok(InterfaceX.DeleteBucketResponse(
                bucket_id=bucket_id,
                deleted=deleted,
                failed=failed,
                total=total,
                keys=keys,
                response_time=rt
            ))
        except Exception as e: 
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(_e)
        
