import mictlanx.interfaces as InterfaceX
from mictlanx.services import AsyncRouter
import mictlanx.errors as EX
from mictlanx.utils.segmentation import Chunk
import humanfriendly as HF
from typing import Dict,Tuple,List
from option import Err,Ok,Result
import asyncio
from mictlanx.types import VerifyType
import httpx 
from tqdm import tqdm
import os 
import time as T
from mictlanx.logger import Log
log         = Log(
    name = __name__,
    console_handler_filter =  lambda x: True,
    error_log=True,
    when="h",
    interval=24,
    to_file=False,
    path=os.environ.get("MICTLANX_LOG_PATH","/mictlanx/client")
    # output_path=
)

class AsyncClientUtils:

    def __init__(self):
        pass

    @staticmethod
    def process_balls_segment(segment: List['InterfaceX.Metadata']) -> Dict[str, InterfaceX. Ball]:
        local_balls: Dict[str, InterfaceX.Ball] = {}

        for chunk in segment:
            ball_id = chunk.ball_id
            if ball_id not in local_balls:
                local_balls[ball_id] = InterfaceX.Ball(bucket_id=chunk.bucket_id,ball_id=ball_id,chunks=[chunk])
            else:
                local_balls[ball_id].add_chunk(chunk)


        return local_balls

    @staticmethod
    def merge_balls(partials: List[Dict[str, InterfaceX.Ball]]) -> Dict[str, InterfaceX.Ball]:
        merged: Dict[str, InterfaceX.Ball] = {}

        for partial in partials:
            for ball_id, ball in partial.items():
                if ball_id in merged:
                    merged[ball_id].merge(other=ball)
                else:
                    merged[ball_id] = ball

        return merged

    @staticmethod
    async def group_chunks(balls_list: List['InterfaceX.Metadata'], num_threads: int = 4) -> Dict[str, InterfaceX.Ball]:
        chunk_size = (len(balls_list) + num_threads - 1) // num_threads
        segments = [balls_list[i * chunk_size:(i + 1) * chunk_size] for i in range(num_threads)]

        tasks = [asyncio.to_thread(AsyncClientUtils.process_balls_segment, segment) for segment in segments]
        partials = await asyncio.gather(*tasks)
        # print("="*10)
        # print(partials)
        result = AsyncClientUtils.merge_balls(partials)
        for bid ,b in result.items():
            b.build()
        return result 

    @staticmethod
    async def merge_chunks(chunks:List[Tuple[InterfaceX.Metadata, memoryview]])->memoryview:
        """Merges memoryview chunks in correct order using metadata index."""
        try:
            num_chunks = len(chunks)
            ordered_chunks = [None] * num_chunks  # ✅ Pre-allocate N holes

            
            async def place_chunk(metadata, chunk):
                """Places chunk in the correct position."""
                index = int(metadata.tags["index"])  # Convert string index to int
                if index <= num_chunks:
                    ordered_chunks[index] = chunk  # ✅ Insert into correct position
                else: 
                    raise EX.ValidationError(message=f"Index out of range:{index} > {num_chunks}")

            # ✅ Process all chunks concurrently
            await asyncio.gather(*(place_chunk(meta, chunk) for meta, chunk in chunks))

            # ✅ Ensure all chunks are present (Optional: Handle missing chunks)
            if None in ordered_chunks:
                raise ValueError("Some chunks are missing")

            # ✅ Merge all chunks into one contiguous memoryview
            merged = b"".join(chunk.tobytes() for chunk in ordered_chunks)

            return memoryview(merged)
        except Exception as e:
            print("MERGE_CHUNKS", e)
            raise e


    @staticmethod
    async def get_chunk(
        client:httpx.Client,
        router: AsyncRouter,
        bucket_id: str,
        key: str,
        timeout: int = 120,
        headers: Dict[str, str] = {},
        chunk_size: str = "256kb",  # Faster large chunk transfers
        verify:VerifyType = False,
    ) -> Result[Tuple[InterfaceX.Metadata, memoryview], EX.MictlanXError]:
        """Ultra-fast download function similar to `curl`."""
        try:
            _chunk_size = HF.parse_size(chunk_size)  # Convert "4MB" to bytes

            # ✅ Fetch metadata (Parallel Execution)
            metadata_result = await router.get_metadata(bucket_id, key, timeout, headers)
            # print("METADATA_RESULTR", metadata_result)
            if metadata_result.is_err:
                return Err(EX.MictlanXError.from_exception(metadata_result.unwrap_err()))
            
            metadata = metadata_result.unwrap().metadata
            expected_size = int(metadata.size)  # ✅ Expected total size

            # ✅ Fetch streaming response using HTTP/2
            # async with httpx.AsyncClient(http2=True, timeout=timeout,verify=verify) as client:
            url = f"{router.base_url()}/api/v4/buckets/{bucket_id}/{key}"
            async with client.stream("GET", url, headers=headers) as response:
                if response.status_code != 200:
                    return Err(EX.MictlanXError(f"HTTP {response.status_code}: Failed to fetch data"))

                # ✅ Optimize TCP settings for speed
                # if hasattr(response, "raw") and hasattr(response.raw, "_fp") and hasattr(response.raw._fp, "fp") and hasattr(response.raw._fp.fp, "_sock"):
                #     sock = response.raw._fp.fp._sock
                #     sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # 🔥 Disable Nagle's Algorithm
                #     sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4194304)  # 🔥 Increase TCP buffer to 4MB

                # ✅ Fast Memory-Mapped Bytearray (Avoids Copies)
                # pbar = tqdm(total=expected_size)
                chunk_data = bytearray(expected_size)
                view = memoryview(chunk_data)
                offset = 0

                async for chunk in response.aiter_bytes(_chunk_size):
                    size = len(chunk)
                    # pbar.update(size)

                    # ✅ Ensure assignment is done correctly with compatible memoryview
                    mv_chunk = memoryview(chunk)  # Convert bytes to memoryview

                    if offset + size > expected_size:
                        size = expected_size - offset  # Avoid overflow

                    view[offset:offset + size] = mv_chunk[:size]  # ✅ Corrected assignment
                    offset += size

                    # percentage = (offset *100) /expected_size
                    # print(f"CHUNK {key}  Downloaded {HF.format_size(size)} -- {HF.format_size(offset)} / {HF.format_size(expected_size)} ({percentage:.2f}%)")
                # pbar.close()

                # ✅ Ensure full file is downloaded
                if offset != expected_size:
                    return Err(EX.MictlanXError(f"Mismatch: Received {offset} bytes, expected {expected_size}"))

                # print(f"{key} Successfully downloaded {HF.format_size(offset)} bytes.")
                return Ok((metadata, memoryview(chunk_data)))  # ✅ Zero-copy memory handling

        except Exception as e:
            return Err(EX.MictlanXError.from_exception(e))


    @staticmethod
    async def _get_chunk(router:AsyncRouter,bucket_id, key:str,timeout:int = 120, headers:Dict[str,str]={},chunk_size:str="128kkb")->Result[Tuple[InterfaceX.Metadata, memoryview],EX.MictlanXError]:
        try:
            metadata_result = router.get_metadata(bucket_id=bucket_id, key=key, timeout=timeout,headers=headers)
            if metadata_result.is_ok:
                data_result = router.get_streaming(bucket_id=bucket_id, key=key,timeout=timeout, headers=headers)
                metadata = metadata_result.unwrap().metadata
                if data_result.is_ok:
                    return Ok((metadata, memoryview(data_result.unwrap().content )) )
                return Err(EX.MictlanXError.from_exception(data_result.unwrap_err()))
            else:
                return Err(EX.MictlanXError.from_exception(metadata_result.unwrap_err()))
        except Exception as e:
            return Err(EX.MictlanXError.from_exception(e))


    @staticmethod
    async def put_chunk(router:AsyncRouter,client_id:str,ball_id:str,bucket_id:str, key:str, chunk:Chunk,metadata:Dict[str,str]={},rf:int=1,timeout:int = 120,chunk_size:str= "256kb")->Result[InterfaceX.PeerPutChunkedResponse, EX.MictlanXError]:
        try:
            size                = chunk.size
            t1_metadata         = T.time()
            put_metadata_result = await router.put_metadata(
                key                = chunk.chunk_id,
                bucket_id          = bucket_id,
                size               = size,
                checksum           = chunk.checksum,
                ball_id            = ball_id,
                content_type       = "application/octet-stream",
                is_disabled        = False,
                replication_factor = rf,
                tags               = {
                    **chunk.metadata,
                    **metadata
                },
                timeout     = timeout,
                headers     = {},
                producer_id = client_id
            )
            rt_metadata       = T.time() - t1_metadata

            log.debug({
                "event":"PUT.METADATA",
                "bucket_id":bucket_id,
                "key":ball_id,
                "size": size,
                "ok":put_metadata_result.is_ok
            })

            if put_metadata_result.is_ok:
                put_metadata_response = put_metadata_result.unwrap()
                # print(put_metadata_response.tasks_ids)
                for task_id in put_metadata_response.tasks_ids:
                    chunks = chunk.to_async_generator(chunk_size=chunk_size)
                    # print("BEFORE",type(chunks),chunks)
                    
                    put_result = await router.put_chunked(
                        task_id = task_id,
                        chunks  = chunks,
                        timeout = timeout,
                        headers = {"Content-Type": "application/octet-stream"}
                    )
                    return put_result
            return Err(put_metadata_result.unwrap_err())
        except Exception as e:
            return Err(EX.MictlanXError.from_exception(e=e))