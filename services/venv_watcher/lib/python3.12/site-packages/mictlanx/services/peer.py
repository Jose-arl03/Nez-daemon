
# Std
import os 
import json as J
from typing import Dict,Any,List,Iterator,AsyncGenerator
# Externals
from xolo.utils import Utils as XoloUtils
import httpx
from retry.api import retry_call
from option import Result,Ok,Err
import humanfriendly as HF
# Locals
import mictlanx.interfaces.responses as ResponseModels
from mictlanx.types import VerifyType
from mictlanx.errors import MictlanXError

class AsyncPeer(object):
    def __init__(self,
                 peer_id: str,
                 ip_addr: str,
                 port: int,
                 protocol: str = "http",
                 api_version:int = 4
    ):
        self.peer_id     = peer_id
        self.ip_addr     = ip_addr
        self.port        = port
        self.protocol    = protocol
        self.api_version = api_version

    async def flush_tasks(self, headers: Dict[str, str] = {}, timeout: int = 120) -> Result[bool, Exception]:
        """
        Default description

        Args:
            headers (str): this are key-values for http requests.
        """
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/tasks"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.delete(url, headers=headers)
            response.raise_for_status()
            return Ok(True)
        except httpx.ConnectError as e:
            return Err(MictlanXError.from_exception(e))
        except Exception as e:
            return Err(e)

    async def get_all_ball_sizes(self, headers: Dict[str, str] = {}, timeout: int = 120, start: int = 0, end: int = 0) -> Result[List[ResponseModels.BallBasicData], Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/xballs/size?start={start}" + ("" if end <= 0 else f"&end={end}")
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            xs = []
            for x in data_json:
                xs.append(ResponseModels.BallBasicData(
                    bucket_id = x[0],
                    key       = x[1],
                    size      = x[2],
                ) )

            return Ok(xs)
            # return Ok([ResponseModels.BallBasicData.model_validate(x) for x in data_json
            # return Ok([ResponseModels.BallBasicData(x) for x in data_json])
        except Exception as e:
            return Err(e)

    async def get_balls_len(self, headers: Dict[str, str] = {}, timeout: int = 120) -> Result[int, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/balls/len"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            return Ok(data_json.get("len", 0))
        except Exception as e:
            return Err(e)

    async def get_state(self, headers: Dict[str, str] = {}, timeout: int = 120, start: int = 0, end: int = 0) -> Result[ResponseModels.PeerCurrentState, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/peers/state?start={start}" + ("" if end <= 0 else f"&end={end}")
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            body = ResponseModels.PeerCurrentState(
                nodes=[ResponseModels.PeerData(**x) for x in data_json.get("nodes", [])],
                balls={k: ResponseModels.BallContext(**v) for k, v in data_json.get("balls", {}).items()}
            )
            return Ok(body)
        except Exception as e:
            return Err(e)

    async def get_stats(self, headers: Dict[str, str] = {}, timeout: int = 120, start: int = 0, end: int = 0) -> Result[ResponseModels.PeerStatsResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/stats?start={start}" + ("" if end <= 0 else f"&end={end}")
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            body = ResponseModels.PeerStatsResponse(
                available_disk=data_json.get("available_disk", 0),
                balls=[ResponseModels.Metadata(**b) for b in data_json.get("balls", [])],
                disk_uf=data_json.get("disk_uf", 0.0),
                peer_id=data_json.get("peer_id", "peer-id"),
                peers=data_json.get("peers", []),
                total_disk=data_json.get("total_disk", 0),
                used_disk=data_json.get("used_disk", 0),
            )
            return Ok(body)
        except Exception as e:
            return Err(e)

    async def get_balls(self, start: int = 0, end: int = 0, headers: Dict[str, str] = {}, timeout: int = 120) -> Result[List[ResponseModels.BallBasicData], Exception]:
        try:
            url = f"{self.base_url()}/api/v4/xballs?start={start}" + ("" if end <= 0 else f"&end={end}")
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            return Ok([ResponseModels.BallBasicData(**b) for b in response.json()])
        except Exception as e:
            return Err(e)

    async def add_peer(self, id: str, disk: int, memory: int, ip_addr: str, port: int, weight: float, used_disk: int = 0, used_memory: int = 0, headers: Dict[str, str] = {}, timeout: int = 3600) -> Result[ResponseModels.StoragePeerResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/nodes"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, headers=headers, json={
                    "id": id,
                    "disk": disk,
                    "memory": memory,
                    "ip_addr": ip_addr,
                    "port": port,
                    "weight": weight,
                    "used_disk": used_disk,
                    "used_memory": used_memory,
                })
            response.raise_for_status()
            data_json = response.json()
            return Ok(ResponseModels.StoragePeerResponse(**data_json))
        except httpx.HTTPError as e:
            if e.response is not None:
                return Err(Exception(e.response.text))
            else:
                return Err(Exception(str(e)))
        except Exception as e:
            return Err(e)

    async def __add_peer(self, id: str, disk: int, memory: int, ip_addr: str, port: int, weight: float, used_disk: int = 0, used_memory: int = 0, headers: Dict[str, str] = {}, timeout: int = 3600):
        result = await self.add_peer(
            id=id,
            disk=disk,
            memory=memory,
            ip_addr=ip_addr,
            port=port,
            weight=weight,
            used_disk=used_disk,
            used_memory=used_memory,
            timeout=timeout,
            headers=headers
        )
        if result.is_err:
            raise result.unwrap_err()
        return result

    async def add_peer_with_retry(
        self,
        id: str,
        disk: int,
        memory: int,
        ip_addr: str,
        port: int,
        weight: float,
        used_disk: int = 0,
        used_memory: int = 0,
        headers: Dict[str, str] = {},
        timeout: int = 3600,
        tries: int = 100,
        delay: int = 1,
        max_delay: int = 5,
        jitter: float = 0.0,
        backoff: float = 1,
        logger: Any = None
    ) -> Result[ResponseModels.StoragePeerResponse, Exception]:
        try:
            result =  await retry_call(
                self.__add_peer,
                fkwargs={
                    "id": id,
                    "disk": disk,
                    "memory": memory,
                    "ip_addr": ip_addr,
                    "port": port,
                    "weight": weight,
                    "used_disk": used_disk,
                    "used_memory": used_memory,
                    "timeout": timeout,
                    "headers": headers,
                },
                tries=tries,
                delay=delay,
                max_delay=max_delay,
                jitter=jitter,
                backoff=backoff,
                logger=logger,
            )
            return result
        except Exception as e:
            return Err(e)

    async def replicate(self, bucket_id: str, key: str, timeout: int = 120, headers: Dict[str, str] = {}) -> Result[ResponseModels.ReplicateResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/{key}/replicate"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            return Ok(ResponseModels.ReplicateResponse(**data_json))
        except httpx.HTTPError as e:
            if e.response is not None:
                return Err(Exception(e.response.text))
            return Err(Exception(e))
        except Exception as e:
            return Err(e)

    async def get_size(self, bucket_id: str, key: str, timeout: int = 120, headers: Dict[str, str] = {}) -> Result[ResponseModels.GetSizeByKey, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/{key}/size"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            return Ok(ResponseModels.GetSizeByKey(**data_json))
        except Exception as e:
            return Err(e)

    async def delete(self, bucket_id: str, key: str, timeout: int = 120, headers: Dict[str, str] = {}) -> Result[ResponseModels.DeletedByKeyResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/{key}"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.delete(url, headers=headers)
            response.raise_for_status()
            return Ok(ResponseModels.DeletedByKeyResponse(
                n_deletes=int(response.headers.get("n-deletes", -1)),
                key=key
            ))
        except Exception as e:
            return Err(e)

    async def delete_by_ball_id(self, ball_id: str, bucket_id: str, timeout: int = 120, headers: Dict[str, str] = {}) -> Result[ResponseModels.DeletedByBallIdResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/bid/{ball_id}"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.delete(url, headers=headers)
            response.raise_for_status()
            return Ok(ResponseModels.DeletedByBallIdResponse(
                n_deletes=int(response.headers.get("n-deletes", -1)),
                ball_id=ball_id
            ))
        except httpx.HTTPError as e:
            return Err(e)
        except Exception as e:
            return Err(e)

    async def get_chunks_metadata(self, ball_id: str, bucket_id: str = "", timeout: int = 120, headers: Dict[str, str] = {}) -> Result[ResponseModels.GroupedBallResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/metadata/{ball_id}/group"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            data_json = response.json()
            grouped_balls = ResponseModels.GroupedBallResponse.model_validate(data_json)
            # chunks_metadata_json = map(lambda x: ResponseModels.Metadata(**x), response.json())
            return Ok(grouped_balls)
        except Exception as e:
            return Err(e)


    async def disable(self, bucket_id: str, key: str, headers: Dict[str, str] = {}) -> Result[bool, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/{key}/disable"
            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers)
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)

    async def put_chunked(self, task_id: str, chunks: AsyncGenerator[bytes, Any], timeout: int = 120, headers: Dict[str, str] = {}) -> Result[ResponseModels.PeerPutChunkedResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/data/{task_id}/chunked"
            async with httpx.AsyncClient(timeout=timeout) as client:
                put_response = await client.post(url, data=chunks, headers=headers)
            put_response.raise_for_status()
            data = ResponseModels.PeerPutChunkedResponse(**J.loads(put_response.content))
            return Ok(data)
        except Exception as e:
            return Err(e)

    @staticmethod
    def empty():
        return AsyncPeer(peer_id="", ip_addr="", port=-1)

    def get_addr(self) -> str:
        return f"{self.ip_addr}:{self.port}"

    def base_url(self) -> str:
        if self.port == -1 or self.port == 0:
            return f"{self.protocol}://{self.ip_addr}"
        return f"{self.protocol}://{self.ip_addr}:{self.port}"

    async def get_metadata(self, bucket_id: str, key: str, timeout: int = 300, headers: Dict[str, str] = {}) -> Result[ResponseModels.GetMetadataResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/metadata/{key}"
            async with httpx.AsyncClient(timeout=timeout) as client:
                get_metadata_response = await client.get(url, headers=headers)
            get_metadata_response.raise_for_status()
            response = ResponseModels.GetMetadataResponse.model_validate(get_metadata_response.json())
            return Ok(response)
        except Exception as e:
            return Err(e)
    
    async def get_by_ball_id(self,bucket_id:str,ball_id, timeout:int = 120, headers:Dict[str,str]={})->Result[ResponseModels.GroupedBallResponse, Exception]:
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/metadata/{ball_id}/group"
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                return Ok(ResponseModels.GroupedBallResponse.model_validate(data))
            
        except Exception as e:
            return Err(e)

    async def get_streaming(self, bucket_id: str, key: str, timeout: int = 300, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[httpx.Response, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/{key}"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            return Ok(response)
        except Exception as e:
            return Err(e)

    async def get_to_file(self, bucket_id: str, key: str, chunk_size: str = "1MB", sink_folder_path: str = "/mictlanx/data", timeout: int = 300, filename: str = "", headers: Dict[str, str] = {}) -> Result[str, Exception]:
        try:
            _chunk_size = HF.parse_size(chunk_size)
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path, exist_ok=True)
            combined_key = XoloUtils.sha256(f"{bucket_id}@{key}".encode()) if filename == "" else filename
            fullpath = f"{sink_folder_path}/{combined_key}"
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/{key}"
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream("GET", url, headers=headers) as response:
                    response.raise_for_status()
                    with open(fullpath, "wb") as f:
                        async for chunk in response.aiter_bytes(_chunk_size):
                            if chunk:
                                f.write(chunk)
            return Ok(fullpath)
        except Exception as e:
            return Err(e)

    async def put_metadata(self, key: str, size: int, checksum: str, producer_id: str, content_type: str, ball_id: str, bucket_id: str, tags: Dict[str, str] = {}, timeout: int = 120, is_disable: bool = False, headers: Dict[str, str] = {}) -> Result[ResponseModels.PeerPutMetadataResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/metadata"
            payload = {
                "key": key,
                "size": size,
                "checksum": checksum,
                "tags": tags,
                "producer_id": producer_id,
                "content_type": content_type,
                "ball_id": ball_id,
                "bucket_id": bucket_id,
                "is_disable": is_disable
            }
            async with httpx.AsyncClient(timeout=timeout) as client:
                put_metadata_response = await client.post(url, json=payload, headers=headers)
            put_metadata_response.raise_for_status()
            res_json = put_metadata_response.json()
            return Ok(ResponseModels.PeerPutMetadataResponse.model_validate(res_json))

        except Exception as e:
            return Err(e)

    async def put_data(self, task_id: str, key: str, value: bytes, content_type: str, timeout: int = 120, headers: Dict[str, str] = {}, file_id: str = "data") -> Result[Any, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/data/{task_id}"
            async with httpx.AsyncClient(timeout=timeout) as client:
                put_response = await client.post(
                    url,
                    files={file_id: (key, value, content_type)},
                    headers=headers,
                )
            put_response.raise_for_status()
            return Ok(())
        except Exception as e:
            return Err(e)

    async def get_bucket_metadata(self, bucket_id: str, timeout: int = 120, headers: Dict[str, str] = {}) -> Result[ResponseModels.GetBucketMetadataResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/metadata"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            return Ok(ResponseModels.GetBucketMetadataResponse.model_validate(response.json()))
        except Exception as e:
            return Err(e)

    async def get_ufs(self, timeout: int = 120, headers: Dict[str, str] = {}) -> Result[ResponseModels.GetUFSResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/stats/ufs"
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            return Ok(ResponseModels.GetUFSResponse.model_validate(response.json()))
        except Exception as e:
            return Err(e)

    async def __get_ufs(self, timeout: int = 120, headers: Dict[str, str] = {}):
        res = await self.get_ufs(timeout=timeout, headers=headers)
        if res.is_err:
            raise res.unwrap_err()
        return res

    async def get_ufs_with_retry(self, timeout: int = 60, headers: Dict[str, str] = {}, tries: int = 100, delay: int = 1, max_delay: int = 5, jitter: float = 0.0, backoff: float = 1, logger: Any = None):
        try:
            result = await retry_call(
                self.__get_ufs,
                fkwargs={
                    "timeout": timeout,
                    "headers": headers
                },
                tries=tries,
                delay=delay,
                max_delay=max_delay,
                jitter=jitter,
                backoff=backoff,
                logger=logger,
            )
            return result
        except Exception as e:
            return Err(e)

    def __eq__(self, other: "AsyncPeer") -> bool:
        if not isinstance(other, AsyncPeer):
            return False
        return (self.ip_addr == other.ip_addr and self.port == other.port) or self.peer_id == other.peer_id

    def __str__(self):
        return f"Peer(id = {self.peer_id}, ip_addr = {self.ip_addr}, port = {self.port})"

    def to_dict(self)->Dict[str,Any]:
        return self.__dict__