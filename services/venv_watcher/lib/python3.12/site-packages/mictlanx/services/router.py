
from typing import Dict,Any,List,AsyncGenerator
import os 
import json as J
# 
from xolo.utils import Utils as XoloUtils
import httpx
from option import Result,Ok,Err
# 
from mictlanx.services import AsyncPeer
import mictlanx.interfaces.responses as ResponseModels
import humanfriendly as HF
from mictlanx.types import VerifyType


class AsyncRouter:
    def __init__(self, router_id: str, ip_addr: str, port: int, protocol: str = "http",http2:bool=False,api_version:int=4):
        self.router_id   = router_id
        self.ip_addr     = ip_addr
        self.port        = port
        self.protocol    = protocol
        self.http2       = http2
        self.api_version = api_version

    def get_addr(self) -> str:
        return f"{self.ip_addr}:{self.port}"

    def base_url(self) -> str:
        if self.port in (-1, 0):
            return f"{self.protocol}://{self.ip_addr}"
        elif self.protocol == "https":
            return f"{self.protocol}://{self.ip_addr}"
        return f"{self.protocol}://{self.ip_addr}:{self.port}"

    def __eq__(self, other: "AsyncRouter") -> bool:
        if not isinstance(other, AsyncRouter):
            return False
        return (self.ip_addr == other.ip_addr and self.port == other.port) or self.router_id == other.peer_id

    def __str__(self):
        return f"Router(id = {self.router_id}, ip_addr={self.ip_addr}, port={self.port})"

    @staticmethod
    def from_str(x: str) -> Result["AsyncRouter", Exception]:
        parts = x.split(":")
        if len(parts) < 3:
            return Err(Exception(f"Invalid string: {x} - expected format <router_id>:<ip_addr>:<port>"))
        router_id, ip_addr, port = parts[0], parts[1], parts[2]
        protocol = "https" if int(port) <= 0 else "http"
        return Ok(AsyncRouter(peer_id=router_id, ip_addr=ip_addr, port=int(port), protocol=protocol))
    @staticmethod
    def from_router(x:"AsyncRouter",http2:bool=False)->'AsyncRouter':
        return AsyncRouter(router_id=x.router_id, ip_addr=x.ip_addr, port=x.port, protocol=x.protocol,http2=http2 )

    async def add_peers(self, peers: List['AsyncPeer'], headers: Dict[str, str] = {}, timeout: int = 120) -> Result[bool, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/xpeers"
            xs = [{
                "protocol": p.protocol,
                "hostname": p.ip_addr,
                "port": p.port,
                "peer_id": p.peer_id
            } for p in peers]
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, headers=headers, json=xs)
                response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    async def update_metadata(self, bucket_id: str, key: str, metadata: Any, headers: Dict[str, str] = {},verify:VerifyType = False,timeout:int=120) -> Result[bool, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/u/buckets/{bucket_id}/{key}"
            data_json = metadata.__dict__
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.post(url, headers=headers, json=data_json)
                response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    

    async def get_streaming(self, bucket_id: str, key: str, timeout: int = 300, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[httpx.Response, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/{key}"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                # We return the response. The caller is responsible for streaming the data.
            return Ok(response)
        except Exception as e:
            return Err(e)
    async def get_to_file(self,
                          bucket_id: str,
                          key: str,
                          chunk_size: str = "1MB",
                          sink_folder_path: str = "/mictlanx/data",
                          timeout: int = 300,
                          filename: str = "",
                          headers: Dict[str, str] = {},
                          extension: str = "",
                          verify:VerifyType = False
                          ) -> Result[str, Exception]:
        try:
            # from humanfriendly import parse_size
            _chunk_size = HF.parse_size(chunk_size)
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path, exist_ok=True)
            combined_key = filename if filename else XoloUtils.sha256(f"{bucket_id}@{key}".encode())
            fullpath = f"{sink_folder_path}/{combined_key}{extension}"

            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                async with client.stream("GET", f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/{key}", headers=headers) as response:
                    response.raise_for_status()
                    with open(fullpath, "wb") as f:
                        async for chunk in response.aiter_bytes(_chunk_size):
                            f.write(chunk)
            return Ok(fullpath)
        except Exception as e:
            return Err(e)    
    async def put_chunked(self, task_id: str, chunks: AsyncGenerator[bytes, Any], timeout: int = 120, headers: Dict[str, str] = {}, verify:VerifyType = False) -> Result[ResponseModels.RouterPutChunkedResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/data/{task_id}/chunked"
            async with httpx.AsyncClient(http2=self.http2,timeout=timeout,verify=verify,limits=httpx.Limits(max_connections=None, max_keepalive_connections=None)) as client:
                # req = client.build_request(method="POST", url=url, headers=headers,timeout=timeout, )
                # put_response = await client.post(url, data=chunks, headers=headers)
                put_response = await client.post(url, content=chunks, headers=headers)
                put_response.raise_for_status()

                # data = ResponseModels.PeerPutChunkedResponse.model_validate(put_response.json())
                print(put_response)
                data = ResponseModels.RouterPutChunkedResponse.model_validate(put_response.json())
                return Ok(data)
        except Exception as e:
            return Err(e)   
    async def delete_by_ball_id(self, ball_id: str, bucket_id: str, timeout: int = 120, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[ResponseModels.DeletedByBallIdResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/bid/{ball_id}"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.delete(url, headers=headers)
                response.raise_for_status()
                content_data = response.json()
                return Ok(ResponseModels.DeletedByBallIdResponse.model_validate(content_data))
        except Exception as e:
            return Err(e)
    async def get_chunks_metadata(self, key: str, bucket_id: str = "", timeout: int = 120, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[ResponseModels.BallMetadata, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/metadata/{key}/chunks"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                # Convert each JSON object into an instance of ResponseModels.Metadata
                data = ResponseModels.BallMetadata.model_validate(response.json())
                # chunks_metadata = map(lambda x: ResponseModels.Metadata(**x), data)
                return Ok(data)
        except Exception as e:
            return Err(e)
    async def delete(self, bucket_id: str, key: str, headers: Dict[str, str] = {},verify:VerifyType = False,timeout:int=120) -> Result[ResponseModels.DeletedByKeyResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/{key}"
            async with httpx.AsyncClient(http2=True,verify=verify,timeout=timeout) as client:
                response = await client.delete(url, headers=headers)
                response.raise_for_status()
                json_data = response.json()
                return Ok(ResponseModels.DeletedByKeyResponse(**json_data) )
        except Exception as e:
            return Err(e)
    async def disable(self, bucket_id: str, key: str, headers: Dict[str, str] = {},verify:VerifyType = False,timeout:int=120) -> Result[bool, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/{key}/disable"
            async with httpx.AsyncClient(verify=verify,timeout=timeout) as client:
                response = await client.post(url, headers=headers)
                response.raise_for_status()
                return Ok(True)
        except Exception as e:
            return Err(e)
    async def put_metadata(self, key: str, size: int, checksum: str, producer_id: str, content_type: str,
                           ball_id: str, bucket_id: str, tags: Dict[str, str] = {}, timeout: int = 120,
                           is_disabled: bool = False, replication_factor: int = 1,
                           headers: Dict[str, str] = {},verify:VerifyType =False) -> Result[ResponseModels.PutMetadataResponse, Exception]:
            try:
                url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/metadata"
                data_json = {
                    "bucket_id": bucket_id,
                    "key": key,
                    "ball_id": ball_id,
                    "checksum": checksum,
                    "size": size,
                    "tags": tags,
                    "producer_id": producer_id,
                    "content_type": content_type,
                    "is_disabled": is_disabled,
                    "replication_factor": replication_factor
                }
                async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                    response = await client.post(url, json=data_json, headers=headers)
                    response.raise_for_status()
                    res_json = response.json()
                    return Ok(ResponseModels.PutMetadataResponse.model_validate(res_json) )
            except Exception as e:
                return Err(e)
    async def put_data(self, task_id: str, key: str, value: bytes, content_type: str, timeout: int = 120,
                       headers: Dict[str, str] = {}, file_id: str = "data",verify:VerifyType = False) -> Result[Any, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/data/{task_id}"
            # For file uploads, using httpx's 'files' parameter:
            files = {file_id: (key, value, content_type)}
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.post(url, files=files, headers=headers)
                response.raise_for_status()
            return Ok(())
        except Exception as e:
            return Err(e)
    async def get_bucket_metadata(self, bucket_id: str, timeout: int = 120, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[ResponseModels.GetRouterBucketMetadataResponse, Exception]:
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/metadata"
            async with httpx.AsyncClient(timeout=timeout,verify =verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                return Ok(ResponseModels.GetRouterBucketMetadataResponse.model_validate(response.json()))
        except Exception as e:
            return Err(e)
    async def get_stats(self, timeout: int = 120, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[Dict[str, ResponseModels.PeerStatsResponse], Exception]:
        try:
            url = f"{self.base_url()}/api/v4/peers/stats"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                json_data = response.json()
                validated = dict(list(map(lambda x:(x[0],ResponseModels.PeerStatsResponse(**x[1])) , json_data.items())))
                return Ok(validated)
                # return Ok(ResponseModels.(**response.json()))
        except Exception as e:
            return Err(e)
    async def get_metadata(self, bucket_id: str, key: str, timeout: int = 300, headers: Dict[str, str] = {},verify:VerifyType=False) -> Result[ResponseModels.GetMetadataResponse, Exception]:
        """
        Asynchronously fetches metadata for a given bucket and key.
        
        Constructs the URL from the base URL, API version, bucket_id, and key, and then sends a GET request.
        On success, it returns an Ok containing an instance of ResponseModels.GetMetadataResponse created from the JSON response.
        On failure, it returns an Err with the exception.
        """
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/metadata/{key}"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                metadata_obj = ResponseModels.GetMetadataResponse.model_validate(response.json())
                return Ok(metadata_obj)
        except Exception as e:
            return Err(e)
    
    async def get_by_checksum_to_file(
        self,
        checksum: str,
        chunk_size: str = "1MB",
        sink_folder_path: str = "/mictlanx/data",
        timeout: int = 300,
        filename: str = "",
        headers: Dict[str, str] = {},
        extension: str = "",
        verify:VerifyType = False
    ) -> Result[str, Exception]:
        """
        Downloads a file identified by a checksum and saves it to disk.
        
        Parameters:
        checksum: The checksum used to identify the file.
        chunk_size: The size of each chunk to read (e.g., "1MB").
        sink_folder_path: Folder path where the file will be saved.
        timeout: Timeout for the HTTP request.
        filename: Optional custom filename. If not provided, the checksum is used.
        headers: HTTP headers to include in the request.
        extension: File extension to append.
        
        Returns:
        A Result containing the full file path on success or an Exception on error.
        """
        try:
            # Convert the chunk size string into bytes
            _chunk_size = HF.parse_size(chunk_size)
            
            # Ensure the sink folder exists
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path, exist_ok=True)
            
            # Determine the filename to use (if none provided, use the checksum)
            combined_key = checksum if filename == "" else filename
            fullpath = f"{sink_folder_path}/{combined_key}{extension}"
            
            # If the file already exists, return its path
            if os.path.exists(fullpath):
                return Ok(fullpath)
            
            # Construct the URL for downloading the file by checksum
            url = f"{self.base_url()}/api/v{4}/buckets/checksum/{checksum}"
            
            # Create an asynchronous HTTP client
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                # Stream the GET response from the URL
                async with client.stream("GET", url, headers=headers) as response:
                    response.raise_for_status()
                    # Open the destination file asynchronously for writing in binary mode
                    # async with aiofiles.open(fullpath, "wb") as f:
                    #     # Read and write chunks as they are received
                    #     async for chunk in response.aiter_bytes(_chunk_size):
                    #         if chunk:
                    #             await f.write(chunk)
            
            return Ok(fullpath)
        
        except Exception as e:
            return Err(e)
