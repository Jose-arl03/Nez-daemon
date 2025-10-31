from typing import Dict ,Any,TypeVar,Generic,List,Optional
import numpy.typing as npt
from dataclasses import dataclass
from pydantic import BaseModel,Field

T = TypeVar("T")


# class ReplicationEvent(BaseModel):


    
class Metadata(BaseModel):
    key:str # Unique identifier 
    size:int # Size in bytes of the data
    checksum:str # Sha256 checksum
    tags:Dict[str,str] # User-defined metadata
    content_type:str # Define the type of the content
    producer_id:str # Unique identifier of the user that allocate the data. 
    ball_id:str # Unique identifier used for segmentation purposes ball_id -> [chunk1, chunk2,...,chunkN]
    bucket_id:str = Field(default="") # Unique identifier used for MictlanX Sync
    is_disabled:bool=Field(default=False)

   
@dataclass
class AsyncGetResponse:
    data:memoryview
    metadatas:List[Metadata]

class BallMetadata(BaseModel):
    bucket_id:str
    ball_id:str
    size:str
    size_bytes:int
    checksum:str
    chunks:List[Metadata]
    

@dataclass
class PeerStatsResponse:
    peer_id:str
    used_disk:int
    total_disk:int 
    available_disk:int
    disk_uf:float
    balls:List[Metadata] 
    peers:List[str]
    def __eliminate_duplicates(self,metadata_list: List[Metadata]) -> List[Metadata]:
        seen = set()
        unique_metadata = []
        for metadata in metadata_list:
            identifier = (metadata.bucket_id, metadata.key)
            if identifier not in seen:
                seen.add(identifier)
                unique_metadata.append(metadata)
        return unique_metadata
    def to_dict(self):
        # Use asdict for simple fields, but manually convert the Metadata objects.
        data = self.__dict__.copy()
        data["balls"] = [ball.model_dump() for ball in self.balls]
        return data
    @staticmethod
    def empty()->'PeerStatsResponse':
        return PeerStatsResponse(
            peer_id="global",
            available_disk=0,
            balls=[],
            disk_uf=0,
            peers=[],
            total_disk=0,
            used_disk=0
        )
    def __add__(self,other:'PeerStatsResponse')->'PeerStatsResponse':
        return PeerStatsResponse(
            peer_id="global",
            used_disk= self.used_disk + other.used_disk,
            total_disk= self.total_disk + other.total_disk,
            available_disk=self.available_disk + other.available_disk,
            balls= self.__eliminate_duplicates(self.balls + other.balls),
            disk_uf= (self.disk_uf + other.disk_uf)/2,
            peers= list(set(self.peers + other.peers))
        )


class ElasticResponse(BaseModel):
    pool_size: int
    response_time: float


class ReplicationResponse(BaseModel):
    replication_event_id: str
    response_time: float


class DeleteBucketResponse(BaseModel):
    bucket_id: str
    deleted: int
    failed: int
    total: int
    keys: List[str]
    response_time: float


class BallContext(BaseModel):
    locations: List[str]
    size: int


class PeerData(BaseModel):
    node_id: str
    ip_addr: str
    port: int


class PeerCurrentState(BaseModel):
    nodes: List[PeerData]
    balls: Dict[str, BallContext]


class BallBasicData(BaseModel):
    bucket_id: str
    key: str
    size: int


class StoragePeerResponse(BaseModel):
    id: str
    disk: int
    memory: int
    ip_addr: str
    port: int
    weight: float
    used_disk: int
    used_memory: int


class GetSizeByKey(BaseModel):
    bucket_id: str
    key: str
    peer_id: str
    size: int


class ReplicateResponse(BaseModel):
    peer_id: str
    replica_peer_id: str
    bucket_id: str
    key: str
    size: int
    ok: bool
    response_time: int


class DeletedResponse(BaseModel):
    n_deletes: int
    key_or_ball_id: str


class DeletedByBallIdResponse(BaseModel):
    n_deletes: int
    ball_id: str


class DeletedByKeyResponse(BaseModel):
    n_deletes: int
    key: str


class DeletedBallResponse(BaseModel):
    n_deletes: int
    ball_id: str


class BucketDeleteResponse(BaseModel):
    n_deleted_objects: int
    response_time: float


class PeerPutChunkedResponse(BaseModel):
    node_id: str
    combined_key: str
    bucket_id: str
    key: str
    size: int
    throughput: float
    service_time: float

class RouterPutChunkedResponse(BaseModel):
    peer_ids:List[str]
    combined_key:str
    bucket_id:str
    key:str
    size:int

class PutChunkedResponse(BaseModel):
    bucket_id: str
    key: str
    size: int
    replicas: List[str]
    throughput: float
    response_time: float


class GetBucketMetadataResponse(BaseModel):
    peer_id: str
    balls: List[Metadata]

    def __str__(self):
        return f"Bucket(n={len(self.balls)})"


class GetRouterBucketMetadataResponse(BaseModel):
    bucket_id: str
    peer_ids: List[str] = []
    balls: List[Metadata] = []
    extra: Dict[str, Any] = {}

    def __str__(self):
        return f"Bucket(n={len(self.balls)})"


class GetMetadataResponse(BaseModel):
    service_time: int
    peer_id: str
    local_peer_id: str
    metadata: Metadata


class GetUFSResponse(BaseModel):
    total_disk: int
    used_disk: int
    disk_uf: float

    def __str__(self):
        return f"GetUFSResponse(total_disk={self.total_disk}, used_disk={self.used_disk}, disk_uf={self.disk_uf})"


class PeerPutMetadataResponse(BaseModel):
    key: str
    service_time: int
    task_id: str
    node_id: str


class PutMetadataResponse(BaseModel):
    key: str
    service_time: float
    tasks_ids: List[str] = []
    bucket_id: str = ""
    replicas: List[str] = []
    extra: Dict[str, Any] = {}


class PutDataResponse(BaseModel):
    service_time: int
    throughput: float




class PutResponse(BaseModel):
    # def __init__(self,response_time:int,throughput:float,replicas:List[str]=[],key:str=""):
        key:str 
        response_time:float
        replicas:List[str]
        throughput:float

@dataclass
class GetToFileResponse:
    path:str
    metadata: Metadata
    response_time:float
    peer_id:str

@dataclass
class UpdateResponse:
    updated:bool
    bucket_id:str
    key:str
    replicas:List[str]
    throughput:float
    response_time:float 


class SummonResponse(BaseModel):
    container_id:str
    service_time:int
    ip_addr:str
    port:int

class SummonServiceResponse(BaseModel):
    id:str
    container_id: str
    created_at:int
    client_id:str

class GroupedBallResponse(BaseModel):
    bucket_id:str
    ball_id:str
    size:int
    balls:List[Metadata]