from abc import ABC
from typing import List,Dict,Optional,Any
# from option import Option,NONE
import humanfriendly as HF
from collections import namedtuple
from pydantic import BaseModel,Field,model_validator
from enum import IntEnum


class ExposedPort(BaseModel):
    # def __init__(self,ip_addr:Option[str], host_port:int, container_port:int, protocol:Option[str]):
    ip_addr:Optional[str] = None 
    host_port:int
    container_port:int
    protocol:Optional[str]=None
    # def to_dict(self): 
    #     x = {}
    #     if(self.ip_addr.is_some):
    #         x["ip_addr"] = self.ip_addr.unwrap()
    #     x["host_port"] = self.host_port
    #     x["container_port"] = self.container_port 
    #     if(self.protocol.is_some):
    #         x["protocol"] = self.protocol.unwrap()
    #     return x
            
    
# MountXBase = namedtuple("MountX","source target mount_type")
# mountx_map = {
#     "BIND":0,
#     "VOLUME":1,
#     "TMPFS":2,
#     "NPIPE":3,
#     "EMPTY":4
# }
class MountType(IntEnum):
    BIND   = 0
    VOLUME = 1
    TMPFS  = 2
    NPIPE  = 3
    EMPTY  = 4
class MountX(BaseModel):
    # adapt fields as in your project
    mount_type: MountType
    source: str
    target: str
    # read_only: Optional[bool] = None
# class MountX(MountXBase):
#     def to_dict(self):
#         return {
#             "source":self.source,
#             "target":self.target,
#             "mount_type":int(self.mount_type)
#         }





class SummonContainerPayload(BaseModel):
    # required
    container_id: str
    image: str
    hostname: str
    exposed_ports: List[ExposedPort]
    envs: Dict[str, str]
    memory: int
    cpu_count: int
    mounts: List[MountX]
    network_id: str

    # optional with defaults/behavior
    selected_node: Optional[str] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    force: bool = True                    # original: force.unwrap_or(True)
    ip_addr: Optional[str] = None         # original: unwrap_or(container_id)
    shm_size: Optional[str] = None        # human-size string, parsed on dump

    @model_validator(mode="after")
    def _apply_defaults_and_merge_labels(self) -> "SummonContainerPayload":
        # ip_addr defaults to container_id if not provided
        if self.ip_addr is None:
            object.__setattr__(self, "ip_addr", self.container_id)

        # merge labels with {"target": "mictlanx"} (yours added at dump-time; we can merge now)
        merged = dict(self.labels)
        merged.setdefault("target", "mictlanx")
        object.__setattr__(self, "labels", merged)

        return self

    def to_payload_dict(self) -> Dict[str, Any]:
        """
        Emit the dict your original `to_dict()` produced.
        (Note: original code did not include `ip_addr`; we keep that behavior.)
        """
        mounts_dump = [m.model_dump() for m in self.mounts]

        data: Dict[str, Any] = {
            "container_id": self.container_id,
            "image": self.image,
            "hostname": self.hostname,
            "exposed_ports": [p.model_dump() for p in self.exposed_ports],
            "envs": self.envs,
            "memory": self.memory,
            "cpu_count": self.cpu_count,
            "mounts": mounts_dump,
            "network_id": self.network_id,
            "labels": self.labels,
            "force": self.force,
        }

        if self.selected_node is not None:
            data["selected_node"] = self.selected_node

        if self.shm_size is not None:
            # replace with: HF.parse_size(self.shm_size) if you have that helper
            data["shm_size"] = HF.parse_size(self.shm_size)

        return data


# class SummonContainerPayload:
#     def __init__(self,
#                  container_id:str,
#                  image:str,
#                 #  image_tag:str,
#                  hostname:str,
#                  exposed_ports:List[ExposedPort],
#                  envs:Dict[str,str],
#                  memory:int,
#                  cpu_count:int,
#                  mounts:List[MountX],
#                  network_id:str,
#                  selected_node:Option[str] = NONE,
#                  labels:Dict[str,str ]= {},
#                  force:bool=False,
#                  ip_addr:Option[str]=NONE,
#                  shm_size:Option[str] =NONE
#                 ):
#         self.container_id  = container_id
#         self.image         = image
#         # self.image_tag     = image_tag 
#         self.hostname      = hostname
#         self.exposed_ports = exposed_ports
#         self.envs          = envs
#         self.memory        = memory
#         self.cpu_count     = cpu_count
#         self.mounts:List[MountX]        = mounts
#         self.network_id    = network_id
#         self.selected_node = selected_node
#         self.labels        = labels
#         self.force         = force.unwrap_or(True)
#         self.ip_addr       = ip_addr.unwrap_or(self.container_id)
#         self.shm_size = shm_size
#     def to_dict(self):
#         mountx = [ i.to_dict() for i in self.mounts]
#         current_dict = {
#             "container_id":self.container_id,
#             "image":self.image,
#             "hostname":self.hostname,
#             "exposed_ports":list(map(lambda x: x.model_dump(),self.exposed_ports)),
#             "envs":self.envs,
#             "memory":self.memory,
#             "cpu_count": self.cpu_count, 
#             "mounts":mountx,
#             "network_id":self.network_id,
#             "labels":{**self.labels,**{"target":"mictlanx"}},
#             "force":self.force,
#         }
#         if self.selected_node.is_some:
#             current_dict["selected_node"] = self.selected_node.unwrap()
#         if self.shm_size.is_some:
#             current_dict["shm_size"]=HF.parse_size(self.shm_size.unwrap())
#         return current_dict
    
# if __name__ == "__main__":
#     sc = SummonContainerPayload(
#         container_id="scm-0",
#         image="secure-clustering:manager",
#         hostname = "scm-0",
#         exposed_ports=[ExposedPort(NONE,6000,6000,NONE)],
#         envs= {
#             "NODE_PORT":"6000",
#             "NODE_HOST":"0.0.0.0",
#             "NODE_PREFIX":"scw-",
#             "MAX_WORKERS":"5",
#             "DOCKER_IMAGE_NAME":"secure-clustering",
#             "DOCKER_IMAGE_TAG":"worker",
#             "MICTLANX_SUMMONER_IP_ADDR":"10.0.0.100",
#             "MICTLANX_SUMMONER_PORT":"15000",
#             "MICTLANX_API_VERSION":"3",
#             "DEBUG":"0",
#             "RELOAD":"0",
#             "LOG_PATH":"/log",
#             "SINK_PATH":"/sink", 
#             "SOURCE_PATH":"/source",
#             "TESTING":"0",
#             "MAX_RETRIES":"10",
#             "LOAD_BALANCING":"0"
#         },
#         memory=1000000000,
#         cpu_count=1,
#         mounts={
#             "/log":"/log",
#             "/sink":"/sink",
#             "/source":"/source"   
#         }
#     )
#     print(sc.to_dict())
