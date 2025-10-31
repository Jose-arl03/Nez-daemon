
import requests as R
from mictlanx.services.models.summoner import *
# from mictlanxv.models.summoner import SummonContainerPayload,ExposedPort
from mictlanx.interfaces.responses import SummonResponse,SummonServiceResponse
from option import Result,Ok,Err,Some,Option,NONE
# from mictlanx.models.summoner import MountX
from ipaddress import IPv4Network
from typing import Tuple,List,Dict
import numpy as np


class Summoner(Service):
    def __init__(self,
        ip_addr:str, 
        port:int,
        protocol:str="http",
        api_version: Option[int] = NONE,
        network:Option[IPv4Network]=NONE,
        storage_peer_image:str = "nachocode/mictlanx:peer-0.0.160-alpha.1"
    ):
        super(Summoner,self).__init__(ip_addr=ip_addr, port = port, api_version=api_version,protocol=protocol)
        self.summon_container_url                = "{}/{}".format(self.base_url,"containers")
        self.summon_service_url                = "{}/{}".format(self.base_url,"services")
        self.delete_container_url = lambda container_id: "{}/containers/{}".format(self.base_url,container_id)
        self.delete_service_url = lambda container_id: "{}/services/{}".format(self.base_url,container_id)
        self.network:IPv4Network = network.unwrap_or(IPv4Network("10.0.0.0/25"))
        self.reserved_ip_addrs = []
        self.reserved_ports =[]
        self.storage_peer_image = storage_peer_image
        # self.complete_operation_url = lambda node_id, operation_type, operation_id: "{}/{}/{}/{}/{}".format(self.base_url,"operations",node_id,operation_type,operation_id)


    
    def __get_available_ip_addr(self,payload:SummonContainerPayload) -> Option[Tuple[str,int]]: 
        if not len(payload.exposed_ports) ==0:
            port = payload.exposed_ports[0].host_port
        else:
             port = np.random.randint(low=50000, high=60000)
        
        if (payload.ip_addr == payload.container_id):
            return Some((payload.container_id,port))
        elif(payload.ip_addr == "0.0.0.0"):
             return Some((payload.ip_addr, port))
        else :
            for ip_addr in self.network.hosts(): 
                if not ip_addr in self.reserved_ip_addrs:
                    port_predicate = port in self.reserved_ip_addrs
                    while port_predicate:
                        port +=1
                    return Some((ip_addr,port))
                else:continue

    def delete_container(self,
                         container_id:str,
                         mode:str ="docker",
                         headers:Dict[str,str]={}
    )->Result[Tuple[()],Exception] : 
        url = self.delete_service_url(container_id) if not mode =="docker" else self.delete_container_url(container_id)
        try:
            response = R.delete(url,headers=headers) 
            response.raise_for_status()
            return Ok(())
        except Exception as e:
                # response:R.Response = e.response
                return Err(e)
                # return Err(ServerInternalError(message = response.headers.get("Error-Message"), metadata = response.headers))
    def health_check(self):
        try:
            url = f"{self.base_url}/health"
            response = R.get(url=url)
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    def stats(self)->Result[Dict[str,SummonServiceResponse],Exception]:
        try:
            url = f"{self.base_url}/stats"
            response = R.get(url=url)
            response.raise_for_status()
            data = response.json()
            summons = data.get("summons",{})
            coverted = {
                key: SummonServiceResponse.model_validate(value)
                for key,value in summons.items()
            }
            return Ok(coverted)
        except Exception as e:
            return Err(e)
    def summon_peer(self,
        container_id:str,
        port:int=-1,
        selected_node:str="0",
        mode:str="docker",
        peers:List[str]=[],
        labels:Dict[str,str]={},
        memory:str = "1GB",
        disk:str = "10GB",
        workers:int = 2,
        user_id:str="1001",
        group_id:str="1002",
        network_id:str ="mictlanx",
        cpu:int = 1
    )->Result[SummonResponse,Exception]:
        port = np.random.randint(low=2000, high=60000) if port <= 1024 else port
        

        memory_bytes = HF.parse_size(memory)
        base_path  = "/app/mictlanx"
        local_path = f"{base_path}/local"
        data_path  = f"{base_path}/data"
        log_path   = f"{base_path}/log"
        payload         = SummonContainerPayload(
            container_id  = container_id,
            image         = self.storage_peer_image,
            hostname      = container_id,
            exposed_ports = [
                ExposedPort(
                    host_port      = port,
                    container_port = port
                )
            ],
            envs= {
                
                "USER_ID":user_id,
                "GROUP_ID":group_id,

                "BIN_NAME":"peer"
                ,
                "NODE_ID":container_id,
                "NODE_PORT":str(port),
                "IP_ADDRESS":container_id,

                "SERVER_IP_ADDR":"0.0.0.0",
                "NODE_DISK_CAPACITY":str(HF.parse_size(disk)),
                "NODE_MEMORY_CAPACITY":str(memory_bytes),
                "BASE_PATH":base_path,
                "LOCAL_PATH":local_path,
                "DATA_PATH":data_path,
                "LOG_PATH":log_path,
                "MIN_INTERVAL_TIME":"15",
                "MAX_INTERVAL_TIME":"60",
                "WORKERS":str(workers),
                "PEERS":" ".join(peers).strip()
            },
            memory=memory_bytes,
            cpu_count=cpu,
            mounts=[
                MountX(
                    source="{}-data".format(container_id),
                    target=data_path,
                    mount_type=MountType.VOLUME
                ),
                MountX(
                    source = "{}-log".format(container_id),
                    target = log_path,
                    mount_type=MountType.VOLUME
                ),
                MountX(
                    source = "{}-local".format(container_id),
                    target = local_path,
                    mount_type=MountType.VOLUME
                )
            ],
            network_id=network_id,
            selected_node=str(selected_node),
            force=True,
            labels=labels
        )

        return self.summon(payload=payload, mode=mode)
    
  
    def summon(self,
               payload:SummonContainerPayload,
               mode:str= "docker",
               headers:Dict[str,str]={}
    )->Result[SummonResponse,Exception]:
        try:
            x  =  self.__get_available_ip_addr(payload=payload)
            if(x.is_none):
                return Err(Exception("Something went wrong getting available ip address."))
                # return Err(ServerInternalError())
            (ip_addr, port) = x.unwrap()

            print("PORT", port)
            payload.envs["NODE_ID"] = str(payload.container_id) 
            payload.envs["NODE_IP_ADDR"] = str(ip_addr)
            payload.envs["NODE_PORT"] = str(port)
            # print(payload.to_dict())
            url =self.summon_container_url if mode == "docker" else self.summon_service_url
            response = R.post(
                url,
                json= payload.to_payload_dict(),
                headers=headers
            )
            # print("RM_RESPONSE",response)
            self.reserved_ip_addrs.append(ip_addr)
            self.reserved_ports.append(port)
            response.raise_for_status()
            response_json = response.json()
            return Ok(SummonResponse.model_validate(response_json))
        except Exception as e:
            return Err(e)
