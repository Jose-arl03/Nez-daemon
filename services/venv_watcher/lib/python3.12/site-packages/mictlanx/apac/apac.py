import mictlanx.v4.interfaces as InterfaceX
from mictlanx.v4.summoner.summoner import Summoner
from mictlanx.utils.index import Utils as IndexUtils
from option import Some,NONE
from mictlanx.v4.apac.contextual_lang import AvailabilityPolicy
from typing import Dict,List,Generator
import numpy as np
from mictlanx.v4.client import Client
from itertools import chain
import time as T

    # def 
# class AvailableResource()




class APaCInterpreter(object):
    def __init__(self,
                 ip_addr:str,
                 port:int=15000,
                 protocol:str="http",
                 api_version:int = 3,
                 mode:str="docker",
                 sep:str =".",
                 routers:str ="mictlanx-router-0:localhost:60666"
                #  client = 
    ):
        self.sep  = sep
        self.mode= mode
        self.summoner = Summoner(
            ip_addr= ip_addr,
            port=port,
            protocol=protocol,
            api_version=Some(api_version),
        )
        self.available_resources:Dict[str, List[InterfaceX.AvailableResource]] = {}
        self.used_ports:List[int] = []
        self.current_ars_by_id:List[InterfaceX.AvailableResourceId] = []
        self.client = Client(
            client_id    = "apac-client-0",
            # 
            routers        = list(IndexUtils.routers_from_str(routers)),
            # 
            debug        = False,
            # 
            # 
            max_workers  = 2,
            # 
            lb_algorithm = "2CHOICES_UF",
            bucket_id="mictlanx"
        )
        # self.client = 
    def __destroy_nodes(self,ap_available_resources_ids:List[InterfaceX.AvailableResourceId]):
        to_delete= []
        for ar_id in self.current_ars_by_id:
            print("AR_ID",ar_id , ar_id in ap_available_resources_ids)
            if not ar_id in ap_available_resources_ids:
                del_res = self.summoner.delete_container(container_id=ar_id,mode=self.mode)
                print("DEL_RES",del_res)
                current_cluster_ars = self.available_resources.setdefault(ar_id.cluster_id,[])
                self.available_resources[ar_id.cluster_id] = list(filter(lambda x : not x.ar_id == ar_id,current_cluster_ars) )
                to_delete.append(ar_id)
        self.current_ars_by_id = list(set(self.current_ars_by_id).difference(set(to_delete)))
                
        print("MAP",self.available_resources)
        print("CUrRENT_ARIDS", self.current_ars_by_id)

    def __get_cluster_id_ar_id(self,amp:AvailabilityPolicy)->Generator[InterfaceX.AvailableResourceId,List[InterfaceX.AvailableResourceId],None]:
        for (cluster_id, nodes) in amp.avaialable_resources.items():
            for n in nodes:
                yield InterfaceX.AvailableResourceId(cluster_id=cluster_id,node_id=n)
                # yield (cluster_id, n,"{}.{}".format(cluster_id,n))
        return []
    def run(self,apm: AvailabilityPolicy):
        ids =  list(self.__get_cluster_id_ar_id(amp=apm))
        print("IDS",ids)
        self.__destroy_nodes(ap_available_resources_ids=ids)
        current_deployed_ar:List[InterfaceX.Peer] = []
        for ar_id in ids:
            print("AR_ID", ar_id)
            # print("DEPLOY",cluster_id,combined_id)
            if ar_id in self.current_ars_by_id:
                print(ar_id,"Already deployed...")
                continue
            cluster_id = ar_id.cluster_id
            node_id = ar_id.node_id
            available_resource_id = str(ar_id)
            port = np.random.randint(low=30000, high=60000)
            print("BEGFORE")
            while port in self.used_ports:
                port = np.random.randint(low=30000, high=60000)
            ar = InterfaceX.AvailableResource(ar_id=ar_id,protocol="http", ip_addr="localhost",port=port)
            _ = self.available_resources.setdefault(cluster_id,[])
            current_peers = list(map(lambda x: "{}:{}".format(x.ar_id,x.port) ,self.available_resources.get(cluster_id,[])))
            # # print(ar.ar_id, current_peers)
            result = self.summoner.summon_peer(
                container_id=available_resource_id,
                port=port,
                selected_node=cluster_id,
                peers=current_peers,
                labels={
                    "cluster_id":cluster_id,
                    "apac.version":apm.version,
                },
                mode=self.mode
            )
            if result.is_ok:
                current_deployed_ar.append(ar.to_peer())
                self.available_resources[cluster_id].append(ar)
                self.used_ports.append(port)
                self.current_ars_by_id.append(ar_id)
            else:
                print("SUMMONER_ERR", result)
        
        added_peers_result = self.client.__get_default_router().add_peers(peers=current_deployed_ar)
        print("ADDED.PEERS.RESULT",added_peers_result)
        _ps = list(chain.from_iterable(self.available_resources.values()))
        print("RESOURCEs",_ps)
        
        for what_combined_key in apm.what:
            wh_splitted = what_combined_key.split(self.sep)
            print("wh_spplited", wh_splitted)
            whn = len(wh_splitted)
            if whn  == 2:
                (bucket_id,data_id) = wh_splitted
                print("BUCKE_TID", bucket_id, "KEY",data_id)
                self.__resolve_data_replication(apm = apm,bucket_id=str(bucket_id), key=str(data_id))
            elif whn == 1:
                    self.__resolve_bucket_replication(apm = apm,bucket_id= wh_splitted[0])
            else:
                print("ERROR")
        # print("_"*20)
        # client.shutdown()
        # print("*"*20)
    def __resolve_data_replication(self,apm:AvailabilityPolicy,bucket_id:str,key:str):
        for where_peer in apm.where:
            # peer = self.client.get_router_by_id(peer_id=where_peer)
            get_res = self.client.get_metadata(bucket_id=bucket_id,key=key, headers={
                "Peer-Id": where_peer
            } ).result()
            print("REPLICATE DATA IN",where_peer,get_res)
    def __resolve_bucket_replication(self, apm:AvailabilityPolicy,bucket_id:str):
        for where_peer in apm.where:
            print("REPLICATE BUCKET IN",bucket_id,where_peer)
        # T.sleep(20)
        # client.get_bucket_metadata(bucket_id=)
    def deploy(self,ps:InterfaceX.ProcessingStructure):
        completed = []
        for (function_id, next_functions) in ps.functions_order.items():
            if function_id in completed:
                continue
            print("DEPLOY",function_id,"NEXT_FUNCTIONS",next_functions)
            for next_function_id in next_functions:
                print("INEER_DEPLOY",next_function_id)
            completed.extend([function_id,*next_functions])
            print("_"*20)
        print(completed)
    

if __name__ =="__main__":
    apac_interpreter = APaCInterpreter(
        protocol="http",ip_addr="localhost",port=15000,sep="."
    )

    ap_str = """
        apac: v1
        available-resources:
            pool-0:
                - peer-0
                - peer-1
            pool-1:
                - peer-0
        who: pool-0.peer-0
        what:
            - bucket_0.bac9b6c65bb832e7a23f936f8b1fdd00051913fc0c483cf6a6f63f89e6588b80
        where:
            - pool-0.peer-1
            - pool-1.peer-0
        how: ACTIVE
        when:
            - bucket0: $GET_COUNTER > 10
            - bucket1: $ACCESS_FREQUENNCY>60.6%
    """
    apac_interpreter.run(
        apm=AvailabilityPolicy.build_from_str(ap_str=ap_str)
    )
    
    T.sleep(100)
    # ap_str = """
    #     tlaloc: v1
    #     available-resources:
    #         pool-0:
    #             - peer-0
    #             - peer-2
    #     who: pool-0.peer-0
    #     what:
    #         - bucket-1.c3f8c3482bd60da5cb8348e4701ef6d43f4dcb56fbe54ee1f2470526f082a80d
    #     where:
    #         - pool-0.peer-2
    #     how: ACTIVE
    #     when:
    #         -bucket0: $GET_COUNTER>10
    #         -bucket1: $ACCESS_FREQUENNCY>60.6%
    # """
    # tlaloc.build(apm=AvailabilityPolicyMetaobject.from_str(ap_str=ap_str))
    # T.sleep(100)
    
    # ps = ProcessingStructure(
    #     key="ps-0",
    #     functions=[
    #         Function(
    #             key="f1",
    #             image="nachocode/xolo:aes-enc",
    #             resources=Resources(cpu=1,memory="1GB"),
    #             bucket_id="test-bucket-0",
    #             endpoint_id="disys0"
    #         ),
    #         Function(
    #             key="f2",
    #             image="nachocode/utils:lz4",
    #             resources=Resources(cpu=1,memory="1GB"),
    #             bucket_id="test-bucket-0",
    #             endpoint_id="disys1"
    #         ),
    #         Function(
    #             key="f3",
    #             image="nachocode/xolo:aes-dec",
    #             resources=Resources(cpu=1,memory="1GB"),
    #             bucket_id="f2-bucket",
    #             endpoint_id="disys2"
    #         ),
    #     ],
    #     functions_order={
    #         "f1":[],
    #         "f2":["f3"],
    #         "f3":[]
    #     },
    # )
    # res = tlaloc.deploy(ps= ps)