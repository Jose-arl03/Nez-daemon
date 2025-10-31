from mictlanx.services import AsyncRouter
# from mictlanx.interfaces import AsyncRouter
from typing import List,Dict

class RouterLoadBalancer():
    
    def __init__(self, routers:List[AsyncRouter]):
        self.routers:Dict[str, AsyncRouter] = dict(map(lambda x :(x.router_id, x), routers))
        self.operation_counter:Dict[str, int] = dict(list(map(lambda x:(x[0],0), self.routers.items())))

    def get_router(self,router_id:str ="",bucket_id:str="",key:str=""):
        if router_id in self.routers:
            self.operation_counter.setdefault(router_id,0)
            self.operation_counter[router_id]+=1
            return self.routers[router_id]
        min_router = min(self.operation_counter.items(), key=lambda x : x[1])
        if min_router:
            router_id = min_router[0]
            self.operation_counter.setdefault(router_id,0)
            self.operation_counter[router_id]+=1
            return self.routers[router_id]
        else:
            x= list(self.routers.values())[0]
            return x


