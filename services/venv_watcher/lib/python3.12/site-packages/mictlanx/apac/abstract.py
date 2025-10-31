from abc import ABC, abstractmethod
import asyncio
from typing import List, Optional, Dict, Any
import time
import random
import httpx


class AbstractAPaCClient(ABC):
    def __init__(self,hostname:str="localhost", port:int=40000 ):
        # super().__init__()
        self.hostname = hostname
        self.port = port
    @abstractmethod
    async def register_put(self, context):
        print(f"[APaC::PUT] Context received: {context}")

    @abstractmethod
    async def register_get(self, context):
        print(f"[APaC::GET] Context received: {context}")

# Abstract Base Class
class AbstractStorageSystem(ABC):
    def __init__(self, apac_client:AbstractAPaCClient):
        self.apac = apac_client  # Hook to APaC interpreter/service

    async def __put(self, context: Dict[str, Any]):
        await self.apac.register_put(context)

    async def __get(self, context: Dict[str, Any]):
        await self.apac.register_get(context)

    @abstractmethod
    async def put(self, bucket_id: str, key: str, size: int):
        pass

    @abstractmethod
    async def get(self, bucket_id: str, key: str):
        pass


# Example APaC client that collects context (mock)


# Your custom storage class
class MyObjectStorage(AbstractStorageSystem):
    async def put(self, bucket_id: str, key: str, size: int):
        start = time.time()
        # Simulate storing data...
        peers = ["sn1", "sn3"]
        time.sleep(random.uniform(0.1, 0.4))  # simulate write delay
        end = time.time()

        context = {
            "operation":"PUT",
            "bucket_id": bucket_id,
            "key": key,
            "size": size,
            "response_time": end - start,
            "peers": peers
        }
        r = await self._AbstractStorageSystem__put(context)
        print(r)

    async def get(self, bucket_id: str, key: str):
        start = time.time()
        # Simulate reading data...
        peers = ["sn2"]
        time.sleep(random.uniform(0.05, 0.2))  # simulate read delay
        end = time.time()

        context = {
            "operation":"GET",
            "bucket_id": bucket_id,
            "key": key,
            "response_time": end - start,
            "peers": peers
        }
        r = await self._AbstractStorageSystem__get(context)
        print(r)
class APaCClient(AbstractAPaCClient):
    def __init__(self, hostname = "localhost", port = 40000):
        super().__init__(hostname, port)
        
    async def register_get(self, context):
        async with httpx.AsyncClient() as c: 
            response = await c.post(f"http://{self.hostname}:{self.port}/register",json= context)
            return response
        
    async def register_put(self, context):
        async with httpx.AsyncClient() as c: 
            response = await c.post(f"http://{self.hostname}:{self.port}/register",json= context)
            return response

# Example usage
apac = APaCClient(hostname="localhost",port=40000)

storage = MyObjectStorage(apac_client=apac)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    for i in range(10000):
        res2 = loop.run_until_complete(storage.get("b1", "xx"))
        loop.run_until_complete(asyncio.sleep(.1))
