import os
# import  aiofiles
from typing import List,Dict,Generator,Iterator
from option import Option,NONE,Some
import json as J
import time as T
from xolo.utils.utils import Utils as XoloUtils
import humanfriendly as HF
from typing import List, Dict,Iterator
import humanfriendly as HF
import mictlanx.interfaces.responses as ResponseModels



class Ball:
    def __init__(self,bucket_id:str,chunks:List[ResponseModels.Metadata]=[],ball_id:str="",checksum:str="" , bucket_relative_path:str="",fullname:str=""):
        self.bucket_id = bucket_id
        self.ball_id  = ball_id
        self.checksum = checksum
        self.size     = 0
        self.chunks = chunks.copy()
        self.bucket_relative_path = bucket_relative_path
        self.full_path = ""
        self.extension = ""
        self.filename = ""
        self.updated_at = -1
        self.fullname= fullname
    def __str__(self):
        return f"Ball(id={self.ball_id}, size = {self.size})"
    def len_chunks(self):
        return len(self.chunks)
    def add_chunk(self, chunk:ResponseModels.Metadata):
        exists = next(filter(lambda x: x.key == chunk.key and chunk.checksum ==x.checksum, self.chunks),-1)
        # print(f"{self.ball_id} {exists}")
        if exists == -1:
            self.chunks.append(chunk)
        
    def build(self):
        if len(self.chunks) >0:
            c = self.chunks[0] 
            self.checksum = c.tags.get("full_checksum","")
            self.ball_id  = c.ball_id
            self.bucket_relative_path = c.tags.get("bucket_relative_path","")
            self.fullname = c.tags.get("fullname","")
            self.full_path = c.tags.get("full_path","")
            self.extension = c.tags.get("extension","")
            self.filename = c.tags.get("filename","")
            # self.updated_at
        self.size = 0
        sum_updated_at = 0
        for c in self.chunks:
            self.size += c.size
            sum_updated_at += int(c.tags.get("updated_at",0))
        self.updated_at = int(sum_updated_at / len(self.chunks))
        

    def merge(self, other: 'Ball'):
        existing_ids = {c.checksum for c in self.chunks}
        # print(self.ball_id,existing_ids)
        for chunk in other.chunks:
            if chunk.checksum not in existing_ids:
                self.chunks.append(chunk)

class Bucket:
    def __init__(self,bucket_id:str,balls:Dict[str, Ball]):
        self.bucket_id = bucket_id
        self.balls=balls.copy()
    def size_bytes(self)->int:
        size = 0
        for b in self:
            size += b.size
        return size
    def size(self)->str:
        size = self.size_bytes()
        return HF.format_size(size)
            
    def __len__(self)->int:
        return len(self.balls)
    def __iter__(self) -> Iterator['Ball']:
        return iter(self.balls.values() )

class PeerStats(object):
    def __init__(self,peer_id:str): 
        self.__peer_id                 = peer_id
        self.total_disk:int            = 0
        self.used_disk                 = 0
        self.put_counter:int           = 0
        self.get_counter:int           = 0 
        self.balls                     = set()
        # 
        self.put_last_arrival_time     = -1
        self.put_sum_interarrival_time = 0
        
        self.get_last_arrival_time     = -1
        self.get_sum_interarrival_time = 0
        self.last_access_by_key:Dict[str,int]  = {}
        self.get_counter_per_key:Dict[str,int] = {}

    def put_frequency(self):
        x =  self.global_counter()
        if  x == 0:
            return 0
        return self.put_counter / x
    
    def get_frequency(self):
        x =  self.global_counter()
        if  x == 0:
            return 0
        return self.get_counter / x

    def get_frecuency_per_ball(self):
        res = {}
        for key, getcounter in self.get_counter_per_key.items():
            if self.get_counter == 0:
                res[key] = 0
            else:
                res[key] = getcounter / self.get_counter
        return res
    def top_N_by_freq(self,N:int):
        xs        = self.get_frecuency_per_ball()
        sorted_xs = list(sorted(xs.items(), key=lambda item: item[1], reverse=True))
        return sorted_xs[:N]
    def get_id(self):
        return self.__peer_id


    
    def put(self,key:str, size:int):
        self.put_counter+=1
        if not key in self.balls:
            self.get_counter_per_key[key] = 0
            self.used_disk+=size
        self.balls.add(key)

    def get(self, key:str, size:int):
        arrival_time = T.time()
        self.get_counter += 1
        self.last_access_by_key.setdefault(key,arrival_time)
        if not key in self.get_counter_per_key:
            self.get_counter_per_key[key] = 1
        else:
            self.get_counter_per_key[key] += 1 
        self.balls.add(key)
    def delete(self,key:str,size:int):
        self.balls.discard(key)
        if self.used_disk >=size:
            self.used_disk-=size
        del self.get_counter_per_key[key]
    
    def calculate_disk_uf(self,size:int = 0 ):
        return  1 - ((self.total_disk - (self.used_disk + size))/self.total_disk)
    
    def available_disk(self):
        return self.total_disk - self.used_disk

    def global_counter(self):
        return self.put_counter + self.get_counter
    
    def __str__(self):
        

        return "PeerStats(peer_id={}, total_disk={}, used_disk={}, available_disk={}, disk_uf={}, puts={}, gets={}, globals={}, put_feq={}, get_feq={}, topN={})".format(
            self.__peer_id,
            self.total_disk,
            self.used_disk,
            self.available_disk(),
            self.calculate_disk_uf(),
            self.put_counter,
            self.get_counter,
            self.global_counter(),
            self.put_frequency(),
            self.get_frequency(),
            self.top_N_by_freq(3)
        )

def check_destroyed(func):
    def wrapper(self,*args, **kwargs):
        if self._Ball__destroyed:
            raise Exception("{} was destroyed".format(self.key))
        result = func(self,*args, **kwargs)
        return result

    return wrapper

class BallX(object):
    def __init__(self,size:int, checksum:str,key:str="", path:Option[str]= NONE, value:bytes = bytes(),tags:Dict[str,str]={}, content_type:str="application/octet-stream") :
        self.size             = size
        self.content_type     = content_type
        self.key              = checksum if key =="" else key
        self.checksum         = checksum
        self.path:Option[str] = path
        self.__mictlanx_path  = "/mictlanx/client/.data/{}".format(self.checksum)
        self.value            = value
        self.tags             = tags
        self.__destroyed      = False
    def __resolve_path(self,path:Option[str]=NONE)->str:
        return path.unwrap_or(self.path.unwrap_or(self.__mictlanx_path))
    
    def from_bytes(key:str, value:bytes)->"Ball":
        size = len(value)
        content_type="application/octet-stream"
        
        checksum = XoloUtils.sha256(value=value)
        return Ball(key=key, size=size, checksum=checksum,value=value,content_type=content_type)
    
    def from_path(path:str,key:str="")->"Ball":
        if not os.path.exists(path):
            raise Exception("File at {} does not exists".format(path))
        (checksum, size) = XoloUtils.sha256_file(path)
        content_type="application/octet-stream"
        ball = Ball(key=key, checksum=checksum,size=size, path=Some(path),content_type=content_type)
        if os.path.exists(ball._Ball__mictlanx_path):
            ball.path = Some(ball._Ball__mictlanx_path)
        return ball
    
    @check_destroyed
    def to_disk(self,path:Option[str]= NONE, mictlanx_path:bool =True, clean:bool = True)->int:
        size = len(self.value)
        if size ==0:
            return -1
        _path = self.__resolve_path(path= Some (self.__mictlanx_path) if mictlanx_path else path )
        directory= os.path.dirname(_path)
        if not os.path.exists(path=directory):
            os.makedirs(directory)
        
        if os.path.exists(_path):
            return 1
        else:
            with open(_path,"wb") as f:
                f.write(self.value)
            if clean:
                self.clean()
            self.path = Some(self.__mictlanx_path)
            return 0

    @check_destroyed
    def to_memory(self,from_mictlanx:bool = True)->int:
        if from_mictlanx:
            self.read_all()
            
        if self.path.is_none:
            return -1
        else:
            self.value = self.read_all()
            return 0
    
    @check_destroyed
    def clean(self):
        self.value=b""

    @check_destroyed
    def destroy(self):
        self.clean()
        path = self.__resolve_path()
        if os.path.exists(path):
            print("Removed {}".format(path))
        self.__destroyed =True
        
    def read_all(self)->bytes:
        with open(self.__resolve_path(path = self.path),"rb") as f:
            return f.read()
        
    def read_gen(self,chunk_size:int=1024)->Generator[bytes, None, int]:
        with open(self.path,"rb") as f:
            size = 0
            while True:
                data = f.read(chunk_size)
                if not data:
                    return size
                size += len(data)
                yield data
    def __eq__(self, __value: "Ball") -> bool:
        return self.checksum == __value.checksum 

    def __str__(self):
        return "Ball(key={}, checksum={}, size={}, content_type={})".format(self.key,self.checksum,self.size,self.content_type)


