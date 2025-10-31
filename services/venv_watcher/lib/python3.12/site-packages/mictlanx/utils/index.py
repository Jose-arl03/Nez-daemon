from typing import List
import hashlib as H
import humanfriendly as HF
from concurrent.futures import ThreadPoolExecutor,as_completed
from typing import Generator,Any,Tuple,AsyncGenerator
import os
from xolo.utils.utils import Utils as XoloUtils
from collections import namedtuple
from pathlib import Path
from option import Some,Option,NONE
import re
# from mictlanx.services import AsyncPeer
# from mictlanx.services.sync import Router,Peer

# 
FileInfoBase = namedtuple("FileInfo","path checksum size")
class FileInfo(FileInfoBase):
    def upadate_path_relative_to(self,relative_to:str):
        _relative_to = Path(relative_to)
        path = Path(self.path).relative_to(_relative_to)
        return FileInfoBase(str(path),self.checksum,self.size)

class Utils(object):
    @staticmethod
    def camel_to_snake(x:str):
        x1 = re.sub(r'([a-z])([A-Z])', r'\1_\2', x)
        x2 = re.sub(r'([A-Z])([A-Z][a-z])', r'\1_\2', x1)  # For cases like "HTTPServer"
        return (x2[0] + x2[1:]).upper()
    @staticmethod
    def to_gen_bytes(data:bytes,chunk_size:str="1MB")->Generator[bytes, None,None]:
        cs = HF.parse_size(chunk_size)
        for i in range(0, len(data), cs):
            yield data[i:i + cs]
    @staticmethod
    async def to_async_gen_bytes(data:bytes,chunk_size:str="1MB")->AsyncGenerator[bytes, None]:
        cs = HF.parse_size(chunk_size)
        for i in range(0, len(data), cs):
            yield data[i:i + cs]   

    @staticmethod
    def split_path(path: str,is_file:bool = True) -> Tuple[str, str, str]:
        path = Path(path)

        # is_file = os.path.isfile(path=path)
        if not is_file:
            return (str(path),"","")  # Not a file with extension

        parent = str(path.parent)
        filename = path.stem      # name without extension
        extension = path.suffix.replace(".","")   # includes the dot, e.g. '.txt'

        return parent, filename, extension
    @staticmethod
    def extract_path_info(path:str)->Tuple[str,str,str]:
        fullname = os.path.basename(path)
        fullname_spliited = fullname.split(".")
        ext = ""
        if len(fullname_spliited) >1:
            ext = fullname_spliited[1]
        filename = fullname_spliited[0]
        return fullname,filename,ext
            
        # fullname = 
    @staticmethod
    def sanitize_str(x: str) -> str:
        pattern = re.compile(r'^[a-zA-Z0-9]+([a-zA-Z0-9]|[-_](?=[a-zA-Z0-9]))*$')
        sanitized = ''.join(re.findall(r'[a-zA-Z0-9-_]', x))
        sanitized = re.sub(r'[-_]{2,}', '-', sanitized)
        sanitized = re.sub(r'^-|-$', '', sanitized)
        if pattern.match(sanitized):
            return sanitized
        else:
            return ''

    @staticmethod
    def get_or_default(iterator:List[Any],i:int=0,default = None)->Option[Any]:
        n = len(iterator)
        try:
            if n ==0:
                return NONE if default is None else Some(default)
            elif n == 1:
                return Some(iterator[0])
            elif i >= n:
                return NONE if default is None else Some(default)
            else:
                return Some(iterator[i])
        except Exception as e:
            return NONE if default is None else Some(default)

    @staticmethod
    def get_checksums_and_sizes(path:str,max_workers:int = 2)->Generator[FileInfo,None,None]:
        futures = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as tp:
            for (root,_, fullnames) in os.walk(path):
                for fullname in fullnames:
                    file_path = "{}/{}".format(root,fullname)
                    fut = tp.submit(XoloUtils.extract_path_sha256_size, path = file_path )
                    futures.append(fut)
            for future in as_completed(futures):
                result =FileInfo(*future.result())
                yield result



    @staticmethod
    def file_to_chunks_gen(path:str, chunk_size:str="1MB"):
        _chunk_size = HF.parse_size(chunk_size)
        with open(path,"rb") as f:
            while True:
                value:bytes                     = f.read(_chunk_size)
                if not value:
                    break
                yield value
                
    @staticmethod
    def calculate_disk_uf(total:int,used:int,size:int = 0 ):
        return  1 - ((total - (used + size))/total)
