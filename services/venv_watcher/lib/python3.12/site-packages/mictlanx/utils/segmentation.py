from __future__ import annotations
from option import Option,NONE,Some
from typing import Dict,Iterator, List,Any,Callable,Tuple,Generator,AsyncGenerator,Union
import mictlanx.interfaces as InterfaceX
import humanfriendly as HF
# from mictlanx.v4.interfaces.index import Metadata
import numpy as np
import math
import numpy.typing as npt
import hashlib as H
import os
import pickle as PK


#
class Chunk(object):
    def __init__(self,group_id:str,index:int,data:bytes,chunk_id:Option[str]=NONE,metadata:Dict[str,str]={}):
        self.group_id = group_id
        self.index    = index
        self.size     = len(data)
        self.data     = data
        self.metadata = {**metadata, "index":str(index), "chunk_size":str(self.size),"group_id":self.group_id}
        hasher = H.sha256()
        hasher.update(self.data)
        self.checksum = hasher.hexdigest()
        self.chunk_id = chunk_id.unwrap_or(self.checksum)
    def __str__(self):
        return "Chunk(chunk_id={}, index={}, size={})".format(self.chunk_id,self.index,self.size)
    @staticmethod
    def from_ndarray(group_id:str,index:int,ndarray:npt.NDArray, metadata:Dict[str,str]={}, chunk_id:Option[str]=NONE):
        metadata["shape"] = str(ndarray.shape)
        metadata["attributes"] = str(ndarray.shape[1])
        metadata["records"] = str(ndarray.shape[0])
        metadata["dtype"] = str(ndarray.dtype)
        return Chunk(group_id=group_id,index= index, data = ndarray.tobytes(order="C"), metadata=metadata,chunk_id=chunk_id )

    @staticmethod
    def from_list(group_id:str, index:int,xs:List[Any], metadata:Dict[str,str]={} , chunk_id:Option[str]=NONE):
        data =PK.dumps(xs)
        return Chunk(group_id=group_id,index= index, data = data, metadata=metadata,chunk_id=chunk_id )
    @staticmethod
    def from_bytes(group_id:str, index:int,data:bytes, metadata:Dict[str,str]={} , chunk_id:Option[str]=NONE):
        return Chunk(group_id=group_id,index= index, data = data, metadata=metadata,chunk_id=chunk_id )
    
    def to_list(self)->Option[List[Any]]:
        try:
            xs = PK.loads(self.data)
            return Some(xs)
        except Exception as e:
            print(e)
            return NONE
    
    def to_ndarray(self)->Option[npt.NDArray]:
        try:
            shape   = eval(self.metadata.get("shape"))
            dtype   = self.metadata.get("dtype","float64")
            ndarray = np.frombuffer(self.data,dtype=dtype).reshape(shape)
            # print(ndarray)
            return Some(ndarray)
        except Exception as e:
            return NONE
    def to_generator(self, chunk_size:str="256kb")->Generator[bytes,None,None]:
        """Generator that yields chunks of `chunk_size` from `data`."""
        # mv = memoryview(self.data)  # ✅ No data copying
        _cs = HF.parse_size(chunk_size)
        for i in range(0, self.size, _cs):
            yield self.data[i:i + _cs]
    
    async def to_async_generator(self, chunk_size:str="256kb")->AsyncGenerator[bytes,None,None]:
        """Generator that yields chunks of `chunk_size` from `data`."""
        _cs   = HF.parse_size(chunk_size)
        total = 0
        for i in range(0, self.size, _cs):
            part = self.data[i:i + _cs]
            part_size = len(part)
            total+= part_size
            yield part



class Chunks(object):
    def __init__(self,chs:Iterator[Chunk],n:int ):
        self.chunks:List[Chunk] = list(chs)
        self.n:int = n 
    
    def sort(self,reverse:bool=False):
        self.chunks.sort(key= lambda chunk: chunk.index,reverse=reverse)
    def __len__(self):
        return len(self.chunks)
    def __iter__(self):
        """
        Returns an iterator object.
        """
        self.current = 0
        return iter(self.chunks)
    def __next__(self):
        """
        Returns the next chunk of data.

        :return: A chunk of the data.
        :raises StopIteration: When all chunks are processed.
        """
        if self.current >= len(self.chunks):
            raise StopIteration  # No more chunks left

        chunk = self.chunks[self.current]
        self.current += 1
        return chunk
    def len(self)->int:
        return self.n
    def iter(self):
        return self.chunks
    
    def sorted_by(self,filter_by:Callable[[Chunk], Any] = lambda x:x.index)->Iterator[Chunk]:
        return sorted(self.chunks, key= filter_by)
    
    @staticmethod
    def _iter_to_chunks(
        group_id:str,
        iterable:Any,
        n:int,
        chunk_prefix:Option[str]=NONE,
        chunk_size:Union[Option[int], Option[str]]=NONE,
        num_chunks:int =1
    ):
        # THE RATIO OF RECORDS PER CHUNK (float)
        data_per_chunk     = chunk_size.unwrap_or(n / num_chunks)
        if type(data_per_chunk) == str:
            data_per_chunk = HF.parse_size(data_per_chunk)
        # Check if the data per chunk is lower or equal to the number of total elements. 
        dpc_is_lower_than_n = data_per_chunk <= n
        if not dpc_is_lower_than_n:
            data_per_chunk = n
        # data per chunk but int
        data_per_chunk_int = int(data_per_chunk)
        # Total number of chunked elements (chunked = element that belongs to a specific chunk)
        total_chunked_elements = 0
        # Current chunk index
        i                      = 0 
        # Check that total number of chunked elements is lower than the total number of elements
        chunks = []
        exact_num_chunks = str(math.ceil(n/data_per_chunk))
        while total_chunked_elements < n:
            # Difference between total number of elements and total chunked elements
            diff = n - total_chunked_elements
            # Chunk metadata 
            metadata = {"index": str(i),"num_chunks":exact_num_chunks }
            # Check if diff is lower than -> if it is lower then drain all the iterable. 
            if diff < data_per_chunk:
                current_total_records_sent = data_per_chunk_int*i
                total_chunked_elements     += n - current_total_records_sent
                records_chunk              = iterable[current_total_records_sent:]
                chunk_metadata             = chunks[-1]
                # chunk_metadata["metadata"] ={"num_chunks": str(n)}
                if type(records_chunk) == np.ndarray:
                    chunk_metadata["data"] = np.concatenate([chunk_metadata["data"], records_chunk])
                else:
                    chunk_metadata["data"] = chunk_metadata["data"]+records_chunk
                if chunk_prefix.is_some:
                    chunk_metadata["chunk_id"] ="{}_{}".format(chunk_prefix.unwrap(),i-1)
            else:
                total_chunked_elements += data_per_chunk_int
                from_index             = i*data_per_chunk_int
                to_index               = ((i+1)*data_per_chunk_int)
                records_chunk          = iterable[from_index: to_index]
                chunk_metadata = {'group_id':group_id, 'index':i, 'data':records_chunk, 'metadata':metadata}
                if chunk_prefix.is_some:
                    chunk_metadata["chunk_id"] ="{}_{}".format(chunk_prefix.unwrap(),i)
                i+=1
                chunks.append(chunk_metadata)
        return chunks
   
    @staticmethod
    def iter_to_chunks(group_id:str,iterable:Any,n:int,chunk_prefix:Option[str]=NONE,chunk_size:Option[int]=NONE,num_chunks:int =1):
        # hashing
        # print("ITERABLE_TYPE",type(iterable))
        # hasher = H.sha256()
        # THE RATIO OF RECORDS PER CHUNK (float)
        data_per_chunk     = chunk_size.unwrap_or(n / num_chunks)
        # Check if the data per chunk is lower or equal to the number of total elements. 
        assert(data_per_chunk <= n)
        # data per chunk but int
        data_per_chunk_int = int(data_per_chunk)
        # Total number of chunked elements (chunked = element that belongs to a specific chunk)
        total_chunked_elements = 0
        # Current chunk index
        i                      = 0 
        # Check that total number of chunked elements is lower than the total number of elements
        while total_chunked_elements < n:
            # Difference between total number of elements and total chunked elements
            diff = n - total_chunked_elements
            # Chunk metadata 
            metadata = {"index": str(i)}
            # Check if diff is lower than -> if it is lower then drain all the iterable. 
            if diff < data_per_chunk:
                current_total_records_sent = data_per_chunk_int*i
                total_chunked_elements     += n - current_total_records_sent
                records_chunk              = iterable[current_total_records_sent:]
            else:
                total_chunked_elements += data_per_chunk_int
                from_index             = i*data_per_chunk_int
                to_index               = ((i+1)*data_per_chunk_int)
                records_chunk          = iterable[from_index: to_index]
            # hasher.update(records_chunk)
            chunk_metadata = {'group_id':group_id, 'index':i, 'data':records_chunk, 'metadata':metadata}
            if chunk_prefix.is_some:
                chunk_metadata["chunk_id"] ="{}_{}".format(chunk_prefix.unwrap(),i)

            i+=1
            yield chunk_metadata
        # return Chunks(chs=__inner(),n = n)    

            # The fractional part of the records per worker.

            # records_fraction = records_per_worker - records_per_worker_int 
            # sum_records_per_worker  = records_per_worker * workers 
            # assert(sum_records_per_worker<= records_len)
    

    @staticmethod
    def from_list(xs:List[Any], group_id:str,chunk_prefix:Option[str]=NONE,chunk_size:Option[int] = NONE,num_chunks:int = 1):
        try:
            n = len(xs)
            def __inner():
                _num_chunks = n if  n < num_chunks else num_chunks
                _xs= Chunks._iter_to_chunks(
                    iterable=xs,
                    group_id = group_id,
                    n = n,
                    num_chunks=_num_chunks,
                    chunk_size=chunk_size,
                    chunk_prefix=chunk_prefix
                )
                for i,x in enumerate(_xs):
                    chunk_id       = Some(x.get("chunk_id",None)).filter(lambda x: not x == None)
                    chunk          = Chunk.from_list(group_id = group_id, index = x["index"], xs=x["data"],metadata = x['metadata'],chunk_id=chunk_id)
                    # chunk.chunk_id = x.get("chunk_id",chunk.chunk_id)
                    # chunk.chunk_id = chunk_prefix.map(lambda x: "{}_{}".format(x,chunk.index)).unwrap_or(chunk.chunk_id)
                    yield chunk
            return Some(Chunks(chs= __inner() , n = n ))
        except Exception as e:
            print(e)
            return NONE      
    @staticmethod
    def from_ndarray(ndarray:npt.NDArray, group_id:str,chunk_prefix:Option[str]=NONE,chunk_size:Option[int] = NONE,num_chunks:int = 1 )->Option[Chunks]:
        
        try:
            def __inner():
                n = ndarray.shape[0]
                _num_chunks = n if  n < num_chunks else num_chunks
                xs= Chunks._iter_to_chunks(
                    iterable=ndarray,
                    group_id = group_id,
                    n = n,
                    num_chunks=_num_chunks,
                    chunk_size=chunk_size,
                    chunk_prefix=chunk_prefix
                )
                for i,x in enumerate(xs):
                    chunk_id       = Some(x.get("chunk_id",None)).filter(lambda x: not x == None)
                    chunk          = Chunk.from_ndarray(group_id = group_id, index = x["index"], ndarray=x["data"],metadata = x['metadata'],chunk_id=chunk_id)
                    # chunk.chunk_id = x.get("chunk_id",chunk.chunk_id)
                    # chunk.chunk_id = chunk_prefix.map(lambda x: "{}_{}".format(x,chunk.index)).unwrap_or(chunk.chunk_id)
                    yield chunk
            return Some(Chunks(chs= __inner() , n = ndarray.shape[0]))
        except Exception as e:
            return NONE

    @staticmethod
    def from_file(path:str,group_id:str,chunk_size:Option[int] = NONE,num_chunks:int =1)->Option[Chunks]:
        try:
            file_size:int              = os.path.getsize(path)
            if file_size <= 0:
                return NONE
            if chunk_size.is_some:
                effective_chunk_size = chunk_size.unwrap()
            else:
                if num_chunks <= 0:
                    raise ValueError("num_chunks must be >= 1")
                effective_chunk_size = max(1024, file_size // num_chunks)

            
            def __inner():
                with open(path,"rb") as f:
                    # print(num_chunks)
                    i=0
                    while True:
                        # metadata = {"index":str(i)}
                        metadata = {}
                        cid = Some(f"{group_id}_{i}")
                        data = f.read(effective_chunk_size)
                        if not data:
                            break
                        # metadata["index"]= str(i)
                        yield Chunk(
                            group_id=group_id,
                            chunk_id=cid,
                            index=i,
                            data=data,
                            metadata=metadata
                        )
                        i += 1
                        # if i >= num_chunks-1:
                        #     data = f.read()
                        #     if not data:
                        #         break
                        #     chunk_metadata = {'group_id':group_id,"chunk_id":cid, 'index':i, 'data':data, 'metadata':metadata}
                        #     chunk = Chunk(**chunk_metadata)
                        #     # i+=1
                        #     yield chunk
                        # else:
                        #     data = f.read(records_per_worker_int)
                        #     if not data:
                        #         break
                        #     chunk_metadata = {'group_id':group_id,"chunk_id":cid, 'index':i, 'data':data, 'metadata':metadata}
                        #     chunk = Chunk(**chunk_metadata)
                        #     i+=1
                        #     yield chunk
                        # Chunk(group_id=group_id,index=i, )

            return Some(Chunks(chs=__inner(), n = file_size))
        except Exception as e:
            return NONE

    @staticmethod
    def from_bytes(data:bytes,group_id:str,chunk_size:Option[int] = NONE,num_chunks:int =1,chunk_prefix:Option[str]=NONE)->Option[Chunks]:
        def __inner():
            xs = Chunks._iter_to_chunks(
                iterable = data,
                group_id = group_id,
                num_chunks = num_chunks,
                n=len(data),
                chunk_size=chunk_size,
                chunk_prefix=chunk_prefix
            ) 
            for x in xs:
                chunk = Chunk(group_id = group_id,chunk_id=Some(x["chunk_id"]),data=x["data"],index=x["index"], metadata = x["metadata"])
                yield chunk
        return Some(Chunks(chs = __inner(), n = len(data)))
        
    @staticmethod
    def from_generator(gen:Generator[bytes,None,None], group_id:str,chunk_size:Option[int] = NONE,num_chunks:int =1)->Option[Chunks]:
        _gen = b"".join(gen)
        n    = len(_gen)
        return Chunks.from_bytes(
            data=_gen,
            group_id=group_id,
            chunk_prefix=Some(group_id),
            chunk_size=chunk_size,
            num_chunks=num_chunks,
        )
    

      
    def to_generator(self)->Generator[bytes,None,None]:
        for chunk in self.iter():
            yield chunk.data

    def to_bytes(self)->bytes:
        xs = memoryview(b"")
        concatenated = bytearray().join(map(lambda x:x.data,self.iter()))
        return memoryview(concatenated).tobytes()

    # GET ndarray and metadata
    def to_ndarray(self)->Option[Tuple[npt.NDArray,InterfaceX.Metadata]]:
        try:
            result   = []
            metadata = InterfaceX.Metadata(id="ID", size=0, checksum="",group_id="",tags={})
            hasher   = H.sha256()
            size     = 0
            for chunk in self.sorted_by(filter_by=lambda chunk:chunk.index):
                if not ("shape" in chunk.metadata or "dtype" in chunk.metadata):
                    return NONE
                dtype   = chunk.metadata.get("dtype","float64")
                shape   = eval(chunk.metadata.get("shape"))
                hasher.update(chunk.data)
                size    += len(chunk.data)
                ndarray = np.frombuffer(chunk.data,dtype= dtype).reshape(shape)
                result.append(ndarray)
            metadata.size     = size
            metadata.checksum = hasher.hexdigest()
            result = np.vstack(result)
            return Some((result,metadata))
        except Exception as e:
            print(e)
            return NONE



if __name__ == "__main__":
    pass
    
    # for chunk in chs.iter():
        # print("CHUNK",chunk)
    # chs = [ 
        # Chunk.from_ndarray(ndarray=np.ones((5,5)), group_id="encrypted_matrix-0",index=0  ),
        # Chunk.from_ndarray(ndarray=np.ones((5,5)),group_id="encrypted_matrix-0" ,index=1 )
    # ]
    # cs = Chunks(chs=chs,n=2)
    # __________________

    # encrypted_matrix0 = np.random.random(size=(1000,10,3))

        # chunk.
    # cs1               = Chunks.from_ndarray(ndarray=encrypted_matrix0 ,num_chunks=3,group_id="encrypted_matrix-0",chunk_prefix=Some("chunk") ).unwrap()
    # for chunk in cs1.chunks:
        # print(chunk.chunk_id,chunk.checksum,chunk.metadata)
    # print(cs1.to_ndarray().unwrap()[0].shape)
    # encrypted_matrix0 = cs1.to_ndarray()
    # print("MATRIX",encrypted_matrix0)
    

    # cs = Chunks.from_bytes(group_id = "key",data=b"12345678900", num_chunks= 2).unwrap() 
    # cs = Chunks.from_file(path="/source/01.pdf",group_id="test",num_chunks=4).unwrap()
    # xs = cs.to_list()
    # h = H.sha256()
    # for c in cs.iter():
        # h.update(c.data)
        # print(c)
    # x = h.hexdigest()
    # print(x)
        # print(c.data)
        # print(x.checksum,x.size)
    # print(xs)
    # cs = Chunks.iter_to_chunks(
    #         group_id="test",
    #         iterable= [0,1,2,3,4,5,6,7],
    #         chunk_size= Some(2),
    #         workers=3, 
    #         n = 8
    #     )
    # for c in cs :
    #     print(c)
    # NDARRAY
    # ndarray = np.ones((100,500))
    # cs_ndarray = Chunks.from_ndarray(ndarray=ndarray,group_id="test",workers=5,chunk_size=Some(50))
    # x = cs_ndarray.sorted_by(filter_by=lambda chunk:chunk.index)
    # for chunk in cs_ndarray.iter():
        # print(str(chunk))
    # print(cs_ndarray.to_ndarray())

    # print(cs.)

