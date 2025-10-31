from typing import TypeVar,OrderedDict as ODT,Tuple,List
from abc import ABC, abstractmethod
from collections import OrderedDict, Counter
import heapq
from mictlanx.interfaces import Metadata
from option import Option, Some, NONE

T = TypeVar("T")




class CacheX(ABC):
    """Abstract Base Class for a simple key-value cache (Key: str -> Value: bytes/memoryview)."""
    @abstractmethod
    def get_keys(self)->List[str]:
        pass
    @abstractmethod
    def get(self, key: str) -> Option[memoryview]:
        """Retrieve a value from the cache."""
        pass

    @abstractmethod
    def put(self, key: str, value: bytes)->int:
        """Insert a value into the cache."""
        pass

    @abstractmethod
    def remove(self, key: str):
        """Remove a value from the cache."""
        pass

    @abstractmethod
    def __len__(self) -> int:
        """Return the current size of the cache."""
        pass

    @abstractmethod
    def clear(self):
        """Clear the cache."""
        pass
    @abstractmethod
    def get_total_storage_capacity(self):
        pass
    @abstractmethod
    def get_used_storage_capacity(self):
        pass

    @abstractmethod
    def get_uf(self):
        pass

class CacheFactory:
    @staticmethod
    def create(eviction_policy:str,capacity_storage:int):
        if eviction_policy == "LRU":
            return LRUCache(capacity_storage = capacity_storage)
        elif eviction_policy == "LFU":
            return LFUCache(capacity_storage = capacity_storage)
        return LRUCache(capacity_storage=capacity_storage)


class LRUCache(CacheX):
    """LRU (Least Recently Used) Cache implementation using OrderedDict."""

    def __init__(self, capacity_storage:int):
        # self.capacity         = capacity
        self.capacity_storage = capacity_storage
        self.used_capacity    = 0
        self.cache:ODT[str,Tuple[Metadata,memoryview]]            = OrderedDict()  # Maintains insertion order

    def get_keys(self):
        return list(self.cache.keys())
    def get(self, key: str) ->Option[Tuple[Metadata,memoryview]]:
        if key in self.cache:
            self.cache.move_to_end(key)  # Mark as recently used
            return Some(self.cache[key])
        return NONE  # Key not found

    def put(self, key: str, value: bytes, metadata:Metadata)->int:
        try:
            size                  = len(value)
            current_used_capacity = self.used_capacity + size
            can_store             = current_used_capacity <= self.capacity_storage
            if key in self.cache:
                self.cache.move_to_end(key)  # Mark as recently used
            # elif len(self.cache) >= self.capacity :
            elif not can_store:
                (_,deleted_value) = self.cache.popitem(last=False)  # Remove the least recently used item
                self.used_capacity-=len(deleted_value[1])

            self.cache[key] = (metadata,memoryview(value))  # Store new value
            self.used_capacity+= size
            return 0
        except Exception as e:
            print(e)
            return -1

    def remove(self, key: str):
        if key in self.cache:
            element = self. cache[key]
            self.used_capacity-= len(element)
            del self.cache[key]

    def __len__(self) -> int:
        return len(self.cache)

    def clear(self):
        self.cache.clear()
        self.used_capacity= 0
    def get_total_storage_capacity(self):
        return self.capacity_storage
    def get_used_storage_capacity(self):
        return self.used_capacity

    def get_uf(self):
        return 1- ((self.get_total_storage_capacity() - self.get_used_storage_capacity())/self.get_total_storage_capacity())


class LFUCache(CacheX):
    """LFU (Least Frequently Used) Cache implementation using a frequency counter and heap."""

    def __init__(self, capacity_storage:int):
        self.capacity_storage = capacity_storage
        self.used_capacity    = 0
        self.cache = {}  # Key -> Value (bytes)
        self.freq_counter = Counter()  # Key -> Frequency
        self.freq_heap = []  # Min-heap to track least frequently used keys

    def get_keys(self):
        return list(self.cache.keys())
    def get(self, key: str) -> Option[Tuple[Metadata,memoryview]]:
        if key in self.cache:
            self.freq_counter[key] += 1
            heapq.heappush(self.freq_heap, (self.freq_counter[key], key))
            return Some(self.cache[key])
        return NONE

    def put(self, key: str, value: bytes,metadata:Metadata) -> int:
        try:
            size                  = len(value)
            current_used_capacity = self.used_capacity + size
            can_store             = current_used_capacity <= self.capacity_storage
            if key in self.cache:
                self.cache[key] = (metadata,value)
                self.freq_counter[key] += 1
            else:
                # if len(self.cache) >= self.capacity:
                if not can_store:
                    # Remove the least frequently used item
                    while self.freq_heap:
                        freq, least_used_key = heapq.heappop(self.freq_heap)
                        if self.freq_counter[least_used_key] == freq:
                            (least_metadata, least_value) = self.cache[least_used_key]
                            self.used_capacity-= len(least_value)
                            del self.cache[least_used_key]
                            del self.freq_counter[least_used_key]
                            break
                self.cache[key] = (metadata,value)
                self.freq_counter[key] = 1
            heapq.heappush(self.freq_heap, (self.freq_counter[key], key))
            return 0
        except Exception as e:
            return -1

    def remove(self, key: str):
        if key in self.cache:
            element = self.cache[key]
            self.used_capacity-= len(element)
            del self.cache[key]
            del self.freq_counter[key]

    def __len__(self) -> int:
        return len(self.cache)

    def clear(self):
        self.cache.clear()
        self.freq_counter.clear()
        self.freq_heap.clear()
        self.used_capacity = 0

    def get_total_storage_capacity(self):
        return self.capacity_storage
    def get_used_storage_capacity(self):
        return self.used_capacity

    def get_uf(self):
        return 1- ((self.get_total_storage_capacity() - self.get_used_storage_capacity())/self.get_total_storage_capacity())

class NoCache(CacheX):
    def get(self, key):
        return NONE
    def put(self, key, value):
        return None
    def remove(self, key):
        return super().remove(key)
    def clear(self):
        return super().clear()
    def __len__(self):
        return 0



# if __name__ =="__main__":
    # lru_cache = LRUCache(capacity=2)
    # x= lru_cache.put("k0",value=memoryview(b"k0"))
    # print("K0 PUT",x)
    # x=lru_cache.put("k1",value=memoryview(b"k1"))
    # y = lru_cache.get("k0")
    # y = lru_cache.get("k0")
    # y = lru_cache.get("k1")
    # print("K1 PUT",x)
    # x=lru_cache.put("k2",value=memoryview(b"k2"))
    # print("K2 PUT",x)