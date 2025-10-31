from pydantic import BaseModel
class PeerModel(BaseModel):
    protocol: str
    ip_addr:str
    port:int
    peer_id:str