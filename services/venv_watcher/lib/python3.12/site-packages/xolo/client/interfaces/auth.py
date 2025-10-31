# xolo/client/interfaces/auth.py
from typing import Dict
from dataclasses import dataclass

@dataclass
class AuthenticatedDTO(object):
    username:str
    first_name:str
    last_name:str
    email:str
    profile_photo:str 
    access_token:str 
    metadata:Dict[str,str]
    temporal_secret:str 
    role:str
@dataclass
class AssignLicenseResponseDTO:
    expires_at: str
    ok:bool
@dataclass
class DeletedLicenseResponseDTO:
    ok:bool
@dataclass
class SelfDeletedLicenseResponseDTO:
    ok:bool

@dataclass
class AssignedScopeResponseDTO:
    name:str
    username:str
    ok:bool