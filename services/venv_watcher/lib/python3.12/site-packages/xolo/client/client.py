# xolo/client/client.py
import requests as R
import json as J
from typing import Dict,List
from xolo.client.interfaces.auth import AuthenticatedDTO,AssignLicenseResponseDTO,AssignedScopeResponseDTO,DeletedLicenseResponseDTO
from option import Result,Ok,Err,Some

class XoloClient(object):
    def __init__(self,hostname:str,  port:int=-1,version:int=4 ):
        self.hostname  = hostname
        self.port     = port
        self.version  = version 
    
    def base_url(self):
        if self.port == -1:
            return "https://{}".format(self.hostname)
        else:
            return "http://{}:{}".format(self.hostname,self.port)
    def create_user(self,
                    username:str,
                    first_name:str,
                    last_name:str,
                    email:str,
                    password:str,
                    profile_photo:str="",
                    role:str = ""
    )->Result[R.Response,Exception]:
        try:
            url  = "{}/api/v{}/users".format(self.base_url(),self.version)
            data = J.dumps({
                "username":username,
                "first_name":first_name,
                "last_name":last_name,
                "email":email,
                "password":password,
                "profile_photo":profile_photo,
                "role": username if role =="" else role
            })
            
            response = R.post(url=url,data=data)
            response.raise_for_status()
            return Ok(response)
        except Exception as e:
            return Err(e)
    
    def verify(self,access_token:str,username:str,secret:str="")->bool:
        try:
            url  = "{}/api/v{}/users/verify".format(self.base_url(),self.version)
            data = J.dumps({
                "access_token":access_token,
                "username":username,
                "secret":secret
            })
            response = R.post(url=url,data=data)
            response.raise_for_status()
            return True
        except Exception as e:
            return False


    def auth(self,
             username:str,
             password:str,
             scope:str
    )->Result[AuthenticatedDTO,Exception]:
        try:
            url = "{}/api/v{}/users/auth".format(self.base_url(),self.version)
            data = J.dumps({
                "username":username,
                "password":password,
                "scope":scope
            })
            response = R.post(url=url,data=data)
            response.raise_for_status()
            data_json = response.json()
            return Ok(
                AuthenticatedDTO(**data_json)
            )
        except Exception as e:
            return Err(e)
    def create_license(
        self,
        username: str,
        scope: str,
        secret: str,
        expires_in: str = "1h",
        force:bool = True,
    )->Result[AssignLicenseResponseDTO, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.version}/licenses/"
            data = J.dumps({
                "username":username,
                "scope":scope,
                "expires_in":expires_in,
                "force":force
            })
            response = R.post(url=url, data = data, headers={"Secret": secret})
            response.raise_for_status()
            json_data = response.json()
            return Ok(AssignLicenseResponseDTO(
                **json_data
            ))
        except Exception as e:
            return Err(e)
    def create_scope(self,scope:str,secret:str="")->Result[bool, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.version}/scopes"
            data = J.dumps({
                "name":scope
            })
            response = R.post(url =url, data=data, headers={"Secret": secret})
            response.raise_for_status()
            json_data = response.json()
            return Ok(True)
        except Exception as e:
            return Err(e)
    def assign_scope(self,
                     username:str, scope:str, secret:str)->Result[AssignedScopeResponseDTO, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.version}/scopes/assign"
            data = J.dumps({
                "username":username,
                "name":scope
            })
            response = R.post(url =url, data=data, headers={"Secret": secret})
            response.raise_for_status()
            json_data = response.json()
            return Ok(
                AssignedScopeResponseDTO(**json_data)
            )
        except Exception as e:
            return Err(e)
    # ACL
    def get_resources_by_role(self,role:str)->Result[Dict[str,List[str]],Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.version}/users/{role}/resources"
            response = R.get(url=url)
            response.raise_for_status()
            print(response)
            return Ok( response.json().get("resources",{} ))
        except Exception as e:
            return Err(e)
        
    def grantx(self,role:str,grants:Dict[str,Dict[str,List[str]]])->Result[bool,Exception]:
        try:
            url = "{}/api/v{}/users/grantx".format(self.base_url(),self.version)
            data = J.dumps({
                "grants":grants,
                "role":role
            })
            response = R.post(url=url, data=data)
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
        
    def grants(self,grants:Dict[str,Dict[str,List[str]]],secret:str)->Result[bool, Exception]:
        try:
            url = "{}/api/v{}/users/grants".format(self.base_url(),self.version)
            data = J.dumps({
                "grants":grants
            })
            response = R.post(url=url, data=data,headers={"Secret":secret})
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    def check(self,role:str,resource:str, permission:str)->bool:
        try:
            url = "{}/api/v{}/users/check".format(self.base_url(),self.version)
            data = J.dumps({
                "role":role,
                "resource":resource,
                "permission":permission
            })
            response = R.post(url=url, data=data)
            response.raise_for_status()
            response_data = response.json()
            return response_data.get("result",False)
        except Exception as e:
            return False
    
    def delete_license(self,username:str, scope:str,force:bool = True,secret:str="")->Result[DeletedLicenseResponseDTO, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.version}/licenses/"
            data = J.dumps({
                "username":username,
                "scope":scope,
                "force":force
            })
            response = R.delete(url=url, data = data, headers={"Secret": secret})
            response.raise_for_status()
            json_data = response.json()
            return Ok(DeletedLicenseResponseDTO(
                **json_data
            )) 
        except Exception as e:
            return Err(e)
     
    def self_delete_license(self,username:str, scope:str,token:str,secret:str,force:bool = True)->Result[DeletedLicenseResponseDTO, Exception]:
        try:
            url = f"{self.base_url()}/api/v{self.version}/licenses/self"
            data = J.dumps({
                "token":token,
                "tmp_secret_key":secret,
                "username":username,
                "scope":scope,
                "force":force
            })
            response = R.delete(url=url, data = data) 
            response.raise_for_status()
            json_data = response.json()
            return Ok(DeletedLicenseResponseDTO(
                **json_data
            )) 
        except Exception as e:
            return Err(e)
