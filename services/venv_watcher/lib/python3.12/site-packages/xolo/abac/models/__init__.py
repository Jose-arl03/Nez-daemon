from typing import List
from pydantic import BaseModel

class AttributeComponent(BaseModel):
    """
    Representa un componente de atributo en un evento, como 'rol' = 'Administrador'.
    """
    attribute:str
    value:str

class Event(BaseModel):
    """
    Representa un evento dentro de una política, compuesto por cinco atributos clave:
    sujeto, activo, espacio, tiempo y acción. Ahora también tiene un event_id.
    """
    event_id:str
    subject:AttributeComponent
    asset:AttributeComponent
    space:AttributeComponent
    time:AttributeComponent
    action:AttributeComponent

class AccessRequest(BaseModel):
    subject: AttributeComponent
    asset: AttributeComponent
    space: AttributeComponent
    time: AttributeComponent
    action: AttributeComponent
    
    @classmethod
    def from_json(cls, path: str) -> "AccessRequest":
        with open(path, "r", encoding="utf-8") as f:
            json_data = f.read()  # read the file as a string
        return cls.model_validate_json(json_data)

class Policy(BaseModel):
    """
    Representa una política completa, la cual contiene uno o más eventos.
    """
    policy_id:str
    description:str
    events:List[Event]
    effect:str

    @classmethod
    def from_json(cls, path: str) -> "Policy":
        with open(path, "r", encoding="utf-8") as f:
            json_data = f.read()  # read the file as a string
        return cls.model_validate_json(json_data)