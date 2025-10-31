# xolo/abac/loader.py
from xolo.abac.models import Policy,AccessRequest
from typing import List
from pathlib import Path
import json as J

class PolicyLoader:
    @staticmethod
    def from_file(filepath: str) -> List[Policy]:
        """
        Carga políticas desde un archivo JSON y las convierte en objetos Policy.

        :param filepath: Ruta al archivo JSON.
        :return: Lista de objetos Policy.
        :raises: ValueError si alguna política está mal formada.
        """
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"{filepath} not found.")

        with open(filepath, "r", encoding="utf-8") as f:
            raw_data = J.load(f)

        policies = []
        for entry in raw_data:
            p = Policy.model_validate(entry)
            policies.append(p)
        return policies
    


class AccessRequestLoader:
    def from_file(filepath: str) -> List[AccessRequest]:
        """
        Carga solicitudes de acceso desde un archivo JSON.

        :param filepath: Ruta al archivo JSON.
        :return: Lista de objetos AccessRequest.
        """
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"{filepath} not found.")

        with open(filepath, "r", encoding="utf-8") as f:
            raw_requests = J.load(f)

        requests = []
        for entry in raw_requests:
            requests.append(AccessRequest.model_validate(entry))
        return requests
