
import httpx
import json
from typing import Dict, Any, List, Optional

class MictlanXAPI:
    """
    A client for interacting with the MictlanX storage service API.
    This class is intended to replicate the functionality of the C++ skycds API.
    """

    def __init__(self, router_url: str, token: Optional[str] = None):
        """
        Initializes the MictlanX API client.

        Args:
            router_url: The base URL of the MictlanX router.
            token: Optional authentication token.
        """
        self.router_url = router_url.rstrip('/')
        self.token = token
        self.client = httpx.AsyncClient()

    async def close(self):
        """Closes the underlying HTTP client."""
        await self.client.aclose()

    async def upload_file(self, bucket: str, file_path: str, file_name: str) -> Dict[str, Any]:
        """
        Uploads a file to a specified bucket in MictlanX.
        This combines the logic of pushContent and insertFile from skycds.
        """
        url = f"{self.router_url}/{bucket}/{file_name}"
        try:
            with open(file_path, "rb") as f:
                content = f.read()
            
            response = await self.client.post(url, content=content, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            print(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            print(f"An error occurred during file upload: {e}")
            raise

    async def download_file(self, bucket: str, file_name: str, destination_path: str) -> bool:
        """
        Downloads a file from MictlanX.
        This corresponds to the 'retrieve' or 'downloadFiles' methods in skycds.
        """
        url = f"{self.router_url}/{bucket}/{file_name}"
        try:
            with open(destination_path, "wb") as f:
                async with self.client.stream("GET", url, timeout=60.0) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_bytes():
                        f.write(chunk)
            return True
        except httpx.HTTPStatusError as e:
            print(f"HTTP error occurred while downloading file: {e.response.status_code} - {e.response.text}")
            return False
        except Exception as e:
            print(f"An error occurred during file download: {e}")
            return False

    async def get_file_info(self, bucket: str, file_name: str) -> Dict[str, Any]:
        """
        Retrieves metadata for a specific file.
        Corresponds to 'getFileInformation' in skycds.
        """
        url = f"{self.router_url}/metadata/{bucket}/{file_name}"
        try:
            response = await self.client.get(url, timeout=10.0)
            
            # MictlanX can return 500 with "404: No available peers" for non-existent files
            if response.status_code == 500:
                try:
                    data = response.json()
                    if data.get("detail") == "404: No available peers":
                        return {"error": "File not found"}
                except json.JSONDecodeError:
                    pass # Not the specific 404-in-500 error, fall through to raise
            
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            print(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
            # Return a specific structure for not found, otherwise re-raise
            if e.response.status_code == 404:
                return {"error": "File not found"}
            raise
        except Exception as e:
            print(f"An error occurred while getting file info: {e}")
            raise

    async def list_files(self, bucket: str) -> List[Dict[str, Any]]:
        """
        Lists all files within a specific bucket.
        Corresponds to 'getFiles' in skycds.
        """
        url = f"{self.router_url}/metadata/{bucket}"
        try:
            response = await self.client.get(url, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            print(f"HTTP error occurred while listing files: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            print(f"An error occurred while listing files: {e}")
            return []

    async def create_bucket(self, bucket_name: str) -> bool:
        """
        Creates a new bucket (catalog in skycds).
        In MictlanX, buckets are created implicitly when the first file is uploaded,
        so this method simply returns True to maintain interface compatibility.
        """
        # MictlanX buckets are created on-the-fly, no explicit API call is needed.
        return True
