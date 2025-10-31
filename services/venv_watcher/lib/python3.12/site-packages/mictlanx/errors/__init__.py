
from typing import Optional
import socket
# 
import httpx
# 
from mictlanx.utils import Utils
class MictlanXError(Exception):
    """Base class for all custom exceptions."""
    
    default_message = "An error occurred."
    status_code = 500  # Default to Internal Server Error

    def __init__(self, message=None, status_code=None):
        self.message = message or self.default_message
        self.status_code = status_code or self.status_code
        super().__init__(self.message)

    def __str__(self):
        return f"{self.__class__.__name__} (status={self.status_code}): {self.message}"
    def get_name(self):
        return Utils.camel_to_snake(self.__class__.__name__)
    
    @staticmethod
    def _root_cause(exc: BaseException) -> BaseException:
        cur = exc
        # Walk __cause__/__context__ to the deepest cause.
        while True:
            nxt = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
            if nxt is None: return cur
            cur = nxt

    @staticmethod
    def _format_endpoint_from_request(req: Optional[httpx.Request]) -> str:
        if not req:
            return ""
        try:
            url = req.url
            # Explicit host:port if available
            host = url.host or ""
            port = url.port or (443 if url.scheme == "https" else 80)
            return f"{url.scheme}://{host}:{port}"
        except Exception:
            return ""
        
    @staticmethod
    def from_exception(e: Exception) -> 'MictlanXError': 
        """Maps an exception to a defined error class based on its status code."""

        status_code = 500  # default
        message = str(e)   # default message

        # If it's from httpx and a non-2xx HTTP response
        if isinstance(e, httpx.RequestError):
            endpoint = MictlanXError._format_endpoint_from_request(getattr(e, "request", None))
            root = MictlanXError._root_cause(e)
            root_name = root.__class__.__name__
            root_msg = str(root) or repr(root)

            # Classify special cases
            if isinstance(e, httpx.ConnectTimeout):
                return RequestTimeoutError(f"Timeout connecting to {endpoint} (connect): {root_name}: {root_msg}")
            if isinstance(e, (httpx.ReadTimeout, httpx.WriteTimeout)):
                return RequestTimeoutError(f"Timeout during request to {endpoint}: {root_name}: {root_msg}")
            if isinstance(e, httpx.ConnectError):
                # Common low-level causes: socket.gaierror (DNS), ConnectionRefusedError, etc.
                if isinstance(root, socket.gaierror):
                    return DNSResolutionError(f"DNS resolution failed for {endpoint}: {root}")
                if isinstance(root, ConnectionRefusedError):
                    return ConnectFailedError(f"Connection refused to {endpoint}: {root}")
                return ConnectFailedError(f"Failed to connect to {endpoint}: {root_name}: {root_msg}")
            if isinstance(e, httpx.ProxyError):
                return NetworkError(f"Proxy error while contacting {endpoint}: {root_name}: {root_msg}")
            if isinstance(e, httpx.RemoteProtocolError):
                return UpstreamProtocolError(f"Remote protocol error from {endpoint}: {root_name}: {root_msg}")

        if isinstance(e, httpx.HTTPStatusError):
            resp = e.response
            status_code = resp.status_code
            # FastAPI puts detail in JSON body
            try:
                detail = resp.json().get("detail","Unknown Error")
                message = detail if isinstance(detail, str) else str(detail)
            except Exception:
                message = resp.text

        # Else if the exception has a status_code attribute (like FastAPI HTTPException locally)
        elif hasattr(e, "status_code"):
            status_code = getattr(e, "status_code", 500)
            if hasattr(e, "detail"):
                message = e.detail if isinstance(e.detail, str) else str(e.detail)

        # Define mapping of status codes to custom exceptions
        ERROR_MAP = {
            400: ValidationError,
            401: AuthenticationError,
            403: PermissionError,
            404: NotFoundError, 
            405: FileAlreadyExists,
            500: UnknownError,
            501: IntegrityError,
            502: PutChunksError,
            503: GetChunkError,
            1000: NetworkError,
            1001: ConnectFailedError,
            1002: DNSResolutionError,
            1004: RequestTimeoutError,     # mirrors 504
            1005: UpstreamProtocolError,
        }

        error_class = ERROR_MAP.get(status_code, UnknownError)
        return error_class(message)


class ValidationError(MictlanXError):
    """Exception raised when a resource is not found."""
    default_message = "Validation failed"
    status_code = 400

class GetChunkError(MictlanXError):
    """Exception raised when a resource is not found."""
    default_message = "Get chunk failed"
    status_code = 503
class PutChunksError(MictlanXError):
    """Exception raised when a resource is not found."""
    default_message = "Put chunks failed"
    status_code = 502

class IntegrityError(MictlanXError):
    """Exception raised when a resource is not found."""
    default_message = "Integrity check failed"
    status_code = 501

class UnknownError(MictlanXError):
    """Exception raised when a resource is not found."""
    default_message = "Uknown error"
    status_code = 500


class NotFoundError(MictlanXError):
    """Exception raised when a resource is not found."""
    default_message = "Resource not found."
    status_code = 404


class AuthenticationError(MictlanXError):
    """Exception raised for authentication failures."""
    default_message = "Authentication failed."
    status_code = 401

class PermissionError(MictlanXError):
    """Exception raised when a user lacks permissions."""
    default_message = "Permission denied."
    status_code = 403

class FileAlreadyExists(MictlanXError):
    """Exception raised when a user lacks permissions."""
    default_message = "File already exists."
    status_code = 405

class NetworkError(MictlanXError):
    default_message = "Network error"
    status_code = 1000 

class ConnectFailedError(NetworkError):
    default_message = "Connection failed"
    status_code = 1001

class DNSResolutionError(NetworkError):
    default_message = "DNS resolution failed"
    status_code = 1002

class RequestTimeoutError(NetworkError):
    default_message = "Request timed out"
    status_code = 1004

class UpstreamProtocolError(MictlanXError):
    default_message = "Upstream protocol error"
    status_code = 1005