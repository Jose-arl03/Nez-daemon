from __future__ import annotations
from typing import List,Iterable, Union
from mictlanx.services import AsyncRouter,AsyncPeer
from urllib.parse import  parse_qs

class MictlanXURI:
    """
    Parses and builds custom mictlanx:// URIs where paths can be part of a peer's address.
    """

    @staticmethod
    def _parse_one_peer(spec: str, default_port: int) -> dict:
        """Parses a single peer specification string (e.g., 'id@host/path:port')."""
        spec = spec.strip().strip('/')
        if not spec:
            return None

        # Separate the ID (if present) from the location part
        if '@' in spec:
            peer_id, location = spec.split('@', 1)
        else:
            peer_id, location = None, spec

        # Separate the host/path from the port, splitting only on the last colon
        # try:
        host_url, port_str = location.rsplit(':', 1)
        try:
            # Try to convert the port to an integer
            port = int(port_str)
            if port < 1 or port > 65535:
                raise ValueError(f"Port out of range: {port}")
        except Exception:
            # print(f"Invalid port: {port_str}, using default {default_port}")
            raise ValueError(f"Invalid port number: {port_str}")
            
        # except (ValueError, TypeError):
            # Handles cases where there's no colon or the part after it isn't a number
            # host_url = location
            # port     = default_port

        return {'id': peer_id, 'host': host_url, 'port': port}

    @staticmethod
    def _parse_internal(uri: str, default_port: int = 60666) -> tuple[list[dict], dict]:
        """A private helper to handle the core manual parsing logic."""
        if not uri.startswith("mictlanx://"):
            raise ValueError("URI must start with mictlanx://")

        rest = uri[len("mictlanx://"):]

        # Separate the main part of the URI from the query string
        main_part, query_string = (rest.split('?', 1) + [''])[:2]
        query_params = parse_qs(query_string)

        # Split the main part into individual peer specifications
        peer_specs = main_part.split(',')

        parsed_peers = [
            MictlanXURI._parse_one_peer(spec, default_port)
            for spec in peer_specs
        ]
        if len(parsed_peers)==0 or all(p is None for p in parsed_peers):
            raise ValueError("No routers specified in the URI")
        # Filter out any potential empty specs (e.g., from a trailing comma)
        return [p for p in parsed_peers if p], query_params

    @staticmethod
    def parse(uri: str) -> List[AsyncRouter]:
        """Parses a mictlanx:// URI into a list of AsyncRouter objects."""
        parsed_peers, query = MictlanXURI._parse_internal(uri)
        protocol    = query.get('protocol', ['https'])[0]
        api_version = int(query.get('api_version', ['4'])[0])
        http2_str   = query.get('http2', ['0'])[0]
        http2       = http2_str.lower() in ('1', 'true', 'yes',"on")

        return [
            AsyncRouter(
                router_id   = p['id'],
                ip_addr     = p['host'],
                port        = p['port'],
                protocol    = protocol,
                api_version = api_version,
                http2       = http2,
            )
            for p in parsed_peers
        ]

    @staticmethod
    def parse_peers(uri: str) -> List[AsyncPeer]:
        """Parses a mictlanx:// URI into a list of AsyncPeer objects."""
        parsed_peers, query = MictlanXURI._parse_internal(uri)

        protocol = query.get('protocol', ['http'])[0]
        api_version = int(query.get('api_version', ['4'])[0])

        return [
            AsyncPeer(
                peer_id=p['id'],
                ip_addr=p['host'],
                port=p['port'],
                protocol=protocol,
                api_version=api_version,
            )
            for p in parsed_peers
        ]

    @staticmethod
    def build(items: Iterable[Union[AsyncRouter, AsyncPeer]]) -> str:
        """Builds a canonical mictlanx:// URI from a list of router or peer objects."""
        items = list(items)
        if not items:
            return "mictlanx://"

        first_item = items[0]
        
        peer_strings = []
        for item in items:
            item_id = getattr(item, 'router_id', getattr(item, 'peer_id', None))
            location = f"{item.ip_addr}:{item.port}"
            peer_strings.append(f"{item_id}@{location}" if item_id else location)
        main_part = ",".join(peer_strings)

        query_parts = {
            "protocol": first_item.protocol,
            "api_version": first_item.api_version,
        }
        if hasattr(first_item, 'http2'):
            query_parts["http2"] = '1' if first_item.http2 else '0'
        
        query = "&".join(f"{k}={v}" for k, v in query_parts.items())

        return f"mictlanx://{main_part}/?{query}"