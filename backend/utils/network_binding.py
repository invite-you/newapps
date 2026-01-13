"""
Network interface binding utilities.
Bind outgoing HTTP requests to a specific local interface/IP when configured.
"""
from __future__ import annotations

import http.client
import os
import socket
from typing import List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter

try:
    import fcntl
    import struct
except ImportError:  # Non-POSIX environments
    fcntl = None
    struct = None

_BOUND_SOURCE_ADDRESS: Optional[str] = None
_REQUESTS_SESSION: Optional[requests.Session] = None
_REQUESTS_SESSION_SOURCE: Optional[str] = None
_URLOPEN_PATCHED = False
_URLOPEN_SOURCE_ADDRESS: Optional[str] = None
_ORIGINAL_HTTP_CONNECTION = http.client.HTTPConnection
_ORIGINAL_HTTPS_CONNECTION = http.client.HTTPSConnection
_ORIGINAL_HTTP_CONNECT = http.client.HTTPConnection.connect
_ORIGINAL_HTTPS_CONNECT = http.client.HTTPSConnection.connect


def _read_operstate(interface: str) -> Optional[str]:
    path = f"/sys/class/net/{interface}/operstate"
    try:
        with open(path, "r", encoding="ascii") as handle:
            return handle.read().strip()
    except OSError:
        return None


def _get_ipv4_for_interface(interface: str) -> Optional[str]:
    if not fcntl or not struct:
        return None
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        ifreq = struct.pack("256s", interface[:15].encode("ascii", errors="ignore"))
        res = fcntl.ioctl(sock.fileno(), 0x8915, ifreq)  # SIOCGIFADDR
        return socket.inet_ntoa(res[20:24])
    except OSError:
        return None
    finally:
        sock.close()


def list_active_ipv4_interfaces() -> List[Tuple[str, str]]:
    interfaces = []
    try:
        names = sorted(os.listdir("/sys/class/net"))
    except OSError:
        names = [name for _, name in socket.if_nameindex()]

    for name in names:
        if name == "lo":
            continue
        ip = _get_ipv4_for_interface(name)
        if not ip:
            continue
        state = _read_operstate(name)
        if state and state not in ("up", "unknown"):
            continue
        interfaces.append((name, ip))

    return interfaces


def probe_interface_url(interface: str, url: str, timeout: float = 10.0) -> bool:
    source_address = _get_ipv4_for_interface(interface)
    if not source_address:
        return False
    try:
        import urllib3.util.connection as urllib3_connection

        urllib3_connection.allowed_gai_family = lambda: socket.AF_INET
    except Exception:
        pass
    session = requests.Session()
    adapter = _SourceAddressAdapter(source_address)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    try:
        response = session.get(url, timeout=timeout)
        return response.status_code == 200
    except requests.RequestException:
        return False


def select_store_interfaces(interfaces: List[Tuple[str, str]]) -> Tuple[Optional[str], Optional[str]]:
    if len(interfaces) < 2:
        return None, None
    sorted_ifaces = sorted(interfaces, key=lambda item: item[0])
    return sorted_ifaces[0][0], sorted_ifaces[1][0]


class _SourceAddressAdapter(HTTPAdapter):
    def __init__(self, source_address: str, **kwargs):
        self._source_address = source_address
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs["source_address"] = (self._source_address, 0)
        super().init_poolmanager(connections, maxsize, block=block, **pool_kwargs)

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        proxy_kwargs["source_address"] = (self._source_address, 0)
        return super().proxy_manager_for(proxy, **proxy_kwargs)


def _get_or_create_requests_session(source_address: Optional[str]) -> requests.Session:
    global _REQUESTS_SESSION, _REQUESTS_SESSION_SOURCE
    if _REQUESTS_SESSION is not None and _REQUESTS_SESSION_SOURCE == source_address:
        return _REQUESTS_SESSION
    session = requests.Session()
    if source_address:
        adapter = _SourceAddressAdapter(source_address)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
    _REQUESTS_SESSION = session
    _REQUESTS_SESSION_SOURCE = source_address
    return session


def get_requests_session() -> requests.Session:
    return _get_or_create_requests_session(_BOUND_SOURCE_ADDRESS)


def _patch_urllib_source_address(source_address: str) -> None:
    global _URLOPEN_PATCHED, _URLOPEN_SOURCE_ADDRESS
    _URLOPEN_SOURCE_ADDRESS = source_address
    if _URLOPEN_PATCHED:
        return
    def _http_connect(self):
        if _URLOPEN_SOURCE_ADDRESS:
            self.source_address = (_URLOPEN_SOURCE_ADDRESS, 0)
        return _ORIGINAL_HTTP_CONNECT(self)

    def _https_connect(self):
        if _URLOPEN_SOURCE_ADDRESS:
            self.source_address = (_URLOPEN_SOURCE_ADDRESS, 0)
        return _ORIGINAL_HTTPS_CONNECT(self)

    http.client.HTTPConnection.connect = _http_connect
    http.client.HTTPSConnection.connect = _https_connect
    _URLOPEN_PATCHED = True


def configure_network_binding(
    interface: Optional[str] = None,
    source_address: Optional[str] = None,
    logger=None,
) -> Optional[str]:
    global _BOUND_SOURCE_ADDRESS

    resolved_source = source_address
    if not resolved_source:
        env_source = os.getenv("SCRAPER_SOURCE_ADDRESS")
        if env_source:
            resolved_source = env_source

    if not resolved_source:
        resolved_interface = interface or os.getenv("SCRAPER_INTERFACE")
        if resolved_interface:
            resolved_source = _get_ipv4_for_interface(resolved_interface)
            if not resolved_source and logger:
                logger.warning(
                    f"[WARN] interface={resolved_interface} has no IPv4 address; binding skipped"
                )

    if not resolved_source:
        return None

    _BOUND_SOURCE_ADDRESS = resolved_source
    _get_or_create_requests_session(_BOUND_SOURCE_ADDRESS)
    _patch_urllib_source_address(_BOUND_SOURCE_ADDRESS)
    try:
        import urllib3.util.connection as urllib3_connection

        urllib3_connection.allowed_gai_family = lambda: socket.AF_INET
    except Exception:
        if logger:
            logger.warning("[WARN] failed to force IPv4 for urllib3")
    if logger:
        logger.info(f"[INFO] network binding enabled | source_address={_BOUND_SOURCE_ADDRESS}")
    return _BOUND_SOURCE_ADDRESS
