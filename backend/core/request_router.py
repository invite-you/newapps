"""
Domain-aware request router with IP rotation and shared rate limiting.

All HTTP requests should flow through this router to ensure:
- domain-based IP selection (round robin)
- process-shared rate limiting (per domain+IP)
- unified patching for urllib-based libraries (e.g., google_play_scraper)
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import warnings
from dataclasses import dataclass
from io import BytesIO
from typing import Dict, Optional, Tuple, Any
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request as UrlRequest

import requests

from core.ip_manager import IPManager, SourceAddressAdapter

logger = logging.getLogger(__name__)

try:
    from urllib3.exceptions import InsecureRequestWarning
except Exception:  # pragma: no cover - optional dependency
    InsecureRequestWarning = None

DEFAULT_DOMAIN_TEST_ENDPOINTS = {
    "itunes.apple.com": "https://itunes.apple.com/lookup?id=284882215&country=US",
    "apps.apple.com": "https://apps.apple.com/sitemaps_apps_index_app_1.xml",
    "play.google.com": "https://play.google.com/store/apps/details?id=com.whatsapp&hl=en&gl=us",
}

DEFAULT_DOMAIN_RATE_LIMITS = {
    "itunes.apple.com": 20,
}

DEFAULT_RATE_LIMIT_PATH = "/tmp/newapps_request_router_rate_limits.json"
DEFAULT_REQUEST_TIMEOUT = 30


def _parse_rate_limits_env() -> Dict[str, int]:
    raw = os.getenv("REQUEST_ROUTER_RATE_LIMITS", "")
    limits: Dict[str, int] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        domain, value = part.split("=", 1)
        domain = domain.strip().lower()
        try:
            limits[domain] = int(value.strip())
        except ValueError:
            continue
    return limits


def _get_default_rate_limit() -> Optional[int]:
    raw = os.getenv("REQUEST_ROUTER_DEFAULT_RATE_LIMIT_PER_MIN", "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def _merge_rate_limits() -> Tuple[Dict[str, int], Optional[int]]:
    limits = DEFAULT_DOMAIN_RATE_LIMITS.copy()
    limits.update(_parse_rate_limits_env())
    default_limit = _get_default_rate_limit()
    return limits, default_limit


try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None


@dataclass
class RateLimitEntry:
    tokens: float
    last_refill: float


class SharedRateLimiter:
    """
    File-backed token bucket rate limiter shared across processes.

    Limits are per domain+IP and stored in a JSON file protected by flock.
    """

    def __init__(
        self,
        per_domain_limits: Dict[str, int],
        default_limit: Optional[int],
        state_path: Optional[str] = None,
    ) -> None:
        self.per_domain_limits = {k.lower(): v for k, v in per_domain_limits.items()}
        self.default_limit = default_limit
        self.state_path = state_path or os.getenv(
            "REQUEST_ROUTER_RATE_LIMIT_PATH",
            DEFAULT_RATE_LIMIT_PATH,
        )
        self.lock_path = f"{self.state_path}.lock"
        state_dir = os.path.dirname(self.state_path)
        if state_dir:
            os.makedirs(state_dir, exist_ok=True)

    def _get_limit(self, domain: str) -> Optional[int]:
        if not domain:
            return None
        limit = self.per_domain_limits.get(domain)
        if limit is not None:
            return limit
        return self.default_limit

    def _load_state(self) -> Dict[str, RateLimitEntry]:
        try:
            with open(self.state_path, "r", encoding="ascii") as handle:
                raw = json.load(handle)
        except FileNotFoundError:
            return {}
        except (json.JSONDecodeError, OSError):
            return {}
        entries: Dict[str, RateLimitEntry] = {}
        for key, value in raw.items():
            try:
                entries[key] = RateLimitEntry(
                    tokens=float(value["tokens"]),
                    last_refill=float(value["last_refill"]),
                )
            except (KeyError, ValueError, TypeError):
                continue
        return entries

    def _save_state(self, entries: Dict[str, RateLimitEntry]) -> None:
        raw: Dict[str, Dict[str, float]] = {}
        for key, entry in entries.items():
            raw[key] = {
                "tokens": entry.tokens,
                "last_refill": entry.last_refill,
            }
        with open(self.state_path, "w", encoding="ascii") as handle:
            json.dump(raw, handle)

    def _acquire_lock(self):
        handle = open(self.lock_path, "w", encoding="ascii")
        if fcntl:
            fcntl.flock(handle, fcntl.LOCK_EX)
        return handle

    def _release_lock(self, handle) -> None:
        if fcntl:
            fcntl.flock(handle, fcntl.LOCK_UN)
        handle.close()

    def acquire(self, domain: str, ip: str) -> None:
        limit = self._get_limit(domain)
        if not limit or limit <= 0:
            return

        key = f"{domain}|{ip}"
        while True:
            lock_handle = self._acquire_lock()
            try:
                entries = self._load_state()
                now = time.time()
                entry = entries.get(key)
                if not entry:
                    entry = RateLimitEntry(tokens=float(limit), last_refill=now)

                refill_rate = float(limit) / 60.0
                elapsed = max(0.0, now - entry.last_refill)
                entry.tokens = min(float(limit), entry.tokens + elapsed * refill_rate)
                entry.last_refill = now

                if entry.tokens >= 1.0:
                    entry.tokens -= 1.0
                    entries[key] = entry
                    self._save_state(entries)
                    return

                needed = 1.0 - entry.tokens
                wait_seconds = max(needed / refill_rate, 0.05)
                entries[key] = entry
                self._save_state(entries)
            finally:
                self._release_lock(lock_handle)

            time.sleep(wait_seconds)


class RequestRouter:
    """
    Routes HTTP requests by domain, rotating IPs and enforcing shared rate limits.
    """

    def __init__(
        self,
        domain_endpoints: Optional[Dict[str, str]] = None,
        rate_limiter: Optional[SharedRateLimiter] = None,
    ) -> None:
        endpoints = domain_endpoints or DEFAULT_DOMAIN_TEST_ENDPOINTS
        self.ip_manager = IPManager(test_endpoints=endpoints)
        limits, default_limit = _merge_rate_limits()
        self.rate_limiter = rate_limiter or SharedRateLimiter(
            limits,
            default_limit,
        )
        self._initialized = False
        self._init_lock = threading.Lock()
        self._failed_ips: Dict[str, list[str]] = {}
        self._session_cache: Dict[Optional[str], requests.Session] = {}
        self._session_lock = threading.Lock()
        self._domain_rotation_index: Dict[str, int] = {}

    def ensure_initialized(self) -> None:
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            self.ip_manager.initialize()
            self._initialized = True

    @staticmethod
    def _extract_domain(url: str) -> str:
        parsed = urlparse(url)
        host = parsed.hostname or parsed.netloc
        if not host:
            return ""
        return host.lower()

    def _get_session(self, ip: Optional[str]) -> requests.Session:
        with self._session_lock:
            session = self._session_cache.get(ip)
            if session:
                return session
            session = requests.Session()
            if ip:
                adapter = SourceAddressAdapter(ip)
                session.mount("http://", adapter)
                session.mount("https://", adapter)
            self._session_cache[ip] = session
            return session

    def _next_ip(self, domain: str) -> Optional[str]:
        exclude = self._failed_ips.get(domain, [])
        if domain in self.ip_manager.store_ip_map:
            return self.ip_manager.get_next_ip_for_store(domain, exclude=exclude)

        candidates = [
            ip for ip in self.ip_manager.available_ips
            if ip not in exclude
        ]
        if not candidates:
            return None
        current_index = self._domain_rotation_index.get(domain, 0)
        if current_index >= len(candidates):
            current_index = 0
        selected = candidates[current_index]
        self._domain_rotation_index[domain] = (current_index + 1) % len(candidates)
        return selected

    def request(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Any] = None,
        timeout: Optional[float] = None,
        allow_redirects: bool = True,
        verify: Optional[bool] = None,
    ) -> requests.Response:
        self.ensure_initialized()
        domain = self._extract_domain(url)
        ip = self._next_ip(domain) if domain else None

        if ip:
            self.rate_limiter.acquire(domain, ip)

        response = self._do_request(
            url=url,
            method=method,
            headers=headers,
            data=data,
            timeout=timeout,
            allow_redirects=allow_redirects,
            verify=verify,
            ip=ip,
            domain=domain,
        )

        if response.status_code == 403 and ip:
            self._failed_ips.setdefault(domain, []).append(ip)
            alt_ip = self._next_ip(domain)
            if alt_ip and alt_ip != ip:
                self.rate_limiter.acquire(domain, alt_ip)
                response = self._do_request(
                    url=url,
                    method=method,
                    headers=headers,
                    data=data,
                    timeout=timeout,
                    allow_redirects=allow_redirects,
                    verify=verify,
                    ip=alt_ip,
                    domain=domain,
                )

        return response

    def _do_request(
        self,
        url: str,
        method: str,
        headers: Optional[Dict[str, str]],
        data: Optional[Any],
        timeout: Optional[float],
        allow_redirects: bool,
        verify: Optional[bool],
        ip: Optional[str],
        domain: str,
    ) -> requests.Response:
        session = self._get_session(ip)
        response = session.request(
            method=method,
            url=url,
            headers=headers,
            data=data,
            timeout=timeout or DEFAULT_REQUEST_TIMEOUT,
            allow_redirects=allow_redirects,
            verify=True if verify is None else verify,
        )
        response.used_ip = ip
        response.requested_domain = domain
        return response

    def urlopen(self, obj: Any, timeout: Optional[float] = None, verify: Optional[bool] = None) -> BytesIO:
        if isinstance(obj, UrlRequest):
            url = obj.full_url
            data = obj.data
            headers = dict(obj.header_items())
            method = obj.get_method()
        else:
            url = str(obj)
            data = None
            headers = None
            method = "GET"

        response = self.request(
            url=url,
            method=method,
            headers=headers,
            data=data,
            timeout=timeout,
            verify=verify,
        )

        if response.status_code >= 400:
            raise HTTPError(
                url,
                response.status_code,
                response.reason or f"HTTP {response.status_code}",
                response.headers,
                BytesIO(response.content),
            )
        return BytesIO(response.content)

    def patch_google_play_scraper(self) -> bool:
        try:
            import google_play_scraper.utils.request as gps_request
        except Exception:
            return False

        if getattr(gps_request, "_REQUEST_ROUTER_PATCHED", False):
            return True

        if InsecureRequestWarning is not None:
            warnings.filterwarnings("ignore", category=InsecureRequestWarning)

        def _router_urlopen(obj):
            return self.urlopen(obj, verify=False)

        gps_request.urlopen = _router_urlopen
        gps_request._REQUEST_ROUTER_PATCHED = True
        logger.info("google_play_scraper patched to use RequestRouter")
        return True


_REQUEST_ROUTER: Optional[RequestRouter] = None
_ROUTER_LOCK = threading.Lock()


def get_request_router() -> RequestRouter:
    global _REQUEST_ROUTER
    if _REQUEST_ROUTER is not None:
        return _REQUEST_ROUTER
    with _ROUTER_LOCK:
        if _REQUEST_ROUTER is None:
            _REQUEST_ROUTER = RequestRouter()
            _REQUEST_ROUTER.patch_google_play_scraper()
        return _REQUEST_ROUTER


def ensure_request_router_patched() -> None:
    router = get_request_router()
    router.patch_google_play_scraper()
