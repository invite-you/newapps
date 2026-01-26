"""
스토어별 IP 바인딩 HTTP 클라이언트

IPManager와 연동하여 스토어에 적합한 IP로 요청을 보냅니다.
에러 발생 시 자동으로 분류하고 재시도합니다.

주요 기능:
- 스토어별 IP 자동 바인딩
- HTTP 에러 코드 분류 (IP_BLOCKED, RATE_LIMITED, SERVER_ERROR 등)
- Rate Limit 백오프 재시도 (5초 → 10초 → 30초)
- IP 차단 시 대체 IP로 자동 전환

사용 예시:
    from core.ip_manager import IPManager
    from core.http_client import StoreHttpClient

    # 초기화
    ip_manager = IPManager()
    ip_manager.initialize()

    client = StoreHttpClient(ip_manager)

    # 요청 실행
    result = client.request(url, 'app_store')

    if result.success:
        data = result.data
        print(f"사용된 IP: {result.used_ip}")
    else:
        print(f"에러: {result.error_code} - {result.error_detail}")
"""
import time
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from core.ip_manager import IPManager, SourceAddressAdapter


# =============================================================================
# 로거 설정
# =============================================================================

logger = logging.getLogger(__name__)


# =============================================================================
# 설정 상수
# =============================================================================

# User-Agent 헤더 (봇 차단 방지)
# 일반적인 브라우저처럼 보이게 설정
DEFAULT_USER_AGENT = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/120.0.0.0 Safari/537.36'
)

# 기본 타임아웃 (초)
# 리뷰 API는 응답이 클 수 있으므로 여유있게 설정
DEFAULT_TIMEOUT = 30

# Rate Limit 백오프 설정
# 429 응답 시 대기 시간 (초)
# 총 3회 재시도: 5초, 10초, 30초 대기 후 시도
RATE_LIMIT_DELAYS = [5, 10, 30]
MAX_RATE_LIMIT_RETRIES = 3


# =============================================================================
# 에러 코드
# =============================================================================

class HttpErrorCode:
    """
    HTTP 에러 코드 상수

    database.review_collection_db.ErrorCode와 동일한 값을 사용합니다.
    수집 상태 기록 시 이 코드가 그대로 DB에 저장됩니다.

    Attributes:
        IP_BLOCKED: IP 차단됨 (HTTP 403)
            - 다른 IP로 즉시 재시도
            - 모든 IP 실패 시 다음 실행에서 재시도

        RATE_LIMITED: 요청 과다 (HTTP 429)
            - 백오프 재시도 (5초 → 10초 → 30초)
            - 3회 재시도 후에도 실패하면 다음 실행에서 재시도

        NETWORK_ERROR: 네트워크 오류
            - Timeout, Connection Error, DNS 오류 등
            - 다음 실행에서 재시도

        SERVER_ERROR: 서버 오류 (HTTP 5xx)
            - 일시적인 서버 문제
            - 다음 실행에서 재시도

        PARSE_ERROR: 응답 파싱 실패
            - JSON 파싱 오류 등
            - 다음 실행에서 재시도

        NO_AVAILABLE_IP: 사용 가능한 IP 없음
            - 모든 IP가 차단되었거나 동작하지 않음
            - 다음 실행에서 IP 재테스트 후 재시도

        SUCCESS: 성공
    """
    IP_BLOCKED = "IP_BLOCKED"
    RATE_LIMITED = "RATE_LIMITED"
    NETWORK_ERROR = "NETWORK_ERROR"
    SERVER_ERROR = "SERVER_ERROR"
    PARSE_ERROR = "PARSE_ERROR"
    NO_AVAILABLE_IP = "NO_AVAILABLE_IP"
    SUCCESS = "SUCCESS"


# =============================================================================
# 결과 데이터 클래스
# =============================================================================

@dataclass
class HttpResult:
    """
    HTTP 요청 결과

    성공/실패 여부와 상세 정보를 담는 불변 객체입니다.

    Attributes:
        success: 성공 여부
            True = 정상 응답 (200-299)
            False = 에러 발생

        data: 응답 데이터 (성공 시)
            parse_json=True면 dict, 아니면 str

        status_code: HTTP 상태 코드 (있는 경우)

        error_code: 에러 코드 (실패 시)
            HttpErrorCode 상수 값

        error_detail: 상세 에러 메시지 (실패 시)
            디버깅/로깅용 상세 정보

        used_ip: 요청에 사용된 IP 주소

    Example:
        >>> result = client.request(url, 'app_store')
        >>> if result.success:
        ...     reviews = result.data['feed']['entry']
        ... else:
        ...     logger.error(f"{result.error_code}: {result.error_detail}")
    """
    success: bool
    data: Optional[Any] = None
    status_code: Optional[int] = None
    error_code: Optional[str] = None
    error_detail: Optional[str] = None
    used_ip: Optional[str] = None


# =============================================================================
# StoreHttpClient 클래스
# =============================================================================

class StoreHttpClient:
    """
    스토어별 IP 바인딩 HTTP 클라이언트 (IP 로테이션 지원)

    IPManager와 연동하여 스토어에 적합한 IP로 요청을 보냅니다.
    에러 발생 시 자동으로 분류하고 재시도합니다.

    IP 로테이션:
    - use_rotation=True (기본값): 요청마다 다른 IP를 라운드 로빈으로 사용
    - IP 4개가 있으면 1→2→3→4→1→2→... 순서로 순환
    - 대상 서버의 차단 위험을 분산시켜 안정적인 수집 가능

    재시도 전략:
    - IP_BLOCKED (403): 해당 IP 제외하고 다음 IP로 즉시 재시도
    - RATE_LIMITED (429): 백오프 재시도 (5초 → 10초 → 30초, 총 3회)
    - 그 외: 재시도 없음 (다음 실행에서 처리)

    Attributes:
        ip_manager: IP 관리자 인스턴스
        user_agent: User-Agent 헤더 값
        timeout: 요청 타임아웃 (초)
        use_rotation: IP 로테이션 사용 여부
        failed_ips: 현재 세션에서 실패한 IP 목록 (스토어별)
            세션 종료 시 reset_failed_ips()로 초기화 가능

    Example:
        >>> ip_manager = IPManager()
        >>> ip_manager.initialize()

        # IP 로테이션 활성화 (기본값)
        >>> client = StoreHttpClient(ip_manager, use_rotation=True)
        >>> result1 = client.request(url1, 'app_store')  # IP 1번 사용
        >>> result2 = client.request(url2, 'app_store')  # IP 2번 사용
        >>> result3 = client.request(url3, 'app_store')  # IP 3번 사용
        >>> result4 = client.request(url4, 'app_store')  # IP 4번 사용
        >>> result5 = client.request(url5, 'app_store')  # IP 1번 사용 (순환)

        >>> if result.success:
        ...     reviews = result.data
        ...     print(f"수집 완료, IP: {result.used_ip}")
        ... else:
        ...     print(f"실패: {result.error_code}")

        # 로테이션 통계 확인
        >>> stats = client.get_rotation_stats()
        >>> print(stats)
    """

    def __init__(
        self,
        ip_manager: IPManager,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: int = DEFAULT_TIMEOUT,
        use_rotation: bool = True,
    ):
        """
        Args:
            ip_manager: 초기화된 IPManager 인스턴스
                initialize()가 호출된 상태여야 함
            user_agent: User-Agent 헤더 값
            timeout: 요청 타임아웃 (초)
            use_rotation: IP 로테이션 사용 여부
                True (기본값): 요청마다 다른 IP를 순환 사용 (라운드 로빈)
                    - 차단 방지를 위해 권장
                    - IP 4개면 1,2,3,4,1,2,3,4... 순서로 사용
                False: 항상 첫 번째 가용 IP 사용
                    - 기존 동작과 호환
        """
        self.ip_manager = ip_manager
        self.user_agent = user_agent
        self.timeout = timeout

        # 현재 세션에서 실패한 IP 목록
        # 스토어별로 관리하여 IP_BLOCKED 시 대체 IP 사용
        # {platform: [failed_ips]}
        self.failed_ips: Dict[str, List[str]] = {}

        # IP 로테이션 사용 여부
        # True: 요청마다 다른 IP 사용 (라운드 로빈)
        # False: 항상 첫 번째 가용 IP 사용 (기존 동작)
        self.use_rotation = use_rotation

    def request(
        self,
        url: str,
        platform: str,
        method: str = 'GET',
        headers: Optional[Dict[str, str]] = None,
        parse_json: bool = True,
    ) -> HttpResult:
        """
        HTTP 요청을 실행합니다.

        스토어에 적합한 IP를 선택하고, 실패 시 자동으로 재시도합니다.

        재시도 동작:
        1. IP_BLOCKED (403): 해당 IP를 제외하고 다른 IP로 즉시 재시도
        2. RATE_LIMITED (429): 백오프 대기 후 재시도 (최대 3회)
        3. 그 외: 재시도 없이 결과 반환

        Args:
            url: 요청 URL
            platform: 스토어 ('app_store' 또는 'play_store')
            method: HTTP 메서드 (기본: GET)
            headers: 추가 헤더 (User-Agent는 자동 설정됨)
            parse_json: JSON 파싱 여부 (기본: True)

        Returns:
            HttpResult 객체

        Example:
            >>> result = client.request(
            ...     'https://itunes.apple.com/us/rss/customerreviews/...',
            ...     'app_store'
            ... )
            >>> if result.success:
            ...     entries = result.data['feed']['entry']
        """
        # 사용할 IP 선택 (이전에 실패한 IP 제외)
        # use_rotation이 True이면 라운드 로빈으로 IP 순환 사용
        # use_rotation이 False이면 항상 첫 번째 가용 IP 사용
        if self.use_rotation:
            ip = self.ip_manager.get_next_ip_for_store(
                platform,
                exclude=self.failed_ips.get(platform, [])
            )
        else:
            ip = self.ip_manager.get_ip_for_store(
                platform,
                exclude=self.failed_ips.get(platform, [])
            )

        if not ip:
            logger.warning(f"{platform}: 사용 가능한 IP 없음")
            return HttpResult(
                success=False,
                error_code=HttpErrorCode.NO_AVAILABLE_IP,
                error_detail=f"사용 가능한 IP 없음 ({platform})"
            )

        # 요청 실행
        result = self._do_request(url, ip, method, headers, parse_json)

        # IP_BLOCKED: 다른 IP로 재시도
        if result.error_code == HttpErrorCode.IP_BLOCKED:
            # 현재 IP를 실패 목록에 추가
            self.failed_ips.setdefault(platform, []).append(ip)
            logger.warning(f"IP {ip} 차단됨 ({platform}), 대체 IP 시도...")

            # 대체 IP로 재귀 호출 (exclude에 실패 IP 포함됨)
            return self.request(url, platform, method, headers, parse_json)

        # RATE_LIMITED: 백오프 재시도
        if result.error_code == HttpErrorCode.RATE_LIMITED:
            retry_result = self._retry_with_backoff(url, ip, method, headers, parse_json)
            if retry_result:
                return retry_result
            # 백오프 재시도도 실패하면 원래 결과 반환

        return result

    def _do_request(
        self,
        url: str,
        ip: str,
        method: str,
        headers: Optional[Dict[str, str]],
        parse_json: bool,
    ) -> HttpResult:
        """
        실제 HTTP 요청을 실행합니다.

        소스 IP를 바인딩하여 요청하고, 응답을 처리합니다.

        Args:
            url: 요청 URL
            ip: 사용할 소스 IP
            method: HTTP 메서드
            headers: 추가 헤더
            parse_json: JSON 파싱 여부

        Returns:
            HttpResult 객체
        """
        try:
            # 세션 생성 및 IP 바인딩
            session = requests.Session()
            adapter = SourceAddressAdapter(ip)
            session.mount('http://', adapter)
            session.mount('https://', adapter)

            # 헤더 설정
            req_headers = {'User-Agent': self.user_agent}
            if headers:
                req_headers.update(headers)

            # 요청 실행
            response = session.request(
                method=method,
                url=url,
                headers=req_headers,
                timeout=self.timeout,
            )

            # 응답 처리
            return self._handle_response(response, ip, parse_json)

        except requests.exceptions.Timeout:
            logger.debug(f"타임아웃: {url} (IP: {ip})")
            return HttpResult(
                success=False,
                error_code=HttpErrorCode.NETWORK_ERROR,
                error_detail="요청 타임아웃",
                used_ip=ip,
            )
        except requests.exceptions.ConnectionError as e:
            logger.debug(f"연결 오류: {url} (IP: {ip}) - {e}")
            return HttpResult(
                success=False,
                error_code=HttpErrorCode.NETWORK_ERROR,
                error_detail=f"연결 오류: {str(e)[:100]}",
                used_ip=ip,
            )
        except Exception as e:
            logger.error(f"예외 발생: {url} (IP: {ip}) - {e}")
            return HttpResult(
                success=False,
                error_code=HttpErrorCode.NETWORK_ERROR,
                error_detail=f"예외: {str(e)[:100]}",
                used_ip=ip,
            )

    def _handle_response(
        self,
        response: requests.Response,
        ip: str,
        parse_json: bool,
    ) -> HttpResult:
        """
        HTTP 응답을 처리하고 결과를 반환합니다.

        상태 코드별 에러 분류:
        - 200-299: 성공
        - 403: IP_BLOCKED
        - 429: RATE_LIMITED
        - 5xx: SERVER_ERROR
        - 그 외: NETWORK_ERROR

        Args:
            response: requests.Response 객체
            ip: 사용된 IP
            parse_json: JSON 파싱 여부

        Returns:
            HttpResult 객체
        """
        status_code = response.status_code

        # 성공 (200-299)
        if 200 <= status_code < 300:
            try:
                if parse_json:
                    data = response.json()
                else:
                    data = response.text

                return HttpResult(
                    success=True,
                    data=data,
                    status_code=status_code,
                    used_ip=ip,
                )
            except ValueError as e:
                # JSON 파싱 실패
                return HttpResult(
                    success=False,
                    status_code=status_code,
                    error_code=HttpErrorCode.PARSE_ERROR,
                    error_detail=f"JSON 파싱 오류: {e}",
                    used_ip=ip,
                )

        # 403 Forbidden (IP 차단)
        if status_code == 403:
            return HttpResult(
                success=False,
                status_code=status_code,
                error_code=HttpErrorCode.IP_BLOCKED,
                error_detail="HTTP 403 Forbidden",
                used_ip=ip,
            )

        # 429 Too Many Requests (Rate Limit)
        if status_code == 429:
            return HttpResult(
                success=False,
                status_code=status_code,
                error_code=HttpErrorCode.RATE_LIMITED,
                error_detail="HTTP 429 Too Many Requests",
                used_ip=ip,
            )

        # 5xx Server Error
        if 500 <= status_code < 600:
            return HttpResult(
                success=False,
                status_code=status_code,
                error_code=HttpErrorCode.SERVER_ERROR,
                error_detail=f"HTTP {status_code}",
                used_ip=ip,
            )

        # 그 외 에러 (400, 401, 404 등)
        return HttpResult(
            success=False,
            status_code=status_code,
            error_code=HttpErrorCode.NETWORK_ERROR,
            error_detail=f"HTTP {status_code}",
            used_ip=ip,
        )

    def _retry_with_backoff(
        self,
        url: str,
        ip: str,
        method: str,
        headers: Optional[Dict[str, str]],
        parse_json: bool,
    ) -> Optional[HttpResult]:
        """
        Rate Limit 발생 시 백오프 재시도합니다.

        RATE_LIMIT_DELAYS에 정의된 간격으로 최대 3회 재시도합니다.
        (5초 → 10초 → 30초)

        Args:
            url: 요청 URL
            ip: 사용할 IP
            method: HTTP 메서드
            headers: 추가 헤더
            parse_json: JSON 파싱 여부

        Returns:
            성공 시 HttpResult
            모든 재시도 실패 시 None
        """
        for i, delay in enumerate(RATE_LIMIT_DELAYS):
            logger.info(
                f"Rate Limited, {delay}초 대기 후 재시도 "
                f"({i + 1}/{MAX_RATE_LIMIT_RETRIES})..."
            )
            time.sleep(delay)

            result = self._do_request(url, ip, method, headers, parse_json)

            if result.success:
                logger.info(f"Rate Limit 재시도 성공 ({i + 1}회)")
                return result

            if result.error_code != HttpErrorCode.RATE_LIMITED:
                # Rate Limit이 아닌 다른 에러면 중단
                logger.debug(f"Rate Limit 재시도 중 다른 에러 발생: {result.error_code}")
                return result

        logger.warning(f"Rate Limit 재시도 {MAX_RATE_LIMIT_RETRIES}회 모두 실패: {url}")
        return None

    def reset_failed_ips(self, platform: Optional[str] = None) -> None:
        """
        실패한 IP 목록을 초기화합니다.

        새 수집 세션 시작 시 호출하여 이전 세션의 실패 기록을 초기화합니다.
        일시적으로 차단되었던 IP가 다시 동작할 수 있기 때문입니다.

        Args:
            platform: 특정 스토어만 초기화 (None이면 전체)

        Example:
            >>> client.reset_failed_ips()  # 전체 초기화
            >>> client.reset_failed_ips('app_store')  # app_store만 초기화
        """
        if platform:
            self.failed_ips[platform] = []
            logger.debug(f"{platform} 실패 IP 목록 초기화")
        else:
            self.failed_ips = {}
            logger.debug("모든 스토어 실패 IP 목록 초기화")

    def get_failed_ips(self, platform: str) -> List[str]:
        """
        현재 세션에서 실패한 IP 목록을 반환합니다.

        Args:
            platform: 스토어

        Returns:
            실패한 IP 목록
        """
        return self.failed_ips.get(platform, []).copy()

    def set_rotation(self, enabled: bool) -> None:
        """
        IP 로테이션 사용 여부를 설정합니다.

        Args:
            enabled: True = 로테이션 활성화, False = 비활성화

        Example:
            >>> client.set_rotation(True)   # 요청마다 IP 순환
            >>> client.set_rotation(False)  # 항상 첫 번째 IP 사용
        """
        self.use_rotation = enabled
        logger.info(f"IP 로테이션: {'활성화' if enabled else '비활성화'}")

    def get_rotation_stats(self) -> Dict[str, Any]:
        """
        IP 로테이션 통계를 반환합니다.

        IPManager의 통계와 현재 클라이언트 상태를 포함합니다.

        Returns:
            로테이션 및 실패 IP 통계
            예: {
                'rotation_enabled': True,
                'stores': {
                    'app_store': {
                        'current_index': 2,
                        'total_ips': 4,
                        'requests': 150,
                        'ips': ['1.1.1.1', '2.2.2.2', ...],
                        'failed_ips': ['3.3.3.3']
                    },
                    ...
                }
            }
        """
        ip_stats = self.ip_manager.get_rotation_stats()

        # 실패 IP 정보 추가
        for store in ip_stats:
            ip_stats[store]['failed_ips'] = self.failed_ips.get(store, [])

        return {
            'rotation_enabled': self.use_rotation,
            'stores': ip_stats,
        }

    def reset_all(self, platform: Optional[str] = None) -> None:
        """
        실패 IP 목록과 로테이션 인덱스를 모두 초기화합니다.

        새로운 수집 세션 시작 시 호출하여 이전 상태를 초기화합니다.

        Args:
            platform: 특정 스토어만 초기화 (None이면 전체)

        Example:
            >>> client.reset_all()  # 전체 초기화
            >>> client.reset_all('app_store')  # app_store만 초기화
        """
        self.reset_failed_ips(platform)
        self.ip_manager.reset_rotation(platform)
        logger.info(f"{'전체' if not platform else platform} 상태 초기화 완료")
