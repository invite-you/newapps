"""
IP 자동 감지 및 스토어별 할당 관리

서버의 모든 외부 IP를 감지하고, 각 스토어 API에 대해
접근 가능한 IP를 테스트하여 매핑합니다.

주요 기능:
- 서버의 모든 외부 IPv4 자동 감지 (hostname -I)
- 각 스토어 엔드포인트에 IP별 접근 테스트
- 동작하는 IP를 스토어별로 매핑
- 실패한 IP 제외하고 대체 IP 제공

사용 예시:
    from core.ip_manager import IPManager

    # 초기화 (파이프라인 시작 시 1회)
    ip_manager = IPManager()
    store_ips = ip_manager.initialize()
    # {'app_store': ['172.31.40.115'], 'play_store': ['172.31.47.39', '172.31.40.115']}

    # 스토어용 IP 얻기
    ip = ip_manager.get_ip_for_store('app_store')
    # '172.31.40.115'

    # 실패한 IP 제외하고 대체 IP 얻기
    alt_ip = ip_manager.get_ip_for_store('app_store', exclude=['172.31.40.115'])
    # None (다른 IP 없음)
"""
import socket
import subprocess
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from urllib3.util.connection import create_connection

import requests
from requests.adapters import HTTPAdapter


# =============================================================================
# 로거 설정
# =============================================================================

logger = logging.getLogger(__name__)


# =============================================================================
# 설정 상수
# =============================================================================

# 스토어별 테스트 엔드포인트
# 각 스토어의 접근성을 테스트하기 위한 URL
# App Store: RSS API (리뷰 수집에 사용하는 것과 동일)
# Play Store: 앱 상세 페이지 (google-play-scraper가 접근하는 것과 동일)
TEST_ENDPOINTS = {
    'app_store': 'https://itunes.apple.com/us/rss/customerreviews/page=1/id=284882215/sortBy=mostRecent/json',
    'play_store': 'https://play.google.com/store/apps/details?id=com.whatsapp&hl=en&gl=us',
}

# 테스트 타임아웃 (초)
# 너무 짧으면 정상 IP도 실패로 판정될 수 있음
TEST_TIMEOUT = 10

# User-Agent 헤더
# 봇 차단을 방지하기 위해 브라우저처럼 보이게 설정
DEFAULT_USER_AGENT = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/120.0.0.0 Safari/537.36'
)


# =============================================================================
# 데이터 클래스
# =============================================================================

@dataclass
class IPTestResult:
    """
    IP 테스트 결과

    각 IP를 스토어 엔드포인트에 테스트한 결과를 저장합니다.

    Attributes:
        ip: 테스트한 IP 주소 (예: '172.31.40.115')
        platform: 테스트한 스토어 ('app_store' 또는 'play_store')
        is_working: 정상 동작 여부
            True = HTTP 200 + 유효한 콘텐츠
            False = 에러 발생 또는 차단
        status_code: HTTP 상태 코드 (있는 경우)
        error: 에러 메시지 (실패한 경우)
        tested_at: 테스트 시간
    """
    ip: str
    platform: str
    is_working: bool
    status_code: Optional[int] = None
    error: Optional[str] = None
    tested_at: datetime = field(default_factory=datetime.now)


# =============================================================================
# 소스 IP 바인딩 어댑터
# =============================================================================

class SourceAddressAdapter(HTTPAdapter):
    """
    특정 소스 IP로 바인딩하는 HTTP 어댑터

    requests 라이브러리에서 특정 네트워크 인터페이스(IP)를
    사용하도록 강제합니다. 서버에 여러 IP가 있을 때
    특정 IP로 요청을 보내야 하는 경우 사용합니다.

    동작 원리:
    - urllib3의 connection pool에 source_address 설정
    - 소켓 생성 시 해당 IP에 바인딩
    - 모든 요청이 해당 IP에서 나가게 됨

    Attributes:
        source_address: 소스 IP 주소

    Example:
        >>> session = requests.Session()
        >>> adapter = SourceAddressAdapter('172.31.40.115')
        >>> session.mount('https://', adapter)
        >>> session.get('https://example.com')  # 172.31.40.115에서 요청
    """

    def __init__(self, source_address: str, *args, **kwargs):
        """
        Args:
            source_address: 소스 IP 주소 (예: '172.31.40.115')
        """
        # 소스 IP를 (IP, PORT) 튜플로 저장
        # PORT 0은 시스템이 자동으로 할당하게 함
        self.source_address = source_address
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        """
        소스 주소를 설정하여 풀 매니저 초기화

        urllib3 PoolManager가 연결을 만들 때 사용할
        source_address를 설정합니다.
        """
        kwargs['source_address'] = (self.source_address, 0)
        super().init_poolmanager(*args, **kwargs)


# =============================================================================
# IPManager 클래스
# =============================================================================

class IPManager:
    """
    서버 IP 자동 감지 및 스토어별 할당 관리

    주요 기능:
    1. 서버의 모든 외부 IP 자동 감지 (hostname -I 명령 사용)
    2. 각 스토어 엔드포인트에 IP별 접근 테스트
    3. 동작하는 IP를 스토어별로 매핑
    4. 수집 시 적절한 IP 제공 (실패한 IP 제외)

    사용 시나리오:
    - 파이프라인 시작 시 initialize() 호출
    - 각 요청마다 get_ip_for_store()로 IP 획득
    - IP 차단 시 exclude 파라미터로 해당 IP 제외

    Attributes:
        test_endpoints: 스토어별 테스트 URL
        available_ips: 서버에서 감지된 모든 IP 목록
        store_ip_map: 스토어별 동작하는 IP 목록
        test_results: 모든 테스트 결과 기록
        last_initialized_at: 마지막 초기화 시간

    Example:
        >>> manager = IPManager()
        >>> store_ips = manager.initialize()
        >>> print(store_ips)
        {'app_store': ['172.31.40.115'], 'play_store': ['172.31.47.39', '172.31.40.115']}

        >>> ip = manager.get_ip_for_store('app_store')
        >>> print(ip)
        '172.31.40.115'

        >>> # IP가 차단된 경우 대체 IP 요청
        >>> alt_ip = manager.get_ip_for_store('app_store', exclude=['172.31.40.115'])
        >>> print(alt_ip)  # None (다른 IP 없음)
    """

    def __init__(self, test_endpoints: Optional[Dict[str, str]] = None):
        """
        IPManager 초기화

        Args:
            test_endpoints: 스토어별 테스트 URL (기본값 사용 시 None)
                기본값: TEST_ENDPOINTS 상수
                커스텀 엔드포인트 사용 시 {'store_name': 'url'} 형식으로 전달
        """
        self.test_endpoints = test_endpoints or TEST_ENDPOINTS

        # 감지된 IP 목록 (initialize() 호출 후 채워짐)
        self.available_ips: List[str] = []

        # 스토어별 동작하는 IP 목록 (initialize() 호출 후 채워짐)
        # 예: {'app_store': ['172.31.40.115'], 'play_store': ['172.31.47.39']}
        self.store_ip_map: Dict[str, List[str]] = {}

        # 모든 테스트 결과 기록 (디버깅/로깅용)
        self.test_results: List[IPTestResult] = []

        # 마지막 초기화 시간
        self.last_initialized_at: Optional[datetime] = None

        # =================================================================
        # IP 로테이션 관련 상태
        # =================================================================

        # 스토어별 현재 로테이션 인덱스
        # 순환(round-robin) 방식으로 IP를 순차 사용하기 위해 관리
        # {platform: current_index}
        self._rotation_index: Dict[str, int] = {}

        # 스토어별 요청 카운트 (모니터링용)
        # {platform: request_count}
        self._request_count: Dict[str, int] = {}

    def discover_ips(self) -> List[str]:
        """
        서버에서 사용 가능한 모든 외부 IP를 감지합니다.

        `hostname -I` 명령어를 사용하여 IP를 가져옵니다.
        로컬호스트(127.x.x.x)와 IPv6 주소는 제외합니다.

        Returns:
            외부 IPv4 주소 목록 (예: ['172.31.47.39', '172.31.40.115'])

        Note:
            이 함수는 Linux/Unix 시스템에서만 동작합니다.
            Windows에서는 다른 방법이 필요합니다.

        Example:
            >>> manager = IPManager()
            >>> ips = manager.discover_ips()
            >>> print(ips)
            ['172.31.47.39', '172.31.40.115']
        """
        try:
            # hostname -I: 모든 네트워크 인터페이스의 IP 출력
            result = subprocess.run(
                ['hostname', '-I'],
                capture_output=True,
                text=True,
                timeout=5
            )

            if result.returncode != 0:
                logger.warning(f"hostname -I 명령 실패: {result.stderr}")
                return []

            # 공백으로 분리 (hostname -I는 공백으로 구분된 IP 목록 출력)
            all_ips = result.stdout.strip().split()

            # IPv4만 필터링 (IPv6 및 로컬호스트 제외)
            external_ips = [
                ip for ip in all_ips
                if self._is_valid_external_ipv4(ip)
            ]

            # 결과 저장
            self.available_ips = external_ips

            logger.info(f"감지된 IP: {external_ips}")

            return external_ips

        except subprocess.TimeoutExpired:
            logger.error("hostname 명령 타임아웃 (5초)")
            return []
        except FileNotFoundError:
            logger.error("hostname 명령을 찾을 수 없음")
            return []
        except Exception as e:
            logger.error(f"IP 감지 실패: {e}")
            return []

    def _is_valid_external_ipv4(self, ip: str) -> bool:
        """
        유효한 외부 IPv4 주소인지 확인합니다.

        제외되는 주소:
        - 127.x.x.x (로컬호스트)
        - IPv6 주소 (':' 포함)
        - 잘못된 형식

        Args:
            ip: 확인할 IP 주소 문자열

        Returns:
            True: 유효한 외부 IPv4
            False: 제외 대상 또는 잘못된 형식
        """
        # IPv6 제외 (':' 포함)
        if ':' in ip:
            return False

        # 로컬호스트 제외
        if ip.startswith('127.'):
            return False

        # IPv4 형식 검증 (4개의 0-255 옥텟)
        try:
            parts = ip.split('.')
            if len(parts) != 4:
                return False
            return all(0 <= int(part) <= 255 for part in parts)
        except ValueError:
            return False

    def test_ip_for_store(self, ip: str, store: str) -> IPTestResult:
        """
        특정 IP가 해당 스토어에 접근 가능한지 테스트합니다.

        해당 IP를 소스 주소로 바인딩하여 HTTP 요청을 보내고,
        정상 응답(200)과 유효한 콘텐츠(100바이트 이상)를 확인합니다.

        테스트 판정 기준:
        - HTTP 200 응답
        - 응답 본문 100바이트 이상 (빈 응답 제외)

        Args:
            ip: 테스트할 IP 주소
            store: 스토어 이름 ('app_store' 또는 'play_store')

        Returns:
            IPTestResult 객체

        Example:
            >>> manager = IPManager()
            >>> result = manager.test_ip_for_store('172.31.40.115', 'app_store')
            >>> print(result.is_working)
            True
            >>> print(result.status_code)
            200
        """
        url = self.test_endpoints.get(store)

        if not url:
            return IPTestResult(
                ip=ip,
                platform=store,
                is_working=False,
                error=f"알 수 없는 스토어: {store}"
            )

        try:
            # 소스 IP 바인딩을 위한 세션 생성
            session = requests.Session()
            adapter = SourceAddressAdapter(ip)
            session.mount('http://', adapter)
            session.mount('https://', adapter)

            # HTTP 요청 실행
            response = session.get(
                url,
                timeout=TEST_TIMEOUT,
                headers={'User-Agent': DEFAULT_USER_AGENT}
            )

            # 응답 검증
            # - HTTP 200 상태 코드
            # - 콘텐츠 100바이트 이상 (의미 있는 응답)
            is_working = (
                response.status_code == 200 and
                len(response.content) > 100
            )

            result = IPTestResult(
                ip=ip,
                platform=store,
                is_working=is_working,
                status_code=response.status_code,
                error=None if is_working else f"HTTP {response.status_code}"
            )

        except requests.exceptions.Timeout:
            result = IPTestResult(
                ip=ip,
                platform=store,
                is_working=False,
                error="타임아웃"
            )
        except requests.exceptions.ConnectionError as e:
            result = IPTestResult(
                ip=ip,
                platform=store,
                is_working=False,
                error=f"연결 오류: {str(e)[:100]}"
            )
        except Exception as e:
            result = IPTestResult(
                ip=ip,
                platform=store,
                is_working=False,
                error=f"예외: {str(e)[:100]}"
            )

        # 결과 기록
        self.test_results.append(result)

        # 로깅
        status = "OK" if result.is_working else f"FAIL ({result.error})"
        logger.debug(f"IP 테스트: {ip} -> {store}: {status}")

        return result

    def initialize(self) -> Dict[str, List[str]]:
        """
        모든 IP를 감지하고 각 스토어에 대해 테스트합니다.

        파이프라인 시작 시 한 번 호출하여 IP 매핑을 설정합니다.
        이 함수 호출 후 get_ip_for_store()를 사용할 수 있습니다.

        수행 단계:
        1. hostname -I로 모든 IP 감지
        2. 각 스토어의 테스트 엔드포인트에 각 IP로 요청
        3. 성공한 IP를 스토어별로 매핑

        Returns:
            스토어별 동작하는 IP 목록
            예: {'app_store': ['172.31.40.115'], 'play_store': ['172.31.47.39']}

        Note:
            동작하는 IP가 없는 스토어는 WARNING 로그를 출력합니다.
            해당 스토어의 수집은 실패할 수 있습니다.

        Example:
            >>> manager = IPManager()
            >>> store_ips = manager.initialize()
            [INFO] IP 자동 감지 및 스토어 테스트 시작...
            [INFO] 감지된 IP: ['172.31.47.39', '172.31.40.115']
            [INFO] app_store: 동작 IP = ['172.31.40.115']
            [INFO] play_store: 동작 IP = ['172.31.47.39', '172.31.40.115']
        """
        logger.info("IP 자동 감지 및 스토어 테스트 시작...")

        # 1. IP 감지
        ips = self.discover_ips()

        if not ips:
            logger.warning("사용 가능한 IP가 없습니다!")
            return {}

        # 2. 각 스토어에 대해 테스트
        self.store_ip_map = {}
        self.test_results = []  # 이전 결과 초기화

        for store in self.test_endpoints:
            working_ips = []

            for ip in ips:
                result = self.test_ip_for_store(ip, store)
                if result.is_working:
                    working_ips.append(ip)

            self.store_ip_map[store] = working_ips

            # 결과 로깅
            if working_ips:
                logger.info(f"{store}: 동작 IP = {working_ips}")
            else:
                logger.warning(f"{store}: 동작하는 IP 없음! 수집이 실패할 수 있습니다.")

        self.last_initialized_at = datetime.now()

        return self.store_ip_map

    def get_ip_for_store(
        self,
        store: str,
        exclude: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        스토어용 IP를 반환합니다.

        실패한 IP를 제외하고 사용 가능한 첫 번째 IP를 반환합니다.
        모든 IP가 제외되면 None을 반환합니다.

        Args:
            store: 스토어 이름 ('app_store' 또는 'play_store')
            exclude: 제외할 IP 목록 (이번 요청에서 실패한 IP들)

        Returns:
            사용 가능한 IP 또는 None (사용 가능한 IP 없음)

        Example:
            >>> manager = IPManager()
            >>> manager.initialize()

            # 첫 번째 요청
            >>> ip = manager.get_ip_for_store('app_store')
            '172.31.40.115'

            # IP가 차단된 경우 대체 IP 요청
            >>> ip = manager.get_ip_for_store('app_store', exclude=['172.31.40.115'])
            None  # 다른 IP 없음
        """
        exclude = exclude or []

        # 제외 목록에 없는 동작 IP 찾기
        candidates = [
            ip for ip in self.store_ip_map.get(store, [])
            if ip not in exclude
        ]

        if not candidates:
            logger.debug(f"{store}: 사용 가능한 IP 없음 (제외: {exclude})")
            return None

        return candidates[0]

    def get_next_ip_for_store(
        self,
        store: str,
        exclude: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        라운드 로빈(순환) 방식으로 다음 IP를 반환합니다.

        IP가 여러 개 있을 경우 순차적으로 돌아가며 사용합니다.
        이를 통해 특정 IP에 요청이 집중되는 것을 방지하고,
        대상 서버의 차단 위험을 분산시킵니다.

        동작 원리:
        1. 스토어의 동작 IP 목록에서 exclude 목록을 제외
        2. 현재 인덱스 위치의 IP 반환
        3. 인덱스를 다음 위치로 이동 (순환)

        예시 (IP 4개: [A, B, C, D]):
        - 1번째 요청: A 반환, 인덱스 1로 이동
        - 2번째 요청: B 반환, 인덱스 2로 이동
        - 3번째 요청: C 반환, 인덱스 3으로 이동
        - 4번째 요청: D 반환, 인덱스 0으로 순환
        - 5번째 요청: A 반환, ...

        Args:
            store: 스토어 이름 ('app_store' 또는 'play_store')
            exclude: 제외할 IP 목록 (차단/실패한 IP들)

        Returns:
            다음 순서의 IP 또는 None (사용 가능한 IP 없음)

        Example:
            >>> manager = IPManager()
            >>> manager.initialize()
            # store_ip_map = {'app_store': ['1.1.1.1', '2.2.2.2', '3.3.3.3', '4.4.4.4']}

            >>> manager.get_next_ip_for_store('app_store')
            '1.1.1.1'
            >>> manager.get_next_ip_for_store('app_store')
            '2.2.2.2'
            >>> manager.get_next_ip_for_store('app_store')
            '3.3.3.3'
            >>> manager.get_next_ip_for_store('app_store')
            '4.4.4.4'
            >>> manager.get_next_ip_for_store('app_store')  # 순환
            '1.1.1.1'

            # IP가 차단된 경우 제외하고 로테이션
            >>> manager.get_next_ip_for_store('app_store', exclude=['2.2.2.2'])
            '1.1.1.1'  또는 '3.3.3.3' 또는 '4.4.4.4' (현재 인덱스에 따라)
        """
        exclude = exclude or []

        # 제외 목록에 없는 동작 IP 목록
        candidates = [
            ip for ip in self.store_ip_map.get(store, [])
            if ip not in exclude
        ]

        if not candidates:
            logger.debug(f"{store}: 로테이션 가능한 IP 없음 (제외: {exclude})")
            return None

        # 현재 로테이션 인덱스 가져오기 (없으면 0으로 초기화)
        current_index = self._rotation_index.get(store, 0)

        # 인덱스가 후보 목록 범위를 벗어나면 0으로 리셋
        # (IP가 제외되어 후보가 줄어든 경우 대비)
        if current_index >= len(candidates):
            current_index = 0

        # 선택된 IP
        selected_ip = candidates[current_index]

        # 다음 인덱스로 이동 (순환)
        next_index = (current_index + 1) % len(candidates)
        self._rotation_index[store] = next_index

        # 요청 카운트 증가 (모니터링용)
        self._request_count[store] = self._request_count.get(store, 0) + 1

        logger.debug(
            f"{store}: 로테이션 IP 선택 [{current_index}/{len(candidates)}] = {selected_ip}"
        )

        return selected_ip

    def reset_rotation(self, store: Optional[str] = None) -> None:
        """
        IP 로테이션 인덱스를 초기화합니다.

        새로운 수집 세션 시작 시 호출하여 로테이션을 처음부터 시작합니다.

        Args:
            store: 특정 스토어만 초기화 (None이면 전체)

        Example:
            >>> manager.reset_rotation()  # 전체 초기화
            >>> manager.reset_rotation('app_store')  # app_store만 초기화
        """
        if store:
            self._rotation_index[store] = 0
            logger.debug(f"{store} 로테이션 인덱스 초기화")
        else:
            self._rotation_index = {}
            logger.debug("모든 스토어 로테이션 인덱스 초기화")

    def get_rotation_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        로테이션 통계를 반환합니다.

        각 스토어별로 현재 인덱스, 총 IP 수, 요청 수를 보여줍니다.

        Returns:
            스토어별 통계
            예: {
                'app_store': {
                    'current_index': 2,
                    'total_ips': 4,
                    'requests': 150,
                    'ips': ['1.1.1.1', '2.2.2.2', '3.3.3.3', '4.4.4.4']
                },
                ...
            }
        """
        stats = {}

        for store, ips in self.store_ip_map.items():
            stats[store] = {
                'current_index': self._rotation_index.get(store, 0),
                'total_ips': len(ips),
                'requests': self._request_count.get(store, 0),
                'ips': ips,
            }

        return stats

    def get_all_ips_for_store(self, store: str) -> List[str]:
        """
        스토어에 사용 가능한 모든 IP 목록을 반환합니다.

        Args:
            store: 스토어 이름

        Returns:
            동작하는 IP 목록 (복사본)
        """
        return self.store_ip_map.get(store, []).copy()

    def has_working_ip(self, store: str) -> bool:
        """
        스토어에 동작하는 IP가 있는지 확인합니다.

        Args:
            store: 스토어 이름

        Returns:
            True: 동작하는 IP 있음
            False: 동작하는 IP 없음
        """
        return len(self.store_ip_map.get(store, [])) > 0

    def get_test_summary(self) -> Dict[str, Dict[str, int]]:
        """
        테스트 결과 요약을 반환합니다.

        Returns:
            스토어별 성공/실패 수
            예: {'app_store': {'success': 1, 'fail': 1}, 'play_store': {'success': 2, 'fail': 0}}
        """
        summary: Dict[str, Dict[str, int]] = {}

        for result in self.test_results:
            if result.platform not in summary:
                summary[result.platform] = {'success': 0, 'fail': 0}

            if result.is_working:
                summary[result.platform]['success'] += 1
            else:
                summary[result.platform]['fail'] += 1

        return summary
