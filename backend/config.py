# -*- coding: utf-8 -*-
"""
전역 설정값 관리
AGENT.MD 지침 5번: 모든 설정값은 전역으로 정의하고, 메인 함수 최상단에 배치
"""
import os
import urllib3

# SSL 경고 무시 (일부 환경에서 SSL 핸드셰이크 문제 해결)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# SSL 검증 비활성화 플래그 (테스트 환경에서만 사용 권장)
SSL_VERIFY = False

# 데이터베이스 설정 (절대경로 사용, database 폴더에 정리)
DATABASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "apps.db")

# 앱스토어 국가 코드 목록
# Google Play와 App Store에서 지원하는 주요 국가
COUNTRIES = [
    # 아시아
    {"code": "kr", "name": "대한민국"},
    {"code": "jp", "name": "일본"},
    {"code": "cn", "name": "중국"},
    {"code": "tw", "name": "대만"},
    {"code": "hk", "name": "홍콩"},
    {"code": "sg", "name": "싱가포르"},
    {"code": "in", "name": "인도"},
    {"code": "id", "name": "인도네시아"},
    {"code": "th", "name": "태국"},
    {"code": "vn", "name": "베트남"},
    {"code": "ph", "name": "필리핀"},
    {"code": "my", "name": "말레이시아"},

    # 북미
    {"code": "us", "name": "미국"},
    {"code": "ca", "name": "캐나다"},
    {"code": "mx", "name": "멕시코"},

    # 유럽
    {"code": "gb", "name": "영국"},
    {"code": "de", "name": "독일"},
    {"code": "fr", "name": "프랑스"},
    {"code": "it", "name": "이탈리아"},
    {"code": "es", "name": "스페인"},
    {"code": "nl", "name": "네덜란드"},
    {"code": "se", "name": "스웨덴"},
    {"code": "no", "name": "노르웨이"},
    {"code": "dk", "name": "덴마크"},
    {"code": "fi", "name": "핀란드"},
    {"code": "pl", "name": "폴란드"},
    {"code": "ru", "name": "러시아"},

    # 오세아니아
    {"code": "au", "name": "호주"},
    {"code": "nz", "name": "뉴질랜드"},

    # 남미
    {"code": "br", "name": "브라질"},
    {"code": "ar", "name": "아르헨티나"},
    {"code": "cl", "name": "칠레"},

    # 중동/아프리카
    {"code": "ae", "name": "아랍에미리트"},
    {"code": "sa", "name": "사우디아라비아"},
    {"code": "za", "name": "남아프리카공화국"},
    {"code": "eg", "name": "이집트"},
]

# 크롤링 설정 - 최대화
# Apple RSS API: 최대 200개 (top-free, top-paid 각각)
# iTunes Lookup API: 한 번에 최대 200개 ID 조회 가능
# Google Play search: 기본 최대 30개, 여러 검색어로 확장
FETCH_LIMIT_PER_COUNTRY = 200  # 국가별 가져올 최대 앱 개수

# Apple RSS API Feed 유형
APPLE_RSS_FEEDS = [
    "top-free",      # 무료 앱 순위
    "top-paid",      # 유료 앱 순위
]

# Google Play 검색어 (다양한 검색어로 최대한 많은 앱 수집)
GOOGLE_PLAY_SEARCH_QUERIES = [
    "new apps 2024",
    "new apps 2025",
    "최신 앱",
    "new release",
    "trending apps",
    "popular apps",
    "best apps",
    "top apps",
    "free apps",
    "유틸리티",
    "생산성",
    "게임",
    "social",
    "entertainment",
    "lifestyle",
    "education",
    "health",
    "finance",
    "shopping",
    "travel",
]

# 주목할만한 앱 선별 기준 (점수 기반)
SCORE_WEIGHTS = {
    "rating": 0.3,           # 평점 (30%)
    "rating_count": 0.2,     # 리뷰 수 (20%)
    "installs": 0.2,         # 설치 수 (20%)
    "freshness": 0.2,        # 최신성 (20%)
    "growth_rate": 0.1,      # 성장률 (10%)
}

MINIMUM_RATING = 4.0         # 최소 평점
MINIMUM_RATING_COUNT = 10    # 최소 리뷰 수
MINIMUM_SCORE = 60           # 최소 종합 점수 (주목 앱 선별)

# 로그 형식
LOG_FORMAT = "[{timestamp}] {step}: {message} (라인: {line_duration}초 | 태스크: {task_duration}초 | 누적: {total_duration}초)"


class TimingTracker:
    """전역 타이밍 추적기"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._reset()
        return cls._instance

    def _reset(self):
        from datetime import datetime
        self._start_time = datetime.now()
        self._last_log_time = datetime.now()
        self._task_start_times = {}

    def reset(self):
        """전체 타이밍 초기화"""
        self._reset()

    def start_task(self, task_name: str):
        """태스크 시작 시간 기록"""
        from datetime import datetime
        self._task_start_times[task_name] = datetime.now()

    def get_timing(self, task_name: str = None) -> dict:
        """
        타이밍 정보 반환

        Returns:
            dict: {
                'line_duration': 마지막 로그 이후 경과 시간,
                'task_duration': 현재 태스크 시작 이후 경과 시간,
                'total_duration': 전체 프로세스 시작 이후 경과 시간
            }
        """
        from datetime import datetime
        now = datetime.now()

        line_duration = (now - self._last_log_time).total_seconds()
        total_duration = (now - self._start_time).total_seconds()

        task_duration = 0.0
        if task_name and task_name in self._task_start_times:
            task_duration = (now - self._task_start_times[task_name]).total_seconds()

        # 마지막 로그 시간 업데이트
        self._last_log_time = now

        return {
            'line_duration': line_duration,
            'task_duration': task_duration,
            'total_duration': total_duration
        }


# 전역 타이밍 트래커 인스턴스
timing_tracker = TimingTracker()

# 요청 타임아웃 설정 (초)
REQUEST_TIMEOUT = 30

# API 요청 간 딜레이 (초) - 레이트 리밋 방지
REQUEST_DELAY = 0.5

# 실패 재시도 제어
FAILED_RETRY_COOLDOWN_MINUTES = 60  # 최근 실패 이후 대기할 최소 시간
FAILED_RETRY_WARNING_THRESHOLD = 5   # 경고 로그를 남길 실패 누적 임계치

# 프록시 설정 (환경변수 또는 직접 설정)
# 예: "http://proxy.example.com:8080" 또는 None
# 환경변수 HTTP_PROXY, HTTPS_PROXY가 설정되어 있으면 우선 사용
HTTP_PROXY = os.environ.get('HTTP_PROXY', None)
HTTPS_PROXY = os.environ.get('HTTPS_PROXY', None)


# 프록시 딕셔너리 생성 (requests 라이브러리 형식)
def get_proxies():
    """프록시 설정 반환 (설정되지 않으면 None)"""
    proxies = {}
    if HTTP_PROXY:
        proxies['http'] = HTTP_PROXY
    if HTTPS_PROXY:
        proxies['https'] = HTTPS_PROXY
    return proxies if proxies else None


def get_request_kwargs():
    """requests 라이브러리용 공통 설정 반환"""
    kwargs = {
        'timeout': REQUEST_TIMEOUT,
        'verify': SSL_VERIFY,
    }
    proxies = get_proxies()
    if proxies:
        kwargs['proxies'] = proxies
    return kwargs


# ============ 날짜 정규화 함수들 ============

from datetime import datetime
import re

# 표준 출력 포맷: ISO 8601
DATE_FORMAT_ISO = "%Y-%m-%dT%H:%M:%SZ"
DATE_FORMAT_DATE_ONLY = "%Y-%m-%d"


def normalize_date(value, fallback: str = None) -> str:
    """
    다양한 형식의 날짜를 ISO 8601 형식으로 정규화

    지원 형식:
    - ISO 8601: "2010-10-06T08:12:41Z"
    - Unix timestamp: 1766466534 (int or float)
    - 한국어 형식: "2010. 2. 25." 또는 "2010. 2. 25"
    - 기타 형식: "Mar 25, 2020", "2020-03-25" 등

    Args:
        value: 날짜 값 (str, int, float, datetime)
        fallback: 파싱 실패 시 반환할 기본값

    Returns:
        ISO 8601 형식 문자열 또는 fallback
    """
    if value is None:
        return fallback

    # datetime 객체
    if isinstance(value, datetime):
        return value.strftime(DATE_FORMAT_ISO)

    # Unix timestamp (int or float)
    if isinstance(value, (int, float)):
        try:
            dt = datetime.fromtimestamp(value)
            return dt.strftime(DATE_FORMAT_ISO)
        except (ValueError, OSError, OverflowError):
            return fallback

    # 문자열 처리
    if not isinstance(value, str):
        return fallback

    value = value.strip()
    if not value:
        return fallback

    # 이미 ISO 8601 형식인 경우 그대로 반환
    if re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value):
        return value

    # 한국어 형식 파싱: "2010. 2. 25." 또는 "2010. 2. 25"
    korean_match = re.match(r'^(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.?$', value)
    if korean_match:
        year, month, day = korean_match.groups()
        try:
            dt = datetime(int(year), int(month), int(day))
            return dt.strftime(DATE_FORMAT_DATE_ONLY)
        except ValueError:
            pass

    # 다양한 날짜 형식 시도
    date_formats = [
        "%Y-%m-%d",                    # 2020-03-25
        "%Y/%m/%d",                    # 2020/03/25
        "%d-%m-%Y",                    # 25-03-2020
        "%d/%m/%Y",                    # 25/03/2020
        "%B %d, %Y",                   # March 25, 2020
        "%b %d, %Y",                   # Mar 25, 2020
        "%d %B %Y",                    # 25 March 2020
        "%d %b %Y",                    # 25 Mar 2020
        "%Y년 %m월 %d일",              # 2020년 03월 25일
    ]

    for fmt in date_formats:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime(DATE_FORMAT_DATE_ONLY)
        except ValueError:
            continue

    # Unix timestamp 문자열
    if value.isdigit():
        try:
            dt = datetime.fromtimestamp(int(value))
            return dt.strftime(DATE_FORMAT_ISO)
        except (ValueError, OSError, OverflowError):
            pass

    return fallback


def normalize_price(price_value, currency: str = None) -> float:
    """
    가격 값을 float으로 정규화

    Args:
        price_value: 가격 값 (str, int, float)
        currency: 통화 코드 (사용하지 않지만 확장성을 위해)

    Returns:
        float 가격 또는 0.0
    """
    if price_value is None:
        return None

    if isinstance(price_value, (int, float)):
        return float(price_value)

    if isinstance(price_value, str):
        # "Free", "무료" 등
        if price_value.lower() in ('free', '무료', '0', ''):
            return 0.0

        # 숫자만 추출
        price_match = re.search(r'[\d.,]+', price_value.replace(',', ''))
        if price_match:
            try:
                return float(price_match.group())
            except ValueError:
                pass

    return None


def normalize_rating(rating_value) -> float:
    """
    평점 값을 float으로 정규화

    Args:
        rating_value: 평점 값 (str, int, float)

    Returns:
        float 평점 또는 None
    """
    if rating_value is None:
        return None

    if isinstance(rating_value, (int, float)):
        return round(float(rating_value), 2)

    if isinstance(rating_value, str):
        try:
            return round(float(rating_value), 2)
        except ValueError:
            pass

    return None


def normalize_count(count_value) -> int:
    """
    숫자 값을 int로 정규화 (설치 수, 리뷰 수 등)

    Args:
        count_value: 숫자 값 (str, int, float)

    Returns:
        int 또는 None
    """
    if count_value is None:
        return None

    if isinstance(count_value, int):
        return count_value

    if isinstance(count_value, float):
        return int(count_value)

    if isinstance(count_value, str):
        # "100,000,000+" -> 100000000
        clean = re.sub(r'[^\d]', '', count_value)
        if clean:
            try:
                return int(clean)
            except ValueError:
                pass

    return None


def normalize_file_size(size_value) -> int:
    """
    파일 크기를 bytes로 정규화

    Args:
        size_value: 파일 크기 값 (str, int, float)

    Returns:
        int bytes 또는 None
    """
    if size_value is None:
        return None

    if isinstance(size_value, (int, float)):
        return int(size_value)

    if isinstance(size_value, str):
        size_value = size_value.strip().upper()

        # "496357376" 숫자 문자열
        if size_value.isdigit():
            return int(size_value)

        # "100M", "1.5G", "500K" 등
        size_match = re.match(r'^([\d.]+)\s*(K|KB|M|MB|G|GB)?$', size_value, re.IGNORECASE)
        if size_match:
            num = float(size_match.group(1))
            unit = (size_match.group(2) or '').upper()

            if unit in ('K', 'KB'):
                return int(num * 1024)
            elif unit in ('M', 'MB'):
                return int(num * 1024 * 1024)
            elif unit in ('G', 'GB'):
                return int(num * 1024 * 1024 * 1024)
            else:
                return int(num)

    return None
