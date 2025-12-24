# -*- coding: utf-8 -*-
"""
전역 설정값 관리
AGENT.MD 지침 5번: 모든 설정값은 전역으로 정의하고, 메인 함수 최상단에 배치
"""
import os
import ssl
import urllib3

# SSL 경고 무시 (일부 환경에서 SSL 핸드셰이크 문제 해결)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# SSL 검증 비활성화 플래그 (테스트 환경에서만 사용 권장)
SSL_VERIFY = False

# 데이터베이스 설정 (절대경로 사용)
DATABASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "apps.db")

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

# Google Play 카테고리 (앱 상세 정보 조회 시 사용)
GOOGLE_PLAY_CATEGORIES = [
    "GAME",
    "PRODUCTIVITY",
    "SOCIAL",
    "ENTERTAINMENT",
    "LIFESTYLE",
    "EDUCATION",
    "HEALTH_AND_FITNESS",
    "FINANCE",
    "SHOPPING",
    "TRAVEL_AND_LOCAL",
    "TOOLS",
    "COMMUNICATION",
    "PHOTOGRAPHY",
    "MUSIC_AND_AUDIO",
    "VIDEO_PLAYERS",
    "NEWS_AND_MAGAZINES",
    "FOOD_AND_DRINK",
    "WEATHER",
    "BUSINESS",
    "SPORTS",
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
