"""
리뷰 수집 시스템 통합 모듈

기존 수집기와 새로운 IP Manager, HTTP Client, 상태 추적 시스템을
연결하는 통합 레이어입니다.

주요 기능:
- IP Manager 초기화 및 스토어별 IP 테스트
- HTTP 클라이언트 인스턴스 관리
- 수집 상태 추적 (review_collection_status 테이블)
- 변경 감지 기반 수집 판단

사용 예시:
    from core.review_collection_integration import ReviewCollectionContext

    # 파이프라인 시작 시 한 번 초기화
    ctx = ReviewCollectionContext()
    ctx.initialize()

    # 수집 여부 판단
    should, mode = ctx.should_collect('284882215', 'app_store', store_count=50000)

    if should:
        # HTTP 요청 (IP 로테이션 적용됨)
        result = ctx.http_client.request(url, 'app_store')

        if result.success:
            # 성공 기록
            ctx.record_success('284882215', 'app_store', 50000, 100)
        else:
            # 실패 기록
            ctx.record_failure('284882215', 'app_store', 50000, result.error_code)
"""
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

from core.ip_manager import IPManager
from core.http_client import StoreHttpClient, HttpResult, HttpErrorCode
from database.review_collection_db import (
    init_review_collection_tables,
    should_collect_reviews,
    record_collection_success,
    record_collection_failure,
    get_review_collection_status,
    save_ip_store_mapping,
    get_working_ips_for_store,
    CollectionMode,
    ErrorCode,
)


# =============================================================================
# 로거 설정
# =============================================================================

logger = logging.getLogger(__name__)


# =============================================================================
# 통합 컨텍스트 클래스
# =============================================================================

class ReviewCollectionContext:
    """
    리뷰 수집 시스템의 통합 컨텍스트

    파이프라인 실행 시 한 번 생성하여 전체 수집 과정에서 사용합니다.
    IP Manager, HTTP Client, 상태 추적을 통합 관리합니다.

    Attributes:
        ip_manager: IP 자동 감지 및 스토어별 할당 관리자
        http_client: IP 로테이션 지원 HTTP 클라이언트
        initialized: 초기화 완료 여부
        stats: 수집 통계 (성공/실패/스킵 카운트)

    Example:
        >>> ctx = ReviewCollectionContext()
        >>> ctx.initialize()

        >>> # 수집 여부 판단
        >>> should, mode = ctx.should_collect('284882215', 'app_store', 50000)

        >>> # HTTP 요청
        >>> result = ctx.http_client.request(url, 'app_store')

        >>> # 결과 기록
        >>> if result.success:
        ...     ctx.record_success('284882215', 'app_store', 50000, 100)
    """

    def __init__(self, use_ip_rotation: bool = True):
        """
        Args:
            use_ip_rotation: IP 로테이션 사용 여부 (기본값: True)
                True: 요청마다 다른 IP 순환 사용
                False: 항상 첫 번째 가용 IP 사용
        """
        self.ip_manager = IPManager()
        self.http_client: Optional[StoreHttpClient] = None
        self.use_ip_rotation = use_ip_rotation
        self.initialized = False

        # 수집 통계
        self.stats = {
            'apps_checked': 0,        # 수집 판단한 앱 수
            'apps_collected': 0,      # 실제 수집한 앱 수
            'apps_skipped': 0,        # 스킵한 앱 수
            'reviews_collected': 0,   # 수집한 리뷰 수
            'errors': 0,              # 에러 발생 수
        }

    def initialize(self, save_to_db: bool = True) -> Dict[str, List[str]]:
        """
        시스템을 초기화합니다.

        수행 작업:
        1. 데이터베이스 테이블 생성 (없으면)
        2. 서버 IP 자동 감지
        3. 각 스토어 엔드포인트에 IP 테스트
        4. HTTP 클라이언트 생성
        5. (선택적) IP 테스트 결과를 DB에 저장

        Args:
            save_to_db: IP 테스트 결과를 DB에 저장할지 여부

        Returns:
            스토어별 동작하는 IP 목록
            예: {'app_store': ['172.31.40.115'], 'play_store': ['172.31.47.39']}

        Example:
            >>> ctx = ReviewCollectionContext()
            >>> store_ips = ctx.initialize()
            >>> print(store_ips)
            {'app_store': ['172.31.40.115'], 'play_store': ['172.31.47.39', '172.31.40.115']}
        """
        logger.info("리뷰 수집 시스템 초기화 시작...")

        # 1. 데이터베이스 테이블 생성
        try:
            init_review_collection_tables()
            logger.info("데이터베이스 테이블 확인 완료")
        except Exception as e:
            logger.warning(f"테이블 초기화 실패 (계속 진행): {e}")

        # 2-3. IP 감지 및 테스트
        store_ips = self.ip_manager.initialize()

        # 4. HTTP 클라이언트 생성
        self.http_client = StoreHttpClient(
            self.ip_manager,
            use_rotation=self.use_ip_rotation
        )

        # 5. IP 테스트 결과 DB 저장
        if save_to_db:
            self._save_ip_test_results()

        self.initialized = True
        logger.info(f"리뷰 수집 시스템 초기화 완료: {store_ips}")

        return store_ips

    def _save_ip_test_results(self) -> None:
        """
        IP 테스트 결과를 데이터베이스에 저장합니다.

        ip_store_mapping 테이블에 결과를 캐시하여 다음 실행 시 참조할 수 있게 합니다.
        """
        for result in self.ip_manager.test_results:
            try:
                save_ip_store_mapping(
                    ip_address=result.ip,
                    platform=result.platform,
                    is_working=result.is_working,
                    error=result.error,
                )
            except Exception as e:
                logger.warning(f"IP 매핑 저장 실패 ({result.ip}, {result.platform}): {e}")

    def should_collect(
        self,
        app_id: str,
        platform: str,
        store_review_count: int,
    ) -> Tuple[bool, Optional[CollectionMode]]:
        """
        리뷰 수집 여부를 판단합니다.

        review_collection_status 테이블을 참조하여 변경 감지 기반으로 판단합니다.

        Args:
            app_id: 앱 ID
            platform: 플랫폼 ('app_store' 또는 'play_store')
            store_review_count: 스토어의 전체 리뷰 수 (apps_metrics.reviews_count)

        Returns:
            (should_collect, mode) 튜플
            - should_collect: 수집 여부
            - mode: 수집 모드 (INITIAL 또는 INCREMENTAL) 또는 None

        Example:
            >>> should, mode = ctx.should_collect('284882215', 'app_store', 50000)
            >>> if should:
            ...     if mode == CollectionMode.INITIAL:
            ...         # 첫 수집: 최대한 많이 수집
            ...     else:
            ...         # 증분 수집: 기존 리뷰 만나면 중단
        """
        self.stats['apps_checked'] += 1

        should, mode, reason = should_collect_reviews(app_id, platform, store_review_count)

        if not should:
            self.stats['apps_skipped'] += 1
            logger.debug(f"수집 스킵: {app_id} ({platform}) - {reason}")

        return should, mode

    def record_success(
        self,
        app_id: str,
        platform: str,
        store_review_count: int,
        collected_count: int,
        collection_limited: bool = False,
        limited_reason: Optional[str] = None,
    ) -> None:
        """
        수집 성공을 기록합니다.

        Args:
            app_id: 앱 ID
            platform: 플랫폼
            store_review_count: 스토어 전체 리뷰 수
            collected_count: 이번에 수집한 리뷰 수
            collection_limited: API 한계 도달 여부
            limited_reason: 한계 원인 (예: 'RSS_PAGE_LIMIT')
        """
        record_collection_success(
            app_id=app_id,
            platform=platform,
            store_review_count=store_review_count,
            collected_count=collected_count,
            collection_limited=collection_limited,
            limited_reason=limited_reason,
        )

        self.stats['apps_collected'] += 1
        self.stats['reviews_collected'] += collected_count

        logger.debug(
            f"수집 성공 기록: {app_id} ({platform}) | "
            f"collected={collected_count}, limited={collection_limited}"
        )

    def record_failure(
        self,
        app_id: str,
        platform: str,
        store_review_count: int,
        error_code: str,
        error_detail: Optional[str] = None,
    ) -> int:
        """
        수집 실패를 기록합니다.

        Args:
            app_id: 앱 ID
            platform: 플랫폼
            store_review_count: 스토어 전체 리뷰 수
            error_code: 에러 코드 (ErrorCode 또는 HttpErrorCode 상수)
            error_detail: 상세 에러 메시지

        Returns:
            연속 실패 횟수
        """
        consecutive_failures = record_collection_failure(
            app_id=app_id,
            platform=platform,
            store_review_count=store_review_count,
            failure_reason=error_code,
            failure_detail=error_detail,
        )

        self.stats['errors'] += 1

        logger.debug(
            f"수집 실패 기록: {app_id} ({platform}) | "
            f"error={error_code}, consecutive={consecutive_failures}"
        )

        return consecutive_failures

    def get_status(self, app_id: str, platform: str) -> Optional[Dict[str, Any]]:
        """
        앱의 수집 상태를 조회합니다.

        Args:
            app_id: 앱 ID
            platform: 플랫폼

        Returns:
            상태 딕셔너리 또는 None
        """
        return get_review_collection_status(app_id, platform)

    def request(
        self,
        url: str,
        platform: str,
        parse_json: bool = True,
    ) -> HttpResult:
        """
        HTTP 요청을 실행합니다 (IP 로테이션 적용).

        Args:
            url: 요청 URL
            platform: 스토어 ('app_store' 또는 'play_store')
            parse_json: JSON 파싱 여부

        Returns:
            HttpResult 객체
        """
        if not self.http_client:
            raise RuntimeError("초기화되지 않음: initialize()를 먼저 호출하세요")

        return self.http_client.request(url, platform, parse_json=parse_json)

    def reset_session(self, platform: Optional[str] = None) -> None:
        """
        세션 상태를 초기화합니다.

        새로운 수집 배치 시작 시 호출하여 이전 실패 기록과 로테이션 인덱스를 초기화합니다.

        Args:
            platform: 특정 스토어만 초기화 (None이면 전체)
        """
        if self.http_client:
            self.http_client.reset_all(platform)

    def get_stats(self) -> Dict[str, Any]:
        """
        수집 통계를 반환합니다.

        Returns:
            통계 딕셔너리
        """
        stats = self.stats.copy()

        # IP 로테이션 통계 추가
        if self.http_client:
            stats['rotation'] = self.http_client.get_rotation_stats()

        return stats

    def get_ip_for_store(self, platform: str) -> Optional[str]:
        """
        스토어에 사용할 IP를 반환합니다.

        기존 network_binding과 호환성을 위한 메서드입니다.

        Args:
            platform: 스토어

        Returns:
            사용 가능한 IP 또는 None
        """
        if self.use_ip_rotation:
            return self.ip_manager.get_next_ip_for_store(platform)
        else:
            return self.ip_manager.get_ip_for_store(platform)


# =============================================================================
# 전역 컨텍스트 (싱글톤)
# =============================================================================

_global_context: Optional[ReviewCollectionContext] = None


def get_review_collection_context(
    use_ip_rotation: bool = True,
    auto_initialize: bool = True,
) -> ReviewCollectionContext:
    """
    전역 리뷰 수집 컨텍스트를 반환합니다.

    싱글톤 패턴으로 전체 파이프라인에서 동일한 컨텍스트를 공유합니다.

    Args:
        use_ip_rotation: IP 로테이션 사용 여부
        auto_initialize: 자동 초기화 여부

    Returns:
        ReviewCollectionContext 인스턴스

    Example:
        >>> ctx = get_review_collection_context()
        >>> result = ctx.request(url, 'app_store')
    """
    global _global_context

    if _global_context is None:
        _global_context = ReviewCollectionContext(use_ip_rotation=use_ip_rotation)
        if auto_initialize:
            _global_context.initialize()

    return _global_context


def reset_review_collection_context() -> None:
    """
    전역 컨텍스트를 초기화합니다.

    테스트 등에서 새로운 컨텍스트가 필요할 때 사용합니다.
    """
    global _global_context
    _global_context = None


# =============================================================================
# 헬퍼 함수
# =============================================================================

def map_http_error_to_db_error(http_error_code: str) -> str:
    """
    HTTP 에러 코드를 DB 에러 코드로 매핑합니다.

    HttpErrorCode -> ErrorCode 변환

    Args:
        http_error_code: HTTP 에러 코드 (HttpErrorCode 상수)

    Returns:
        DB 에러 코드 (ErrorCode 상수)
    """
    mapping = {
        HttpErrorCode.IP_BLOCKED: ErrorCode.IP_BLOCKED,
        HttpErrorCode.RATE_LIMITED: ErrorCode.RATE_LIMITED,
        HttpErrorCode.NETWORK_ERROR: ErrorCode.NETWORK_ERROR,
        HttpErrorCode.SERVER_ERROR: ErrorCode.SERVER_ERROR,
        HttpErrorCode.PARSE_ERROR: ErrorCode.PARSE_ERROR,
        HttpErrorCode.NO_AVAILABLE_IP: ErrorCode.NO_AVAILABLE_IP,
    }

    return mapping.get(http_error_code, ErrorCode.NETWORK_ERROR)
