"""
리뷰 수집 상태 관리 데이터베이스 모듈

이 모듈은 리뷰 수집의 상태, 실패 원인, 변경 감지를 위한
review_collection_status 테이블을 관리합니다.

주요 기능:
- 리뷰 수집 상태 추적 (성공/실패/한계 도달)
- 변경 감지 (스토어 리뷰 수 비교)
- 에러 분류 및 연속 실패 추적
- IP-스토어 매핑 캐시

사용 예시:
    from database.review_collection_db import (
        init_review_collection_tables,
        should_collect_reviews,
        record_collection_success,
        record_collection_failure,
    )

    # 테이블 초기화
    init_review_collection_tables()

    # 수집 여부 판단
    should, mode, reason = should_collect_reviews(app_id, platform, new_count)

    if should:
        # 수집 실행...
        record_collection_success(app_id, platform, store_count, collected)
    else:
        logger.info(f"Skip: {reason}")
"""
import os
import logging
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, Tuple
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row


# =============================================================================
# 로거 설정
# =============================================================================

logger = logging.getLogger(__name__)


# =============================================================================
# 데이터베이스 연결 설정
# =============================================================================

# 환경 변수에서 DB 연결 정보 로드
# APP_DETAILS_DB_* 환경 변수를 사용하여 기존 설정과 일관성 유지
DB_HOST = os.getenv("APP_DETAILS_DB_HOST", "localhost")
DB_PORT = int(os.getenv("APP_DETAILS_DB_PORT", "5432"))
DB_NAME = os.getenv("APP_DETAILS_DB_NAME", "app_details")
DB_USER = os.getenv("APP_DETAILS_DB_USER", "app_details")
DB_PASSWORD = os.getenv("APP_DETAILS_DB_PASSWORD", "")

# 연결 재사용 여부 (싱글톤 패턴)
DB_REUSE_CONNECTION = os.getenv("APP_DETAILS_DB_REUSE_CONNECTION", "true").lower() in (
    "1", "true", "yes", "y"
)

# 연결 풀 (싱글톤)
_connection: Optional[psycopg.Connection] = None


def _build_dsn() -> str:
    """
    데이터베이스 연결 문자열(DSN)을 생성합니다.

    환경 변수 APP_DETAILS_DB_DSN이 설정되어 있으면 그 값을 사용하고,
    그렇지 않으면 개별 환경 변수로 DSN을 구성합니다.

    Returns:
        PostgreSQL 연결 문자열
    """
    dsn = os.getenv("APP_DETAILS_DB_DSN")
    if dsn:
        return dsn
    return (
        f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD}"
    )


def get_connection() -> psycopg.Connection:
    """
    데이터베이스 연결을 반환합니다.

    DB_REUSE_CONNECTION이 True면 싱글톤 패턴으로 연결을 재사용합니다.
    연결이 끊어진 경우 자동으로 재연결합니다.

    Returns:
        psycopg.Connection 객체
    """
    global _connection

    if not DB_REUSE_CONNECTION:
        return psycopg.connect(_build_dsn(), row_factory=dict_row)

    if _connection is None or _connection.closed:
        _connection = psycopg.connect(_build_dsn(), row_factory=dict_row)
        logger.debug("DB 연결 생성")

    return _connection


def release_connection(conn: psycopg.Connection) -> None:
    """
    연결을 해제합니다.

    싱글톤 모드에서는 실제로 닫지 않고 재사용을 위해 유지합니다.
    비싱글톤 모드에서는 연결을 닫습니다.

    Args:
        conn: 해제할 연결 객체
    """
    if not DB_REUSE_CONNECTION:
        conn.close()


@contextmanager
def db_cursor():
    """
    데이터베이스 커서를 제공하는 컨텍스트 매니저.

    자동으로 커밋하고, 예외 발생 시 롤백합니다.

    Usage:
        with db_cursor() as cursor:
            cursor.execute("SELECT * FROM ...")
            result = cursor.fetchone()

    Yields:
        psycopg.Cursor 객체
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


# =============================================================================
# 에러 코드 상수
# =============================================================================

class ErrorCode:
    """
    리뷰 수집 실패 원인 코드

    각 코드는 재시도 전략을 결정하는 데 사용됩니다.

    Attributes:
        IP_BLOCKED: IP 차단 (HTTP 403) - 다른 IP로 즉시 재시도
        RATE_LIMITED: 요청 과다 (HTTP 429) - 백오프 후 재시도
        NETWORK_ERROR: 네트워크 오류 - 다음 실행 시 재시도
        SERVER_ERROR: 서버 오류 (5xx) - 다음 실행 시 재시도
        APP_NOT_FOUND: 앱 삭제됨 - 재시도 안 함 (영구 실패)
        NO_REVIEWS: 리뷰 없음 - 변경 시만 재시도
        API_LIMIT_REACHED: API 한계 도달 - 정상 완료 (한계 표시)
        PARSE_ERROR: 파싱 실패 - 다음 실행 시 재시도
        NO_AVAILABLE_IP: 사용 가능 IP 없음 - 다음 실행 시 재시도
    """
    IP_BLOCKED = "IP_BLOCKED"
    RATE_LIMITED = "RATE_LIMITED"
    NETWORK_ERROR = "NETWORK_ERROR"
    SERVER_ERROR = "SERVER_ERROR"
    APP_NOT_FOUND = "APP_NOT_FOUND"
    NO_REVIEWS = "NO_REVIEWS"
    API_LIMIT_REACHED = "API_LIMIT_REACHED"
    PARSE_ERROR = "PARSE_ERROR"
    NO_AVAILABLE_IP = "NO_AVAILABLE_IP"


class CollectionMode(Enum):
    """
    리뷰 수집 모드

    수집 전략을 결정하는 데 사용됩니다.

    Attributes:
        INITIAL: 첫 수집 - 최대한 많이 수집 (API 한계까지)
        INCREMENTAL: 증분 수집 - 기존 review_id 만나면 중단
    """
    INITIAL = "initial"
    INCREMENTAL = "incremental"


# =============================================================================
# 테이블 초기화
# =============================================================================

def init_review_collection_tables() -> None:
    """
    리뷰 수집 관련 테이블을 생성합니다.

    이미 존재하는 테이블은 건너뜁니다 (IF NOT EXISTS).

    생성되는 테이블:
    - review_collection_status: 앱별 리뷰 수집 상태 추적
    - ip_store_mapping: IP-스토어 매핑 캐시 (선택적)

    생성되는 인덱스:
    - idx_rcs_platform_failure: 실패 원인별 조회
    - idx_rcs_consecutive_failures: 연속 실패 앱 조회
    - idx_rcs_last_attempt: 최근 시도 순 조회
    """
    with db_cursor() as cursor:
        # =====================================================================
        # review_collection_status 테이블
        # 앱별 리뷰 수집 상태를 추적하는 핵심 테이블
        # =====================================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS review_collection_status (
                -- 식별자: 앱 ID와 플랫폼의 조합이 기본키
                app_id TEXT NOT NULL,
                platform TEXT NOT NULL,

                -- 시간 추적
                -- last_attempt_at: 성공/실패 관계없이 마지막 시도 시간
                -- last_success_at: 마지막으로 성공한 시간 (실패해도 갱신 안 됨)
                last_attempt_at TIMESTAMPTZ,
                last_success_at TIMESTAMPTZ,

                -- 변경 감지
                -- last_known_store_review_count: 스토어 API에서 알려준 전체 리뷰 수
                --   예: 50,000 (스토어에 표시된 리뷰 수)
                --   용도: 다음 수집 시 비교하여 증가 여부 판단
                -- collected_review_count: 우리 DB에 실제 저장된 리뷰 수
                --   예: 2,100 (API 한계로 전체는 못 가져옴)
                last_known_store_review_count INTEGER,
                collected_review_count INTEGER DEFAULT 0,

                -- 실패 추적
                -- last_failure_reason: ErrorCode 상수 값 (예: 'RATE_LIMITED')
                -- last_failure_detail: 상세 에러 메시지 (디버깅용)
                -- consecutive_failures: 연속 실패 횟수 (성공 시 0으로 초기화)
                last_failure_reason TEXT,
                last_failure_detail TEXT,
                consecutive_failures INTEGER DEFAULT 0,

                -- 수집 한계
                -- collection_limited: API 한계로 전체 리뷰를 수집하지 못함
                -- collection_limited_reason: 한계 원인 (예: 'RSS_PAGE_LIMIT')
                collection_limited BOOLEAN DEFAULT FALSE,
                collection_limited_reason TEXT,

                -- 메타데이터
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),

                PRIMARY KEY (app_id, platform)
            )
        """)

        # 실패 원인별 조회 인덱스
        # 용도: 특정 에러가 발생한 앱 목록 조회
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rcs_platform_failure
            ON review_collection_status (platform, last_failure_reason)
            WHERE last_failure_reason IS NOT NULL
        """)

        # 연속 실패 조회 인덱스
        # 용도: 반복적으로 실패하는 앱 모니터링
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rcs_consecutive_failures
            ON review_collection_status (consecutive_failures DESC)
            WHERE consecutive_failures > 0
        """)

        # 최근 시도 순 조회 인덱스
        # 용도: 최근 수집 활동 모니터링
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rcs_last_attempt
            ON review_collection_status (platform, last_attempt_at DESC)
        """)

        # =====================================================================
        # ip_store_mapping 테이블 (선택적)
        # IP-스토어 매핑을 캐시하여 매번 테스트하지 않도록 함
        # =====================================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ip_store_mapping (
                -- IP 주소와 스토어의 조합이 기본키
                ip_address TEXT NOT NULL,
                platform TEXT NOT NULL,

                -- 동작 여부 및 테스트 결과
                is_working BOOLEAN NOT NULL,
                last_tested_at TIMESTAMPTZ NOT NULL,
                last_error TEXT,

                PRIMARY KEY (ip_address, platform)
            )
        """)

    logger.info("리뷰 수집 테이블 초기화 완료")


# =============================================================================
# 상태 조회 함수
# =============================================================================

def get_review_collection_status(app_id: str, platform: str) -> Optional[Dict[str, Any]]:
    """
    앱의 리뷰 수집 상태를 조회합니다.

    Args:
        app_id: 앱 ID (예: '284882215', 'com.whatsapp')
        platform: 플랫폼 ('app_store' 또는 'play_store')

    Returns:
        상태 딕셔너리 또는 None (레코드 없음)

        반환 필드:
        - app_id, platform: 식별자
        - last_attempt_at: 마지막 시도 시간
        - last_success_at: 마지막 성공 시간
        - last_known_store_review_count: 스토어 전체 리뷰 수
        - collected_review_count: 수집된 리뷰 수
        - last_failure_reason: 실패 원인 코드
        - last_failure_detail: 상세 에러
        - consecutive_failures: 연속 실패 횟수
        - collection_limited: 한계 도달 여부
        - collection_limited_reason: 한계 원인

    Example:
        >>> status = get_review_collection_status('284882215', 'app_store')
        >>> if status:
        ...     print(f"마지막 수집: {status['last_success_at']}")
        ...     print(f"스토어 리뷰: {status['last_known_store_review_count']}")
    """
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT * FROM review_collection_status
            WHERE app_id = %s AND platform = %s
        """, (app_id, platform))
        return cursor.fetchone()


def get_total_collected_count(app_id: str, platform: str) -> int:
    """
    앱의 실제 수집된 리뷰 수를 app_reviews 테이블에서 조회합니다.

    review_collection_status.collected_review_count와 별개로
    실제 DB에 저장된 리뷰 수를 직접 카운트합니다.

    Args:
        app_id: 앱 ID
        platform: 플랫폼

    Returns:
        수집된 리뷰 수 (레코드 없으면 0)
    """
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*) as count FROM app_reviews
            WHERE app_id = %s AND platform = %s
        """, (app_id, platform))
        result = cursor.fetchone()
        return result['count'] if result else 0


# =============================================================================
# 상태 저장 함수
# =============================================================================

def upsert_review_collection_status(
    app_id: str,
    platform: str,
    last_attempt_at: Optional[datetime] = None,
    last_success_at: Optional[datetime] = None,
    last_known_store_review_count: Optional[int] = None,
    collected_review_count: Optional[int] = None,
    last_failure_reason: Optional[str] = None,
    last_failure_detail: Optional[str] = None,
    consecutive_failures: Optional[int] = None,
    collection_limited: Optional[bool] = None,
    collection_limited_reason: Optional[str] = None,
) -> None:
    """
    리뷰 수집 상태를 업데이트하거나 삽입합니다 (UPSERT).

    None인 필드는 업데이트하지 않고 기존 값을 유지합니다.
    단, last_failure_reason과 last_failure_detail은 None을 전달하면
    해당 값으로 업데이트됩니다 (실패 초기화용).

    Args:
        app_id: 앱 ID
        platform: 플랫폼
        last_attempt_at: 마지막 시도 시간
        last_success_at: 마지막 성공 시간
        last_known_store_review_count: 스토어 전체 리뷰 수
        collected_review_count: 실제 수집한 리뷰 수
        last_failure_reason: 실패 원인 코드 (None으로 초기화 가능)
        last_failure_detail: 상세 에러 메시지 (None으로 초기화 가능)
        consecutive_failures: 연속 실패 횟수
        collection_limited: API 한계 도달 여부
        collection_limited_reason: 한계 원인

    Note:
        이 함수는 저수준 함수입니다. 일반적으로는 record_collection_success()
        또는 record_collection_failure()를 사용하세요.
    """
    now = datetime.now()

    with db_cursor() as cursor:
        cursor.execute("""
            INSERT INTO review_collection_status (
                app_id, platform,
                last_attempt_at, last_success_at,
                last_known_store_review_count, collected_review_count,
                last_failure_reason, last_failure_detail, consecutive_failures,
                collection_limited, collection_limited_reason,
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (app_id, platform) DO UPDATE SET
                last_attempt_at = COALESCE(%s, review_collection_status.last_attempt_at),
                last_success_at = COALESCE(%s, review_collection_status.last_success_at),
                last_known_store_review_count = COALESCE(%s, review_collection_status.last_known_store_review_count),
                collected_review_count = COALESCE(%s, review_collection_status.collected_review_count),
                last_failure_reason = %s,
                last_failure_detail = %s,
                consecutive_failures = COALESCE(%s, review_collection_status.consecutive_failures),
                collection_limited = COALESCE(%s, review_collection_status.collection_limited),
                collection_limited_reason = COALESCE(%s, review_collection_status.collection_limited_reason),
                updated_at = %s
        """, (
            # INSERT 값
            app_id, platform,
            last_attempt_at, last_success_at,
            last_known_store_review_count, collected_review_count,
            last_failure_reason, last_failure_detail, consecutive_failures,
            collection_limited, collection_limited_reason,
            now, now,
            # UPDATE 값 (COALESCE로 None이면 기존 값 유지)
            last_attempt_at, last_success_at,
            last_known_store_review_count, collected_review_count,
            # 실패 관련 필드는 COALESCE 없이 직접 업데이트 (None으로 초기화 가능)
            last_failure_reason, last_failure_detail,
            consecutive_failures,
            collection_limited, collection_limited_reason,
            now
        ))


def record_collection_success(
    app_id: str,
    platform: str,
    store_review_count: int,
    collected_count: int,
    collection_limited: bool = False,
    limited_reason: Optional[str] = None,
) -> None:
    """
    수집 성공을 기록합니다.

    성공 시 실패 관련 필드(last_failure_reason, last_failure_detail,
    consecutive_failures)를 초기화합니다.

    Args:
        app_id: 앱 ID
        platform: 플랫폼
        store_review_count: 스토어의 전체 리뷰 수 (API에서 확인한 값)
        collected_count: 이번에 새로 수집한 리뷰 수
        collection_limited: API 한계 도달 여부
            True = 전체 리뷰를 다 수집하지 못함 (예: RSS 10페이지 한계)
        limited_reason: 한계 원인 (예: 'RSS_PAGE_LIMIT', 'MAX_REVIEWS_REACHED')

    Example:
        >>> record_collection_success(
        ...     app_id='284882215',
        ...     platform='app_store',
        ...     store_review_count=50000,
        ...     collected_count=2100,
        ...     collection_limited=True,
        ...     limited_reason='RSS_PAGE_LIMIT'
        ... )
    """
    now = datetime.now()

    # 실제 DB에 저장된 총 리뷰 수 조회
    total_collected = get_total_collected_count(app_id, platform)

    upsert_review_collection_status(
        app_id=app_id,
        platform=platform,
        last_attempt_at=now,
        last_success_at=now,
        last_known_store_review_count=store_review_count,
        collected_review_count=total_collected,
        last_failure_reason=None,    # 성공 시 초기화
        last_failure_detail=None,    # 성공 시 초기화
        consecutive_failures=0,       # 성공 시 초기화
        collection_limited=collection_limited,
        collection_limited_reason=limited_reason,
    )

    logger.debug(
        f"수집 성공 기록: {app_id} ({platform}) | "
        f"store={store_review_count}, collected={collected_count}, total={total_collected}"
    )


def record_collection_failure(
    app_id: str,
    platform: str,
    store_review_count: int,
    failure_reason: str,
    failure_detail: Optional[str] = None,
) -> int:
    """
    수집 실패를 기록합니다.

    연속 실패 횟수를 1 증가시킵니다.

    Args:
        app_id: 앱 ID
        platform: 플랫폼
        store_review_count: 스토어의 전체 리뷰 수
        failure_reason: 실패 원인 코드 (ErrorCode 상수 사용)
        failure_detail: 상세 에러 메시지 (디버깅/로깅용)

    Returns:
        업데이트된 연속 실패 횟수

    Example:
        >>> failures = record_collection_failure(
        ...     app_id='284882215',
        ...     platform='app_store',
        ...     store_review_count=50000,
        ...     failure_reason=ErrorCode.RATE_LIMITED,
        ...     failure_detail='HTTP 429 after 3 retries'
        ... )
        >>> print(f"연속 {failures}회 실패")
    """
    now = datetime.now()

    # 현재 상태 조회
    current = get_review_collection_status(app_id, platform)
    current_failures = (current.get('consecutive_failures', 0) if current else 0)
    new_failures = current_failures + 1

    upsert_review_collection_status(
        app_id=app_id,
        platform=platform,
        last_attempt_at=now,
        last_known_store_review_count=store_review_count,
        last_failure_reason=failure_reason,
        last_failure_detail=failure_detail,
        consecutive_failures=new_failures,
    )

    logger.debug(
        f"수집 실패 기록: {app_id} ({platform}) | "
        f"reason={failure_reason}, consecutive={new_failures}"
    )

    return new_failures


# =============================================================================
# 수집 판단 함수
# =============================================================================

def should_collect_reviews(
    app_id: str,
    platform: str,
    new_store_count: int,
) -> Tuple[bool, Optional[CollectionMode], Optional[str]]:
    """
    리뷰 수집 여부를 결정합니다.

    판단 기준 (순서대로):
    1. 상태 없음 (첫 수집) → 수집 (INITIAL 모드)
    2. 영구 실패 앱 → 스킵
    3. 스토어 리뷰 수 증가 → 수집 (INCREMENTAL 모드)
    4. 스토어 리뷰 수 0 → 스킵
    5. 변화 없음 → 스킵

    Args:
        app_id: 앱 ID
        platform: 플랫폼
        new_store_count: 새로 확인한 스토어 전체 리뷰 수
            (상세정보 수집 시 apps_metrics.reviews_count에서 획득)

    Returns:
        (should_collect, mode, skip_reason) 튜플
        - should_collect: 수집 여부 (True/False)
        - mode: 수집 모드 (CollectionMode) 또는 None
        - skip_reason: 스킵 사유 문자열 또는 None

    Example:
        >>> should, mode, reason = should_collect_reviews(
        ...     app_id='284882215',
        ...     platform='app_store',
        ...     new_store_count=50000
        ... )
        >>> if should:
        ...     if mode == CollectionMode.INITIAL:
        ...         collect_all_reviews(app_id)
        ...     else:
        ...         collect_new_reviews_only(app_id)
        ... else:
        ...     logger.info(f"Skip {app_id}: {reason}")
    """
    # 상태 조회
    status = get_review_collection_status(app_id, platform)

    # 1. 첫 수집 (상태 레코드가 없음)
    if status is None:
        return True, CollectionMode.INITIAL, None

    # 2. 영구 실패 체크 (failed_apps 테이블 확인)
    if _is_permanently_failed(app_id, platform):
        return False, None, "permanently_failed"

    # 3. 스토어 리뷰 수 비교
    last_known = status.get('last_known_store_review_count') or 0

    if new_store_count > last_known:
        # 리뷰 증가 → 증분 수집
        return True, CollectionMode.INCREMENTAL, None

    if new_store_count == 0:
        # 리뷰 없음
        return False, None, "no_reviews_on_store"

    # 4. 변화 없음
    return False, None, "no_change"


def _is_permanently_failed(app_id: str, platform: str) -> bool:
    """
    앱이 영구 실패 상태인지 확인합니다.

    failed_apps 테이블의 is_permanent 필드를 확인합니다.
    영구 실패 앱은 다시 수집을 시도하지 않습니다.

    Args:
        app_id: 앱 ID
        platform: 플랫폼

    Returns:
        True: 영구 실패 상태 (수집 건너뜀)
        False: 정상 또는 일시 실패 (수집 가능)
    """
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT is_permanent FROM failed_apps
            WHERE app_id = %s AND platform = %s
        """, (app_id, platform))
        result = cursor.fetchone()
        return result is not None and result.get('is_permanent', False)


# =============================================================================
# IP 매핑 관리 함수
# =============================================================================

def save_ip_store_mapping(
    ip_address: str,
    platform: str,
    is_working: bool,
    error: Optional[str] = None,
) -> None:
    """
    IP-스토어 매핑을 저장합니다.

    IP 테스트 결과를 캐시하여 다음 실행 시 참조할 수 있게 합니다.

    Args:
        ip_address: IP 주소 (예: '172.31.40.115')
        platform: 스토어 ('app_store' 또는 'play_store')
        is_working: 동작 여부
        error: 에러 메시지 (실패 시)
    """
    now = datetime.now()

    with db_cursor() as cursor:
        cursor.execute("""
            INSERT INTO ip_store_mapping (ip_address, platform, is_working, last_tested_at, last_error)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ip_address, platform) DO UPDATE SET
                is_working = %s,
                last_tested_at = %s,
                last_error = %s
        """, (
            ip_address, platform, is_working, now, error,
            is_working, now, error
        ))


def get_working_ips_for_store(platform: str) -> List[str]:
    """
    스토어에 동작하는 IP 목록을 조회합니다.

    Args:
        platform: 스토어

    Returns:
        동작하는 IP 주소 목록
    """
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT ip_address FROM ip_store_mapping
            WHERE platform = %s AND is_working = TRUE
            ORDER BY last_tested_at DESC
        """, (platform,))
        return [row['ip_address'] for row in cursor.fetchall()]


# =============================================================================
# 모니터링/통계 함수
# =============================================================================

def get_failure_stats(platform: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    실패 원인별 통계를 조회합니다.

    Args:
        platform: 특정 플랫폼만 조회 (None이면 전체)

    Returns:
        [{'platform': 'app_store', 'reason': 'RATE_LIMITED', 'count': 10}, ...]
    """
    with db_cursor() as cursor:
        if platform:
            cursor.execute("""
                SELECT platform, last_failure_reason as reason, COUNT(*) as count
                FROM review_collection_status
                WHERE last_failure_reason IS NOT NULL AND platform = %s
                GROUP BY platform, last_failure_reason
                ORDER BY count DESC
            """, (platform,))
        else:
            cursor.execute("""
                SELECT platform, last_failure_reason as reason, COUNT(*) as count
                FROM review_collection_status
                WHERE last_failure_reason IS NOT NULL
                GROUP BY platform, last_failure_reason
                ORDER BY count DESC
            """)
        return list(cursor.fetchall())


def get_consecutive_failure_apps(min_failures: int = 3) -> List[Dict[str, Any]]:
    """
    연속 실패 앱 목록을 조회합니다.

    Args:
        min_failures: 최소 연속 실패 횟수

    Returns:
        연속 실패 앱 목록
    """
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT app_id, platform, consecutive_failures, last_failure_reason, last_attempt_at
            FROM review_collection_status
            WHERE consecutive_failures >= %s
            ORDER BY consecutive_failures DESC
        """, (min_failures,))
        return list(cursor.fetchall())


def get_collection_stats_24h(platform: Optional[str] = None) -> Dict[str, int]:
    """
    최근 24시간 수집 현황을 조회합니다.

    Args:
        platform: 특정 플랫폼만 조회 (None이면 전체)

    Returns:
        {'attempted': 100, 'succeeded': 80, 'failed': 20}
    """
    with db_cursor() as cursor:
        if platform:
            cursor.execute("""
                SELECT
                    COUNT(*) as attempted,
                    COUNT(CASE WHEN last_failure_reason IS NULL THEN 1 END) as succeeded,
                    COUNT(CASE WHEN last_failure_reason IS NOT NULL THEN 1 END) as failed
                FROM review_collection_status
                WHERE last_attempt_at > NOW() - INTERVAL '24 hours'
                  AND platform = %s
            """, (platform,))
        else:
            cursor.execute("""
                SELECT
                    COUNT(*) as attempted,
                    COUNT(CASE WHEN last_failure_reason IS NULL THEN 1 END) as succeeded,
                    COUNT(CASE WHEN last_failure_reason IS NOT NULL THEN 1 END) as failed
                FROM review_collection_status
                WHERE last_attempt_at > NOW() - INTERVAL '24 hours'
            """)
        result = cursor.fetchone()
        return {
            'attempted': result['attempted'],
            'succeeded': result['succeeded'],
            'failed': result['failed'],
        }
