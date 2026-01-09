"""
App Details Database
앱 상세정보, 다국어 정보, 수치 데이터, 리뷰를 저장하는 DB
변경 시에만 누적 저장 (이력 관리)
"""
import os
import json
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row
from utils.logger import get_timestamped_logger

# psycopg DSN 참고: https://www.psycopg.org/psycopg3/docs/basic/usage.html
DB_DSN = os.getenv("APP_DETAILS_DB_DSN")
DB_HOST = os.getenv("APP_DETAILS_DB_HOST", "localhost")
DB_PORT = int(os.getenv("APP_DETAILS_DB_PORT", "5432"))
DB_NAME = os.getenv("APP_DETAILS_DB_NAME", "app_details")
DB_USER = os.getenv("APP_DETAILS_DB_USER", "app_details")
DB_PASSWORD = os.getenv("APP_DETAILS_DB_PASSWORD", "")

# 연결 재시도 설정
DB_CONNECT_MAX_RETRIES = int(os.getenv("APP_DETAILS_DB_CONNECT_MAX_RETRIES", "5"))
DB_CONNECT_RETRY_DELAY_SEC = float(os.getenv("APP_DETAILS_DB_CONNECT_RETRY_DELAY_SEC", "2.0"))

# 연결 재사용 설정 (기본값: true)
DB_REUSE_CONNECTION = os.getenv("APP_DETAILS_DB_REUSE_CONNECTION", "true").lower() in ("1", "true", "yes")

# 전역 싱글톤 연결
_DB_CONNECTION: Optional[psycopg.Connection] = None
PARTITION_COUNT = 64
APP_REVIEWS_PARTITION_COUNT = 64
LOG_FILE_PREFIX = "app_details_db"
DB_LOGGER = get_timestamped_logger("app_details_db", file_prefix=LOG_FILE_PREFIX)

# 비교 제외 필드
EXCLUDE_COMPARE_FIELDS = {'id', 'recorded_at'}


def normalize_date_format(date_str: Optional[str]) -> Optional[str]:
    """날짜 문자열을 ISO 8601 형식 (YYYY-MM-DDTHH:MM:SS)으로 정규화합니다.

    지원 형식:
    - ISO 8601: "2024-01-15T10:30:00Z", "2024-01-15T10:30:00-07:00"
    - 날짜만: "2024-01-15"
    - 영문: "Mar 15, 2024"
    """
    if not date_str:
        return None

    # ISO 형식 (T 포함)
    if 'T' in date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.replace(tzinfo=None).isoformat()
        except (ValueError, TypeError):
            pass

    # "Mar 15, 2024" 형식
    try:
        return datetime.strptime(date_str, "%b %d, %Y").isoformat()
    except (ValueError, TypeError):
        pass

    # "2024-03-15" 형식
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").isoformat()
    except (ValueError, TypeError):
        pass

    return date_str


def _build_dsn() -> str:
    """DB DSN 문자열을 구성합니다."""
    return DB_DSN or (
        f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD}"
    )


def _connect_with_retry() -> psycopg.Connection:
    """DB 연결을 생성합니다. 실패 시 재시도합니다."""
    dsn = _build_dsn()
    last_exc = None
    for attempt in range(1, DB_CONNECT_MAX_RETRIES + 1):
        step_label = f"DB_CONNECT_ATTEMPT_{attempt}"
        start_ts = datetime.now().isoformat()
        start_monotonic = time.monotonic()
        DB_LOGGER.info("[STEP START] %s | %s", step_label, start_ts)
        try:
            conn = psycopg.connect(dsn, row_factory=dict_row)
            elapsed = time.monotonic() - start_monotonic
            end_ts = datetime.now().isoformat()
            DB_LOGGER.info(
                "[STEP END] %s | %s | elapsed=%.2fs | status=SUCCESS",
                step_label,
                end_ts,
                elapsed,
            )
            if attempt > 1:
                DB_LOGGER.info("DB 연결 복구 완료: 시도 횟수=%s", attempt)
            return conn
        except psycopg.OperationalError as exc:
            elapsed = time.monotonic() - start_monotonic
            end_ts = datetime.now().isoformat()
            DB_LOGGER.info(
                "[STEP END] %s | %s | elapsed=%.2fs | status=FAIL",
                step_label,
                end_ts,
                elapsed,
            )
            last_exc = exc
            if "database system is starting up" in str(exc) and attempt < DB_CONNECT_MAX_RETRIES:
                DB_LOGGER.warning(
                    "DB 시작 대기 중: %s초 후 재시도 (%s/%s)",
                    DB_CONNECT_RETRY_DELAY_SEC,
                    attempt,
                    DB_CONNECT_MAX_RETRIES,
                )
                time.sleep(DB_CONNECT_RETRY_DELAY_SEC)
                continue
            break
    raise last_exc


def get_connection() -> psycopg.Connection:
    """DB 연결을 반환합니다. 재사용 설정 시 싱글톤 연결을 반환합니다."""
    global _DB_CONNECTION
    if DB_REUSE_CONNECTION:
        if _DB_CONNECTION and not _DB_CONNECTION.closed:
            return _DB_CONNECTION
        if _DB_CONNECTION and _DB_CONNECTION.closed:
            DB_LOGGER.info("닫힌 DB 연결 감지: 재연결을 시도합니다.")
        _DB_CONNECTION = _connect_with_retry()
        return _DB_CONNECTION
    return _connect_with_retry()


def release_connection(conn: Optional[psycopg.Connection]):
    """재사용 설정에 맞춰 연결을 정리합니다."""
    if not conn:
        return
    if DB_REUSE_CONNECTION:
        if not conn.closed and conn.info.transaction_status != TransactionStatus.IDLE:
            DB_LOGGER.info("DB 트랜잭션 정리: rollback 수행")
            conn.rollback()
        return
    if not conn.closed:
        conn.close()


def close_connection():
    """전역 DB 연결을 닫습니다."""
    global _DB_CONNECTION
    if _DB_CONNECTION and not _DB_CONNECTION.closed:
        _DB_CONNECTION.close()
    _DB_CONNECTION = None


# ============================================================
# DB 헬퍼 함수들 (중복 코드 제거)
# ============================================================


@contextmanager
def db_cursor(commit: bool = False):
    """DB 커서를 반환하는 컨텍스트 매니저.

    Args:
        commit: True면 작업 완료 후 자동 커밋

    Yields:
        cursor: DB 커서

    Example:
        with db_cursor() as cursor:
            cursor.execute("SELECT ...")
            return cursor.fetchone()
    """
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            yield cursor
        if commit:
            conn.commit()
    finally:
        release_connection(conn)


def _fetch_one(query: str, params: tuple = ()) -> Optional[Dict]:
    """단일 행을 조회합니다."""
    with db_cursor() as cursor:
        cursor.execute(query, params)
        row = cursor.fetchone()
        return dict(row) if row else None


def _fetch_all(query: str, params: tuple = ()) -> List[Dict]:
    """모든 행을 조회합니다."""
    with db_cursor() as cursor:
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def _exists(query: str, params: tuple = ()) -> bool:
    """레코드 존재 여부를 확인합니다."""
    with db_cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchone() is not None


def _execute(query: str, params: tuple = (), commit: bool = True) -> int:
    """쿼리를 실행하고 영향받은 행 수를 반환합니다."""
    with db_cursor(commit=commit) as cursor:
        cursor.execute(query, params)
        return cursor.rowcount


# 화이트리스트: SQL 인젝션 방지용 (헬퍼 함수에서 참조)
_VALID_COLLECTION_FIELDS = {'details_collected_at', 'reviews_collected_at'}
_VALID_TABLES = {'apps', 'apps_localized', 'apps_metrics', 'app_reviews', 'failed_apps', 'collection_status'}


def _validate_table(table: str) -> None:
    """테이블 이름을 화이트리스트로 검증합니다."""
    if table not in _VALID_TABLES:
        raise ValueError(f"Invalid table: {table}. Must be one of: {_VALID_TABLES}")


def _insert_returning(table: str, data: Dict, returning: str = 'id') -> Any:
    """INSERT 후 RETURNING 값을 반환합니다. 원본 data를 수정하지 않습니다."""
    _validate_table(table)

    # 원본 딕셔너리를 수정하지 않도록 복사
    data_copy = data.copy()
    data_copy['recorded_at'] = datetime.now().isoformat()

    columns = ', '.join(data_copy.keys())
    placeholders = ', '.join(['%s' for _ in data_copy])

    with db_cursor(commit=True) as cursor:
        cursor.execute(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING {returning}",
            list(data_copy.values())
        )
        return cursor.fetchone()[returning]


def _get_latest_record(table: str, app_id: str, platform: str,
                       extra_where: str = '', extra_params: tuple = ()) -> Optional[Dict]:
    """테이블에서 최신 레코드를 조회합니다."""
    _validate_table(table)

    where_clause = f"app_id = %s AND platform = %s{' AND ' + extra_where if extra_where else ''}"
    params = (app_id, platform) + extra_params

    return _fetch_one(f"""
        SELECT * FROM {table}
        WHERE {where_clause}
        ORDER BY recorded_at DESC
        LIMIT 1
    """, params)


def _insert_if_changed(table: str, data: Dict, existing: Optional[Dict]) -> Tuple[bool, int]:
    """기존 레코드와 비교 후 변경 시에만 삽입합니다.

    Returns:
        (is_new_record, record_id)
    """
    if existing and compare_records(existing, data):
        return False, existing['id']

    record_id = _insert_returning(table, data, 'id')
    return True, record_id


def init_database():
    """DB 테이블을 초기화합니다."""
    conn = get_connection()
    cursor = conn.cursor()

    # apps: 앱 메타데이터 (변경 시에만 누적)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS apps (
            id BIGSERIAL,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,             -- 'app_store' or 'play_store'
            bundle_id TEXT,
            version TEXT,
            developer TEXT,
            developer_id TEXT,
            developer_email TEXT,
            developer_website TEXT,
            icon_url TEXT,
            header_image TEXT,
            screenshots TEXT,                   -- JSON array
            price REAL,
            currency TEXT,
            free BOOLEAN,
            has_iap BOOLEAN,
            category_id TEXT,
            genre_id TEXT,
            genre_name TEXT,                    -- 장르명 (현지화)
            content_rating TEXT,
            content_rating_description TEXT,
            min_os_version TEXT,
            file_size BIGINT,
            supported_devices TEXT,             -- JSON array
            release_date TEXT,
            updated_date TEXT,
            privacy_policy_url TEXT,
            recorded_at TEXT NOT NULL,
            PRIMARY KEY (app_id, id)
        ) PARTITION BY HASH (app_id)
    """)

    for partition_index in range(PARTITION_COUNT):
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS apps_p{partition_index}
            PARTITION OF apps
            FOR VALUES WITH (MODULUS {PARTITION_COUNT}, REMAINDER {partition_index})
        """)

    # apps_localized: 다국어 텍스트 (변경 시에만 누적)
    # 최적화: title+description이 기준 언어와 동일하면 저장하지 않음
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS apps_localized (
            id BIGSERIAL,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            language TEXT NOT NULL,
            title TEXT,
            summary TEXT,
            description TEXT,
            release_notes TEXT,
            recorded_at TEXT NOT NULL,
            PRIMARY KEY (app_id, id)
        ) PARTITION BY HASH (app_id)
    """)

    for partition_index in range(PARTITION_COUNT):
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS apps_localized_p{partition_index}
            PARTITION OF apps_localized
            FOR VALUES WITH (MODULUS {PARTITION_COUNT}, REMAINDER {partition_index})
        """)

    # apps_metrics: 수치 데이터 (변경 시에만 누적)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS apps_metrics (
            id BIGSERIAL,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            score REAL,
            ratings INTEGER,
            reviews_count INTEGER,
            installs TEXT,                      -- "100,000+" 형태
            installs_exact INTEGER,             -- 정확한 수치 (Play Store)
            histogram TEXT,                     -- JSON array [1점, 2점, 3점, 4점, 5점]
            recorded_at TEXT NOT NULL,
            PRIMARY KEY (app_id, id)
        ) PARTITION BY HASH (app_id)
    """)

    for partition_index in range(PARTITION_COUNT):
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS apps_metrics_p{partition_index}
            PARTITION OF apps_metrics
            FOR VALUES WITH (MODULUS {PARTITION_COUNT}, REMAINDER {partition_index})
        """)

    # app_reviews: 리뷰 (실행당 최대 20000건 수집, 이후 누적)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app_reviews (
            id BIGSERIAL,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            review_id TEXT NOT NULL,            -- 외부 리뷰 ID
            country TEXT,
            language TEXT,
            user_name TEXT,
            user_image TEXT,
            score INTEGER,
            title TEXT,                         -- App Store만
            content TEXT,
            thumbs_up_count INTEGER,
            app_version TEXT,
            reviewed_at TEXT,
            reply_content TEXT,
            replied_at TEXT,
            recorded_at TEXT NOT NULL,
            UNIQUE(app_id, platform, review_id),
            PRIMARY KEY (app_id, id)
        ) PARTITION BY HASH (app_id)
    """)

    for remainder in range(APP_REVIEWS_PARTITION_COUNT):
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS app_reviews_p{remainder}
            PARTITION OF app_reviews
            FOR VALUES WITH (MODULUS {APP_REVIEWS_PARTITION_COUNT}, REMAINDER {remainder})
        """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS app_reviews_latest_idx
        ON app_reviews (app_id, platform, reviewed_at DESC)
    """)

    # failed_apps: 영구 실패 앱 (재시도 안 함)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS failed_apps (
            id BIGSERIAL PRIMARY KEY,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            reason TEXT,                        -- not_found, removed, etc.
            failed_at TEXT NOT NULL,
            UNIQUE(app_id, platform)
        )
    """)

    # collection_status: 수집 상태 추적
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS collection_status (
            id BIGSERIAL,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            details_collected_at TEXT,          -- 상세정보 마지막 수집 시각
            reviews_collected_at TEXT,          -- 리뷰 마지막 수집 시각
            reviews_total_count INTEGER DEFAULT 0,  -- 현재 수집된 총 리뷰 수
            initial_review_done INTEGER DEFAULT 0,  -- 최초 수집 완료 여부 (이후 중복 리뷰 발견 시 중단)
            UNIQUE(app_id, platform),
            PRIMARY KEY (app_id, id)
        ) PARTITION BY HASH (app_id)
    """)

    for partition_index in range(PARTITION_COUNT):
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS collection_status_p{partition_index}
            PARTITION OF collection_status
            FOR VALUES WITH (MODULUS {PARTITION_COUNT}, REMAINDER {partition_index})
        """)

    comment_sqls = [
        "COMMENT ON TABLE apps IS '앱 스토어/플레이 스토어 메타데이터의 변경 이력을 누적 저장하는 테이블로, 수집 원본 응답을 기록해 추후 비교/분석에 사용한다.'",
        "COMMENT ON COLUMN apps.id IS '내부 기본키로 각 수집 기록을 식별하며 조인/추적에 사용한다. BIGSERIAL 자동 생성이므로 NULL을 허용하지 않는다.'",
        "COMMENT ON COLUMN apps.app_id IS '스토어에서 제공하는 앱 고유 식별자(앱스토어 numeric ID 또는 플레이스토어 패키지명)로 수집 응답에서 가져오며 모든 조회의 기준 키이므로 NULL 불가.'",
        "COMMENT ON COLUMN apps.platform IS 'app_store 또는 play_store 구분값으로 수집 파이프라인의 소스 식별에 사용하며 필수 필드이므로 NULL 불가.'",
        "COMMENT ON COLUMN apps.bundle_id IS '앱 번들/패키지 식별자(주로 iOS bundle id)로 원본 응답에 존재할 때만 사용하며 일부 스토어에서 미제공될 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.version IS '스토어에 표시되는 앱 버전 문자열로 업데이트 이력 비교에 사용하며 원본에 없을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.developer IS '개발사/개발자명으로 스토어 응답에서 수집되어 화면 표시/필터링에 사용하며 미제공 가능성이 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.developer_id IS '스토어의 개발자 계정/퍼블리셔 식별자이며 개발자 기준 집계에 사용하지만 일부 스토어에서 제공되지 않아 NULL 허용.'",
        "COMMENT ON COLUMN apps.developer_email IS '개발자 연락 이메일로 스토어 메타데이터에서 수집되며 연락처 표시/검증에 사용하나 미기재 가능성이 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.developer_website IS '개발자 공식 웹사이트 URL로 스토어 응답에서 수집되며 외부 링크 제공에 사용하고 선택 항목이라 NULL 허용.'",
        "COMMENT ON COLUMN apps.icon_url IS '앱 아이콘 이미지 URL로 스토어 응답에서 수집되며 UI 표시용으로 사용하나 간혹 누락될 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.header_image IS '앱 상단 헤더 이미지 URL(주로 플레이스토어)로 수집 응답에서 가져오며 없는 경우가 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.screenshots IS '스크린샷 URL 목록(JSON 배열)으로 원본 응답에서 수집하여 UI 갤러리 표시에 사용하며 미제공 시 NULL 허용.'",
        "COMMENT ON COLUMN apps.price IS '현재 가격(숫자)으로 스토어 응답에서 수집하여 결제/가격 분석에 사용하며 무료만 제공되거나 누락될 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.currency IS '가격 통화 코드로 스토어 응답에서 수집되며 가격 표시/환산에 사용하나 가격 정보가 없으면 NULL 허용.'",
        "COMMENT ON COLUMN apps.free IS '무료 여부 플래그로 스토어 응답에서 추출하며 가격 분석에 사용하고 일부 응답에서 미제공될 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.has_iap IS '인앱결제 존재 여부 플래그로 스토어 응답에서 수집해 과금 분석에 사용하지만 제공되지 않을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.category_id IS '스토어 카테고리 ID로 원본 응답에서 수집되어 분류/필터링에 사용하며 스토어별 제공 방식 차이로 NULL 허용.'",
        "COMMENT ON COLUMN apps.genre_id IS '장르 ID로 스토어 응답에서 수집되어 분류에 사용하며 미제공 가능성이 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.genre_name IS '현지화된 장르명 문자열로 스토어 응답에서 수집되어 UI 표시/검색에 사용하며 없을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.content_rating IS '연령 등급 코드로 스토어에서 제공되며 연령 제한 표시/정책 분석에 사용하지만 일부 앱은 미제공되어 NULL 허용.'",
        "COMMENT ON COLUMN apps.content_rating_description IS '연령 등급 설명 문구로 스토어 응답에서 수집되며 상세 표시/분석에 사용하고 제공되지 않을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.min_os_version IS '지원 최소 OS 버전으로 스토어 메타데이터에서 수집되어 호환성 분석에 사용하나 누락될 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.file_size IS '설치 파일 크기(바이트)로 스토어 응답에서 수집되어 용량 분석에 사용하나 제공되지 않을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.supported_devices IS '지원 기기 목록(JSON 배열)으로 스토어 응답에서 수집되며 호환성 표시/분석에 사용하고 미제공 시 NULL 허용.'",
        "COMMENT ON COLUMN apps.release_date IS '최초 출시일로 스토어 응답에서 수집되어 연혁 분석에 사용하며 오래된 앱은 누락될 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.updated_date IS '마지막 업데이트 일자로 스토어 응답에서 수집되어 버려진 앱 판단 등에 사용하며 제공되지 않을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.privacy_policy_url IS '개인정보처리방침 URL로 스토어 응답에서 수집되어 법적 링크 제공에 사용하며 미기재 가능성이 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps.recorded_at IS '본 레코드를 수집/기록한 시각으로 변경 이력 정렬에 사용되며 시스템에서 항상 기록하므로 NULL 불가.'",
        "COMMENT ON TABLE apps_localized IS '앱의 다국어 텍스트(제목/설명/릴리즈 노트) 변경 이력을 누적 저장하는 테이블로, 현지화 비교 및 UI 표시용으로 사용한다.'",
        "COMMENT ON COLUMN apps_localized.id IS '내부 기본키로 다국어 레코드 식별 및 조인에 사용하며 자동 생성이므로 NULL 불가.'",
        "COMMENT ON COLUMN apps_localized.app_id IS '앱 고유 식별자로 원본 스토어 응답에서 수집되며 앱별 로컬라이즈 이력 조회에 필수이므로 NULL 불가.'",
        "COMMENT ON COLUMN apps_localized.platform IS '스토어 구분값(app_store/play_store)으로 수집 소스를 명확히 하기 위해 사용하며 NULL 불가.'",
        "COMMENT ON COLUMN apps_localized.language IS '언어 코드(예: en-US, ko-KR)로 스토어 응답에서 수집되어 언어별 표시/비교에 사용하며 NULL 불가.'",
        "COMMENT ON COLUMN apps_localized.title IS '로컬라이즈된 앱 제목으로 스토어 응답에서 수집되어 UI 표시에 사용하고 일부 언어에서 누락될 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps_localized.summary IS '로컬라이즈된 요약 문구로 스토어 응답에서 수집되어 목록 표시에 사용하며 제공되지 않을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps_localized.description IS '로컬라이즈된 상세 설명으로 스토어 응답에서 수집되어 상세 페이지에 사용하고 일부 언어에서 누락될 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps_localized.release_notes IS '로컬라이즈된 릴리즈 노트로 스토어 응답에서 수집되어 업데이트 내용 표시에 사용하며 없는 경우가 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps_localized.recorded_at IS '본 로컬라이즈 레코드의 수집 시각으로 이력 정렬에 사용되며 시스템에서 항상 기록하므로 NULL 불가.'",
        "COMMENT ON TABLE apps_metrics IS '앱의 평점/리뷰 수 등 수치 지표의 변경 이력을 저장하는 테이블로, 시계열 분석과 품질 평가에 사용한다.'",
        "COMMENT ON COLUMN apps_metrics.id IS '내부 기본키로 수치 레코드 식별 및 조인에 사용하며 자동 생성이므로 NULL 불가.'",
        "COMMENT ON COLUMN apps_metrics.app_id IS '앱 고유 식별자로 스토어 응답에서 수집되어 앱별 지표 조회에 필수이므로 NULL 불가.'",
        "COMMENT ON COLUMN apps_metrics.platform IS '스토어 구분값으로 수집 소스 식별에 사용하며 NULL 불가.'",
        "COMMENT ON COLUMN apps_metrics.score IS '평균 평점(예: 4.5)으로 스토어 응답에서 수집되어 품질 지표로 사용하고 제공되지 않을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps_metrics.ratings IS '평점 개수로 스토어 응답에서 수집되어 규모 파악에 사용하며 일부 응답에서 누락되어 NULL 허용.'",
        "COMMENT ON COLUMN apps_metrics.reviews_count IS '리뷰 총 개수로 스토어 응답에서 수집되어 수집 범위 판단에 사용하나 제공되지 않을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps_metrics.installs IS '설치 수 구간 문자열(예: \"100,000+\")로 플레이스토어에서 제공되며 마케팅 분석에 사용하고 앱스토어에는 없어 NULL 허용.'",
        "COMMENT ON COLUMN apps_metrics.installs_exact IS '설치 수 정확 값(플레이스토어 제공 시)으로 정밀 분석에 사용하며 제공되지 않을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps_metrics.histogram IS '별점 분포(JSON 배열 [1~5])로 스토어 응답에서 수집되어 평점 구조 분석에 사용하나 제공되지 않을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN apps_metrics.recorded_at IS '수치 데이터 수집 시각으로 변경 이력 분석에 사용되며 항상 기록하므로 NULL 불가.'",
        "COMMENT ON TABLE app_reviews IS '앱 리뷰 원문을 누적 저장하는 테이블로, 정성 분석/모니터링 및 중복 수집 방지에 사용한다.'",
        "COMMENT ON COLUMN app_reviews.id IS '내부 기본키로 리뷰 레코드 식별 및 조인에 사용하며 자동 생성이므로 NULL 불가.'",
        "COMMENT ON COLUMN app_reviews.app_id IS '앱 고유 식별자로 스토어 응답에서 수집되어 리뷰를 앱과 연결하는 핵심 키이므로 NULL 불가.'",
        "COMMENT ON COLUMN app_reviews.platform IS '스토어 구분값으로 리뷰 출처를 구분하는 데 사용하며 NULL 불가.'",
        "COMMENT ON COLUMN app_reviews.review_id IS '스토어가 부여한 리뷰 고유 ID로 중복 수집 방지에 사용하며 필수이므로 NULL 불가.'",
        "COMMENT ON COLUMN app_reviews.country IS '리뷰 작성 국가 코드로 스토어 응답에서 수집되어 국가별 분석에 사용하고 일부 리뷰는 미제공되어 NULL 허용.'",
        "COMMENT ON COLUMN app_reviews.language IS '리뷰 언어 코드로 스토어 응답에서 수집되어 언어별 분석에 사용하며 미제공 가능성이 있어 NULL 허용.'",
        "COMMENT ON COLUMN app_reviews.user_name IS '리뷰 작성자 표시명으로 스토어 응답에서 수집되어 UI 표시/분석에 사용하나 익명화될 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN app_reviews.user_image IS '리뷰 작성자 프로필 이미지 URL로 스토어 응답에서 수집되어 UI에 사용하며 제공되지 않을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN app_reviews.score IS '리뷰 평점(정수)로 스토어 응답에서 수집되어 품질 분석에 사용하며 일부 리뷰에서 누락될 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN app_reviews.title IS '리뷰 제목(App Store 전용)으로 스토어 응답에서 수집되어 표시/분석에 사용하고 Play Store에는 없어 NULL 허용.'",
        "COMMENT ON COLUMN app_reviews.content IS '리뷰 본문 텍스트로 스토어 응답에서 수집되어 감성/키워드 분석에 사용하며 비어 있을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN app_reviews.thumbs_up_count IS '도움돼요/좋아요 수로 스토어 응답에서 수집되어 영향력 분석에 사용하나 제공되지 않을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN app_reviews.app_version IS '리뷰 작성 시점의 앱 버전으로 스토어 응답에서 수집되어 버전별 이슈 분석에 사용하며 누락될 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN app_reviews.reviewed_at IS '리뷰 작성 시각으로 스토어 응답에서 수집되어 시계열 분석에 사용하나 일부 리뷰는 누락되어 NULL 허용.'",
        "COMMENT ON COLUMN app_reviews.reply_content IS '개발자 답변 내용으로 스토어 응답에서 수집되어 대응 분석에 사용하며 답변이 없으면 NULL 허용.'",
        "COMMENT ON COLUMN app_reviews.replied_at IS '개발자 답변 시각으로 스토어 응답에서 수집되어 응답 속도 분석에 사용하고 답변이 없으면 NULL 허용.'",
        "COMMENT ON COLUMN app_reviews.recorded_at IS '리뷰 레코드 수집 시각으로 중복 수집 관리에 사용되며 항상 기록하므로 NULL 불가.'",
        "COMMENT ON TABLE failed_apps IS '수집 불가로 판단된 앱을 영구 실패 목록으로 관리하는 테이블로, 반복 수집 시도를 방지하는 데 사용한다.'",
        "COMMENT ON COLUMN failed_apps.id IS '내부 기본키로 실패 레코드 식별에 사용하며 자동 생성이므로 NULL 불가.'",
        "COMMENT ON COLUMN failed_apps.app_id IS '실패한 앱의 고유 식별자로 스토어 응답/오류에서 추출되며 재시도 차단에 사용하므로 NULL 불가.'",
        "COMMENT ON COLUMN failed_apps.platform IS '스토어 구분값으로 실패 출처를 구분하며 NULL 불가.'",
        "COMMENT ON COLUMN failed_apps.reason IS '실패 사유(예: not_found, removed)로 수집 로직에서 기록되어 진단/리포트에 사용하며 정보가 없을 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN failed_apps.failed_at IS '실패 판정 시각으로 수집 로직에서 기록되어 이력 관리에 사용하며 NULL 불가.'",
        "COMMENT ON TABLE collection_status IS '앱별 상세/리뷰 수집 상태를 관리하는 테이블로, 수집 스케줄링과 중복 수집 방지에 사용한다.'",
        "COMMENT ON COLUMN collection_status.id IS '내부 기본키로 상태 레코드 식별에 사용하며 자동 생성이므로 NULL 불가.'",
        "COMMENT ON COLUMN collection_status.app_id IS '앱 고유 식별자로 상태를 앱에 매핑하기 위한 핵심 키이므로 NULL 불가.'",
        "COMMENT ON COLUMN collection_status.platform IS '스토어 구분값으로 수집 상태의 출처를 구분하며 NULL 불가.'",
        "COMMENT ON COLUMN collection_status.details_collected_at IS '상세정보 마지막 수집 시각으로 수집 스케줄 판단에 사용하며 아직 수집 전일 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN collection_status.reviews_collected_at IS '리뷰 마지막 수집 시각으로 수집 스케줄 판단에 사용하며 아직 수집 전일 수 있어 NULL 허용.'",
        "COMMENT ON COLUMN collection_status.reviews_total_count IS '현재까지 수집된 리뷰 총수로 진행률 계산에 사용하며 초기에는 0으로 유지되므로 NULL 허용하지 않고 기본값을 둔다.'",
        "COMMENT ON COLUMN collection_status.initial_review_done IS '최초 리뷰 수집 완료 여부(0/1)로 이후 중복 수집 중단 판단에 사용하며 기본값 0을 사용하므로 NULL 허용하지 않는다.'",
    ]

    for comment_sql in comment_sqls:
        cursor.execute(comment_sql)

    # 인덱스 생성
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_apps_latest ON apps(app_id, platform, recorded_at DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_apps_localized_latest ON apps_localized(app_id, platform, language, recorded_at DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_apps_metrics_latest ON apps_metrics(app_id, platform, recorded_at DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_app_reviews_app_id ON app_reviews(app_id, platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_app_reviews_review_id ON app_reviews(review_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_app_reviews_reviewed_at ON app_reviews(reviewed_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_failed_apps_app_id ON failed_apps(app_id, platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_collection_status_app_platform ON collection_status(app_id, platform)")

    conn.commit()
    release_connection(conn)
    DB_LOGGER.info("Database initialized.")


def normalize_json_field(value: Any) -> str:
    """JSON 필드를 정렬하여 문자열로 변환합니다."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    if isinstance(value, list):
        # 리스트 정렬 (순서 무관 비교)
        try:
            sorted_list = sorted(value, key=lambda x: json.dumps(x, sort_keys=True) if isinstance(x, dict) else str(x))
            return json.dumps(sorted_list, sort_keys=True, ensure_ascii=False)
        except TypeError:
            return json.dumps(value, sort_keys=True, ensure_ascii=False)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False)


def normalize_value_for_comparison(value):
    """비교를 위해 값을 정규화합니다."""
    if value is None:
        return None
    # 숫자 타입은 float로 통일하여 비교 (0 == 0.0)
    if isinstance(value, (int, float)):
        return float(value)
    return value


def compare_records(existing: Dict, new_data: Dict, exclude_fields: set = None) -> bool:
    """두 레코드를 비교합니다. 동일하면 True, 다르면 False."""
    if exclude_fields is None:
        exclude_fields = EXCLUDE_COMPARE_FIELDS

    for key, new_value in new_data.items():
        if key in exclude_fields:
            continue

        existing_value = existing.get(key)

        # JSON 필드 정규화
        if key in ('screenshots', 'supported_devices', 'histogram'):
            existing_value = normalize_json_field(existing_value)
            new_value = normalize_json_field(new_value)

        # None과 빈 문자열 동일 처리
        if existing_value in (None, '', 'null') and new_value in (None, '', 'null'):
            continue

        # 숫자 타입 정규화 (int/float 간 비교 문제 해결)
        existing_normalized = normalize_value_for_comparison(existing_value)
        new_normalized = normalize_value_for_comparison(new_value)

        # 둘 다 숫자면 직접 비교, 아니면 문자열 비교
        if isinstance(existing_normalized, float) and isinstance(new_normalized, float):
            if existing_normalized != new_normalized:
                return False
        elif str(existing_value) != str(new_value):
            return False

    return True


def get_latest_app(app_id: str, platform: str) -> Optional[Dict]:
    """앱의 최신 메타데이터를 반환합니다."""
    return _get_latest_record('apps', app_id, platform)


def insert_app(data: Dict) -> Tuple[bool, int]:
    """앱 메타데이터를 삽입합니다. 변경이 있을 때만 삽입.
    Returns: (is_new_record, record_id)
    """
    existing = get_latest_app(data['app_id'], data['platform'])
    return _insert_if_changed('apps', data, existing)


def get_latest_app_localized(app_id: str, platform: str, language: str) -> Optional[Dict]:
    """앱의 최신 다국어 데이터를 반환합니다."""
    return _get_latest_record('apps_localized', app_id, platform,
                               extra_where='language = %s', extra_params=(language,))


def insert_app_localized(data: Dict) -> Tuple[bool, int]:
    """앱 다국어 데이터를 삽입합니다. 변경이 있을 때만 삽입."""
    existing = get_latest_app_localized(data['app_id'], data['platform'], data['language'])
    return _insert_if_changed('apps_localized', data, existing)


def get_latest_app_metrics(app_id: str, platform: str) -> Optional[Dict]:
    """앱의 최신 수치 데이터를 반환합니다."""
    return _get_latest_record('apps_metrics', app_id, platform)


def insert_app_metrics(data: Dict) -> Tuple[bool, int]:
    """앱 수치 데이터를 삽입합니다. 변경이 있을 때만 삽입."""
    existing = get_latest_app_metrics(data['app_id'], data['platform'])
    return _insert_if_changed('apps_metrics', data, existing)


def review_exists(app_id: str, platform: str, review_id: str) -> bool:
    """리뷰가 이미 존재하는지 확인합니다."""
    return _exists(
        "SELECT 1 FROM app_reviews WHERE app_id = %s AND platform = %s AND review_id = %s",
        (app_id, platform, review_id)
    )


def insert_review(data: Dict) -> bool:
    """리뷰를 삽입합니다. 이미 존재하면 False 반환. 원본 data를 수정하지 않습니다."""
    data_copy = data.copy()
    data_copy['recorded_at'] = datetime.now().isoformat()

    columns = ', '.join(data_copy.keys())
    placeholders = ', '.join(['%s' for _ in data_copy])

    with db_cursor(commit=True) as cursor:
        cursor.execute(
            f"INSERT INTO app_reviews ({columns}) VALUES ({placeholders}) "
            "ON CONFLICT (app_id, platform, review_id) DO NOTHING",
            list(data_copy.values())
        )
        return cursor.rowcount > 0


def insert_reviews_batch(reviews: List[Dict]) -> int:
    """리뷰를 배치로 삽입합니다. 삽입된 개수 반환. 원본 데이터를 수정하지 않습니다."""
    if not reviews:
        return 0

    now = datetime.now().isoformat()
    inserted = 0

    with db_cursor(commit=True) as cursor:
        for data in reviews:
            data_copy = data.copy()
            data_copy['recorded_at'] = now

            columns = ', '.join(data_copy.keys())
            placeholders = ', '.join(['%s' for _ in data_copy])
            cursor.execute(
                f"INSERT INTO app_reviews ({columns}) VALUES ({placeholders}) "
                "ON CONFLICT (app_id, platform, review_id) DO NOTHING",
                list(data_copy.values())
            )
            if cursor.rowcount > 0:
                inserted += 1

    return inserted


def is_failed_app(app_id: str, platform: str) -> bool:
    """영구 실패 앱인지 확인합니다."""
    return _exists("SELECT 1 FROM failed_apps WHERE app_id = %s AND platform = %s",
                   (app_id, platform))


def mark_app_failed(app_id: str, platform: str, reason: str):
    """앱을 영구 실패로 표시합니다."""
    now = datetime.now().isoformat()
    _execute("""
        INSERT INTO failed_apps (app_id, platform, reason, failed_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (app_id, platform) DO NOTHING
    """, (app_id, platform, reason, now))


def get_collection_status(app_id: str, platform: str) -> Optional[Dict]:
    """수집 상태를 반환합니다."""
    return _fetch_one("SELECT * FROM collection_status WHERE app_id = %s AND platform = %s",
                      (app_id, platform))


def update_collection_status(app_id: str, platform: str,
                              details_collected: bool = False,
                              reviews_collected: bool = False,
                              reviews_count: int = None,
                              initial_review_done: bool = None):
    """수집 상태를 업데이트합니다."""
    now = datetime.now().isoformat()
    existing = get_collection_status(app_id, platform)

    with db_cursor(commit=True) as cursor:
        if existing:
            updates = []
            params = []

            if details_collected:
                updates.append("details_collected_at = %s")
                params.append(now)

            if reviews_collected:
                updates.append("reviews_collected_at = %s")
                params.append(now)

            if reviews_count is not None:
                updates.append("reviews_total_count = %s")
                params.append(reviews_count)

            if initial_review_done is not None:
                updates.append("initial_review_done = %s")
                params.append(1 if initial_review_done else 0)

            if updates:
                params.extend([app_id, platform])
                cursor.execute(f"""
                    UPDATE collection_status SET {', '.join(updates)}
                    WHERE app_id = %s AND platform = %s
                """, params)
        else:
            cursor.execute("""
                INSERT INTO collection_status (app_id, platform, details_collected_at, reviews_collected_at, reviews_total_count, initial_review_done)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                app_id, platform,
                now if details_collected else None,
                now if reviews_collected else None,
                reviews_count or 0,
                1 if initial_review_done else 0
            ))


def get_review_count(app_id: str, platform: str) -> int:
    """앱의 수집된 리뷰 수를 반환합니다."""
    result = _fetch_one("SELECT COUNT(*) as count FROM app_reviews WHERE app_id = %s AND platform = %s",
                        (app_id, platform))
    return result['count'] if result else 0


def get_latest_review_id(app_id: str, platform: str) -> Optional[str]:
    """가장 최근에 수집된 리뷰의 review_id를 반환합니다."""
    result = _fetch_one("""
        SELECT review_id FROM app_reviews
        WHERE app_id = %s AND platform = %s
        ORDER BY reviewed_at DESC
        LIMIT 1
    """, (app_id, platform))
    return result['review_id'] if result else None


def get_all_review_ids(app_id: str, platform: str) -> set:
    """앱의 모든 리뷰 ID를 반환합니다."""
    rows = _fetch_all("SELECT review_id FROM app_reviews WHERE app_id = %s AND platform = %s",
                      (app_id, platform))
    return {row['review_id'] for row in rows}


# ============================================================
# 버려진 앱 기준 (업계 표준 및 공식 정책 기반)
# - Pixalate: 2년 이상 업데이트 안 됨 = Abandoned
# - Google Play: 2년 이상 업데이트 안 됨 = 검색 제외/제거
# - Apple: 3년 이상 + 다운로드 극소 = 제거 대상
# 보수적으로 2년 기준 채택
# ============================================================
ABANDONED_THRESHOLD_DAYS = 730  # 2년
ABANDONED_COLLECTION_INTERVAL_DAYS = 7  # 버려진 앱 수집 주기


def parse_date(date_str: str) -> Optional[datetime]:
    """날짜 문자열을 파싱합니다."""
    if not date_str:
        return None
    try:
        if 'T' in date_str:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        else:
            dt = datetime.fromisoformat(date_str)
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except (ValueError, TypeError):
        return None


def get_failed_app_ids(platform: str) -> set:
    """실패한 앱 ID 목록을 반환합니다."""
    rows = _fetch_all("SELECT app_id FROM failed_apps WHERE platform = %s", (platform,))
    return {row['app_id'] for row in rows}


def get_abandoned_apps_to_skip(platform: str, collected_at_field: str) -> set:
    """7일 이내에 수집된 버려진 앱 ID를 반환합니다 (수집 건너뛸 대상).

    Args:
        platform: 'app_store' or 'play_store'
        collected_at_field: 'details_collected_at' or 'reviews_collected_at'

    Raises:
        ValueError: collected_at_field가 허용되지 않은 값인 경우
    """
    # SQL 인젝션 방지: 화이트리스트 검증
    if collected_at_field not in _VALID_COLLECTION_FIELDS:
        raise ValueError(f"Invalid collected_at_field: {collected_at_field}. "
                         f"Must be one of: {_VALID_COLLECTION_FIELDS}")

    # 7일 이내에 수집되었고, 2년 이상 업데이트 안 된 앱
    rows = _fetch_all(f"""
        SELECT cs.app_id
        FROM collection_status cs
        LEFT JOIN (
            SELECT app_id, platform, updated_date, release_date,
                   ROW_NUMBER() OVER (PARTITION BY app_id, platform ORDER BY recorded_at DESC) as rn
            FROM apps
        ) a ON cs.app_id = a.app_id AND cs.platform = a.platform AND a.rn = 1
        WHERE cs.platform = %s
          AND cs.{collected_at_field} IS NOT NULL
          AND cs.{collected_at_field} > (now() - interval '{ABANDONED_COLLECTION_INTERVAL_DAYS} days')
          AND (
              -- 2년 이상 업데이트 안 됨 (버려진 앱)
              (a.updated_date IS NOT NULL AND a.updated_date::date < (now() - interval '{ABANDONED_THRESHOLD_DAYS} days')::date)
              OR (a.updated_date IS NULL AND a.release_date IS NOT NULL AND a.release_date::date < (now() - interval '{ABANDONED_THRESHOLD_DAYS} days')::date)
          )
    """, (platform,))

    return {row['app_id'] for row in rows}


def get_stats() -> Dict[str, Any]:
    """DB 통계를 반환합니다."""
    stats = {}

    # 각 테이블 레코드 수 (하드코딩된 테이블명 - SQL 인젝션 위험 없음)
    with db_cursor() as cursor:
        for table in _VALID_TABLES:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            stats[table] = cursor.fetchone()['count']

        # 플랫폼별 앱 수 (unique)
        cursor.execute("""
            SELECT platform, COUNT(DISTINCT app_id) as count FROM apps GROUP BY platform
        """)
        stats['apps_by_platform'] = {row['platform']: row['count'] for row in cursor.fetchall()}

        # 플랫폼별 리뷰 수
        cursor.execute("""
            SELECT platform, COUNT(*) as count FROM app_reviews GROUP BY platform
        """)
        stats['reviews_by_platform'] = {row['platform']: row['count'] for row in cursor.fetchall()}

    return stats


if __name__ == '__main__':
    init_database()
    DB_LOGGER.info("Database schema created successfully.")
    stats = get_stats()
    DB_LOGGER.info(f"Stats: {stats}")
