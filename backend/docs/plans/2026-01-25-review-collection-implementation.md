# 리뷰 수집 시스템 재설계 구현 계획

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 리뷰 수집 시스템의 IP 자동 감지, 에러 분류, 변경 감지 기반 수집 기능 구현

**Architecture:** core/ 모듈에 IP 관리와 HTTP 클라이언트를 구현하고, 기존 수집기들이 이를 사용하도록 수정. 새로운 review_collection_status 테이블로 수집 상태와 에러를 추적.

**Tech Stack:** Python 3.12, PostgreSQL, psycopg, requests, google-play-scraper

---

## Task 1: 데이터베이스 스키마 생성

**Files:**
- Create: `database/review_collection_db.py`
- Create: `tests/test_review_collection_db.py`

**Step 1: review_collection_status 테이블 생성 함수 작성**

```python
# database/review_collection_db.py
"""
리뷰 수집 상태 관리 데이터베이스 모듈

이 모듈은 리뷰 수집의 상태, 실패 원인, 변경 감지를 위한
review_collection_status 테이블을 관리합니다.
"""
import os
from datetime import datetime
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row


# =============================================================================
# 데이터베이스 연결 설정
# =============================================================================

# 환경 변수에서 DB 연결 정보 로드
DB_HOST = os.getenv("APP_DETAILS_DB_HOST", "localhost")
DB_PORT = int(os.getenv("APP_DETAILS_DB_PORT", "5432"))
DB_NAME = os.getenv("APP_DETAILS_DB_NAME", "app_details")
DB_USER = os.getenv("APP_DETAILS_DB_USER", "app_details")
DB_PASSWORD = os.getenv("APP_DETAILS_DB_PASSWORD", "")

# 연결 풀 (싱글톤)
_connection: Optional[psycopg.Connection] = None


def _build_dsn() -> str:
    """데이터베이스 연결 문자열(DSN)을 생성합니다."""
    dsn = os.getenv("APP_DETAILS_DB_DSN")
    if dsn:
        return dsn
    return f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"


def get_connection() -> psycopg.Connection:
    """
    데이터베이스 연결을 반환합니다.

    싱글톤 패턴으로 연결을 재사용합니다.
    연결이 끊어진 경우 자동으로 재연결합니다.
    """
    global _connection
    if _connection is None or _connection.closed:
        _connection = psycopg.connect(_build_dsn(), row_factory=dict_row)
    return _connection


def release_connection(conn: psycopg.Connection) -> None:
    """연결을 해제합니다 (현재는 재사용을 위해 유지)."""
    pass  # 싱글톤이므로 실제로 닫지 않음


@contextmanager
def db_cursor():
    """
    데이터베이스 커서를 제공하는 컨텍스트 매니저.

    자동으로 커밋하고, 예외 발생 시 롤백합니다.

    Usage:
        with db_cursor() as cursor:
            cursor.execute("SELECT * FROM ...")
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
# 테이블 초기화
# =============================================================================

def init_review_collection_tables() -> None:
    """
    리뷰 수집 관련 테이블을 생성합니다.

    생성되는 테이블:
    - review_collection_status: 앱별 리뷰 수집 상태 추적
    - ip_store_mapping: IP-스토어 매핑 캐시 (선택적)
    """
    with db_cursor() as cursor:
        # review_collection_status 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS review_collection_status (
                -- 식별자
                app_id TEXT NOT NULL,
                platform TEXT NOT NULL,

                -- 시간 추적
                last_attempt_at TIMESTAMPTZ,          -- 마지막 수집 시도 시간
                last_success_at TIMESTAMPTZ,          -- 마지막 성공 시간

                -- 변경 감지
                last_known_store_review_count INTEGER, -- 스토어의 전체 리뷰 수 (지난번)
                collected_review_count INTEGER DEFAULT 0, -- 실제 수집한 리뷰 수

                -- 실패 추적
                last_failure_reason TEXT,              -- 실패 원인 코드
                last_failure_detail TEXT,              -- 상세 에러 메시지
                consecutive_failures INTEGER DEFAULT 0, -- 연속 실패 횟수

                -- 수집 한계
                collection_limited BOOLEAN DEFAULT FALSE, -- API 한계 도달 여부
                collection_limited_reason TEXT,           -- 한계 원인

                -- 메타
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),

                PRIMARY KEY (app_id, platform)
            )
        """)

        # 인덱스 생성
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rcs_platform_failure
            ON review_collection_status (platform, last_failure_reason)
            WHERE last_failure_reason IS NOT NULL
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rcs_consecutive_failures
            ON review_collection_status (consecutive_failures DESC)
            WHERE consecutive_failures > 0
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rcs_last_attempt
            ON review_collection_status (platform, last_attempt_at DESC)
        """)

        # ip_store_mapping 테이블 (선택적)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ip_store_mapping (
                ip_address TEXT NOT NULL,
                platform TEXT NOT NULL,

                is_working BOOLEAN NOT NULL,
                last_tested_at TIMESTAMPTZ NOT NULL,
                last_error TEXT,

                PRIMARY KEY (ip_address, platform)
            )
        """)
```

**Step 2: 테스트 파일 생성**

```python
# tests/test_review_collection_db.py
"""
review_collection_db 모듈 테스트
"""
import pytest
from database.review_collection_db import (
    init_review_collection_tables,
    get_connection,
    db_cursor,
)


class TestReviewCollectionDB:
    """review_collection_status 테이블 테스트"""

    def test_init_creates_tables(self):
        """테이블 생성 테스트"""
        # Given: 데이터베이스 연결
        init_review_collection_tables()

        # When: 테이블 존재 확인
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'review_collection_status'
                )
            """)
            result = cursor.fetchone()

        # Then: 테이블이 존재해야 함
        assert result['exists'] is True

    def test_init_creates_ip_store_mapping(self):
        """ip_store_mapping 테이블 생성 테스트"""
        init_review_collection_tables()

        with db_cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'ip_store_mapping'
                )
            """)
            result = cursor.fetchone()

        assert result['exists'] is True
```

**Step 3: 테스트 실행**

```bash
cd /home/ubuntu/newapps/backend
python -m pytest tests/test_review_collection_db.py -v
```

**Step 4: 커밋**

```bash
git add database/review_collection_db.py tests/test_review_collection_db.py
git commit -m "feat: add review_collection_status table schema"
```

---

## Task 2: 수집 상태 CRUD 함수 구현

**Files:**
- Modify: `database/review_collection_db.py`
- Modify: `tests/test_review_collection_db.py`

**Step 1: 상태 조회/저장 함수 추가**

```python
# database/review_collection_db.py (하단에 추가)

# =============================================================================
# 에러 코드 상수
# =============================================================================

class ErrorCode:
    """
    리뷰 수집 실패 원인 코드

    각 코드는 재시도 전략을 결정하는 데 사용됩니다.
    """
    IP_BLOCKED = "IP_BLOCKED"           # IP 차단 (403) → 다른 IP로 재시도
    RATE_LIMITED = "RATE_LIMITED"       # 요청 과다 (429) → 백오프 후 재시도
    NETWORK_ERROR = "NETWORK_ERROR"     # 네트워크 오류 → 다음 실행 시 재시도
    SERVER_ERROR = "SERVER_ERROR"       # 서버 오류 (5xx) → 다음 실행 시 재시도
    APP_NOT_FOUND = "APP_NOT_FOUND"     # 앱 삭제 → 재시도 안 함 (영구)
    NO_REVIEWS = "NO_REVIEWS"           # 리뷰 없음 → 변경 시만 재시도
    API_LIMIT_REACHED = "API_LIMIT_REACHED"  # API 한계 → 정상 완료
    PARSE_ERROR = "PARSE_ERROR"         # 파싱 실패 → 다음 실행 시 재시도
    NO_AVAILABLE_IP = "NO_AVAILABLE_IP" # 사용 가능 IP 없음


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

    Example:
        >>> status = get_review_collection_status('284882215', 'app_store')
        >>> print(status['last_known_store_review_count'])
        50000
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

    Args:
        app_id: 앱 ID
        platform: 플랫폼

    Returns:
        수집된 리뷰 수
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

    None인 필드는 업데이트하지 않습니다 (기존 값 유지).

    Args:
        app_id: 앱 ID
        platform: 플랫폼
        last_attempt_at: 마지막 시도 시간
        last_success_at: 마지막 성공 시간
        last_known_store_review_count: 스토어 전체 리뷰 수
        collected_review_count: 실제 수집한 리뷰 수
        last_failure_reason: 실패 원인 코드
        last_failure_detail: 상세 에러 메시지
        consecutive_failures: 연속 실패 횟수
        collection_limited: API 한계 도달 여부
        collection_limited_reason: 한계 원인
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
            # UPDATE 값
            last_attempt_at, last_success_at,
            last_known_store_review_count, collected_review_count,
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

    성공 시 실패 관련 필드를 초기화합니다.

    Args:
        app_id: 앱 ID
        platform: 플랫폼
        store_review_count: 스토어의 전체 리뷰 수
        collected_count: 이번에 수집한 리뷰 수
        collection_limited: API 한계 도달 여부
        limited_reason: 한계 원인 (예: 'RSS_PAGE_LIMIT')
    """
    now = datetime.now()
    total_collected = get_total_collected_count(app_id, platform)

    upsert_review_collection_status(
        app_id=app_id,
        platform=platform,
        last_attempt_at=now,
        last_success_at=now,
        last_known_store_review_count=store_review_count,
        collected_review_count=total_collected,
        last_failure_reason=None,  # 성공 시 초기화
        last_failure_detail=None,
        consecutive_failures=0,    # 성공 시 초기화
        collection_limited=collection_limited,
        collection_limited_reason=limited_reason,
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

    연속 실패 횟수를 증가시킵니다.

    Args:
        app_id: 앱 ID
        platform: 플랫폼
        store_review_count: 스토어의 전체 리뷰 수
        failure_reason: 실패 원인 코드 (ErrorCode 상수)
        failure_detail: 상세 에러 메시지

    Returns:
        업데이트된 연속 실패 횟수
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

    return new_failures
```

**Step 2: 테스트 추가**

```python
# tests/test_review_collection_db.py (추가)

from database.review_collection_db import (
    init_review_collection_tables,
    get_review_collection_status,
    upsert_review_collection_status,
    record_collection_success,
    record_collection_failure,
    ErrorCode,
    db_cursor,
)
from datetime import datetime


class TestReviewCollectionStatus:
    """수집 상태 CRUD 테스트"""

    @pytest.fixture(autouse=True)
    def setup(self):
        """각 테스트 전 테이블 초기화"""
        init_review_collection_tables()
        # 테스트 데이터 정리
        with db_cursor() as cursor:
            cursor.execute("""
                DELETE FROM review_collection_status
                WHERE app_id LIKE 'test_%'
            """)

    def test_get_nonexistent_returns_none(self):
        """존재하지 않는 앱 조회 시 None 반환"""
        result = get_review_collection_status('test_nonexistent', 'app_store')
        assert result is None

    def test_upsert_creates_new_record(self):
        """새 레코드 생성 테스트"""
        upsert_review_collection_status(
            app_id='test_app_1',
            platform='app_store',
            last_known_store_review_count=1000,
        )

        result = get_review_collection_status('test_app_1', 'app_store')
        assert result is not None
        assert result['last_known_store_review_count'] == 1000

    def test_upsert_updates_existing_record(self):
        """기존 레코드 업데이트 테스트"""
        # 첫 삽입
        upsert_review_collection_status(
            app_id='test_app_2',
            platform='app_store',
            last_known_store_review_count=1000,
        )

        # 업데이트
        upsert_review_collection_status(
            app_id='test_app_2',
            platform='app_store',
            last_known_store_review_count=1500,
        )

        result = get_review_collection_status('test_app_2', 'app_store')
        assert result['last_known_store_review_count'] == 1500

    def test_record_success_resets_failures(self):
        """성공 기록 시 실패 카운터 초기화"""
        # 먼저 실패 기록
        record_collection_failure(
            app_id='test_app_3',
            platform='app_store',
            store_review_count=1000,
            failure_reason=ErrorCode.RATE_LIMITED,
        )

        status = get_review_collection_status('test_app_3', 'app_store')
        assert status['consecutive_failures'] == 1

        # 성공 기록
        record_collection_success(
            app_id='test_app_3',
            platform='app_store',
            store_review_count=1000,
            collected_count=100,
        )

        status = get_review_collection_status('test_app_3', 'app_store')
        assert status['consecutive_failures'] == 0
        assert status['last_failure_reason'] is None

    def test_record_failure_increments_counter(self):
        """실패 기록 시 카운터 증가"""
        for i in range(3):
            failures = record_collection_failure(
                app_id='test_app_4',
                platform='play_store',
                store_review_count=500,
                failure_reason=ErrorCode.NETWORK_ERROR,
            )
            assert failures == i + 1

        status = get_review_collection_status('test_app_4', 'play_store')
        assert status['consecutive_failures'] == 3
```

**Step 3: 테스트 실행**

```bash
python -m pytest tests/test_review_collection_db.py -v
```

**Step 4: 커밋**

```bash
git add database/review_collection_db.py tests/test_review_collection_db.py
git commit -m "feat: add review collection status CRUD functions"
```

---

## Task 3: 수집 판단 함수 구현

**Files:**
- Modify: `database/review_collection_db.py`
- Modify: `tests/test_review_collection_db.py`

**Step 1: 수집 여부 판단 함수 추가**

```python
# database/review_collection_db.py (하단에 추가)

from enum import Enum


class CollectionMode(Enum):
    """
    리뷰 수집 모드

    INITIAL: 첫 수집 - 최대한 많이 수집 (API 한계까지)
    INCREMENTAL: 증분 수집 - 기존 review_id 만나면 중단
    """
    INITIAL = "initial"
    INCREMENTAL = "incremental"


def should_collect_reviews(
    app_id: str,
    platform: str,
    new_store_count: int,
) -> tuple[bool, Optional[CollectionMode], Optional[str]]:
    """
    리뷰 수집 여부를 결정합니다.

    판단 기준:
    1. 첫 수집 (상태 없음) → 수집 (INITIAL)
    2. 영구 실패 앱 → 스킵
    3. 스토어 리뷰 수 증가 → 수집 (INCREMENTAL)
    4. 스토어 리뷰 수 0 → 스킵
    5. 변화 없음 → 스킵

    Args:
        app_id: 앱 ID
        platform: 플랫폼
        new_store_count: 새로 확인한 스토어 전체 리뷰 수

    Returns:
        (should_collect, mode, skip_reason) 튜플
        - should_collect: 수집 여부
        - mode: 수집 모드 (CollectionMode) 또는 None
        - skip_reason: 스킵 사유 또는 None

    Example:
        >>> should, mode, reason = should_collect_reviews('12345', 'app_store', 5000)
        >>> if should:
        ...     collect_reviews(app_id, mode)
    """
    # 상태 조회
    status = get_review_collection_status(app_id, platform)

    # 1. 첫 수집
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
    """
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT is_permanent FROM failed_apps
            WHERE app_id = %s AND platform = %s
        """, (app_id, platform))
        result = cursor.fetchone()
        return result and result.get('is_permanent', False)
```

**Step 2: 테스트 추가**

```python
# tests/test_review_collection_db.py (추가)

from database.review_collection_db import (
    should_collect_reviews,
    CollectionMode,
)


class TestShouldCollectReviews:
    """수집 판단 로직 테스트"""

    @pytest.fixture(autouse=True)
    def setup(self):
        init_review_collection_tables()
        with db_cursor() as cursor:
            cursor.execute("DELETE FROM review_collection_status WHERE app_id LIKE 'test_%'")

    def test_first_collection_returns_initial_mode(self):
        """첫 수집은 INITIAL 모드"""
        should, mode, reason = should_collect_reviews('test_new_app', 'app_store', 1000)

        assert should is True
        assert mode == CollectionMode.INITIAL
        assert reason is None

    def test_increased_count_returns_incremental_mode(self):
        """리뷰 수 증가 시 INCREMENTAL 모드"""
        # 기존 상태 설정
        upsert_review_collection_status(
            app_id='test_existing',
            platform='app_store',
            last_known_store_review_count=1000,
        )

        should, mode, reason = should_collect_reviews('test_existing', 'app_store', 1500)

        assert should is True
        assert mode == CollectionMode.INCREMENTAL
        assert reason is None

    def test_no_change_returns_skip(self):
        """변화 없으면 스킵"""
        upsert_review_collection_status(
            app_id='test_unchanged',
            platform='app_store',
            last_known_store_review_count=1000,
        )

        should, mode, reason = should_collect_reviews('test_unchanged', 'app_store', 1000)

        assert should is False
        assert mode is None
        assert reason == "no_change"

    def test_zero_reviews_returns_skip(self):
        """리뷰 0건이면 스킵"""
        should, mode, reason = should_collect_reviews('test_no_reviews', 'app_store', 0)

        assert should is False
        assert reason == "no_reviews_on_store"
```

**Step 3: 테스트 실행**

```bash
python -m pytest tests/test_review_collection_db.py::TestShouldCollectReviews -v
```

**Step 4: 커밋**

```bash
git add database/review_collection_db.py tests/test_review_collection_db.py
git commit -m "feat: add should_collect_reviews decision function"
```

---

## Task 4: IP Manager 구현

**Files:**
- Create: `core/__init__.py`
- Create: `core/ip_manager.py`
- Create: `tests/test_ip_manager.py`

**Step 1: core 패키지 및 IPManager 생성**

```python
# core/__init__.py
"""
Core 모듈

리뷰 수집 시스템의 핵심 기능을 제공합니다:
- ip_manager: IP 자동 감지 및 스토어별 할당
- http_client: IP 바인딩 HTTP 클라이언트
- error_classifier: 에러 분류
"""
```

```python
# core/ip_manager.py
"""
IP 자동 감지 및 스토어별 할당 관리

서버의 모든 외부 IP를 감지하고, 각 스토어 API에 대해
접근 가능한 IP를 테스트하여 매핑합니다.

사용 예시:
    ip_manager = IPManager()
    store_ips = ip_manager.initialize()
    # {'app_store': ['172.31.40.115'], 'play_store': ['172.31.47.39', '172.31.40.115']}
"""
import socket
import subprocess
import logging
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass, field

import requests


# =============================================================================
# 설정
# =============================================================================

# 스토어별 테스트 엔드포인트
# 각 스토어의 접근성을 테스트하기 위한 URL
TEST_ENDPOINTS = {
    'app_store': 'https://itunes.apple.com/us/rss/customerreviews/page=1/id=284882215/sortBy=mostRecent/json',
    'play_store': 'https://play.google.com/store/apps/details?id=com.whatsapp&hl=en&gl=us',
}

# 테스트 타임아웃 (초)
TEST_TIMEOUT = 10

# User-Agent 헤더
DEFAULT_USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'

# 로거
logger = logging.getLogger(__name__)


# =============================================================================
# 데이터 클래스
# =============================================================================

@dataclass
class IPTestResult:
    """
    IP 테스트 결과

    Attributes:
        ip: 테스트한 IP 주소
        platform: 테스트한 스토어
        is_working: 정상 동작 여부
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
# IPManager 클래스
# =============================================================================

class IPManager:
    """
    서버 IP 자동 감지 및 스토어별 할당 관리

    주요 기능:
    1. 서버의 모든 외부 IP 자동 감지
    2. 각 스토어 엔드포인트에 IP별 접근 테스트
    3. 동작하는 IP를 스토어별로 매핑
    4. 수집 시 적절한 IP 제공

    Attributes:
        available_ips: 서버에서 사용 가능한 IP 목록
        store_ip_map: 스토어별 동작하는 IP 목록
        test_results: 테스트 결과 기록
        last_initialized_at: 마지막 초기화 시간

    Example:
        >>> manager = IPManager()
        >>> store_ips = manager.initialize()
        >>> print(store_ips)
        {'app_store': ['172.31.40.115'], 'play_store': ['172.31.47.39']}

        >>> ip = manager.get_ip_for_store('app_store')
        >>> print(ip)
        '172.31.40.115'
    """

    def __init__(self, test_endpoints: Optional[Dict[str, str]] = None):
        """
        IPManager 초기화

        Args:
            test_endpoints: 스토어별 테스트 URL (기본값 사용 시 None)
        """
        self.test_endpoints = test_endpoints or TEST_ENDPOINTS
        self.available_ips: List[str] = []
        self.store_ip_map: Dict[str, List[str]] = {}
        self.test_results: List[IPTestResult] = []
        self.last_initialized_at: Optional[datetime] = None

    def discover_ips(self) -> List[str]:
        """
        서버에서 사용 가능한 모든 외부 IP를 감지합니다.

        `hostname -I` 명령어를 사용하여 IP를 가져옵니다.
        로컬호스트(127.0.0.1)와 IPv6 주소는 제외합니다.

        Returns:
            외부 IP 주소 목록

        Example:
            >>> manager = IPManager()
            >>> ips = manager.discover_ips()
            >>> print(ips)
            ['172.31.47.39', '172.31.40.115']
        """
        try:
            result = subprocess.run(
                ['hostname', '-I'],
                capture_output=True,
                text=True,
                timeout=5
            )

            if result.returncode != 0:
                logger.warning(f"hostname -I 실패: {result.stderr}")
                return []

            # 공백으로 분리하고 필터링
            all_ips = result.stdout.strip().split()

            # IPv4만 필터링 (IPv6 및 로컬호스트 제외)
            external_ips = [
                ip for ip in all_ips
                if self._is_valid_external_ipv4(ip)
            ]

            self.available_ips = external_ips
            logger.info(f"감지된 IP: {external_ips}")

            return external_ips

        except subprocess.TimeoutExpired:
            logger.error("hostname 명령 타임아웃")
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
        """
        if ':' in ip:  # IPv6
            return False
        if ip.startswith('127.'):  # 로컬호스트
            return False

        # IPv4 형식 검증
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
        정상 응답(200)과 유효한 콘텐츠를 확인합니다.

        Args:
            ip: 테스트할 IP 주소
            store: 스토어 이름 ('app_store' 또는 'play_store')

        Returns:
            IPTestResult 객체
        """
        url = self.test_endpoints.get(store)
        if not url:
            return IPTestResult(
                ip=ip,
                platform=store,
                is_working=False,
                error=f"Unknown store: {store}"
            )

        try:
            # 소스 IP 바인딩을 위한 커스텀 어댑터
            session = requests.Session()

            # 소스 IP 바인딩
            source_adapter = SourceAddressAdapter(ip)
            session.mount('http://', source_adapter)
            session.mount('https://', source_adapter)

            response = session.get(
                url,
                timeout=TEST_TIMEOUT,
                headers={'User-Agent': DEFAULT_USER_AGENT}
            )

            # 응답 검증
            is_working = (
                response.status_code == 200 and
                len(response.content) > 100  # 의미 있는 응답인지
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
                ip=ip, platform=store, is_working=False, error="Timeout"
            )
        except requests.exceptions.ConnectionError as e:
            result = IPTestResult(
                ip=ip, platform=store, is_working=False, error=f"Connection error: {e}"
            )
        except Exception as e:
            result = IPTestResult(
                ip=ip, platform=store, is_working=False, error=str(e)
            )

        self.test_results.append(result)

        status = "OK" if result.is_working else f"FAIL ({result.error})"
        logger.debug(f"IP 테스트: {ip} -> {store}: {status}")

        return result

    def initialize(self) -> Dict[str, List[str]]:
        """
        모든 IP를 감지하고 각 스토어에 대해 테스트합니다.

        파이프라인 시작 시 한 번 호출하여 IP 매핑을 설정합니다.

        Returns:
            스토어별 동작하는 IP 목록
            예: {'app_store': ['172.31.40.115'], 'play_store': ['172.31.47.39']}
        """
        logger.info("IP 자동 감지 및 스토어 테스트 시작...")

        # 1. IP 감지
        ips = self.discover_ips()

        if not ips:
            logger.warning("사용 가능한 IP가 없습니다!")
            return {}

        # 2. 각 스토어에 대해 테스트
        self.store_ip_map = {}

        for store in self.test_endpoints:
            working_ips = []

            for ip in ips:
                result = self.test_ip_for_store(ip, store)
                if result.is_working:
                    working_ips.append(ip)

            self.store_ip_map[store] = working_ips

            if working_ips:
                logger.info(f"{store}: 동작 IP = {working_ips}")
            else:
                logger.warning(f"{store}: 동작하는 IP 없음!")

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

        Args:
            store: 스토어 이름
            exclude: 제외할 IP 목록 (이번 요청에서 실패한 IP)

        Returns:
            사용 가능한 IP 또는 None
        """
        exclude = exclude or []

        candidates = [
            ip for ip in self.store_ip_map.get(store, [])
            if ip not in exclude
        ]

        return candidates[0] if candidates else None

    def get_all_ips_for_store(self, store: str) -> List[str]:
        """스토어에 사용 가능한 모든 IP 목록을 반환합니다."""
        return self.store_ip_map.get(store, []).copy()


# =============================================================================
# 소스 IP 바인딩 어댑터
# =============================================================================

class SourceAddressAdapter(requests.adapters.HTTPAdapter):
    """
    특정 소스 IP로 바인딩하는 HTTP 어댑터

    requests 라이브러리에서 특정 네트워크 인터페이스(IP)를
    사용하도록 강제합니다.
    """

    def __init__(self, source_address: str, *args, **kwargs):
        """
        Args:
            source_address: 소스 IP 주소 (예: '172.31.40.115')
        """
        self.source_address = source_address
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        """소스 주소를 설정하여 풀 매니저 초기화"""
        kwargs['source_address'] = (self.source_address, 0)
        super().init_poolmanager(*args, **kwargs)
```

**Step 2: 테스트 파일 생성**

```python
# tests/test_ip_manager.py
"""
IPManager 테스트
"""
import pytest
from unittest.mock import patch, MagicMock
from core.ip_manager import IPManager, IPTestResult


class TestIPManager:
    """IPManager 클래스 테스트"""

    def test_discover_ips_parses_hostname_output(self):
        """hostname -I 출력 파싱 테스트"""
        manager = IPManager()

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='172.31.47.39 172.31.40.115 \n'
            )

            ips = manager.discover_ips()

        assert ips == ['172.31.47.39', '172.31.40.115']

    def test_discover_ips_excludes_localhost(self):
        """로컬호스트 IP 제외 테스트"""
        manager = IPManager()

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='127.0.0.1 172.31.47.39\n'
            )

            ips = manager.discover_ips()

        assert '127.0.0.1' not in ips
        assert '172.31.47.39' in ips

    def test_discover_ips_excludes_ipv6(self):
        """IPv6 주소 제외 테스트"""
        manager = IPManager()

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='172.31.47.39 fe80::1 ::1\n'
            )

            ips = manager.discover_ips()

        assert ips == ['172.31.47.39']

    def test_is_valid_external_ipv4(self):
        """IPv4 검증 테스트"""
        manager = IPManager()

        assert manager._is_valid_external_ipv4('172.31.47.39') is True
        assert manager._is_valid_external_ipv4('192.168.1.1') is True
        assert manager._is_valid_external_ipv4('127.0.0.1') is False  # localhost
        assert manager._is_valid_external_ipv4('::1') is False  # IPv6
        assert manager._is_valid_external_ipv4('invalid') is False

    def test_get_ip_for_store_excludes_failed(self):
        """실패한 IP 제외 테스트"""
        manager = IPManager()
        manager.store_ip_map = {
            'app_store': ['172.31.47.39', '172.31.40.115']
        }

        # 첫 번째 IP 제외
        ip = manager.get_ip_for_store('app_store', exclude=['172.31.47.39'])
        assert ip == '172.31.40.115'

    def test_get_ip_for_store_returns_none_when_all_excluded(self):
        """모든 IP 제외 시 None 반환"""
        manager = IPManager()
        manager.store_ip_map = {
            'app_store': ['172.31.47.39']
        }

        ip = manager.get_ip_for_store('app_store', exclude=['172.31.47.39'])
        assert ip is None


class TestIPManagerIntegration:
    """IPManager 통합 테스트 (실제 네트워크 사용)"""

    @pytest.mark.integration
    def test_initialize_with_real_network(self):
        """실제 네트워크로 초기화 테스트"""
        manager = IPManager()
        store_ips = manager.initialize()

        # 최소 하나의 IP가 감지되어야 함
        assert len(manager.available_ips) > 0

        # 결과가 딕셔너리여야 함
        assert isinstance(store_ips, dict)
```

**Step 3: 테스트 실행**

```bash
python -m pytest tests/test_ip_manager.py -v -k "not integration"
```

**Step 4: 커밋**

```bash
git add core/__init__.py core/ip_manager.py tests/test_ip_manager.py
git commit -m "feat: add IPManager for auto IP detection"
```

---

## Task 5: HTTP 클라이언트 구현

**Files:**
- Create: `core/http_client.py`
- Create: `tests/test_http_client.py`

**Step 1: StoreHttpClient 구현**

```python
# core/http_client.py
"""
스토어별 IP 바인딩 HTTP 클라이언트

주요 기능:
- 스토어별 IP 자동 바인딩
- HTTP 에러 코드 분류
- Rate Limit 백오프 재시도
- IP 차단 시 대체 IP 사용

사용 예시:
    ip_manager = IPManager()
    ip_manager.initialize()

    client = StoreHttpClient(ip_manager)
    result = client.request(url, 'app_store')

    if result.success:
        data = result.data
    else:
        print(f"Error: {result.error_code}")
"""
import time
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

import requests

from core.ip_manager import IPManager, SourceAddressAdapter


# =============================================================================
# 설정
# =============================================================================

# User-Agent 헤더 (봇 차단 방지)
DEFAULT_USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'

# 기본 타임아웃 (초)
DEFAULT_TIMEOUT = 30

# Rate Limit 백오프 설정
RATE_LIMIT_DELAYS = [5, 10, 30]  # 초
MAX_RATE_LIMIT_RETRIES = 3

# 로거
logger = logging.getLogger(__name__)


# =============================================================================
# 에러 코드
# =============================================================================

class HttpErrorCode:
    """
    HTTP 에러 코드 상수

    database.review_collection_db.ErrorCode와 동일한 값을 사용합니다.
    """
    IP_BLOCKED = "IP_BLOCKED"           # 403
    RATE_LIMITED = "RATE_LIMITED"       # 429
    NETWORK_ERROR = "NETWORK_ERROR"     # Timeout, Connection Error
    SERVER_ERROR = "SERVER_ERROR"       # 5xx
    PARSE_ERROR = "PARSE_ERROR"         # JSON 파싱 실패
    NO_AVAILABLE_IP = "NO_AVAILABLE_IP" # 사용 가능 IP 없음
    SUCCESS = "SUCCESS"                 # 성공


# =============================================================================
# 결과 데이터 클래스
# =============================================================================

@dataclass
class HttpResult:
    """
    HTTP 요청 결과

    Attributes:
        success: 성공 여부
        data: 응답 데이터 (성공 시)
        status_code: HTTP 상태 코드
        error_code: 에러 코드 (실패 시)
        error_detail: 상세 에러 메시지 (실패 시)
        used_ip: 사용된 IP 주소
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
    스토어별 IP 바인딩 HTTP 클라이언트

    IPManager와 연동하여 스토어에 적합한 IP로 요청을 보냅니다.
    에러 발생 시 자동으로 분류하고 재시도합니다.

    Attributes:
        ip_manager: IP 관리자 인스턴스
        user_agent: User-Agent 헤더 값
        timeout: 요청 타임아웃 (초)
        failed_ips: 현재 세션에서 실패한 IP 목록 (스토어별)

    Example:
        >>> client = StoreHttpClient(ip_manager)
        >>> result = client.request('https://itunes.apple.com/...', 'app_store')
        >>> if result.success:
        ...     reviews = result.data
    """

    def __init__(
        self,
        ip_manager: IPManager,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        """
        Args:
            ip_manager: 초기화된 IPManager 인스턴스
            user_agent: User-Agent 헤더 값
            timeout: 요청 타임아웃 (초)
        """
        self.ip_manager = ip_manager
        self.user_agent = user_agent
        self.timeout = timeout
        self.failed_ips: Dict[str, List[str]] = {}  # {platform: [failed_ips]}

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

        스토어에 적합한 IP를 사용하고, 실패 시 자동 재시도합니다.

        Args:
            url: 요청 URL
            platform: 스토어 ('app_store' 또는 'play_store')
            method: HTTP 메서드 (기본: GET)
            headers: 추가 헤더
            parse_json: JSON 파싱 여부 (기본: True)

        Returns:
            HttpResult 객체
        """
        # 사용할 IP 선택 (실패한 IP 제외)
        ip = self.ip_manager.get_ip_for_store(
            platform,
            exclude=self.failed_ips.get(platform, [])
        )

        if not ip:
            return HttpResult(
                success=False,
                error_code=HttpErrorCode.NO_AVAILABLE_IP,
                error_detail=f"No available IP for {platform}"
            )

        # 요청 실행
        result = self._do_request(url, ip, method, headers, parse_json)

        # IP_BLOCKED면 다른 IP로 재시도
        if result.error_code == HttpErrorCode.IP_BLOCKED:
            self.failed_ips.setdefault(platform, []).append(ip)
            logger.warning(f"IP {ip} blocked for {platform}, trying alternate...")
            return self.request(url, platform, method, headers, parse_json)

        # RATE_LIMITED면 백오프 재시도
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

            # 상태 코드별 처리
            return self._handle_response(response, ip, parse_json)

        except requests.exceptions.Timeout:
            return HttpResult(
                success=False,
                error_code=HttpErrorCode.NETWORK_ERROR,
                error_detail="Request timeout",
                used_ip=ip,
            )
        except requests.exceptions.ConnectionError as e:
            return HttpResult(
                success=False,
                error_code=HttpErrorCode.NETWORK_ERROR,
                error_detail=f"Connection error: {e}",
                used_ip=ip,
            )
        except Exception as e:
            return HttpResult(
                success=False,
                error_code=HttpErrorCode.NETWORK_ERROR,
                error_detail=f"Unexpected error: {e}",
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
                data = response.json() if parse_json else response.text
                return HttpResult(
                    success=True,
                    data=data,
                    status_code=status_code,
                    used_ip=ip,
                )
            except ValueError as e:
                return HttpResult(
                    success=False,
                    status_code=status_code,
                    error_code=HttpErrorCode.PARSE_ERROR,
                    error_detail=f"JSON parse error: {e}",
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

        # 기타 에러
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

        Args:
            url: 요청 URL
            ip: 사용할 IP
            method: HTTP 메서드
            headers: 추가 헤더
            parse_json: JSON 파싱 여부

        Returns:
            성공 시 HttpResult, 모두 실패 시 None
        """
        for i, delay in enumerate(RATE_LIMIT_DELAYS):
            logger.info(f"Rate limited, waiting {delay}s before retry {i+1}/{MAX_RATE_LIMIT_RETRIES}...")
            time.sleep(delay)

            result = self._do_request(url, ip, method, headers, parse_json)

            if result.success:
                return result

            if result.error_code != HttpErrorCode.RATE_LIMITED:
                # Rate limit이 아닌 다른 에러면 중단
                return result

        logger.warning(f"Rate limit retries exhausted for {url}")
        return None

    def reset_failed_ips(self, platform: Optional[str] = None) -> None:
        """
        실패한 IP 목록을 초기화합니다.

        새 수집 세션 시작 시 호출하여 이전 세션의 실패 기록을 초기화합니다.

        Args:
            platform: 특정 스토어만 초기화 (None이면 전체)
        """
        if platform:
            self.failed_ips[platform] = []
        else:
            self.failed_ips = {}
```

**Step 2: 테스트 파일 생성**

```python
# tests/test_http_client.py
"""
StoreHttpClient 테스트
"""
import pytest
from unittest.mock import MagicMock, patch
from core.http_client import StoreHttpClient, HttpResult, HttpErrorCode
from core.ip_manager import IPManager


class TestStoreHttpClient:
    """StoreHttpClient 클래스 테스트"""

    @pytest.fixture
    def mock_ip_manager(self):
        """모의 IPManager 생성"""
        manager = MagicMock(spec=IPManager)
        manager.get_ip_for_store.return_value = '172.31.47.39'
        return manager

    def test_returns_no_available_ip_when_none(self, mock_ip_manager):
        """IP 없을 때 에러 반환"""
        mock_ip_manager.get_ip_for_store.return_value = None
        client = StoreHttpClient(mock_ip_manager)

        result = client.request('https://example.com', 'app_store')

        assert result.success is False
        assert result.error_code == HttpErrorCode.NO_AVAILABLE_IP

    @patch('core.http_client.requests.Session')
    def test_successful_request(self, mock_session_class, mock_ip_manager):
        """성공적인 요청 테스트"""
        # Mock 설정
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': 'test'}

        mock_session = MagicMock()
        mock_session.request.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = StoreHttpClient(mock_ip_manager)
        result = client.request('https://example.com', 'app_store')

        assert result.success is True
        assert result.data == {'data': 'test'}
        assert result.used_ip == '172.31.47.39'

    @patch('core.http_client.requests.Session')
    def test_ip_blocked_tries_alternate(self, mock_session_class, mock_ip_manager):
        """IP 차단 시 다른 IP로 재시도"""
        # 첫 번째 요청: 403
        # 두 번째 요청: 200
        mock_response_403 = MagicMock()
        mock_response_403.status_code = 403

        mock_response_200 = MagicMock()
        mock_response_200.status_code = 200
        mock_response_200.json.return_value = {'success': True}

        mock_session = MagicMock()
        mock_session.request.side_effect = [mock_response_403, mock_response_200]
        mock_session_class.return_value = mock_session

        # 두 번째 호출에서 다른 IP 반환
        mock_ip_manager.get_ip_for_store.side_effect = [
            '172.31.47.39',  # 첫 번째 (차단됨)
            '172.31.40.115', # 두 번째 (성공)
        ]

        client = StoreHttpClient(mock_ip_manager)
        result = client.request('https://example.com', 'app_store')

        assert result.success is True
        assert '172.31.47.39' in client.failed_ips.get('app_store', [])

    def test_reset_failed_ips(self, mock_ip_manager):
        """실패 IP 초기화 테스트"""
        client = StoreHttpClient(mock_ip_manager)
        client.failed_ips = {
            'app_store': ['172.31.47.39'],
            'play_store': ['172.31.40.115'],
        }

        client.reset_failed_ips('app_store')

        assert client.failed_ips['app_store'] == []
        assert client.failed_ips['play_store'] == ['172.31.40.115']
```

**Step 3: 테스트 실행**

```bash
python -m pytest tests/test_http_client.py -v
```

**Step 4: 커밋**

```bash
git add core/http_client.py tests/test_http_client.py
git commit -m "feat: add StoreHttpClient with error handling"
```

---

## Task 6: 기존 수집기 통합

**Files:**
- Modify: `scrapers/app_store_reviews_collector.py`
- Modify: `scrapers/play_store_reviews_collector.py`
- Modify: `collect_app_details.py`

**Step 1: App Store 수집기 수정**

(기존 코드를 수정하여 새 HTTP 클라이언트와 상태 관리 사용)

상세 구현은 기존 파일 분석 후 진행합니다.

**Step 2: Play Store 수집기 수정**

(기존 코드를 수정하여 새 상태 관리 사용)

**Step 3: 메인 파이프라인 수정**

(IP Manager 초기화 및 수집기에 전달)

**Step 4: 통합 테스트**

```bash
python -m pytest tests/ -v
```

**Step 5: 커밋**

```bash
git add scrapers/ collect_app_details.py
git commit -m "feat: integrate new HTTP client and status tracking"
```

---

## Task 7: 전체 통합 테스트

**Files:**
- Create: `tests/test_integration_review_collection.py`

**Step 1: 통합 테스트 작성**

실제 DB와 네트워크를 사용한 End-to-End 테스트

**Step 2: 테스트 실행**

```bash
python -m pytest tests/test_integration_review_collection.py -v --integration
```

**Step 3: 커밋**

```bash
git add tests/test_integration_review_collection.py
git commit -m "test: add integration tests for review collection"
```

---

## 실행 순서 요약

1. Task 1: DB 스키마 생성 ✅
2. Task 2: 상태 CRUD 함수 ✅
3. Task 3: 수집 판단 함수 ✅
4. Task 4: IP Manager ✅
5. Task 5: HTTP 클라이언트 ✅
6. Task 6: 기존 수집기 통합
7. Task 7: 통합 테스트
