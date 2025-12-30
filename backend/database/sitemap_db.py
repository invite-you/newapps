# -*- coding: utf-8 -*-
"""
Sitemap 데이터 트래킹 데이터베이스
- 앱 ID의 최초 발견 시간 기록
- Delta tracking (변경된 앱만 처리)
- 누적 히스토리 저장
"""
import sqlite3
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional, Tuple

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import (
    FAILED_RETRY_COOLDOWN_MINUTES,
    FAILED_RETRY_WARNING_THRESHOLD,
    timing_tracker,
)
from database.db import log_step

# 영구 제외 에러 사유 (재시도 불필요)
PERMANENT_FAILURE_REASONS = frozenset(["not_found_404", "app_removed"])

# Sitemap 데이터베이스 경로 (database 폴더에 정리)
SITEMAP_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "sitemap_tracking.db")


def _normalize_country(code: Optional[str]) -> Optional[str]:
    """2자리 국가 코드를 소문자로 정규화"""
    if not code:
        return None
    trimmed = str(code).strip().lower()
    if len(trimmed) != 2 or not trimmed.isalpha():
        return None
    return trimmed


def _dict_factory(cursor, row):
    """sqlite3 결과를 딕셔너리로 변환하는 팩토리"""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def get_connection():
    """Sitemap 데이터베이스 연결 반환 (WAL 모드, 타임아웃 설정)"""
    db_dir = os.path.dirname(SITEMAP_DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    # timeout=30으로 lock 대기 시간 설정
    conn = sqlite3.connect(SITEMAP_DB_PATH, timeout=30)
    conn.row_factory = _dict_factory  # dict로 반환하여 .get() 사용 가능

    # WAL 모드 활성화 - 동시 읽기/쓰기 성능 향상
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")

    return conn


def init_sitemap_database():
    """Sitemap 트래킹 데이터베이스 초기화"""
    timing_tracker.start_task("Sitemap DB 초기화")

    conn = get_connection()
    cursor = conn.cursor()

    # 앱 발견 기록 테이블 (최초 발견, 마지막 확인 시간)
    # 신규 앱 여부는 first_seen_at 날짜로 판별
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app_discovery (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,           -- 'google_play' or 'app_store'
            first_seen_at TIMESTAMP NOT NULL, -- 최초 발견 시간
            last_seen_at TIMESTAMP NOT NULL,  -- 마지막 확인 시간
            sitemap_source TEXT,              -- sitemap 파일명 (예: sitemap1.xml.gz)
            country_code TEXT,                -- 국가 코드 (App Store)
            -- Sitemap에서 제공하는 추가 정보
            lastmod TEXT,                     -- 마지막 수정 시간 (sitemap에서)
            changefreq TEXT,                  -- 변경 빈도 (daily, weekly, monthly 등)
            priority REAL,                    -- 우선순위 (0.0 ~ 1.0)
            app_url TEXT,                     -- 앱 스토어 URL
            UNIQUE(app_id, platform)
        )
    """)

    # 새 컬럼 추가 (기존 DB 마이그레이션)
    try:
        cursor.execute("ALTER TABLE app_discovery ADD COLUMN lastmod TEXT")
    except sqlite3.OperationalError:
        pass  # 컬럼이 이미 존재함
    try:
        cursor.execute("ALTER TABLE app_discovery ADD COLUMN changefreq TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE app_discovery ADD COLUMN priority REAL")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE app_discovery ADD COLUMN app_url TEXT")
    except sqlite3.OperationalError:
        pass

    # Sitemap 스냅샷 기록 테이블 (수집 기록)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sitemap_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT NOT NULL,
            snapshot_date DATE NOT NULL,
            sitemap_url TEXT,
            total_apps INTEGER DEFAULT 0,     -- 해당 sitemap의 전체 앱 수
            new_apps INTEGER DEFAULT 0,       -- 신규 발견 앱 수
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(platform, snapshot_date, sitemap_url)
        )
    """)

    # 앱 히스토리 테이블 (일별 변화 추적)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            recorded_at DATE NOT NULL,
            in_sitemap INTEGER DEFAULT 1,     -- sitemap에 존재 여부
            chart_position INTEGER,           -- 차트 순위 (있는 경우)
            chart_type TEXT,                  -- 차트 종류
            country_code TEXT,
            UNIQUE(app_id, platform, recorded_at, country_code)
        )
    """)

    # 앱 메트릭 히스토리 테이블 (시계열 분석용)
    # 매일 앱의 주요 지표를 저장하여 시간에 따른 변화 추적
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app_metrics_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            country_code TEXT NOT NULL,
            recorded_date DATE NOT NULL,

            -- 평점 관련 지표
            rating REAL,                      -- 평점 (0-5)
            rating_count INTEGER,             -- 총 평점 수
            rating_count_current_version INTEGER,  -- 현재 버전 평점 수

            -- 리뷰 관련 지표
            reviews_count INTEGER,            -- 리뷰 수
            histogram TEXT,                   -- 별점별 분포 (JSON)

            -- 설치 수 (Google Play)
            installs_min INTEGER,             -- 최소 설치 수
            installs_exact INTEGER,           -- 정확한 설치 수 (가능한 경우)

            -- 차트 순위
            chart_position INTEGER,           -- 차트 순위
            chart_type TEXT,                  -- 차트 종류 (top-free, top-paid 등)

            -- 점수 및 상태
            score REAL,                       -- 계산된 종합 점수
            is_featured INTEGER,              -- 주목 앱 여부

            -- 가격 정보
            price REAL,                       -- 현재 가격
            currency TEXT,                    -- 통화

            -- 앱 내 구매/광고
            has_iap INTEGER,                  -- 앱 내 구매 여부
            contains_ads INTEGER,             -- 광고 포함 여부

            -- 버전 정보 (업데이트 추적)
            version TEXT,                     -- 현재 버전

            -- 타임스탬프
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE(app_id, platform, country_code, recorded_date)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS failed_app_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            country_code TEXT,
            reason TEXT,
            failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            retry_count INTEGER DEFAULT 1,
            UNIQUE(app_id, platform, country_code)
        )
    """)

    # 인덱스 생성
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_discovery_platform ON app_discovery(platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_discovery_first_seen ON app_discovery(first_seen_at DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_discovery_last_seen ON app_discovery(last_seen_at DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_app ON app_history(app_id, platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_date ON app_history(recorded_at DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_date ON sitemap_snapshots(snapshot_date DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_failed_app ON failed_app_details(app_id, platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_failed_at ON failed_app_details(failed_at DESC)")

    # 앱 메트릭 히스토리 인덱스 (시계열 조회 최적화)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metrics_app ON app_metrics_history(app_id, platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metrics_date ON app_metrics_history(recorded_date DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metrics_country ON app_metrics_history(country_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metrics_app_date ON app_metrics_history(app_id, platform, country_code, recorded_date DESC)")

    # 실패한 sitemap URL 기록 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS failed_sitemap_urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sitemap_url TEXT NOT NULL,
            platform TEXT NOT NULL,            -- 'google_play' or 'app_store'
            reason TEXT,
            failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            retry_count INTEGER DEFAULT 1,
            last_success_at TIMESTAMP,         -- 마지막 성공 시간
            UNIQUE(sitemap_url, platform)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_failed_sitemap_url ON failed_sitemap_urls(sitemap_url, platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_failed_sitemap_at ON failed_sitemap_urls(failed_at DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_failed_sitemap_retry ON failed_sitemap_urls(retry_count)")

    conn.commit()
    conn.close()


def get_known_app_ids(platform: str) -> Set[str]:
    """이미 알려진 앱 ID 목록 반환"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT app_id FROM app_discovery WHERE platform = ?",
        (platform,)
    )

    known_ids = {row['app_id'] for row in cursor.fetchall()}
    conn.close()

    return known_ids


def save_discovered_apps(
    app_ids: List[str],
    platform: str,
    sitemap_source: str = None,
    country_code: str = None,
    app_metadata: Dict[str, Dict] = None
) -> Tuple[int, int]:
    """
    발견된 앱 ID들을 저장 (배치 처리로 성능 및 lock 문제 해결)

    Args:
        app_ids: 앱 ID 목록
        platform: 플랫폼 ('google_play' 또는 'app_store')
        sitemap_source: sitemap 파일명
        country_code: 국가 코드
        app_metadata: 앱별 메타데이터 {app_id: {lastmod, changefreq, priority, url}}

    Returns:
        (new_count, updated_count): 신규 앱 수, 업데이트된 앱 수
    """
    if not app_ids:
        return 0, 0

    conn = get_connection()
    cursor = conn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_count = 0
    updated_count = 0
    app_metadata = app_metadata or {}

    # 배치 크기 설정
    batch_size = 500

    try:
        # 기존 앱 ID들 조회 (쿼리 변수 한도 초과 방지 위해 청크 단위 조회)
        existing_ids: Set[str] = set()
        lookup_chunk = 400  # platform 바인딩까지 포함해 999 제한을 피하기 위한 안전선
        for start in range(0, len(app_ids), lookup_chunk):
            chunk = app_ids[start:start + lookup_chunk]
            placeholders = ','.join(['?' for _ in chunk])
            cursor.execute(f"""
                SELECT app_id FROM app_discovery
                WHERE platform = ? AND app_id IN ({placeholders})
            """, (platform, *chunk))
            existing_ids.update({row['app_id'] for row in cursor.fetchall()})

        # 신규 앱과 기존 앱 분류 (메타데이터 포함)
        new_apps = []
        update_apps = []

        for app_id in app_ids:
            meta = app_metadata.get(app_id, {})
            meta_country = _normalize_country(meta.get('country_code'))
            effective_country = meta_country or _normalize_country(country_code)
            lastmod = meta.get('lastmod')
            changefreq = meta.get('changefreq')
            priority = meta.get('priority')
            app_url = meta.get('url')

            if app_id not in existing_ids:
                new_apps.append((
                    app_id, platform, now, now, sitemap_source, effective_country,
                    lastmod, changefreq, priority, app_url
                ))
            else:
                update_apps.append((
                    now, sitemap_source, lastmod, changefreq, priority, app_url,
                    effective_country, app_id, platform
                ))

        # 배치 INSERT (신규 앱)
        for i in range(0, len(new_apps), batch_size):
            batch = new_apps[i:i + batch_size]
            cursor.executemany("""
                INSERT OR IGNORE INTO app_discovery
                (app_id, platform, first_seen_at, last_seen_at, sitemap_source, country_code,
                 lastmod, changefreq, priority, app_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            new_count += cursor.rowcount

        # 배치 UPDATE (기존 앱)
        for i in range(0, len(update_apps), batch_size):
            batch = update_apps[i:i + batch_size]
            cursor.executemany("""
                UPDATE app_discovery
                SET last_seen_at = ?, sitemap_source = ?,
                    lastmod = COALESCE(?, lastmod),
                    changefreq = COALESCE(?, changefreq),
                    priority = COALESCE(?, priority),
                    app_url = COALESCE(?, app_url),
                    country_code = COALESCE(?, country_code)
                WHERE app_id = ? AND platform = ?
            """, batch)
            updated_count += cursor.rowcount

        conn.commit()

    except sqlite3.Error as e:
        log_step("Sitemap DB", f"[오류] 배치 저장 실패: {type(e).__name__}: {str(e)}", "Sitemap DB")
        conn.rollback()
    finally:
        conn.close()

    return new_count, updated_count


def save_sitemap_snapshot(
    platform: str,
    sitemap_url: str,
    total_apps: int,
    new_apps: int
):
    """Sitemap 스냅샷 기록 저장"""
    conn = get_connection()
    cursor = conn.cursor()

    today = datetime.now().strftime("%Y-%m-%d")

    cursor.execute("""
        INSERT OR REPLACE INTO sitemap_snapshots
        (platform, snapshot_date, sitemap_url, total_apps, new_apps)
        VALUES (?, ?, ?, ?, ?)
    """, (platform, today, sitemap_url, total_apps, new_apps))

    conn.commit()
    conn.close()


def save_app_history(
    app_id: str,
    platform: str,
    country_code: str = None,
    chart_position: int = None,
    chart_type: str = None
):
    """앱 히스토리 기록 저장"""
    conn = get_connection()
    cursor = conn.cursor()

    today = datetime.now().strftime("%Y-%m-%d")

    cursor.execute("""
        INSERT OR REPLACE INTO app_history
        (app_id, platform, recorded_at, in_sitemap, chart_position, chart_type, country_code)
        VALUES (?, ?, ?, 1, ?, ?, ?)
    """, (app_id, platform, today, chart_position, chart_type, country_code))

    conn.commit()
    conn.close()


def upsert_failed_app_detail(app_id: str, platform: str, country_code: Optional[str], reason: str) -> int:
    """실패 기록을 upsert하고 누적 횟수를 반환"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO failed_app_details (app_id, platform, country_code, reason, failed_at, retry_count)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 1)
        ON CONFLICT(app_id, platform, country_code) DO UPDATE SET
            reason = excluded.reason,
            failed_at = CURRENT_TIMESTAMP,
            retry_count = failed_app_details.retry_count + 1
    """, (app_id, platform, country_code, reason[:500] if reason else None))

    cursor.execute("""
        SELECT retry_count FROM failed_app_details
        WHERE app_id = ? AND platform = ? AND (country_code = ? OR (country_code IS NULL AND ? IS NULL))
    """, (app_id, platform, country_code, country_code))
    row = cursor.fetchone()
    conn.commit()
    conn.close()

    retry_count = row['retry_count'] if row else 0
    if retry_count >= FAILED_RETRY_WARNING_THRESHOLD:
        log_step("경고", f"재시도 {retry_count}회 초과: {app_id} ({reason})", "경고")
    return retry_count


def clear_failed_app_detail(app_id: str, platform: str, country_code: Optional[str]):
    """성공 시 실패 기록 제거"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM failed_app_details
        WHERE app_id = ? AND platform = ? AND (country_code = ? OR (country_code IS NULL AND ? IS NULL))
    """, (app_id, platform, country_code, country_code))
    conn.commit()
    conn.close()


def _get_failed_details(platform: str, candidate_apps: List[Tuple[str, Optional[str]]]) -> Dict[Tuple[str, Optional[str]], Dict]:
    """특정 플랫폼의 실패 기록을 조회"""
    if not candidate_apps:
        return {}

    app_ids = {app_id for app_id, _ in candidate_apps}
    conn = get_connection()
    cursor = conn.cursor()
    placeholders = ','.join(['?' for _ in app_ids])
    cursor.execute(f"""
        SELECT app_id, country_code, reason, failed_at, retry_count
        FROM failed_app_details
        WHERE platform = ? AND app_id IN ({placeholders})
        ORDER BY failed_at DESC
    """, (platform, *app_ids))
    rows = cursor.fetchall()
    conn.close()

    result: Dict[Tuple[str, Optional[str]], Dict] = {}
    for row in rows:
        key = (row['app_id'], row['country_code'])
        if key not in result:
            result[key] = row  # row_factory가 이미 dict 반환

    return result


def _parse_failed_at(value: Optional[str]) -> Optional[datetime]:
    """문자열 타임스탬프 파싱"""
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def prioritize_for_retry(platform: str, candidate_apps: List[Tuple[str, Optional[str]]], limit: int) -> List[Tuple[str, Optional[str]]]:
    """
    실패 기록을 고려하여 재시도 우선순위를 정리
    - 영구 제외 앱 (404 등) 필터링
    - 실패 횟수가 적은 순서
    - 오래전에 실패한 항목 우선
    - 최근 실패 후 쿨다운 시간 내 항목은 제외
    """
    failed_map = _get_failed_details(platform, candidate_apps)
    now = datetime.now()
    allowed: List[Tuple[int, datetime, str, Optional[str]]] = []
    skipped_recent = 0
    skipped_permanent = 0

    for app_id, country_code in candidate_apps:
        failed_info = failed_map.get((app_id, country_code)) or failed_map.get((app_id, None))
        if not failed_info:
            # 실패 기록 없음 - 최우선 처리
            allowed.append((0, datetime.min, app_id, country_code))
            continue

        reason = failed_info.get('reason', '')
        # 영구 제외 대상 (404 등)
        if reason in PERMANENT_FAILURE_REASONS:
            skipped_permanent += 1
            continue

        retry_count = failed_info.get('retry_count', 0)
        failed_at = _parse_failed_at(failed_info.get('failed_at'))

        if failed_at and now - failed_at < timedelta(minutes=FAILED_RETRY_COOLDOWN_MINUTES):
            skipped_recent += 1
            continue

        allowed.append((retry_count, failed_at or datetime.min, app_id, country_code))

    allowed.sort(key=lambda item: (item[0], item[1]))

    if skipped_permanent or skipped_recent:
        log_step(
            "재시도 필터",
            f"[{platform}] 영구제외={skipped_permanent}, 쿨다운={skipped_recent}, 대상={len(allowed)}/{len(candidate_apps)}",
            "재시도 필터"
        )
    return [(app_id, country_code) for _, _, app_id, country_code in allowed][:limit]


def get_new_apps_since(platform: str, since_date: str) -> List[Dict]:
    """특정 날짜 이후 발견된 새 앱 목록 반환"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT app_id, first_seen_at, last_seen_at, sitemap_source, country_code
        FROM app_discovery
        WHERE platform = ? AND first_seen_at >= ?
        ORDER BY first_seen_at DESC
    """, (platform, since_date))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return results


def get_recently_discovered_apps(platform: str, days: int = 7) -> List[Dict]:
    """최근 N일 내 발견된 앱 목록"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT app_id, first_seen_at, last_seen_at, sitemap_source, country_code
        FROM app_discovery
        WHERE platform = ? AND first_seen_at >= datetime('now', ?)
        ORDER BY first_seen_at DESC
    """, (platform, f'-{days} days'))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return results


def get_discovery_stats() -> Dict:
    """발견 통계 반환"""
    conn = get_connection()
    cursor = conn.cursor()

    stats = {}

    # 플랫폼별 전체 앱 수
    cursor.execute("""
        SELECT platform, COUNT(*) as total
        FROM app_discovery
        GROUP BY platform
    """)
    stats['by_platform'] = {row['platform']: {'total': row['total']}
                           for row in cursor.fetchall()}

    # 최근 7일 신규 발견 앱 수
    cursor.execute("""
        SELECT platform, COUNT(*) as count
        FROM app_discovery
        WHERE first_seen_at >= datetime('now', '-7 days')
        GROUP BY platform
    """)
    stats['last_7_days'] = {row['platform']: row['count'] for row in cursor.fetchall()}

    # 오늘 신규 발견 앱 수
    cursor.execute("""
        SELECT platform, COUNT(*) as count
        FROM app_discovery
        WHERE date(first_seen_at) = date('now')
        GROUP BY platform
    """)
    stats['today'] = {row['platform']: row['count'] for row in cursor.fetchall()}

    conn.close()

    return stats


def get_missing_apps(platform: str, days: int = 30) -> List[str]:
    """최근 N일간 sitemap에서 사라진 앱 ID 목록"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT app_id
        FROM app_discovery
        WHERE platform = ? AND last_seen_at < datetime('now', ?)
        ORDER BY last_seen_at DESC
    """, (platform, f'-{days} days'))

    results = [row['app_id'] for row in cursor.fetchall()]
    conn.close()

    return results


# ============ 실패한 Sitemap URL 관리 함수들 ============

def upsert_failed_sitemap_url(sitemap_url: str, platform: str, reason: str) -> int:
    """
    실패한 sitemap URL 기록을 upsert하고 누적 횟수를 반환

    Args:
        sitemap_url: 실패한 sitemap URL
        platform: 플랫폼 ('google_play' 또는 'app_store')
        reason: 실패 사유

    Returns:
        누적 재시도 횟수
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO failed_sitemap_urls (sitemap_url, platform, reason, failed_at, retry_count)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, 1)
        ON CONFLICT(sitemap_url, platform) DO UPDATE SET
            reason = excluded.reason,
            failed_at = CURRENT_TIMESTAMP,
            retry_count = failed_sitemap_urls.retry_count + 1
    """, (sitemap_url, platform, reason[:500] if reason else None))

    cursor.execute("""
        SELECT retry_count FROM failed_sitemap_urls
        WHERE sitemap_url = ? AND platform = ?
    """, (sitemap_url, platform))
    row = cursor.fetchone()
    conn.commit()
    conn.close()

    retry_count = row['retry_count'] if row else 0
    if retry_count >= FAILED_RETRY_WARNING_THRESHOLD:
        log_step("경고", f"Sitemap 재시도 {retry_count}회 초과: {sitemap_url}", "Sitemap 재시도")
    return retry_count


def clear_failed_sitemap_url(sitemap_url: str, platform: str):
    """
    성공 시 실패 기록 제거 (또는 last_success_at 업데이트)

    Args:
        sitemap_url: 성공한 sitemap URL
        platform: 플랫폼
    """
    conn = get_connection()
    cursor = conn.cursor()

    # 성공하면 기록 삭제
    cursor.execute("""
        DELETE FROM failed_sitemap_urls
        WHERE sitemap_url = ? AND platform = ?
    """, (sitemap_url, platform))

    conn.commit()
    conn.close()


def get_failed_sitemap_urls(platform: str, max_retry_count: int = 10) -> List[Dict]:
    """
    재시도가 필요한 실패한 sitemap URL 목록 반환

    Args:
        platform: 플랫폼 ('google_play' 또는 'app_store')
        max_retry_count: 최대 재시도 횟수 (이 이상이면 제외)

    Returns:
        재시도가 필요한 sitemap URL 정보 목록
    """
    conn = get_connection()
    cursor = conn.cursor()

    # 쿨다운 시간이 지난 항목만 반환
    cursor.execute("""
        SELECT sitemap_url, reason, failed_at, retry_count
        FROM failed_sitemap_urls
        WHERE platform = ?
          AND retry_count < ?
          AND failed_at < datetime('now', ?)
        ORDER BY retry_count ASC, failed_at ASC
    """, (platform, max_retry_count, f'-{FAILED_RETRY_COOLDOWN_MINUTES} minutes'))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return results


def get_sitemap_retry_stats(platform: str) -> Dict:
    """
    Sitemap 재시도 통계 반환

    Args:
        platform: 플랫폼

    Returns:
        통계 정보 딕셔너리
    """
    conn = get_connection()
    cursor = conn.cursor()

    # 전체 실패 기록 수
    cursor.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN retry_count >= ? THEN 1 ELSE 0 END) as exceeded_threshold
        FROM failed_sitemap_urls
        WHERE platform = ?
    """, (FAILED_RETRY_WARNING_THRESHOLD, platform))
    row = cursor.fetchone()

    stats = {
        'total_failed': row['total'] if row else 0,
        'exceeded_threshold': row['exceeded_threshold'] if row else 0
    }

    # 쿨다운 후 재시도 가능한 개수
    cursor.execute("""
        SELECT COUNT(*) as retryable
        FROM failed_sitemap_urls
        WHERE platform = ?
          AND retry_count < ?
          AND failed_at < datetime('now', ?)
    """, (platform, FAILED_RETRY_WARNING_THRESHOLD * 2, f'-{FAILED_RETRY_COOLDOWN_MINUTES} minutes'))
    row = cursor.fetchone()
    stats['retryable'] = row['retryable'] if row else 0

    conn.close()
    return stats


# ============ 앱 메트릭 히스토리 관리 함수들 (시계열 분석) ============

# 메트릭 변화 감지 임계값 (Delta Storage용)
# - 값이 0이면 모든 변화 감지 (권장)
# - 값이 양수면 해당 임계값 이상 변화 시에만 저장
#
# 사용 예시:
#   'rating': 0       → 평점(0-5)이 조금이라도 변하면 저장
#   'rating': 0.1     → 평점이 0.1 이상 변할 때만 저장 (예: 4.5→4.6)
#   'rating_count': 0 → 평점 수가 1개라도 변하면 저장
#   'rating_count': 0.05 → 평점 수가 5% 이상 변할 때만 저장
#
# 단위 설명:
#   rating, score, price: 절대값 비교 (예: 0.1 = 0.1점 차이)
#   rating_count, reviews_count, installs_*: 비율 비교 (예: 0.01 = 1% 차이)
#   chart_position: 절대값 비교 (예: 1 = 순위 1단계 차이)
METRICS_CHANGE_THRESHOLDS = {
    'rating': 0,              # 평점 (0-5), 절대값 비교
    'rating_count': 0,        # 평점 수, 비율 비교 (0.01 = 1%)
    'reviews_count': 0,       # 리뷰 수, 비율 비교
    'installs_min': 0,        # 최소 설치 수, 비율 비교
    'installs_exact': 0,      # 정확한 설치 수, 비율 비교
    'chart_position': 0,      # 차트 순위, 절대값 비교
    'score': 0,               # 계산된 점수 (0-100), 절대값 비교
    'price': 0,               # 가격, 절대값 비교
}

# 데이터 보관 기간 (일) - cleanup_old_metrics() 호출 시 사용
METRICS_RETENTION_DAYS = 90


def _has_significant_metric_change(old: Dict, new: Dict) -> bool:
    """
    두 메트릭 간에 유의미한 변화가 있는지 확인

    Args:
        old: 이전 메트릭 딕셔너리
        new: 새 메트릭 딕셔너리

    Returns:
        유의미한 변화 여부
    """
    if not old:
        return True  # 이전 데이터가 없으면 항상 저장

    for field, threshold in METRICS_CHANGE_THRESHOLDS.items():
        old_val = old.get(field)
        new_val = new.get(field)

        # 둘 다 None이면 변화 없음
        if old_val is None and new_val is None:
            continue

        # 하나만 None이면 변화 있음
        if old_val is None or new_val is None:
            return True

        # 순위는 절대값 비교
        if field == 'chart_position':
            if abs(old_val - new_val) >= threshold:
                return True
            continue

        # 평점은 절대값 비교
        if field in ('rating', 'score', 'price'):
            if abs(old_val - new_val) >= threshold:
                return True
            continue

        # 나머지는 퍼센트 비교
        if old_val > 0:
            pct_change = abs(new_val - old_val) / old_val
            if pct_change >= threshold:
                return True
        elif new_val > 0:
            return True  # 0에서 양수로 변화

    # 버전 변화 감지
    if old.get('version') != new.get('version') and new.get('version'):
        return True

    # is_featured 변화 감지
    if old.get('is_featured') != new.get('is_featured'):
        return True

    return False


def save_app_metrics_batch(apps_data: List[Dict], recorded_date: str = None) -> Tuple[int, int]:
    """
    여러 앱의 메트릭을 배치로 저장 (Delta Storage - 변경분만 저장)

    Args:
        apps_data: 앱 정보 딕셔너리 리스트
        recorded_date: 기록 날짜 (기본값: 오늘)

    Returns:
        (저장된 앱 수, 스킵된 앱 수)
    """
    if not apps_data:
        return 0, 0

    conn = get_connection()
    cursor = conn.cursor()

    if not recorded_date:
        recorded_date = datetime.now().strftime("%Y-%m-%d")

    saved_count = 0
    skipped_count = 0
    batch_size = 500

    try:
        # 1. 기존 최신 메트릭 조회 (비교용)
        app_keys = [(app.get('app_id'), app.get('platform'), app.get('country_code'))
                    for app in apps_data if app.get('app_id')]

        existing_metrics: Dict[Tuple[str, str, str], Dict] = {}

        # 청크 단위로 조회 (SQLite 변수 제한 회피)
        chunk_size = 300
        for i in range(0, len(app_keys), chunk_size):
            chunk = app_keys[i:i + chunk_size]
            # 복합 키로 조회
            conditions = " OR ".join(
                ["(app_id = ? AND platform = ? AND country_code = ?)"] * len(chunk)
            )
            params = [val for key in chunk for val in key]

            cursor.execute(f"""
                SELECT app_id, platform, country_code,
                       rating, rating_count, reviews_count,
                       installs_min, installs_exact,
                       chart_position, score, price, version, is_featured
                FROM app_metrics_history
                WHERE ({conditions})
                  AND recorded_date = (
                      SELECT MAX(recorded_date) FROM app_metrics_history h2
                      WHERE h2.app_id = app_metrics_history.app_id
                        AND h2.platform = app_metrics_history.platform
                        AND h2.country_code = app_metrics_history.country_code
                  )
            """, params)

            for row in cursor.fetchall():
                key = (row['app_id'], row['platform'], row['country_code'])
                existing_metrics[key] = dict(row)

        # 2. 변경된 앱만 필터링
        apps_to_save = []
        for app in apps_data:
            if not app.get('app_id'):
                continue

            key = (app.get('app_id'), app.get('platform'), app.get('country_code'))
            existing = existing_metrics.get(key)

            # 새 메트릭 구성
            new_metrics = {
                'rating': app.get('rating'),
                'rating_count': app.get('rating_count'),
                'reviews_count': app.get('reviews_count'),
                'installs_min': app.get('installs_min'),
                'installs_exact': app.get('installs_exact'),
                'chart_position': app.get('chart_position'),
                'score': app.get('score'),
                'price': app.get('price'),
                'version': app.get('version'),
                'is_featured': app.get('is_featured'),
            }

            if _has_significant_metric_change(existing, new_metrics):
                apps_to_save.append(app)
            else:
                skipped_count += 1

        # 3. 변경된 앱만 저장
        batch = []
        for app in apps_to_save:
            batch.append((
                app.get('app_id'),
                app.get('platform'),
                app.get('country_code'),
                recorded_date,
                app.get('rating'),
                app.get('rating_count'),
                app.get('rating_count_current_version'),
                app.get('reviews_count'),
                app.get('histogram'),
                app.get('installs_min'),
                app.get('installs_exact'),
                app.get('chart_position'),
                app.get('chart_type'),
                app.get('score'),
                app.get('is_featured'),
                app.get('price'),
                app.get('currency'),
                app.get('has_iap'),
                app.get('contains_ads'),
                app.get('version'),
            ))

            if len(batch) >= batch_size:
                cursor.executemany("""
                    INSERT OR REPLACE INTO app_metrics_history (
                        app_id, platform, country_code, recorded_date,
                        rating, rating_count, rating_count_current_version,
                        reviews_count, histogram,
                        installs_min, installs_exact,
                        chart_position, chart_type,
                        score, is_featured,
                        price, currency,
                        has_iap, contains_ads,
                        version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                saved_count += len(batch)
                batch = []

        # 남은 배치 처리
        if batch:
            cursor.executemany("""
                INSERT OR REPLACE INTO app_metrics_history (
                    app_id, platform, country_code, recorded_date,
                    rating, rating_count, rating_count_current_version,
                    reviews_count, histogram,
                    installs_min, installs_exact,
                    chart_position, chart_type,
                    score, is_featured,
                    price, currency,
                    has_iap, contains_ads,
                    version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            saved_count += len(batch)

        conn.commit()
        log_step("메트릭 저장", f"Delta 저장: {saved_count}개 변경, {skipped_count}개 스킵", "메트릭 저장")

    except sqlite3.Error as e:
        log_step("메트릭 저장", f"[오류] 배치 저장 실패: {e}", "메트릭 저장")
        conn.rollback()
    finally:
        conn.close()

    return saved_count, skipped_count


def cleanup_old_metrics(retention_days: int = None) -> int:
    """
    오래된 메트릭 데이터 삭제

    Args:
        retention_days: 보관 기간 (기본값: METRICS_RETENTION_DAYS)

    Returns:
        삭제된 레코드 수
    """
    if retention_days is None:
        retention_days = METRICS_RETENTION_DAYS

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            DELETE FROM app_metrics_history
            WHERE recorded_date < date('now', ?)
        """, (f'-{retention_days} days',))

        deleted_count = cursor.rowcount
        conn.commit()

        if deleted_count > 0:
            log_step("메트릭 정리", f"{retention_days}일 이전 데이터 {deleted_count}개 삭제", "메트릭 정리")

        return deleted_count

    except sqlite3.Error as e:
        log_step("메트릭 정리", f"[오류] 정리 실패: {e}", "메트릭 정리")
        conn.rollback()
        return 0
    finally:
        conn.close()


def get_metrics_storage_stats() -> Dict:
    """
    메트릭 저장소 통계 조회

    Returns:
        저장소 통계 딕셔너리
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT app_id || platform || country_code) as unique_apps,
                COUNT(DISTINCT recorded_date) as days_recorded,
                MIN(recorded_date) as oldest_date,
                MAX(recorded_date) as newest_date
            FROM app_metrics_history
        """)
        stats = dict(cursor.fetchone())

        # 일별 평균 레코드 수
        if stats['days_recorded'] and stats['days_recorded'] > 0:
            stats['avg_records_per_day'] = round(stats['total_records'] / stats['days_recorded'], 1)
        else:
            stats['avg_records_per_day'] = 0

        return stats

    except sqlite3.Error as e:
        log_step("메트릭 통계", f"[오류] 통계 조회 실패: {e}", "메트릭 통계")
        return {}
    finally:
        conn.close()


def get_app_metrics_timeseries(
    app_id: str,
    platform: str,
    country_code: str = None,
    days: int = 30,
    metrics: List[str] = None
) -> List[Dict]:
    """
    앱의 시계열 메트릭 데이터 조회

    Args:
        app_id: 앱 ID
        platform: 플랫폼
        country_code: 국가 코드 (None이면 모든 국가)
        days: 조회 기간 (일)
        metrics: 조회할 메트릭 목록 (None이면 전체)

    Returns:
        시계열 데이터 리스트
    """
    conn = get_connection()
    cursor = conn.cursor()

    # 기본 메트릭 목록
    all_metrics = [
        'rating', 'rating_count', 'reviews_count',
        'installs_min', 'installs_exact',
        'chart_position', 'score', 'price', 'version'
    ]
    selected_metrics = metrics if metrics else all_metrics

    # 유효한 메트릭만 선택
    valid_metrics = [m for m in selected_metrics if m in all_metrics]
    if not valid_metrics:
        valid_metrics = all_metrics

    columns = ', '.join(['recorded_date', 'country_code'] + valid_metrics)

    if country_code:
        cursor.execute(f"""
            SELECT {columns}
            FROM app_metrics_history
            WHERE app_id = ? AND platform = ? AND country_code = ?
              AND recorded_date >= date('now', ?)
            ORDER BY recorded_date ASC
        """, (app_id, platform, country_code, f'-{days} days'))
    else:
        cursor.execute(f"""
            SELECT {columns}
            FROM app_metrics_history
            WHERE app_id = ? AND platform = ?
              AND recorded_date >= date('now', ?)
            ORDER BY recorded_date ASC, country_code
        """, (app_id, platform, f'-{days} days'))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return results


def get_metrics_changes(
    app_id: str,
    platform: str,
    country_code: str,
    compare_days: int = 7
) -> Dict:
    """
    특정 기간 동안의 메트릭 변화량 계산

    Args:
        app_id: 앱 ID
        platform: 플랫폼
        country_code: 국가 코드
        compare_days: 비교 기간 (일)

    Returns:
        메트릭별 변화량 딕셔너리
    """
    conn = get_connection()
    cursor = conn.cursor()

    # 현재 데이터 (가장 최근)
    cursor.execute("""
        SELECT rating, rating_count, reviews_count, installs_min, installs_exact,
               chart_position, score, price
        FROM app_metrics_history
        WHERE app_id = ? AND platform = ? AND country_code = ?
        ORDER BY recorded_date DESC
        LIMIT 1
    """, (app_id, platform, country_code))
    current = cursor.fetchone()

    # 과거 데이터 (compare_days일 전 근처)
    cursor.execute("""
        SELECT rating, rating_count, reviews_count, installs_min, installs_exact,
               chart_position, score, price
        FROM app_metrics_history
        WHERE app_id = ? AND platform = ? AND country_code = ?
          AND recorded_date <= date('now', ?)
        ORDER BY recorded_date DESC
        LIMIT 1
    """, (app_id, platform, country_code, f'-{compare_days} days'))
    past = cursor.fetchone()

    conn.close()

    if not current or not past:
        return {}

    # 변화량 계산
    changes = {}
    numeric_fields = ['rating', 'rating_count', 'reviews_count', 'installs_min',
                      'installs_exact', 'chart_position', 'score', 'price']

    for field in numeric_fields:
        curr_val = current.get(field)
        past_val = past.get(field)

        if curr_val is not None and past_val is not None:
            diff = curr_val - past_val
            pct_change = ((curr_val - past_val) / past_val * 100) if past_val != 0 else None
            changes[field] = {
                'current': curr_val,
                'past': past_val,
                'diff': diff,
                'pct_change': round(pct_change, 2) if pct_change is not None else None
            }

    return changes


def get_top_growing_apps(
    platform: str = None,
    country_code: str = None,
    metric: str = 'rating_count',
    days: int = 7,
    limit: int = 50
) -> List[Dict]:
    """
    특정 메트릭 기준으로 가장 많이 성장한 앱 조회

    Args:
        platform: 플랫폼 (None이면 전체)
        country_code: 국가 코드 (None이면 전체)
        metric: 비교 메트릭 (rating_count, reviews_count, installs_min 등)
        days: 비교 기간 (일)
        limit: 최대 결과 수

    Returns:
        성장률 순으로 정렬된 앱 리스트
    """
    conn = get_connection()
    cursor = conn.cursor()

    # 유효한 메트릭 확인
    valid_metrics = ['rating', 'rating_count', 'reviews_count', 'installs_min',
                     'installs_exact', 'score']
    if metric not in valid_metrics:
        metric = 'rating_count'

    # 동적 쿼리 구성
    where_clauses = ["recorded_date >= date('now', ?)"]
    params = [f'-{days} days']

    if platform:
        where_clauses.append("platform = ?")
        params.append(platform)

    if country_code:
        where_clauses.append("country_code = ?")
        params.append(country_code)

    where_sql = " AND ".join(where_clauses)

    cursor.execute(f"""
        WITH date_range AS (
            SELECT
                app_id, platform, country_code,
                MIN(recorded_date) as first_date,
                MAX(recorded_date) as last_date
            FROM app_metrics_history
            WHERE {where_sql}
            GROUP BY app_id, platform, country_code
            HAVING first_date != last_date
        ),
        first_metrics AS (
            SELECT m.app_id, m.platform, m.country_code, m.{metric} as first_value
            FROM app_metrics_history m
            JOIN date_range d ON m.app_id = d.app_id
                AND m.platform = d.platform
                AND m.country_code = d.country_code
                AND m.recorded_date = d.first_date
        ),
        last_metrics AS (
            SELECT m.app_id, m.platform, m.country_code, m.{metric} as last_value
            FROM app_metrics_history m
            JOIN date_range d ON m.app_id = d.app_id
                AND m.platform = d.platform
                AND m.country_code = d.country_code
                AND m.recorded_date = d.last_date
        )
        SELECT
            f.app_id, f.platform, f.country_code,
            f.first_value,
            l.last_value,
            (l.last_value - f.first_value) as diff,
            CASE WHEN f.first_value > 0
                THEN ROUND((l.last_value - f.first_value) * 100.0 / f.first_value, 2)
                ELSE NULL
            END as growth_pct
        FROM first_metrics f
        JOIN last_metrics l ON f.app_id = l.app_id
            AND f.platform = l.platform
            AND f.country_code = l.country_code
        WHERE l.last_value > f.first_value
        ORDER BY diff DESC
        LIMIT ?
    """, params + [limit])

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return results


def get_metrics_stats(
    platform: str = None,
    country_code: str = None,
    days: int = 30
) -> Dict:
    """
    전체 메트릭 통계 조회

    Args:
        platform: 플랫폼 (None이면 전체)
        country_code: 국가 코드 (None이면 전체)
        days: 조회 기간 (일)

    Returns:
        통계 정보 딕셔너리
    """
    conn = get_connection()
    cursor = conn.cursor()

    # 동적 WHERE 절 구성
    where_clauses = ["recorded_date >= date('now', ?)"]
    params = [f'-{days} days']

    if platform:
        where_clauses.append("platform = ?")
        params.append(platform)

    if country_code:
        where_clauses.append("country_code = ?")
        params.append(country_code)

    where_sql = " AND ".join(where_clauses)

    # 기본 통계
    cursor.execute(f"""
        SELECT
            COUNT(DISTINCT app_id || platform || country_code) as unique_apps,
            COUNT(*) as total_records,
            COUNT(DISTINCT recorded_date) as days_recorded,
            MIN(recorded_date) as first_date,
            MAX(recorded_date) as last_date
        FROM app_metrics_history
        WHERE {where_sql}
    """, params)

    row = cursor.fetchone()
    stats = dict(row) if row else {}

    # 플랫폼별 통계
    cursor.execute(f"""
        SELECT
            platform,
            COUNT(DISTINCT app_id) as app_count,
            AVG(rating) as avg_rating,
            AVG(rating_count) as avg_rating_count
        FROM app_metrics_history
        WHERE {where_sql}
        GROUP BY platform
    """, params)

    stats['by_platform'] = {row['platform']: dict(row) for row in cursor.fetchall()}

    conn.close()
    return stats


if __name__ == "__main__":
    init_sitemap_database()
    print("Sitemap 트래킹 데이터베이스가 초기화되었습니다.")
