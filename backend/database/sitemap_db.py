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
from datetime import datetime
from typing import List, Dict, Set, Optional, Tuple

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import LOG_FORMAT, timing_tracker

# Sitemap 데이터베이스 경로
SITEMAP_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "sitemap_tracking.db")


def log_step(step: str, message: str, task_name: Optional[str] = None):
    """
    타임스탬프 로그 출력

    Args:
        step: 단계 이름
        message: 메시지
        task_name: 태스크 이름 (태스크별 소요시간 추적용)
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timing = timing_tracker.get_timing(task_name)
    print(LOG_FORMAT.format(
        timestamp=timestamp,
        step=step,
        message=message,
        line_duration=f"{timing['line_duration']:.2f}",
        task_duration=f"{timing['task_duration']:.2f}",
        total_duration=f"{timing['total_duration']:.2f}"
    ))


def get_connection():
    """Sitemap 데이터베이스 연결 반환 (WAL 모드, 타임아웃 설정)"""
    db_dir = os.path.dirname(SITEMAP_DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    # timeout=30으로 lock 대기 시간 설정
    conn = sqlite3.connect(SITEMAP_DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    # WAL 모드 활성화 - 동시 읽기/쓰기 성능 향상
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")

    return conn


def init_sitemap_database():
    """Sitemap 트래킹 데이터베이스 초기화"""
    timing_tracker.start_task("Sitemap DB 초기화")
    log_step("Sitemap DB 초기화", "시작", "Sitemap DB 초기화")

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

    # 인덱스 생성
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_discovery_platform ON app_discovery(platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_discovery_first_seen ON app_discovery(first_seen_at DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_discovery_last_seen ON app_discovery(last_seen_at DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_app ON app_history(app_id, platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_date ON app_history(recorded_at DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_date ON sitemap_snapshots(snapshot_date DESC)")

    conn.commit()
    conn.close()

    log_step("Sitemap DB 초기화", "완료", "Sitemap DB 초기화")


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
        # 기존 앱 ID들 조회 (한 번에)
        placeholders = ','.join(['?' for _ in app_ids])
        cursor.execute(f"""
            SELECT app_id FROM app_discovery
            WHERE platform = ? AND app_id IN ({placeholders})
        """, (platform, *app_ids))
        existing_ids = {row['app_id'] for row in cursor.fetchall()}

        # 신규 앱과 기존 앱 분류 (메타데이터 포함)
        new_apps = []
        update_apps = []

        for app_id in app_ids:
            meta = app_metadata.get(app_id, {})
            lastmod = meta.get('lastmod')
            changefreq = meta.get('changefreq')
            priority = meta.get('priority')
            app_url = meta.get('url')

            if app_id not in existing_ids:
                new_apps.append((
                    app_id, platform, now, now, sitemap_source, country_code,
                    lastmod, changefreq, priority, app_url
                ))
            else:
                update_apps.append((
                    now, sitemap_source, lastmod, changefreq, priority, app_url,
                    app_id, platform
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
                    app_url = COALESCE(?, app_url)
                WHERE app_id = ? AND platform = ?
            """, batch)
            updated_count += cursor.rowcount

        conn.commit()

    except sqlite3.Error as e:
        print(f"  [오류] 배치 저장 실패: {e}")
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


if __name__ == "__main__":
    init_sitemap_database()
    print("Sitemap 트래킹 데이터베이스가 초기화되었습니다.")
