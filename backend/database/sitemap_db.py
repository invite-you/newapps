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
from config import LOG_FORMAT

# Sitemap 데이터베이스 경로
SITEMAP_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "sitemap_tracking.db")


def log_step(step: str, message: str, start_time: Optional[datetime] = None):
    """타임스탬프 로그 출력"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    duration = (datetime.now() - start_time).total_seconds() if start_time else 0
    print(LOG_FORMAT.format(
        timestamp=timestamp,
        step=step,
        message=message,
        duration=f"{duration:.2f}"
    ))


def get_connection():
    """Sitemap 데이터베이스 연결 반환"""
    db_dir = os.path.dirname(SITEMAP_DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(SITEMAP_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_sitemap_database():
    """Sitemap 트래킹 데이터베이스 초기화"""
    start_time = datetime.now()
    log_step("Sitemap DB 초기화", "시작", start_time)

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
            sitemap_source TEXT,              -- sitemap 파일 출처
            country_code TEXT,                -- 국가 코드 (App Store)
            UNIQUE(app_id, platform)
        )
    """)

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

    log_step("Sitemap DB 초기화", "완료", start_time)


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
    country_code: str = None
) -> Tuple[int, int]:
    """
    발견된 앱 ID들을 저장

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

    for app_id in app_ids:
        try:
            # INSERT OR IGNORE로 신규 삽입 시도
            cursor.execute("""
                INSERT OR IGNORE INTO app_discovery
                (app_id, platform, first_seen_at, last_seen_at, sitemap_source, country_code)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (app_id, platform, now, now, sitemap_source, country_code))

            if cursor.rowcount > 0:
                new_count += 1
            else:
                # 이미 존재하면 last_seen_at 업데이트
                cursor.execute("""
                    UPDATE app_discovery
                    SET last_seen_at = ?, sitemap_source = ?
                    WHERE app_id = ? AND platform = ?
                """, (now, sitemap_source, app_id, platform))
                updated_count += 1

        except sqlite3.Error as e:
            print(f"  [오류] 앱 저장 실패 ({app_id}): {e}")
            continue

    conn.commit()
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
