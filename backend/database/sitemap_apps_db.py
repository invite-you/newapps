"""
Sitemap Apps Database
앱 스토어 sitemap에서 수집한 앱 로컬라이제이션 정보를 저장하는 DB
"""
import sqlite3
import os
from datetime import datetime
from typing import Optional, List, Dict, Any

DATABASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_PATH = os.path.join(DATABASE_DIR, 'sitemap_apps.db')


def get_connection() -> sqlite3.Connection:
    """DB 연결을 반환합니다."""
    conn = sqlite3.connect(DATABASE_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_database():
    """DB 테이블을 초기화합니다."""
    conn = get_connection()
    cursor = conn.cursor()

    # sitemap_files: xml.gz 파일별 MD5 해시 저장
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sitemap_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT NOT NULL,           -- 'app_store' or 'play_store'
            file_url TEXT NOT NULL UNIQUE,    -- sitemap xml.gz 파일 URL
            md5_hash TEXT,                    -- 파일의 MD5 해시
            last_collected_at TEXT,           -- 마지막 수집 시각
            app_count INTEGER DEFAULT 0,      -- 해당 파일에서 수집된 앱 수
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)

    # app_localizations: 앱 ID + language + country 별 정보 저장
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app_localizations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT NOT NULL,           -- 'app_store' or 'play_store'
            app_id TEXT NOT NULL,             -- 앱 ID (App Store: 숫자, Play Store: 패키지명)
            language TEXT NOT NULL,           -- 언어 코드 (ko, en, ja 등)
            country TEXT NOT NULL,            -- 국가 코드 (kr, us, jp 등)
            href TEXT NOT NULL,               -- 해당 로컬라이제이션의 URL
            source_file TEXT NOT NULL,        -- 수집된 sitemap 파일명
            first_seen_at TEXT DEFAULT (datetime('now')),  -- 처음 발견 시각
            last_seen_at TEXT DEFAULT (datetime('now')),   -- 마지막 발견 시각
            UNIQUE(platform, app_id, language, country)
        )
    """)

    # 인덱스 생성
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_app_localizations_platform ON app_localizations(platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_app_localizations_app_id ON app_localizations(app_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_app_localizations_country ON app_localizations(country)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_app_localizations_language ON app_localizations(language)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_app_localizations_first_seen ON app_localizations(first_seen_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sitemap_files_platform ON sitemap_files(platform)")

    conn.commit()
    conn.close()
    print(f"Database initialized at {DATABASE_PATH}")


def get_sitemap_file_hash(file_url: str) -> Optional[str]:
    """특정 sitemap 파일의 저장된 MD5 해시를 반환합니다."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT md5_hash FROM sitemap_files WHERE file_url = ?", (file_url,))
    row = cursor.fetchone()
    conn.close()
    return row['md5_hash'] if row else None


def update_sitemap_file(platform: str, file_url: str, md5_hash: str, app_count: int):
    """sitemap 파일 정보를 업데이트합니다."""
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    cursor.execute("""
        INSERT INTO sitemap_files (platform, file_url, md5_hash, last_collected_at, app_count, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(file_url) DO UPDATE SET
            md5_hash = excluded.md5_hash,
            last_collected_at = excluded.last_collected_at,
            app_count = excluded.app_count,
            updated_at = excluded.updated_at
    """, (platform, file_url, md5_hash, now, app_count, now))

    conn.commit()
    conn.close()


def upsert_app_localization(platform: str, app_id: str, language: str, country: str,
                            href: str, source_file: str) -> bool:
    """앱 로컬라이제이션 정보를 추가하거나 업데이트합니다. 새로 추가된 경우 True 반환."""
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    # 기존 레코드 확인
    cursor.execute("""
        SELECT id FROM app_localizations
        WHERE platform = ? AND app_id = ? AND language = ? AND country = ?
    """, (platform, app_id, language, country))
    existing = cursor.fetchone()

    if existing:
        # 기존 레코드 업데이트
        cursor.execute("""
            UPDATE app_localizations
            SET href = ?, source_file = ?, last_seen_at = ?
            WHERE platform = ? AND app_id = ? AND language = ? AND country = ?
        """, (href, source_file, now, platform, app_id, language, country))
        is_new = False
    else:
        # 새 레코드 추가
        cursor.execute("""
            INSERT INTO app_localizations (platform, app_id, language, country, href, source_file, first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (platform, app_id, language, country, href, source_file, now, now))
        is_new = True

    conn.commit()
    conn.close()
    return is_new


def upsert_app_localizations_batch(localizations: List[Dict[str, Any]]) -> int:
    """앱 로컬라이제이션 정보를 배치로 추가하거나 업데이트합니다. 새로 추가된 개수 반환."""
    if not localizations:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    new_count = 0

    for loc in localizations:
        # 기존 레코드 확인
        cursor.execute("""
            SELECT id FROM app_localizations
            WHERE platform = ? AND app_id = ? AND language = ? AND country = ?
        """, (loc['platform'], loc['app_id'], loc['language'], loc['country']))
        existing = cursor.fetchone()

        if existing:
            cursor.execute("""
                UPDATE app_localizations
                SET href = ?, source_file = ?, last_seen_at = ?
                WHERE platform = ? AND app_id = ? AND language = ? AND country = ?
            """, (loc['href'], loc['source_file'], now,
                  loc['platform'], loc['app_id'], loc['language'], loc['country']))
        else:
            cursor.execute("""
                INSERT INTO app_localizations (platform, app_id, language, country, href, source_file, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (loc['platform'], loc['app_id'], loc['language'], loc['country'],
                  loc['href'], loc['source_file'], now, now))
            new_count += 1

    conn.commit()
    conn.close()
    return new_count


def get_stats() -> Dict[str, Any]:
    """DB 통계를 반환합니다."""
    conn = get_connection()
    cursor = conn.cursor()

    # 전체 앱 로컬라이제이션 수
    cursor.execute("SELECT COUNT(*) as count FROM app_localizations")
    total_localizations = cursor.fetchone()['count']

    # 플랫폼별 통계
    cursor.execute("""
        SELECT platform, COUNT(DISTINCT app_id) as app_count, COUNT(*) as localization_count
        FROM app_localizations
        GROUP BY platform
    """)
    platform_stats = {row['platform']: {'apps': row['app_count'], 'localizations': row['localization_count']}
                      for row in cursor.fetchall()}

    # sitemap 파일 수
    cursor.execute("SELECT platform, COUNT(*) as count FROM sitemap_files GROUP BY platform")
    sitemap_counts = {row['platform']: row['count'] for row in cursor.fetchall()}

    conn.close()

    return {
        'total_localizations': total_localizations,
        'platform_stats': platform_stats,
        'sitemap_file_counts': sitemap_counts
    }


if __name__ == '__main__':
    init_database()
    print("Database schema created successfully.")
    stats = get_stats()
    print(f"Stats: {stats}")
