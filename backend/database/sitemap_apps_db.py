"""
Sitemap Apps Database
앱 스토어 sitemap에서 수집한 앱 로컬라이제이션 정보를 저장하는 DB

최적화: href 필드 제거 (불필요 - URL은 app_id/country로 재구성 가능)
"""
import os
from datetime import datetime
from typing import Optional, List, Dict, Any

import psycopg
from psycopg.rows import dict_row
from utils.logger import get_timestamped_logger

# psycopg DSN 참고: https://www.psycopg.org/psycopg3/docs/basic/usage.html
DB_DSN = os.getenv("SITEMAP_DB_DSN")
DB_HOST = os.getenv("SITEMAP_DB_HOST", "localhost")
DB_PORT = int(os.getenv("SITEMAP_DB_PORT", "5432"))
DB_NAME = os.getenv("SITEMAP_DB_NAME", "sitemap_apps")
DB_USER = os.getenv("SITEMAP_DB_USER", "sitemap_apps")
DB_PASSWORD = os.getenv("SITEMAP_DB_PASSWORD", "")
LOG_FILE_PREFIX = "sitemap_apps_db"
DB_LOGGER = get_timestamped_logger("sitemap_apps_db", file_prefix=LOG_FILE_PREFIX)


def get_connection() -> psycopg.Connection:
    """DB 연결을 반환합니다."""
    dsn = DB_DSN or (
        f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD}"
    )
    conn = psycopg.connect(dsn, row_factory=dict_row)
    return conn


def init_database():
    """DB 테이블을 초기화합니다."""
    conn = get_connection()
    cursor = conn.cursor()

    # sitemap_files: xml.gz 파일별 MD5 해시 저장
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sitemap_files (
            id BIGSERIAL PRIMARY KEY,
            platform TEXT NOT NULL,           -- 'app_store' or 'play_store'
            file_url TEXT NOT NULL UNIQUE,    -- sitemap xml.gz 파일 URL
            md5_hash TEXT,                    -- 파일의 MD5 해시
            last_collected_at TIMESTAMPTZ,    -- 마지막 수집 시각
            app_count INTEGER DEFAULT 0,      -- 해당 파일에서 수집된 앱 수
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # app_localizations: 앱 ID + language + country 별 정보 저장
    # 최적화: href 필드 제거 (URL은 platform/country/app_id로 재구성 가능)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app_localizations (
            id BIGSERIAL PRIMARY KEY,
            platform TEXT NOT NULL,           -- 'app_store' or 'play_store'
            app_id TEXT NOT NULL,             -- 앱 ID (App Store: 숫자, Play Store: 패키지명)
            language TEXT NOT NULL,           -- 언어 코드 (ko, en, ja 등)
            country TEXT NOT NULL,            -- 국가 코드 (kr, us, jp 등)
            source_file TEXT NOT NULL,        -- 수집된 sitemap 파일명
            first_seen_at TIMESTAMPTZ DEFAULT NOW(),  -- 처음 발견 시각
            last_seen_at TIMESTAMPTZ DEFAULT NOW(),   -- 마지막 발견 시각
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
    DB_LOGGER.info("Database initialized.")


def get_sitemap_file_hash(file_url: str) -> Optional[str]:
    """특정 sitemap 파일의 저장된 MD5 해시를 반환합니다."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT md5_hash FROM sitemap_files WHERE file_url = %s", (file_url,))
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
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT(file_url) DO UPDATE SET
            md5_hash = EXCLUDED.md5_hash,
            last_collected_at = EXCLUDED.last_collected_at,
            app_count = EXCLUDED.app_count,
            updated_at = EXCLUDED.updated_at
    """, (platform, file_url, md5_hash, now, app_count, now))

    conn.commit()
    conn.close()


def upsert_app_localization(platform: str, app_id: str, language: str, country: str,
                            source_file: str) -> bool:
    """앱 로컬라이제이션 정보를 추가하거나 업데이트합니다. 새로 추가된 경우 True 반환."""
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    cursor.execute("""
        INSERT INTO app_localizations (platform, app_id, language, country, source_file, first_seen_at, last_seen_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(platform, app_id, language, country) DO UPDATE SET
            source_file = EXCLUDED.source_file,
            last_seen_at = EXCLUDED.last_seen_at
        RETURNING (xmax = 0) AS inserted
    """, (platform, app_id, language, country, source_file, now, now))
    is_new = cursor.fetchone()['inserted']

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

    # (platform, app_id, language, country) 키 기준으로 중복 제거
    aggregated = {}
    for loc in localizations:
        key = (loc['platform'], loc['app_id'], loc['language'], loc['country'])
        aggregated[key] = loc

    values = [
        (
            loc['platform'],
            loc['app_id'],
            loc['language'],
            loc['country'],
            loc['source_file'],
            now,
            now
        )
        for loc in aggregated.values()
    ]

    try:
        value_placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s)"] * len(values))
        insert_sql = f"""
            INSERT INTO app_localizations (
                platform, app_id, language, country, source_file, first_seen_at, last_seen_at
            )
            VALUES {value_placeholders}
            ON CONFLICT(platform, app_id, language, country) DO UPDATE SET
                source_file = EXCLUDED.source_file,
                last_seen_at = EXCLUDED.last_seen_at
            RETURNING (xmax = 0) AS inserted
        """
        flattened_values = [item for row in values for item in row]

        cursor.execute(insert_sql, flattened_values)
        inserted_rows = cursor.fetchall()
        new_count = sum(1 for row in inserted_rows if row['inserted'])
        conn.commit()
    finally:
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
    DB_LOGGER.info("Database schema created successfully.")
    stats = get_stats()
    DB_LOGGER.info(f"Stats: {stats}")
