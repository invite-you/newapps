"""
App Details Database
앱 상세정보, 다국어 정보, 수치 데이터, 리뷰를 저장하는 DB
변경 시에만 누적 저장 (이력 관리)
"""
import sqlite3
import os
import json
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

DATABASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_PATH = os.path.join(DATABASE_DIR, 'app_details.db')

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

    # apps: 앱 메타데이터 (변경 시에만 누적)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS apps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            free INTEGER,                       -- boolean
            has_iap INTEGER,                    -- boolean
            category_id TEXT,
            genre_id TEXT,
            genre_name TEXT,                    -- 장르명 (현지화)
            content_rating TEXT,
            content_rating_description TEXT,
            min_os_version TEXT,
            file_size INTEGER,
            supported_devices TEXT,             -- JSON array
            release_date TEXT,
            updated_date TEXT,
            privacy_policy_url TEXT,
            recorded_at TEXT NOT NULL
        )
    """)

    # apps_localized: 다국어 텍스트 (변경 시에만 누적)
    # 최적화: title+description이 기준 언어와 동일하면 저장하지 않음
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS apps_localized (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            language TEXT NOT NULL,
            title TEXT,
            summary TEXT,
            description TEXT,
            release_notes TEXT,
            recorded_at TEXT NOT NULL
        )
    """)

    # apps_metrics: 수치 데이터 (변경 시에만 누적)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS apps_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            score REAL,
            ratings INTEGER,
            reviews_count INTEGER,
            installs TEXT,                      -- "100,000+" 형태
            installs_exact INTEGER,             -- 정확한 수치 (Play Store)
            histogram TEXT,                     -- JSON array [1점, 2점, 3점, 4점, 5점]
            recorded_at TEXT NOT NULL
        )
    """)

    # app_reviews: 리뷰 (실행당 최대 20000건 수집, 이후 누적)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            UNIQUE(app_id, platform, review_id)
        )
    """)

    # failed_apps: 영구 실패 앱 (재시도 안 함)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS failed_apps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            details_collected_at TEXT,          -- 상세정보 마지막 수집 시각
            reviews_collected_at TEXT,          -- 리뷰 마지막 수집 시각
            reviews_total_count INTEGER DEFAULT 0,  -- 현재 수집된 총 리뷰 수
            initial_review_done INTEGER DEFAULT 0,  -- 최초 수집 완료 여부 (이후 중복 리뷰 발견 시 중단)
            UNIQUE(app_id, platform)
        )
    """)

    # 인덱스 생성
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_apps_app_id ON apps(app_id, platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_apps_recorded_at ON apps(recorded_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_apps_localized_app_id ON apps_localized(app_id, platform, language)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_apps_metrics_app_id ON apps_metrics(app_id, platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_app_reviews_app_id ON app_reviews(app_id, platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_app_reviews_review_id ON app_reviews(review_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_app_reviews_reviewed_at ON app_reviews(reviewed_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_failed_apps_app_id ON failed_apps(app_id, platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_collection_status_app_id ON collection_status(app_id, platform)")

    conn.commit()
    conn.close()
    print(f"Database initialized at {DATABASE_PATH}")


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
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM apps
        WHERE app_id = ? AND platform = ?
        ORDER BY recorded_at DESC
        LIMIT 1
    """, (app_id, platform))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def insert_app(data: Dict) -> Tuple[bool, int]:
    """앱 메타데이터를 삽입합니다. 변경이 있을 때만 삽입.
    Returns: (is_new_record, record_id)
    """
    app_id = data['app_id']
    platform = data['platform']

    # 최신 레코드와 비교
    existing = get_latest_app(app_id, platform)
    if existing and compare_records(existing, data):
        return False, existing['id']

    # 새 레코드 삽입
    conn = get_connection()
    cursor = conn.cursor()
    data['recorded_at'] = datetime.now().isoformat()

    columns = ', '.join(data.keys())
    placeholders = ', '.join(['?' for _ in data])
    cursor.execute(f"INSERT INTO apps ({columns}) VALUES ({placeholders})", list(data.values()))

    record_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return True, record_id


def get_latest_app_localized(app_id: str, platform: str, language: str) -> Optional[Dict]:
    """앱의 최신 다국어 데이터를 반환합니다."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM apps_localized
        WHERE app_id = ? AND platform = ? AND language = ?
        ORDER BY recorded_at DESC
        LIMIT 1
    """, (app_id, platform, language))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def insert_app_localized(data: Dict) -> Tuple[bool, int]:
    """앱 다국어 데이터를 삽입합니다. 변경이 있을 때만 삽입."""
    app_id = data['app_id']
    platform = data['platform']
    language = data['language']

    existing = get_latest_app_localized(app_id, platform, language)
    if existing and compare_records(existing, data):
        return False, existing['id']

    conn = get_connection()
    cursor = conn.cursor()
    data['recorded_at'] = datetime.now().isoformat()

    columns = ', '.join(data.keys())
    placeholders = ', '.join(['?' for _ in data])
    cursor.execute(f"INSERT INTO apps_localized ({columns}) VALUES ({placeholders})", list(data.values()))

    record_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return True, record_id


def get_latest_app_metrics(app_id: str, platform: str) -> Optional[Dict]:
    """앱의 최신 수치 데이터를 반환합니다."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM apps_metrics
        WHERE app_id = ? AND platform = ?
        ORDER BY recorded_at DESC
        LIMIT 1
    """, (app_id, platform))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def insert_app_metrics(data: Dict) -> Tuple[bool, int]:
    """앱 수치 데이터를 삽입합니다. 변경이 있을 때만 삽입."""
    app_id = data['app_id']
    platform = data['platform']

    existing = get_latest_app_metrics(app_id, platform)
    if existing and compare_records(existing, data):
        return False, existing['id']

    conn = get_connection()
    cursor = conn.cursor()
    data['recorded_at'] = datetime.now().isoformat()

    columns = ', '.join(data.keys())
    placeholders = ', '.join(['?' for _ in data])
    cursor.execute(f"INSERT INTO apps_metrics ({columns}) VALUES ({placeholders})", list(data.values()))

    record_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return True, record_id


def review_exists(app_id: str, platform: str, review_id: str) -> bool:
    """리뷰가 이미 존재하는지 확인합니다."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1 FROM app_reviews
        WHERE app_id = ? AND platform = ? AND review_id = ?
    """, (app_id, platform, review_id))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists


def insert_review(data: Dict) -> bool:
    """리뷰를 삽입합니다. 이미 존재하면 False 반환."""
    if review_exists(data['app_id'], data['platform'], data['review_id']):
        return False

    conn = get_connection()
    cursor = conn.cursor()
    data['recorded_at'] = datetime.now().isoformat()

    columns = ', '.join(data.keys())
    placeholders = ', '.join(['?' for _ in data])

    try:
        cursor.execute(f"INSERT INTO app_reviews ({columns}) VALUES ({placeholders})", list(data.values()))
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False


def insert_reviews_batch(reviews: List[Dict]) -> int:
    """리뷰를 배치로 삽입합니다. 삽입된 개수 반환."""
    if not reviews:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    inserted = 0

    for data in reviews:
        data['recorded_at'] = now
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])

        try:
            cursor.execute(f"INSERT INTO app_reviews ({columns}) VALUES ({placeholders})", list(data.values()))
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # 중복 리뷰

    conn.commit()
    conn.close()
    return inserted


def is_failed_app(app_id: str, platform: str) -> bool:
    """영구 실패 앱인지 확인합니다."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1 FROM failed_apps WHERE app_id = ? AND platform = ?
    """, (app_id, platform))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists


def mark_app_failed(app_id: str, platform: str, reason: str):
    """앱을 영구 실패로 표시합니다."""
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    cursor.execute("""
        INSERT OR REPLACE INTO failed_apps (app_id, platform, reason, failed_at)
        VALUES (?, ?, ?, ?)
    """, (app_id, platform, reason, now))

    conn.commit()
    conn.close()


def get_collection_status(app_id: str, platform: str) -> Optional[Dict]:
    """수집 상태를 반환합니다."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM collection_status WHERE app_id = ? AND platform = ?
    """, (app_id, platform))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def update_collection_status(app_id: str, platform: str,
                              details_collected: bool = False,
                              reviews_collected: bool = False,
                              reviews_count: int = None,
                              initial_review_done: bool = None):
    """수집 상태를 업데이트합니다."""
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    # 기존 상태 확인
    cursor.execute("""
        SELECT * FROM collection_status WHERE app_id = ? AND platform = ?
    """, (app_id, platform))
    existing = cursor.fetchone()

    if existing:
        updates = []
        params = []

        if details_collected:
            updates.append("details_collected_at = ?")
            params.append(now)

        if reviews_collected:
            updates.append("reviews_collected_at = ?")
            params.append(now)

        if reviews_count is not None:
            updates.append("reviews_total_count = ?")
            params.append(reviews_count)

        if initial_review_done is not None:
            updates.append("initial_review_done = ?")
            params.append(1 if initial_review_done else 0)

        if updates:
            params.extend([app_id, platform])
            cursor.execute(f"""
                UPDATE collection_status SET {', '.join(updates)}
                WHERE app_id = ? AND platform = ?
            """, params)
    else:
        cursor.execute("""
            INSERT INTO collection_status (app_id, platform, details_collected_at, reviews_collected_at, reviews_total_count, initial_review_done)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            app_id, platform,
            now if details_collected else None,
            now if reviews_collected else None,
            reviews_count or 0,
            1 if initial_review_done else 0
        ))

    conn.commit()
    conn.close()


def get_review_count(app_id: str, platform: str) -> int:
    """앱의 수집된 리뷰 수를 반환합니다."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) as count FROM app_reviews WHERE app_id = ? AND platform = ?
    """, (app_id, platform))
    count = cursor.fetchone()['count']
    conn.close()
    return count


def get_latest_review_id(app_id: str, platform: str) -> Optional[str]:
    """가장 최근에 수집된 리뷰의 review_id를 반환합니다."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT review_id FROM app_reviews
        WHERE app_id = ? AND platform = ?
        ORDER BY reviewed_at DESC
        LIMIT 1
    """, (app_id, platform))
    row = cursor.fetchone()
    conn.close()
    return row['review_id'] if row else None


def get_all_review_ids(app_id: str, platform: str) -> set:
    """앱의 모든 리뷰 ID를 반환합니다."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT review_id FROM app_reviews WHERE app_id = ? AND platform = ?
    """, (app_id, platform))
    ids = {row['review_id'] for row in cursor.fetchall()}
    conn.close()
    return ids


# ============================================================
# 버려진 앱 기준 (업계 표준 및 공식 정책 기반)
# - Pixalate: 2년 이상 업데이트 안 됨 = Abandoned
# - Google Play: 2년 이상 업데이트 안 됨 = 검색 제외/제거
# - Apple: 3년 이상 + 다운로드 극소 = 제거 대상
# 보수적으로 2년 기준 채택
# ============================================================
ABANDONED_THRESHOLD_DAYS = 730  # 2년 = 730일


def is_abandoned_app(app_id: str, platform: str) -> bool:
    """앱이 버려진 앱인지 확인합니다.

    기준: 마지막 업데이트(updated_date)가 2년 이상 경과한 앱
         업데이트 이력이 없으면 릴리즈 날짜(release_date) 기준

    Returns:
        True: 버려진 앱 (2년 이상 업데이트 안 됨)
        False: 활성 앱 또는 정보 없음
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT updated_date, release_date FROM apps
        WHERE app_id = ? AND platform = ?
        ORDER BY recorded_at DESC
        LIMIT 1
    """, (app_id, platform))

    row = cursor.fetchone()
    conn.close()

    if not row:
        return False  # 정보 없으면 활성으로 간주 (새로 수집할 앱)

    # updated_date 우선, 없으면 release_date 사용
    date_str = row['updated_date'] or row['release_date']
    if not date_str:
        return True  # 날짜 정보 둘 다 없으면 버려진 앱으로 간주

    try:
        # 날짜 형식: "2024-01-15T10:30:00Z" 또는 "2024-01-15"
        if 'T' in date_str:
            ref_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        else:
            ref_date = datetime.fromisoformat(date_str)

        # timezone-naive로 변환하여 비교
        if ref_date.tzinfo is not None:
            ref_date = ref_date.replace(tzinfo=None)

        days_since = (datetime.now() - ref_date).days
        return days_since >= ABANDONED_THRESHOLD_DAYS
    except (ValueError, TypeError):
        return False  # 파싱 실패 시 활성으로 간주


def get_apps_needing_update(platform: str, limit: int = 1000) -> Tuple[List[str], set]:
    """업데이트가 필요한 앱 ID 목록을 반환합니다.

    수집 주기:
    - 신규 앱 (수집 이력 없음): 즉시 수집
    - 활성 앱 (2년 이내 업데이트): 매일 수집
    - 버려진 앱 (2년 이상 업데이트 안 됨): 7일에 1번 수집

    Returns:
        (수집이 필요한 앱 ID 리스트, 제외된 앱 ID set)
    """
    conn = get_connection()
    cursor = conn.cursor()

    # 실패한 앱 목록 (SQL에서 제외하고, 반환값에도 포함)
    cursor.execute("""
        SELECT app_id FROM failed_apps WHERE platform = ?
    """, (platform,))
    failed_ids = {row['app_id'] for row in cursor.fetchall()}

    # 이미 수집된 앱 중 업데이트 주기가 지난 앱 확인
    # failed_apps는 SQL 단계에서 제외하여 불필요한 처리 방지
    cursor.execute("""
        SELECT cs.app_id, cs.details_collected_at, a.updated_date, a.release_date
        FROM collection_status cs
        LEFT JOIN (
            SELECT app_id, platform, updated_date, release_date,
                   ROW_NUMBER() OVER (PARTITION BY app_id, platform ORDER BY recorded_at DESC) as rn
            FROM apps
        ) a ON cs.app_id = a.app_id AND cs.platform = a.platform AND a.rn = 1
        WHERE cs.platform = ?
          AND cs.details_collected_at IS NOT NULL
          AND cs.app_id NOT IN (SELECT app_id FROM failed_apps WHERE platform = ?)
    """, (platform, platform))

    needs_update = []
    skip_ids = set()

    for row in cursor.fetchall():
        app_id = row['app_id']
        collected_at_str = row['details_collected_at']
        # updated_date 우선, 없으면 release_date 사용
        ref_date_str = row['updated_date'] or row['release_date']

        try:
            collected_at = datetime.fromisoformat(collected_at_str)
            hours_since_collection = (datetime.now() - collected_at).total_seconds() / 3600

            # 버려진 앱 여부 판단
            # 날짜 정보가 둘 다 없으면 버려진 앱으로 간주
            is_abandoned = True if not ref_date_str else False
            if ref_date_str:
                try:
                    if 'T' in ref_date_str:
                        ref_date = datetime.fromisoformat(ref_date_str.replace('Z', '+00:00'))
                    else:
                        ref_date = datetime.fromisoformat(ref_date_str)
                    if ref_date.tzinfo is not None:
                        ref_date = ref_date.replace(tzinfo=None)
                    days_since = (datetime.now() - ref_date).days
                    is_abandoned = days_since >= ABANDONED_THRESHOLD_DAYS
                except (ValueError, TypeError):
                    pass

            # 수집 주기 결정
            if is_abandoned:
                # 버려진 앱: 7일(168시간)에 1번
                if hours_since_collection >= 168:
                    needs_update.append(app_id)
                else:
                    skip_ids.add(app_id)
            else:
                # 활성 앱: 매일(24시간)에 1번
                if hours_since_collection >= 24:
                    needs_update.append(app_id)
                else:
                    skip_ids.add(app_id)
        except (ValueError, TypeError):
            # 파싱 실패 시 재수집 대상
            needs_update.append(app_id)

    conn.close()

    # 아직 수집 안 된 신규 앱은 별도로 처리해야 함
    # 이 함수는 기존에 수집된 앱 중 업데이트 필요한 것만 반환
    return needs_update[:limit], skip_ids | failed_ids


def get_stats() -> Dict[str, Any]:
    """DB 통계를 반환합니다."""
    conn = get_connection()
    cursor = conn.cursor()

    stats = {}

    # 각 테이블 레코드 수
    for table in ['apps', 'apps_localized', 'apps_metrics', 'app_reviews', 'failed_apps', 'collection_status']:
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

    conn.close()
    return stats


if __name__ == '__main__':
    init_database()
    print("Database schema created successfully.")
    stats = get_stats()
    print(f"Stats: {stats}")
