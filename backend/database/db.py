# -*- coding: utf-8 -*-
"""
데이터베이스 연결 및 초기화
확장된 스키마: App Store/Google Play의 모든 필드 저장
"""
import sqlite3
import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import DATABASE_PATH, LOG_FORMAT, timing_tracker


def log_step(step, message, task_name=None):
    """
    타임스탬프 로그 출력 (AGENT.MD 지침 4번)

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
    """데이터베이스 연결 반환 (WAL 모드, 타임아웃 설정)"""
    db_dir = os.path.dirname(DATABASE_PATH)
    # 디렉토리가 비어있지 않고 존재하지 않으면 생성
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    # timeout=30으로 lock 대기 시간 설정
    conn = sqlite3.connect(DATABASE_PATH, timeout=30)
    conn.row_factory = sqlite3.Row  # 딕셔너리 형태로 결과 반환

    # WAL 모드 활성화 - 동시 읽기/쓰기 성능 향상
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")

    return conn


def init_database(force_reset: bool = False):
    """
    데이터베이스 초기화 - 확장된 스키마

    Args:
        force_reset: True면 테이블 삭제 후 재생성 (기존 데이터 삭제됨)
    """
    timing_tracker.start_task("데이터베이스 초기화")
    log_step("데이터베이스 초기화", "시작", "데이터베이스 초기화")

    conn = get_connection()
    cursor = conn.cursor()

    # 기존 테이블 삭제 (스키마 변경 시에만 사용)
    if force_reset:
        cursor.execute("DROP TABLE IF EXISTS apps")

    # 확장된 앱 정보 테이블 - App Store/Google Play 모든 필드 포함
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS apps (
            -- 기본 식별 정보
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app_id TEXT NOT NULL,
            bundle_id TEXT,
            platform TEXT NOT NULL,
            country_code TEXT NOT NULL,

            -- 앱 기본 정보
            title TEXT NOT NULL,
            developer TEXT,
            developer_id TEXT,
            developer_email TEXT,
            developer_website TEXT,
            developer_address TEXT,
            seller_name TEXT,

            -- 아이콘 및 이미지
            icon_url TEXT,
            icon_url_small TEXT,
            icon_url_large TEXT,
            header_image TEXT,
            screenshots TEXT,  -- JSON 배열

            -- 평점 및 리뷰
            rating REAL,
            rating_count INTEGER,
            rating_count_current_version INTEGER,
            rating_current_version REAL,
            reviews_count INTEGER,
            histogram TEXT,  -- JSON: 별점별 리뷰 수

            -- 설치 및 가격
            installs TEXT,
            installs_min INTEGER,
            installs_exact INTEGER,
            price REAL,
            price_formatted TEXT,
            currency TEXT,
            free INTEGER,

            -- 카테고리
            category TEXT,
            category_id TEXT,
            genres TEXT,  -- JSON 배열
            genre_ids TEXT,  -- JSON 배열

            -- 설명
            description TEXT,
            description_html TEXT,
            summary TEXT,
            release_notes TEXT,

            -- 날짜 정보
            release_date TEXT,
            updated_date TEXT,
            current_version_release_date TEXT,

            -- 버전 및 기술 정보
            version TEXT,
            minimum_os_version TEXT,
            file_size INTEGER,
            file_size_formatted TEXT,
            supported_devices TEXT,  -- JSON 배열
            languages TEXT,  -- JSON 배열

            -- 콘텐츠 등급
            content_rating TEXT,
            content_rating_description TEXT,
            advisories TEXT,  -- JSON 배열

            -- 앱 내 구매 및 광고
            has_iap INTEGER,
            iap_price_range TEXT,
            contains_ads INTEGER,
            ad_supported INTEGER,
            game_center_enabled INTEGER,

            -- URL 정보
            url TEXT,
            store_url TEXT,
            privacy_policy_url TEXT,

            -- 순위 정보 (RSS에서)
            chart_position INTEGER,
            chart_type TEXT,

            -- 기타 메타 정보
            features TEXT,  -- JSON 배열
            permissions TEXT,  -- JSON 배열

            -- 점수 및 주목 앱 선별
            score REAL DEFAULT 0,
            is_featured INTEGER DEFAULT 0,

            -- 타임스탬프
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            -- 유니크 제약 조건
            UNIQUE(app_id, platform, country_code)
        )
    """)
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_apps_updated_at
        AFTER UPDATE ON apps
        FOR EACH ROW
        WHEN NEW.updated_at = OLD.updated_at
        BEGIN
            UPDATE apps SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
        END;
    """)

    # 인덱스 생성 (성능 최적화)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_platform ON apps(platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_country ON apps(country_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_score ON apps(score DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_featured ON apps(is_featured)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON apps(created_at DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_updated_date ON apps(updated_date DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_release_date ON apps(release_date DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rating ON apps(rating DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_category ON apps(category)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_developer ON apps(developer)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_chart ON apps(chart_type, chart_position)")

    conn.commit()
    conn.close()

    log_step("데이터베이스 초기화", "완료", "데이터베이스 초기화")


def get_app_columns():
    """앱 테이블의 컬럼 목록 반환"""
    return [
        'app_id', 'bundle_id', 'platform', 'country_code',
        'title', 'developer', 'developer_id', 'developer_email',
        'developer_website', 'developer_address', 'seller_name',
        'icon_url', 'icon_url_small', 'icon_url_large', 'header_image', 'screenshots',
        'rating', 'rating_count', 'rating_count_current_version',
        'rating_current_version', 'reviews_count', 'histogram',
        'installs', 'installs_min', 'installs_exact', 'price', 'price_formatted',
        'currency', 'free',
        'category', 'category_id', 'genres', 'genre_ids',
        'description', 'description_html', 'summary', 'release_notes',
        'release_date', 'updated_date', 'current_version_release_date',
        'version', 'minimum_os_version', 'file_size', 'file_size_formatted',
        'supported_devices', 'languages',
        'content_rating', 'content_rating_description', 'advisories',
        'has_iap', 'iap_price_range', 'contains_ads', 'ad_supported', 'game_center_enabled',
        'url', 'store_url', 'privacy_policy_url',
        'chart_position', 'chart_type',
        'features', 'permissions',
        'score', 'is_featured',
    ]


if __name__ == "__main__":
    init_database()
