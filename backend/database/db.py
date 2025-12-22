# -*- coding: utf-8 -*-
"""
데이터베이스 연결 및 초기화
"""
import sqlite3
import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import DATABASE_PATH, LOG_FORMAT


def log_step(step, message, start_time=None):
    """타임스탬프 로그 출력 (AGENT.MD 지침 4번)"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    duration = (datetime.now() - start_time).total_seconds() if start_time else 0
    print(LOG_FORMAT.format(
        timestamp=timestamp,
        step=step,
        message=message,
        duration=f"{duration:.2f}"
    ))


def get_connection():
    """데이터베이스 연결 반환"""
    db_dir = os.path.dirname(DATABASE_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row  # 딕셔너리 형태로 결과 반환
    return conn


def init_database():
    """데이터베이스 초기화"""
    start_time = datetime.now()
    log_step("데이터베이스 초기화", "시작", start_time)

    conn = get_connection()
    cursor = conn.cursor()

    # 앱 정보 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS apps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            country_code TEXT NOT NULL,
            title TEXT NOT NULL,
            developer TEXT,
            icon_url TEXT,
            rating REAL,
            rating_count INTEGER,
            installs TEXT,
            price TEXT,
            category TEXT,
            description TEXT,
            release_date TEXT,
            updated_date TEXT,
            version TEXT,
            url TEXT,
            score REAL DEFAULT 0,
            is_featured INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(app_id, platform, country_code)
        )
    """)

    # 인덱스 생성 (성능 최적화)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_platform ON apps(platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_country ON apps(country_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_score ON apps(score DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_featured ON apps(is_featured)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON apps(created_at DESC)")

    conn.commit()
    conn.close()

    log_step("데이터베이스 초기화", "완료", start_time)


if __name__ == "__main__":
    init_database()
