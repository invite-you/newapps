# -*- coding: utf-8 -*-
"""
메인 실행 스크립트
전체 프로세스: DB 초기화 -> 데이터 수집 -> 점수 계산
"""
import sys
from datetime import datetime

from database.db import init_database, log_step
from scrapers import google_play_scraper, app_store_scraper
from analyzer import app_analyzer


def main():
    """전체 프로세스 실행"""
    total_start = datetime.now()
    log_step("전체 프로세스", "시작", total_start)

    # 1. 데이터베이스 초기화
    init_database()

    # 2. Google Play Store 데이터 수집
    google_play_scraper.scrape_all_countries()

    # 3. App Store 데이터 수집
    app_store_scraper.scrape_all_countries()

    # 4. 앱 점수 계산 및 주목 앱 선별
    app_analyzer.analyze_and_update_scores()

    log_step("전체 프로세스", "완료", total_start)
    print("\n" + "="*50)
    print("데이터 수집 및 분석이 완료되었습니다!")
    print("API 서버를 실행하여 웹사이트에서 확인하세요.")
    print("="*50)


if __name__ == "__main__":
    main()
