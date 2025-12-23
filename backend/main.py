# -*- coding: utf-8 -*-
"""
메인 실행 스크립트
전체 프로세스: DB 초기화 -> Sitemap 수집 -> 상세정보 수집 -> 점수 계산

수집 모드:
1. sitemap: Sitemap 기반 전체 앱 ID 수집 (권장)
2. search: 검색어 기반 앱 수집 (기존 방식)
3. both: 두 가지 모드 모두 실행
"""
import sys
import argparse
from datetime import datetime

from database.db import init_database, log_step
from database.sitemap_db import init_sitemap_database, get_discovery_stats
from scrapers import google_play_scraper, app_store_scraper
from scrapers.sitemap_collector import collect_all_sitemaps
from scrapers.sitemap_details_fetcher import fetch_all_new_app_details
from analyzer import app_analyzer


def run_sitemap_collection(google_limit: int = None, appstore_limit: int = None):
    """
    Sitemap 기반 앱 수집 실행

    Args:
        google_limit: Google Play sitemap 처리 수 제한 (None이면 전체)
        appstore_limit: App Store sitemap 타입별 처리 수 제한 (None이면 전체)
    """
    start_time = datetime.now()
    log_step("Sitemap 수집 모드", "시작", start_time)

    # Sitemap DB 초기화
    init_sitemap_database()

    # 1. Sitemap에서 앱 ID 수집
    log_step("1단계", "Sitemap에서 앱 ID 수집", datetime.now())
    sitemap_results = collect_all_sitemaps(google_limit, appstore_limit)

    # 2. 수집 통계 출력
    stats = get_discovery_stats()
    print("\n발견된 앱 통계:")
    for platform, data in stats.get('by_platform', {}).items():
        today_count = stats.get('today', {}).get(platform, 0)
        print(f"  {platform}: 전체 {data['total']:,}개, 오늘 신규 {today_count:,}개")

    log_step("Sitemap 수집 모드", "완료", start_time)

    return sitemap_results


def run_details_collection(google_limit: int = 200, appstore_limit: int = 500):
    """
    Sitemap에서 발견된 앱의 상세 정보 수집

    Args:
        google_limit: Google Play 앱 수집 제한
        appstore_limit: App Store 앱 수집 제한
    """
    start_time = datetime.now()
    log_step("상세정보 수집", "시작", start_time)

    # Apps DB 초기화
    init_database()

    # 상세 정보 수집
    log_step("2단계", "상세 정보 수집", datetime.now())
    details_results = fetch_all_new_app_details(google_limit, appstore_limit)

    log_step("상세정보 수집", "완료", start_time)

    return details_results


def run_search_collection():
    """검색어 기반 앱 수집 실행 (기존 방식)"""
    start_time = datetime.now()
    log_step("검색 수집 모드", "시작", start_time)

    # DB 초기화
    init_database()

    # Google Play Store 데이터 수집
    google_play_scraper.scrape_all_countries()

    # App Store 데이터 수집
    app_store_scraper.scrape_all_countries()

    log_step("검색 수집 모드", "완료", start_time)


def run_analysis():
    """앱 점수 계산 및 주목 앱 선별"""
    log_step("3단계", "앱 분석 및 점수 계산", datetime.now())
    app_analyzer.analyze_and_update_scores()


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(
        description='앱 마켓 데이터 수집 및 분석',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  # Sitemap 기반 전체 수집 (권장)
  python main.py --mode sitemap

  # 빠른 테스트 (적은 sitemap 수)
  python main.py --mode sitemap --google-sitemap-limit 10 --appstore-sitemap-limit 5

  # 기존 검색 기반 수집
  python main.py --mode search

  # Sitemap 수집만 (상세정보 수집 없이)
  python main.py --mode sitemap --sitemap-only

  # 상세정보 수집만 (이전에 sitemap 수집한 후)
  python main.py --details-only
        """
    )

    parser.add_argument('--mode', choices=['sitemap', 'search', 'both'],
                        default='sitemap',
                        help='수집 모드 (기본: sitemap)')

    # Sitemap 옵션
    parser.add_argument('--google-sitemap-limit', type=int, default=None,
                        help='Google Play sitemap 처리 수 제한 (기본: 전체)')
    parser.add_argument('--appstore-sitemap-limit', type=int, default=None,
                        help='App Store sitemap 타입별 처리 수 제한 (기본: 전체)')
    parser.add_argument('--sitemap-only', action='store_true',
                        help='Sitemap 수집만 실행 (상세정보 수집 안 함)')

    # 상세정보 수집 옵션
    parser.add_argument('--details-only', action='store_true',
                        help='상세정보 수집만 실행')
    parser.add_argument('--google-limit', type=int, default=200,
                        help='Google Play 상세정보 수집 제한 (기본: 200)')
    parser.add_argument('--appstore-limit', type=int, default=500,
                        help='App Store 상세정보 수집 제한 (기본: 500)')

    # 분석 옵션
    parser.add_argument('--skip-analysis', action='store_true',
                        help='분석 단계 건너뛰기')

    args = parser.parse_args()

    # 전체 시작
    total_start = datetime.now()
    log_step("전체 프로세스", "시작", total_start)

    print("\n" + "=" * 60)
    print("앱 마켓 데이터 수집 및 분석")
    print("=" * 60)
    print(f"모드: {args.mode}")
    print("=" * 60 + "\n")

    try:
        # 상세정보만 수집
        if args.details_only:
            run_details_collection(args.google_limit, args.appstore_limit)
            if not args.skip_analysis:
                run_analysis()

        # Sitemap 모드
        elif args.mode == 'sitemap':
            run_sitemap_collection(args.google_sitemap_limit, args.appstore_sitemap_limit)

            if not args.sitemap_only:
                run_details_collection(args.google_limit, args.appstore_limit)

            if not args.skip_analysis:
                run_analysis()

        # 검색 모드 (기존 방식)
        elif args.mode == 'search':
            run_search_collection()

            if not args.skip_analysis:
                run_analysis()

        # 둘 다
        elif args.mode == 'both':
            run_sitemap_collection(args.google_sitemap_limit, args.appstore_sitemap_limit)

            if not args.sitemap_only:
                run_details_collection(args.google_limit, args.appstore_limit)

            run_search_collection()

            if not args.skip_analysis:
                run_analysis()

    except KeyboardInterrupt:
        print("\n\n중단됨 (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        print(f"\n오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    log_step("전체 프로세스", "완료", total_start)

    print("\n" + "=" * 60)
    print("데이터 수집 및 분석이 완료되었습니다!")
    print("API 서버를 실행하여 웹사이트에서 확인하세요:")
    print("  cd api && npm start")
    print("=" * 60)


if __name__ == "__main__":
    main()
