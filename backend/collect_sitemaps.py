#!/usr/bin/env python3
"""
Sitemap 수집 메인 스크립트
App Store와 Play Store sitemap에서 앱 로컬라이제이션 정보를 수집합니다.

Usage:
    python collect_sitemaps.py              # 모든 스토어 수집
    python collect_sitemaps.py --app-store  # App Store만 수집
    python collect_sitemaps.py --play-store # Play Store만 수집
    python collect_sitemaps.py --stats      # 통계만 출력
"""
import sys
import os
import argparse
from datetime import datetime

# 경로 설정
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.sitemap_apps_db import init_database, get_stats
from scrapers.app_store_sitemap_collector import AppStoreSitemapCollector
from scrapers.play_store_sitemap_collector import PlayStoreSitemapCollector


def print_stats():
    """DB 통계를 출력합니다."""
    stats = get_stats()
    print("\n" + "=" * 60)
    print("Sitemap Apps Database Statistics")
    print("=" * 60)
    print(f"Total Localizations: {stats['total_localizations']:,}")
    print()

    if stats['platform_stats']:
        print("Platform Statistics:")
        for platform, data in stats['platform_stats'].items():
            print(f"  {platform}:")
            print(f"    - Unique Apps: {data['apps']:,}")
            print(f"    - Localizations: {data['localizations']:,}")
    else:
        print("No data collected yet.")

    print()
    if stats['sitemap_file_counts']:
        print("Sitemap Files Tracked:")
        for platform, count in stats['sitemap_file_counts'].items():
            print(f"  {platform}: {count:,} files")

    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Collect app localizations from App Store and Play Store sitemaps'
    )
    parser.add_argument('--app-store', action='store_true', help='Collect from App Store only')
    parser.add_argument('--play-store', action='store_true', help='Collect from Play Store only')
    parser.add_argument('--stats', action='store_true', help='Print statistics only')
    parser.add_argument('--quiet', '-q', action='store_true', help='Quiet mode (less output)')

    args = parser.parse_args()

    # DB 초기화
    init_database()

    # 통계만 출력
    if args.stats:
        print_stats()
        return

    # 수집할 스토어 결정
    collect_app_store = args.app_store or (not args.app_store and not args.play_store)
    collect_play_store = args.play_store or (not args.app_store and not args.play_store)

    verbose = not args.quiet

    print(f"\n{'=' * 60}")
    print(f"Sitemap Collection Started at {datetime.now().isoformat()}")
    print(f"{'=' * 60}\n")

    all_stats = {}

    # App Store 수집
    if collect_app_store:
        print("\n>>> Collecting from App Store...\n")
        collector = AppStoreSitemapCollector(verbose=verbose)
        all_stats['app_store'] = collector.collect_all()

    # Play Store 수집
    if collect_play_store:
        print("\n>>> Collecting from Play Store...\n")
        collector = PlayStoreSitemapCollector(verbose=verbose)
        all_stats['play_store'] = collector.collect_all()

    # 결과 요약
    print(f"\n{'=' * 60}")
    print(f"Collection Completed at {datetime.now().isoformat()}")
    print(f"{'=' * 60}")

    for platform, stats in all_stats.items():
        print(f"\n{platform}:")
        print(f"  - Sitemap files processed: {stats['sitemap_files_processed']}")
        print(f"  - Sitemap files skipped (unchanged): {stats['sitemap_files_skipped']}")
        print(f"  - New localizations: {stats['new_localizations']}")
        print(f"  - Total localizations in processed files: {stats['total_localizations']}")
        if 'skipped_non_apps' in stats:
            print(f"  - Non-app entries skipped: {stats['skipped_non_apps']}")
        print(f"  - Errors: {stats['errors']}")

    # 전체 DB 통계 출력
    print_stats()


if __name__ == '__main__':
    main()
