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
import time
from datetime import datetime

# 경로 설정
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.sitemap_apps_db import init_database, get_stats
from scrapers.app_store_sitemap_collector import AppStoreSitemapCollector
from scrapers.play_store_sitemap_collector import PlayStoreSitemapCollector
from utils.logger import get_timestamped_logger

LOG_FILE_PREFIX = "collect_sitemaps"


def print_stats(logger):
    """DB 통계를 출력합니다."""
    stats = get_stats()
    logger.info("\n" + "=" * 60)
    logger.info("Sitemap Apps Database Statistics")
    logger.info("=" * 60)
    logger.info(f"Total Localizations: {stats['total_localizations']:,}")
    logger.info("")

    if stats['platform_stats']:
        logger.info("Platform Statistics:")
        for platform, data in stats['platform_stats'].items():
            logger.info(f"  {platform}:")
            logger.info(f"    - Unique Apps: {data['apps']:,}")
            logger.info(f"    - Localizations: {data['localizations']:,}")
    else:
        logger.info("No data collected yet.")

    logger.info("")
    if stats['sitemap_file_counts']:
        logger.info("Sitemap Files Tracked:")
        for platform, count in stats['sitemap_file_counts'].items():
            logger.info(f"  {platform}: {count:,} files")

    logger.info("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Collect app localizations from App Store and Play Store sitemaps'
    )
    parser.add_argument('--app-store', action='store_true', help='Collect from App Store only')
    parser.add_argument('--play-store', action='store_true', help='Collect from Play Store only')
    parser.add_argument('--stats', action='store_true', help='Print statistics only')
    parser.add_argument('--quiet', '-q', action='store_true', help='Quiet mode (less output)')

    args = parser.parse_args()
    logger = get_timestamped_logger("collect_sitemaps", file_prefix=LOG_FILE_PREFIX)
    start_ts = datetime.now().isoformat()
    start_perf = time.perf_counter()

    logger.info(f"[STEP START] collect_sitemaps | {start_ts}")

    # DB 초기화
    init_database()

    # 통계만 출력
    if args.stats:
        print_stats(logger)
        elapsed = time.perf_counter() - start_perf
        logger.info(
            f"[STEP END] collect_sitemaps | {datetime.now().isoformat()} | "
            f"elapsed={elapsed:.2f}s | status=STATS_ONLY"
        )
        return

    # 수집할 스토어 결정
    collect_app_store = args.app_store or (not args.app_store and not args.play_store)
    collect_play_store = args.play_store or (not args.app_store and not args.play_store)

    verbose = not args.quiet

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Sitemap Collection Started at {start_ts}")
    logger.info(f"{'=' * 60}\n")

    all_stats = {}

    # App Store 수집
    if collect_app_store:
        logger.info("\n>>> Collecting from App Store...\n")
        collector = AppStoreSitemapCollector(verbose=verbose)
        all_stats['app_store'] = collector.collect_all()

    # Play Store 수집
    if collect_play_store:
        logger.info("\n>>> Collecting from Play Store...\n")
        collector = PlayStoreSitemapCollector(verbose=verbose)
        all_stats['play_store'] = collector.collect_all()

    # 결과 요약
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Collection Completed at {datetime.now().isoformat()}")
    logger.info(f"{'=' * 60}")

    for platform, stats in all_stats.items():
        logger.info(f"\n{platform}:")
        logger.info(f"  - Sitemap files processed: {stats['sitemap_files_processed']}")
        logger.info(f"  - Sitemap files skipped (unchanged): {stats['sitemap_files_skipped']}")
        logger.info(f"  - New localizations: {stats['new_localizations']}")
        logger.info(f"  - Total localizations in processed files: {stats['total_localizations']}")
        if 'skipped_non_apps' in stats:
            logger.info(f"  - Non-app entries skipped: {stats['skipped_non_apps']}")
        logger.info(f"  - Errors: {stats['errors']}")

    # 전체 DB 통계 출력
    print_stats(logger)
    elapsed = time.perf_counter() - start_perf
    logger.info(
        f"[STEP END] collect_sitemaps | {datetime.now().isoformat()} | "
        f"elapsed={elapsed:.2f}s | status=OK"
    )


if __name__ == '__main__':
    main()
