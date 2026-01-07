#!/usr/bin/env python3
"""
앱 상세정보 및 리뷰 수집 메인 스크립트

Usage:
    python collect_app_details.py                    # 모든 스토어의 상세정보 + 리뷰 수집
    python collect_app_details.py --app-store        # App Store만
    python collect_app_details.py --play-store       # Play Store만
    python collect_app_details.py --details-only     # 상세정보만 수집
    python collect_app_details.py --reviews-only     # 리뷰만 수집
    python collect_app_details.py --limit 100        # 앱 개수 제한
    python collect_app_details.py --stats            # 통계만 출력
"""
import sys
import os
import argparse
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.app_details_db import init_database, get_stats


def print_stats():
    """DB 통계를 출력합니다."""
    stats = get_stats()
    print("\n" + "=" * 60)
    print("App Details Database Statistics")
    print("=" * 60)

    print("\nTable Record Counts:")
    for table in ['apps', 'apps_localized', 'apps_metrics', 'app_reviews', 'failed_apps', 'collection_status']:
        print(f"  {table}: {stats.get(table, 0):,}")

    print("\nApps by Platform:")
    for platform, count in stats.get('apps_by_platform', {}).items():
        print(f"  {platform}: {count:,} unique apps")

    print("\nReviews by Platform:")
    for platform, count in stats.get('reviews_by_platform', {}).items():
        print(f"  {platform}: {count:,} reviews")

    print("=" * 60 + "\n")


def collect_app_store_details(limit: Optional[int]):
    """App Store 상세정보를 수집합니다."""
    from scrapers.app_store_details_collector import (
        AppStoreDetailsCollector, get_apps_to_collect
    )

    app_ids = get_apps_to_collect(limit=limit)
    print(f"[App Store] Found {len(app_ids)} apps to collect details")

    if app_ids:
        collector = AppStoreDetailsCollector(verbose=True)
        return collector.collect_batch(app_ids)
    return {}


def collect_app_store_reviews(limit: Optional[int]):
    """App Store 리뷰를 수집합니다."""
    from scrapers.app_store_reviews_collector import (
        AppStoreReviewsCollector, get_apps_for_review_collection
    )

    app_ids = get_apps_for_review_collection(limit=limit)
    print(f"[App Store] Found {len(app_ids)} apps to collect reviews")

    if app_ids:
        collector = AppStoreReviewsCollector(verbose=True)
        return collector.collect_batch(app_ids)
    return {}


def collect_play_store_details(limit: Optional[int]):
    """Play Store 상세정보를 수집합니다."""
    from scrapers.play_store_details_collector import (
        PlayStoreDetailsCollector, get_apps_to_collect
    )

    app_ids = get_apps_to_collect(limit=limit)
    print(f"[Play Store] Found {len(app_ids)} apps to collect details")

    if app_ids:
        collector = PlayStoreDetailsCollector(verbose=True)
        return collector.collect_batch(app_ids)
    return {}


def collect_play_store_reviews(limit: Optional[int]):
    """Play Store 리뷰를 수집합니다."""
    from scrapers.play_store_reviews_collector import (
        PlayStoreReviewsCollector, get_apps_for_review_collection
    )

    app_ids = get_apps_for_review_collection(limit=limit)
    print(f"[Play Store] Found {len(app_ids)} apps to collect reviews")

    if app_ids:
        collector = PlayStoreReviewsCollector(verbose=True)
        return collector.collect_batch(app_ids)
    return {}


def main():
    parser = argparse.ArgumentParser(
        description='Collect app details and reviews from App Store and Play Store'
    )
    parser.add_argument('--app-store', action='store_true', help='Collect from App Store only')
    parser.add_argument('--play-store', action='store_true', help='Collect from Play Store only')
    parser.add_argument('--details-only', action='store_true', help='Collect details only (no reviews)')
    parser.add_argument('--reviews-only', action='store_true', help='Collect reviews only (no details)')
    parser.add_argument('--limit', type=int, default=None, help='Max apps to process (default: unlimited)')
    parser.add_argument('--stats', action='store_true', help='Print statistics only')
    parser.add_argument('--quiet', '-q', action='store_true', help='Quiet mode')

    args = parser.parse_args()

    # DB 초기화
    init_database()

    # 통계만 출력
    if args.stats:
        print_stats()
        return

    # 수집 대상 결정
    collect_app_store = args.app_store or (not args.app_store and not args.play_store)
    collect_play_store = args.play_store or (not args.app_store and not args.play_store)
    collect_details = not args.reviews_only
    collect_reviews = not args.details_only

    print(f"\n{'=' * 60}")
    print(f"App Details Collection Started at {datetime.now().isoformat()}")
    print(f"{'=' * 60}")
    print(f"Limit: {args.limit if args.limit is not None else 'unlimited'} apps per task")
    print()

    all_stats = {}

    # App Store 수집
    if collect_app_store:
        if collect_details:
            print("\n>>> Collecting App Store Details...\n")
            all_stats['app_store_details'] = collect_app_store_details(args.limit)

        if collect_reviews:
            print("\n>>> Collecting App Store Reviews...\n")
            all_stats['app_store_reviews'] = collect_app_store_reviews(args.limit)

    # Play Store 수집
    if collect_play_store:
        if collect_details:
            print("\n>>> Collecting Play Store Details...\n")
            all_stats['play_store_details'] = collect_play_store_details(args.limit)

        if collect_reviews:
            print("\n>>> Collecting Play Store Reviews...\n")
            all_stats['play_store_reviews'] = collect_play_store_reviews(args.limit)

    # 결과 요약
    print(f"\n{'=' * 60}")
    print(f"Collection Completed at {datetime.now().isoformat()}")
    print(f"{'=' * 60}")

    for task, stats in all_stats.items():
        if stats:
            print(f"\n{task}:")
            for key, value in stats.items():
                print(f"  {key}: {value}")

    # 전체 DB 통계 출력
    print_stats()


if __name__ == '__main__':
    main()
