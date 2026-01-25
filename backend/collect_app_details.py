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
import time
import signal
import subprocess
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.app_details_db import init_database, get_stats, generate_session_id
from database.db_errors import DatabaseUnavailableError, DB_UNAVAILABLE_EXIT_CODE
from utils.logger import get_timestamped_logger
from utils.network_binding import list_active_ipv4_interfaces, select_store_interfaces, probe_interface_url

LOG_FILE_PREFIX = "collect_app_details"


def _terminate_process_group(proc: subprocess.Popen, logger, label: str) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=10)
        logger.warning(f"[WARN] {label} terminated with SIGTERM")
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        logger.warning(f"[WARN] {label} terminated with SIGKILL")
    except Exception as exc:
        logger.warning(f"[WARN] {label} termination failed: {exc}")


def print_stats(logger):
    """DB 통계를 출력합니다."""
    stats = get_stats()
    logger.info("\n" + "=" * 60)
    logger.info("App Details Database Statistics")
    logger.info("=" * 60)

    logger.info("\nTable Record Counts:")
    for table in ['apps', 'apps_localized', 'apps_metrics', 'app_reviews', 'failed_apps', 'collection_status']:
        logger.info(f"  {table}: {stats.get(table, 0):,}")

    logger.info("\nApps by Platform:")
    for platform, count in stats.get('apps_by_platform', {}).items():
        logger.info(f"  {platform}: {count:,} unique apps")

    logger.info("\nReviews by Platform:")
    for platform, count in stats.get('reviews_by_platform', {}).items():
        logger.info(f"  {platform}: {count:,} reviews")

    logger.info("=" * 60 + "\n")


def collect_app_store_details(limit: Optional[int], session_id: str, logger):
    """App Store 상세정보를 수집합니다."""
    from scrapers.app_store_details_collector import (
        AppStoreDetailsCollector, get_apps_to_collect
    )

    app_ids = get_apps_to_collect(limit=limit, session_id=session_id)
    logger.info(f"[App Store] Found {len(app_ids)} apps to collect details")

    if app_ids:
        collector = AppStoreDetailsCollector(verbose=True, session_id=session_id)
        return collector.collect_batch(app_ids)
    return {}


def collect_app_store_reviews(limit: Optional[int], session_id: str, logger):
    """App Store 리뷰를 수집합니다."""
    from scrapers.app_store_reviews_collector import (
        AppStoreReviewsCollector, get_apps_for_review_collection
    )

    app_ids = get_apps_for_review_collection(limit=limit, session_id=session_id)
    logger.info(f"[App Store] Found {len(app_ids)} apps to collect reviews")

    if app_ids:
        collector = AppStoreReviewsCollector(verbose=True, session_id=session_id)
        return collector.collect_batch(app_ids)
    return {}


def collect_play_store_details(limit: Optional[int], session_id: str, logger):
    """Play Store 상세정보를 수집합니다."""
    from scrapers.play_store_details_collector import (
        PlayStoreDetailsCollector, get_apps_to_collect
    )

    app_ids = get_apps_to_collect(limit=limit, session_id=session_id)
    logger.info(f"[Play Store] Found {len(app_ids)} apps to collect details")

    if app_ids:
        collector = PlayStoreDetailsCollector(verbose=True, session_id=session_id)
        return collector.collect_batch(app_ids)
    return {}


def collect_play_store_reviews(limit: Optional[int], session_id: str, logger):
    """Play Store 리뷰를 수집합니다."""
    from scrapers.play_store_reviews_collector import (
        PlayStoreReviewsCollector, get_apps_for_review_collection
    )

    app_ids = get_apps_for_review_collection(limit=limit, session_id=session_id)
    logger.info(f"[Play Store] Found {len(app_ids)} apps to collect reviews")

    if app_ids:
        collector = PlayStoreReviewsCollector(verbose=True, session_id=session_id)
        return collector.collect_batch(app_ids)
    return {}


def _run_collect_app_details(args, logger, start_ts: str, start_perf: float) -> int:
    # DB 초기화
    init_database()

    # 통계만 출력
    if args.stats:
        print_stats(logger)
        elapsed = time.perf_counter() - start_perf
        logger.info(
            f"[STEP END] collect_app_details | {datetime.now().isoformat()} | "
            f"elapsed={elapsed:.2f}s | status=STATS_ONLY"
        )
        return 0

    # 세션 ID 생성 (전체 실행에서 동일한 ID 사용)
    session_id = generate_session_id()
    logger.info(f"Session ID: {session_id}")

    # 수집 대상 결정
    collect_app_store = args.app_store or (not args.app_store and not args.play_store)
    collect_play_store = args.play_store or (not args.app_store and not args.play_store)
    collect_details = not args.reviews_only
    collect_reviews = not args.details_only

    if os.getenv("SCRAPER_CHILD_PROCESS") != "1":
        interfaces = list_active_ipv4_interfaces()
        if collect_app_store and collect_play_store and len(interfaces) >= 2:
            app_iface, play_iface = select_store_interfaces(interfaces)
            app_iface_ok = app_iface and probe_interface_url(
                app_iface, "https://itunes.apple.com/lookup?id=284882215&country=US"
            )
            play_iface_ok = play_iface and probe_interface_url(
                play_iface, "https://play.google.com/sitemaps/sitemaps-index-0.xml"
            )
            if not app_iface_ok:
                logger.warning(f"[WARN] App Store interface healthcheck failed: {app_iface}")
                app_iface = None
            if not play_iface_ok:
                logger.warning(f"[WARN] Play Store interface healthcheck failed: {play_iface}")
                play_iface = None
            if app_iface and play_iface:
                logger.info(
                    "[INFO] parallel app collection enabled | "
                    f"app_store={app_iface} | play_store={play_iface}"
                )
                child_env = os.environ.copy()
                child_env["SCRAPER_CHILD_PROCESS"] = "1"

                app_args = [sys.executable, os.path.abspath(__file__), "--app-store"]
                play_args = [sys.executable, os.path.abspath(__file__), "--play-store"]
                if args.details_only:
                    app_args.append("--details-only")
                    play_args.append("--details-only")
                if args.reviews_only:
                    app_args.append("--reviews-only")
                    play_args.append("--reviews-only")
                if args.limit is not None:
                    app_args.extend(["--limit", str(args.limit)])
                    play_args.extend(["--limit", str(args.limit)])
                if args.quiet:
                    app_args.append("--quiet")
                    play_args.append("--quiet")

                app_env = child_env.copy()
                play_env = child_env.copy()
                app_env["SCRAPER_INTERFACE"] = app_iface
                play_env["SCRAPER_INTERFACE"] = play_iface

                app_proc = subprocess.Popen(
                    app_args,
                    env=app_env,
                    cwd=os.path.dirname(os.path.abspath(__file__)),
                    start_new_session=True,
                )
                play_proc = subprocess.Popen(
                    play_args,
                    env=play_env,
                    cwd=os.path.dirname(os.path.abspath(__file__)),
                    start_new_session=True,
                )

                try:
                    app_code = app_proc.wait()
                    play_code = play_proc.wait()
                    if app_code == DB_UNAVAILABLE_EXIT_CODE or play_code == DB_UNAVAILABLE_EXIT_CODE:
                        logger.error(
                            "[ERROR] parallel app collection DB unavailable | "
                            f"app_store={app_code} | play_store={play_code}"
                        )
                        _terminate_process_group(app_proc, logger, "app_store")
                        _terminate_process_group(play_proc, logger, "play_store")
                        elapsed = time.perf_counter() - start_perf
                        logger.info(
                            f"[STEP END] collect_app_details | {datetime.now().isoformat()} | "
                            f"elapsed={elapsed:.2f}s | status=DB_UNAVAILABLE"
                        )
                        return DB_UNAVAILABLE_EXIT_CODE
                    if app_code != 0 or play_code != 0:
                        logger.error(
                            "[ERROR] parallel app collection failed | "
                            f"app_store={app_code} | play_store={play_code}"
                        )
                        _terminate_process_group(app_proc, logger, "app_store")
                        _terminate_process_group(play_proc, logger, "play_store")
                        elapsed = time.perf_counter() - start_perf
                        logger.info(
                            f"[STEP END] collect_app_details | {datetime.now().isoformat()} | "
                            f"elapsed={elapsed:.2f}s | status=FAIL"
                        )
                        return 1
                except KeyboardInterrupt:
                    logger.warning("[WARN] interrupt received, terminating child processes")
                    _terminate_process_group(app_proc, logger, "app_store")
                    _terminate_process_group(play_proc, logger, "play_store")
                    raise
                logger.info("[INFO] parallel app collection finished successfully")
                elapsed = time.perf_counter() - start_perf
                logger.info(
                    f"[STEP END] collect_app_details | {datetime.now().isoformat()} | "
                    f"elapsed={elapsed:.2f}s | status=OK"
                )
                return 0
            logger.info("[INFO] parallel app collection disabled: interface healthcheck failed")

    logger.info(f"\n{'=' * 60}")
    logger.info(f"App Details Collection Started at {start_ts}")
    logger.info(f"{'=' * 60}")
    logger.info(f"Limit: {args.limit if args.limit is not None else 'unlimited'} apps per task")
    logger.info("")

    all_stats = {}

    # App Store 수집
    if collect_app_store:
        if collect_details:
            logger.info("\n>>> Collecting App Store Details...\n")
            all_stats['app_store_details'] = collect_app_store_details(args.limit, session_id, logger)

        if collect_reviews:
            logger.info("\n>>> Collecting App Store Reviews...\n")
            all_stats['app_store_reviews'] = collect_app_store_reviews(args.limit, session_id, logger)

    # Play Store 수집
    if collect_play_store:
        if collect_details:
            logger.info("\n>>> Collecting Play Store Details...\n")
            all_stats['play_store_details'] = collect_play_store_details(args.limit, session_id, logger)

        if collect_reviews:
            logger.info("\n>>> Collecting Play Store Reviews...\n")
            all_stats['play_store_reviews'] = collect_play_store_reviews(args.limit, session_id, logger)

    # 결과 요약
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Collection Completed at {datetime.now().isoformat()}")
    logger.info(f"{'=' * 60}")

    for task, stats in all_stats.items():
        if stats:
            logger.info(f"\n{task}:")
            for key, value in stats.items():
                logger.info(f"  {key}: {value}")

    # 전체 DB 통계 출력
    print_stats(logger)
    elapsed = time.perf_counter() - start_perf
    logger.info(
        f"[STEP END] collect_app_details | {datetime.now().isoformat()} | "
        f"elapsed={elapsed:.2f}s | status=OK"
    )
    return 0


def main() -> int:
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
    logger = get_timestamped_logger("collect_app_details", file_prefix=LOG_FILE_PREFIX)
    start_ts = datetime.now().isoformat()
    start_perf = time.perf_counter()

    logger.info(f"[STEP START] collect_app_details | {start_ts}")

    try:
        return _run_collect_app_details(args, logger, start_ts, start_perf)
    except DatabaseUnavailableError:
        logger.error("[ERROR] DB unavailable; aborting app details collection")
        return DB_UNAVAILABLE_EXIT_CODE


if __name__ == '__main__':
    raise SystemExit(main())
