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
import signal
import subprocess
from datetime import datetime

# 경로 설정
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.sitemap_apps_db import init_database, get_stats
from scrapers.app_store_sitemap_collector import AppStoreSitemapCollector
from scrapers.play_store_sitemap_collector import PlayStoreSitemapCollector
from database.db_errors import DatabaseUnavailableError, DB_UNAVAILABLE_EXIT_CODE
from utils.logger import get_timestamped_logger
from utils.network_binding import list_active_ipv4_interfaces, select_store_interfaces, probe_interface_url

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


def _run_collect_sitemaps(args, logger, start_ts: str, start_perf: float) -> int:
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
        return 0

    # 수집할 스토어 결정
    collect_app_store = args.app_store or (not args.app_store and not args.play_store)
    collect_play_store = args.play_store or (not args.app_store and not args.play_store)

    verbose = not args.quiet

    if os.getenv("SCRAPER_CHILD_PROCESS") != "1":
        interfaces = list_active_ipv4_interfaces()
        if collect_app_store and collect_play_store and len(interfaces) >= 2:
            app_iface, play_iface = select_store_interfaces(interfaces)
            app_iface_ok = app_iface and probe_interface_url(
                app_iface, "https://apps.apple.com/sitemaps_apps_index_app_1.xml"
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
                    "[INFO] parallel sitemap collection enabled | "
                    f"app_store={app_iface} | play_store={play_iface}"
                )
                child_env = os.environ.copy()
                child_env["SCRAPER_CHILD_PROCESS"] = "1"

                app_args = [sys.executable, os.path.abspath(__file__), "--app-store"]
                play_args = [sys.executable, os.path.abspath(__file__), "--play-store"]
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
                            "[ERROR] parallel sitemap collection DB unavailable | "
                            f"app_store={app_code} | play_store={play_code}"
                        )
                        _terminate_process_group(app_proc, logger, "app_store")
                        _terminate_process_group(play_proc, logger, "play_store")
                        elapsed = time.perf_counter() - start_perf
                        logger.info(
                            f"[STEP END] collect_sitemaps | {datetime.now().isoformat()} | "
                            f"elapsed={elapsed:.2f}s | status=DB_UNAVAILABLE"
                        )
                        return DB_UNAVAILABLE_EXIT_CODE
                    if app_code != 0 or play_code != 0:
                        logger.error(
                            "[ERROR] parallel sitemap collection failed | "
                            f"app_store={app_code} | play_store={play_code}"
                        )
                        _terminate_process_group(app_proc, logger, "app_store")
                        _terminate_process_group(play_proc, logger, "play_store")
                        elapsed = time.perf_counter() - start_perf
                        logger.info(
                            f"[STEP END] collect_sitemaps | {datetime.now().isoformat()} | "
                            f"elapsed={elapsed:.2f}s | status=FAIL"
                        )
                        return 1
                except KeyboardInterrupt:
                    logger.warning("[WARN] interrupt received, terminating child processes")
                    _terminate_process_group(app_proc, logger, "app_store")
                    _terminate_process_group(play_proc, logger, "play_store")
                    raise
                logger.info("[INFO] parallel sitemap collection finished successfully")
                elapsed = time.perf_counter() - start_perf
                logger.info(
                    f"[STEP END] collect_sitemaps | {datetime.now().isoformat()} | "
                    f"elapsed={elapsed:.2f}s | status=OK"
                )
                return 0
            logger.info("[INFO] parallel sitemap collection disabled: interface healthcheck failed")

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
    return 0


def main() -> int:
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

    try:
        return _run_collect_sitemaps(args, logger, start_ts, start_perf)
    except DatabaseUnavailableError:
        logger.error("[ERROR] DB unavailable; aborting sitemap collection")
        return DB_UNAVAILABLE_EXIT_CODE


if __name__ == '__main__':
    raise SystemExit(main())
