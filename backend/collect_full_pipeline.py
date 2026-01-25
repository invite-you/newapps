#!/usr/bin/env python3
import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime

import psycopg
from psycopg import sql
from database.db_errors import DatabaseUnavailableError, DB_UNAVAILABLE_EXIT_CODE

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 데몬 모드 관련 설정
LOOP_INTERVAL_SECONDS = 10  # 파이프라인 완료 후 대기 시간 (초)
_shutdown_requested = False


def _signal_handler(signum, frame):
    """시그널 핸들러: SIGTERM/SIGINT 수신 시 graceful shutdown 요청"""
    global _shutdown_requested
    _shutdown_requested = True
PYTHON_BIN = sys.executable
DEFAULT_LIMIT = None
DEFAULT_RUN_TESTS = False
LOG_FILE_PREFIX = "collect_full_pipeline"
# psycopg DSN 참고: https://www.psycopg.org/psycopg3/docs/basic/usage.html
DB_DSN = os.getenv("APP_DETAILS_DB_DSN")
DB_HOST = os.getenv("APP_DETAILS_DB_HOST", "localhost")
DB_PORT = int(os.getenv("APP_DETAILS_DB_PORT", "5432"))
DB_NAME = os.getenv("APP_DETAILS_DB_NAME", "app_details")
DB_USER = os.getenv("APP_DETAILS_DB_USER", "app_details")
DB_PASSWORD = os.getenv("APP_DETAILS_DB_PASSWORD", "")
PARTITION_CHECK_ENABLED = os.getenv("APP_DETAILS_MONTHLY_PARTITION_CHECK", "true").lower() in (
    "1",
    "true",
    "yes",
    "y",
)
PARTITION_ENFORCE = os.getenv("APP_DETAILS_MONTHLY_PARTITION_ENFORCE", "true").lower() in (
    "1",
    "true",
    "yes",
    "y",
)
PARTITION_PARENT_TABLE = os.getenv("APP_DETAILS_MONTHLY_PARTITION_PARENT", "app_reviews_monthly")
PARTITION_SCHEMA = os.getenv("APP_DETAILS_MONTHLY_PARTITION_SCHEMA", "public")
PARTITION_NAME_TEMPLATE = os.getenv(
    "APP_DETAILS_MONTHLY_PARTITION_NAME",
    "{parent}_p{yyyymm}",
)


def log_step_start(step_name: str, logger) -> float:
    start_ts = datetime.now().isoformat()
    logger.info(f"[STEP START] {step_name} | {start_ts}")
    return time.perf_counter()


def log_step_end(step_name: str, start_perf: float, status: str, logger) -> None:
    end_ts = datetime.now().isoformat()
    elapsed = time.perf_counter() - start_perf
    logger.info(f"[STEP END] {step_name} | {end_ts} | elapsed={elapsed:.2f}s | status={status}")


def run_script(step_name: str, script_name: str, args: list, logger) -> None:
    start_perf = log_step_start(step_name, logger)
    status = "OK"
    try:
        cmd = [PYTHON_BIN, os.path.join(BASE_DIR, script_name), *args]
        logger.info(f"[RUN] {' '.join(cmd)}")
        result = subprocess.run(cmd, check=False, cwd=BASE_DIR)
        if result.returncode == DB_UNAVAILABLE_EXIT_CODE:
            status = "FAIL"
            raise DatabaseUnavailableError(f"{script_name} DB unavailable")
        if result.returncode != 0:
            status = "FAIL"
            raise RuntimeError(f"{script_name} failed with exit code {result.returncode}")
    except Exception:
        status = "FAIL"
        logger.exception(f"[ERROR] {script_name} 실행 실패")
        raise
    finally:
        log_step_end(step_name, start_perf, status, logger)


def build_dsn() -> str:
    """DB DSN을 반환합니다."""
    if DB_DSN:
        return DB_DSN
    return (
        f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD}"
    )


def get_current_month_range() -> tuple[str, datetime.date, datetime.date]:
    """현재 월 파티션 생성에 필요한 범위 정보를 반환합니다."""
    month_start = datetime.now().date().replace(day=1)
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1)
    partition_name = PARTITION_NAME_TEMPLATE.format(
        parent=PARTITION_PARENT_TABLE,
        yyyymm=month_start.strftime("%Y%m"),
    )
    return partition_name, month_start, next_month


def ensure_current_month_partition(logger) -> None:
    step_name = "ENSURE_MONTHLY_PARTITION"
    start_perf = log_step_start(step_name, logger)
    status = "OK"
    conn = None
    try:
        if not PARTITION_CHECK_ENABLED:
            logger.info("[INFO] 월별 파티션 점검이 비활성화되어 건너뜁니다.")
            status = "SKIP"
            return
        if not PARTITION_PARENT_TABLE:
            raise ValueError("월별 파티션 대상 테이블이 비어 있습니다.")

        partition_name, range_start, range_end = get_current_month_range()
        logger.info(
            "[INFO] 월별 파티션 확인 시작: "
            f"schema={PARTITION_SCHEMA}, parent={PARTITION_PARENT_TABLE}, "
            f"child={partition_name}, range=({range_start.isoformat()}~{range_end.isoformat()})"
        )

        conn = psycopg.connect(build_dsn())
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT 1
            FROM pg_class parent
            JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
            WHERE parent.relname = %s
              AND parent_ns.nspname = %s
            LIMIT 1
            """,
            (PARTITION_PARENT_TABLE, PARTITION_SCHEMA),
        )
        parent_exists = cursor.fetchone() is not None

        if not parent_exists:
            logger.warning(
                "[WARN] 월별 파티션 대상 테이블이 없어 점검을 건너뜁니다. "
                f"schema={PARTITION_SCHEMA}, parent={PARTITION_PARENT_TABLE}"
            )
            status = "SKIP"
            return

        cursor.execute(
            """
            SELECT 1
            FROM pg_class child
            JOIN pg_inherits inh ON inh.inhrelid = child.oid
            JOIN pg_class parent ON inh.inhparent = parent.oid
            JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
            JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
            WHERE child.relname = %s
              AND parent.relname = %s
              AND child_ns.nspname = %s
              AND parent_ns.nspname = %s
            LIMIT 1
            """,
            (partition_name, PARTITION_PARENT_TABLE, PARTITION_SCHEMA, PARTITION_SCHEMA),
        )
        exists = cursor.fetchone() is not None

        if exists:
            logger.info("[INFO] 월별 파티션이 이미 존재합니다.")
            return

        logger.info("[INFO] 월별 파티션이 없어 생성합니다.")
        create_sql = sql.SQL(
            "CREATE TABLE {schema}.{child} "
            "PARTITION OF {schema}.{parent} "
            "FOR VALUES FROM ({range_start}) TO ({range_end})"
        ).format(
            schema=sql.Identifier(PARTITION_SCHEMA),
            child=sql.Identifier(partition_name),
            parent=sql.Identifier(PARTITION_PARENT_TABLE),
            range_start=sql.Literal(range_start),
            range_end=sql.Literal(range_end),
        )
        cursor.execute(create_sql)
        conn.commit()
        logger.info("[INFO] 월별 파티션 생성 완료.")
    except Exception as exc:
        status = "FAIL"
        logger.exception(f"[ERROR] 월별 파티션 점검/생성 실패: {exc}")
        if PARTITION_ENFORCE:
            logger.error("[ERROR] 파티션 점검 실패로 파이프라인을 중단합니다.")
            raise
        logger.warning("[WARN] 파티션 점검 실패를 무시하고 파이프라인을 계속합니다.")
    finally:
        if conn is not None:
            conn.close()
        log_step_end(step_name, start_perf, status, logger)


def run_pipeline(limit: int | None, run_tests: bool, logger) -> bool:
    """
    단일 파이프라인 사이클을 실행합니다.

    Returns:
        bool: 성공 시 True, 실패 시 False
    """
    global _shutdown_requested

    start_ts = datetime.now().isoformat()
    start_perf = time.perf_counter()
    logger.info(f"[STEP START] collect_full_pipeline | {start_ts}")

    logger.info("=" * 70)
    logger.info(f"Full Pipeline Started at {start_ts}")
    logger.info("=" * 70)
    logger.info(f"Limit: {limit if limit is not None else 'unlimited'}")
    logger.info(f"Run tests: {run_tests}")
    logger.info("")

    try:
        if _shutdown_requested:
            logger.info("[INFO] 종료 요청 감지, 파이프라인 중단")
            return False

        ensure_current_month_partition(logger)

        if _shutdown_requested:
            logger.info("[INFO] 종료 요청 감지, 파이프라인 중단")
            return False

        run_script("SITEMAP_COLLECTION_ALL", "collect_sitemaps.py", [], logger)

        if _shutdown_requested:
            logger.info("[INFO] 종료 요청 감지, 파이프라인 중단")
            return False

        details_args = ["--limit", str(limit)] if limit is not None else []
        run_script(
            "DETAILS_AND_REVIEWS_ALL",
            "collect_app_details.py",
            details_args,
            logger,
        )

        if run_tests and not _shutdown_requested:
            run_script("COMPREHENSIVE_TESTS", "test_comprehensive.py", [], logger)

        logger.info("")
        logger.info("=" * 70)
        logger.info(f"Full Pipeline Completed at {datetime.now().isoformat()}")
        logger.info("=" * 70)
        elapsed = time.perf_counter() - start_perf
        logger.info(
            f"[STEP END] collect_full_pipeline | {datetime.now().isoformat()} | "
            f"elapsed={elapsed:.2f}s | status=OK"
        )
        return True

    except DatabaseUnavailableError:
        elapsed = time.perf_counter() - start_perf
        logger.exception(
            f"[STEP END] collect_full_pipeline | {datetime.now().isoformat()} | "
            f"elapsed={elapsed:.2f}s | status=DB_UNAVAILABLE"
        )
        raise
    except Exception:
        elapsed = time.perf_counter() - start_perf
        logger.exception(
            f"[STEP END] collect_full_pipeline | {datetime.now().isoformat()} | "
            f"elapsed={elapsed:.2f}s | status=FAIL"
        )
        return False


def main() -> int:
    global _shutdown_requested

    parser = argparse.ArgumentParser(
        description="Run full sitemap/details/reviews collection (tests optional)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Max apps to process per store (default: unlimited)",
    )
    parser.add_argument(
        "--run-tests",
        action="store_true",
        help="Run comprehensive tests at the end",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run in daemon mode (continuous loop with restarts)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=LOOP_INTERVAL_SECONDS,
        help=f"Seconds to wait between pipeline cycles in daemon mode (default: {LOOP_INTERVAL_SECONDS})",
    )
    args = parser.parse_args()

    from utils.logger import get_timestamped_logger
    logger = get_timestamped_logger("collect_full_pipeline", file_prefix=LOG_FILE_PREFIX)

    limit = args.limit
    run_tests = args.run_tests if args.run_tests else DEFAULT_RUN_TESTS
    daemon_mode = args.daemon
    interval = args.interval

    # 시그널 핸들러 등록 (graceful shutdown 지원)
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    if not daemon_mode:
        # 단일 실행 모드 (기존 동작)
        try:
            success = run_pipeline(limit, run_tests, logger)
        except DatabaseUnavailableError:
            return DB_UNAVAILABLE_EXIT_CODE
        return 0 if success else 1

    # 데몬 모드: 무한 루프로 파이프라인 반복 실행
    logger.info("=" * 70)
    logger.info("[DAEMON] 데몬 모드로 시작합니다.")
    logger.info(f"[DAEMON] 파이프라인 완료 후 {interval}초 대기 후 재시작")
    logger.info("=" * 70)

    cycle_count = 0
    while not _shutdown_requested:
        cycle_count += 1
        logger.info(f"[DAEMON] ===== 사이클 #{cycle_count} 시작 =====")

        try:
            run_pipeline(limit, run_tests, logger)
        except DatabaseUnavailableError:
            cooldown = max(60, interval)
            logger.error(
                "[DAEMON] DB unavailable; %s초 대기 후 다음 사이클 재시도",
                cooldown,
            )
            for _ in range(cooldown):
                if _shutdown_requested:
                    break
                time.sleep(1)
            continue
        except Exception:
            logger.exception("[DAEMON] 파이프라인 실행 중 예외 발생 (다음 사이클에서 재시도)")

        if _shutdown_requested:
            logger.info("[DAEMON] 종료 요청 감지, 루프 종료")
            break

        logger.info(f"[DAEMON] 사이클 #{cycle_count} 완료, {interval}초 대기 후 재시작...")

        # 대기 시간 동안에도 종료 요청 확인 (1초 단위)
        for _ in range(interval):
            if _shutdown_requested:
                break
            time.sleep(1)

    logger.info("[DAEMON] 데몬이 정상 종료되었습니다.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
