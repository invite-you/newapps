#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_BIN = sys.executable
DEFAULT_LIMIT = None
DEFAULT_RUN_TESTS = False


def log_step_start(step_name: str) -> float:
    start_ts = datetime.now().isoformat()
    print(f"[STEP START] {step_name} | {start_ts}")
    return time.perf_counter()


def log_step_end(step_name: str, start_perf: float, status: str) -> None:
    end_ts = datetime.now().isoformat()
    elapsed = time.perf_counter() - start_perf
    print(f"[STEP END] {step_name} | {end_ts} | elapsed={elapsed:.2f}s | status={status}")


def run_script(step_name: str, script_name: str, args: list) -> None:
    start_perf = log_step_start(step_name)
    status = "OK"
    try:
        cmd = [PYTHON_BIN, os.path.join(BASE_DIR, script_name), *args]
        result = subprocess.run(cmd, check=False, cwd=BASE_DIR)
        if result.returncode != 0:
            status = "FAIL"
            raise RuntimeError(f"{script_name} failed with exit code {result.returncode}")
    except Exception:
        status = "FAIL"
        raise
    finally:
        log_step_end(step_name, start_perf, status)


def main() -> int:
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
    args = parser.parse_args()

    limit = args.limit
    run_tests = args.run_tests if args.run_tests else DEFAULT_RUN_TESTS

    print("=" * 70)
    print(f"Full Pipeline Started at {datetime.now().isoformat()}")
    print("=" * 70)
    print(f"Limit: {limit if limit is not None else 'unlimited'}")
    print(f"Run tests: {run_tests}")
    print()

    run_script("SITEMAP_COLLECTION_ALL", "collect_sitemaps.py", [])
    details_args = ["--limit", str(limit)] if limit is not None else []
    run_script(
        "DETAILS_AND_REVIEWS_ALL",
        "collect_app_details.py",
        details_args,
    )

    if run_tests:
        run_script("COMPREHENSIVE_TESTS", "test_comprehensive.py", [])

    print()
    print("=" * 70)
    print(f"Full Pipeline Completed at {datetime.now().isoformat()}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
