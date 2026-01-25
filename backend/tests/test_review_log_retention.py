import os
import time

from utils import logger as logger_utils


def test_cleanup_old_logs_removes_stale_files(tmp_path):
    old_file = tmp_path / "collector_appstorereviews_20000101_000000.log"
    new_file = tmp_path / "collector_appstorereviews_20990101_000000.log"
    other_file = tmp_path / "collector_other_20000101_000000.log"

    old_file.write_text("old")
    new_file.write_text("new")
    other_file.write_text("other")

    now = time.time()
    old_time = now - (366 * 24 * 60 * 60)
    new_time = now - (24 * 60 * 60)
    os.utime(old_file, (old_time, old_time))
    os.utime(new_file, (new_time, new_time))
    os.utime(other_file, (old_time, old_time))

    removed = logger_utils.cleanup_old_logs(
        prefixes=["collector_appstorereviews"],
        max_age_days=365,
        log_dir=str(tmp_path),
    )

    assert removed == 1
    assert not old_file.exists()
    assert new_file.exists()
    assert other_file.exists()


def test_get_run_logger_creates_distinct_files(tmp_path):
    logger1 = logger_utils.get_run_logger(
        name="TestRunLogger",
        file_prefix="collector_appstorereviews",
        log_dir=str(tmp_path),
        timestamp="20260101_000000_000001",
    )
    logger1.info("first")
    logger_utils.close_logger_handlers(logger1)

    logger2 = logger_utils.get_run_logger(
        name="TestRunLogger",
        file_prefix="collector_appstorereviews",
        log_dir=str(tmp_path),
        timestamp="20260101_000000_000002",
    )
    logger2.info("second")
    logger_utils.close_logger_handlers(logger2)

    files = sorted(tmp_path.glob("collector_appstorereviews_*.log"))
    assert len(files) == 2
