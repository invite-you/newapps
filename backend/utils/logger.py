"""
로깅 모듈
콘솔과 파일에 동시에 로그를 출력합니다.
"""
import os
import sys
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional, List

# 로그 디렉토리 설정
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')

# 기본 설정
DEFAULT_LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
DEFAULT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10MB
DEFAULT_BACKUP_COUNT = 5


def ensure_log_dir(log_dir: Optional[str] = None) -> str:
    """로그 디렉토리 생성"""
    target_dir = log_dir or LOG_DIR
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    return target_dir


def _build_timestamped_log_file(prefix: str, timestamp: Optional[str] = None) -> str:
    """타임스탬프가 포함된 로그 파일명을 생성합니다."""
    resolved_timestamp = timestamp or datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{prefix}_{resolved_timestamp}.log"


def get_logger(
    name: str,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    console: bool = True,
    file_logging: bool = True,
    log_dir: Optional[str] = None,
    rotate: bool = True,
    force_new_handlers: bool = False
) -> logging.Logger:
    """
    로거를 생성하거나 가져옵니다.

    Args:
        name: 로거 이름 (예: 'app_store_details', 'long_running_test')
        log_file: 로그 파일 이름 (없으면 name + '.log' 사용)
        level: 로그 레벨 (기본: INFO)
        console: 콘솔 출력 여부 (기본: True)
        file_logging: 파일 로깅 여부 (기본: True)

    Returns:
        설정된 Logger 인스턴스
    """
    logger = logging.getLogger(name)

    # 이미 핸들러가 있으면 기존 로거 반환
    if logger.handlers and not force_new_handlers:
        return logger
    if logger.handlers and force_new_handlers:
        for handler in list(logger.handlers):
            try:
                handler.close()
            finally:
                logger.removeHandler(handler)

    logger.setLevel(level)
    formatter = logging.Formatter(DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT)

    # 콘솔 핸들러
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # 파일 핸들러
    if file_logging:
        log_dir = ensure_log_dir(log_dir)

        if log_file is None:
            log_file = f"{name}.log"

        log_path = os.path.join(log_dir, log_file)

        if rotate:
            file_handler = RotatingFileHandler(
                log_path,
                maxBytes=DEFAULT_MAX_BYTES,
                backupCount=DEFAULT_BACKUP_COUNT,
                encoding='utf-8'
            )
        else:
            file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_timestamped_logger(
    name: str,
    file_prefix: str,
    level: int = logging.INFO,
    console: bool = True,
    file_logging: bool = True
) -> logging.Logger:
    """타임스탬프 파일명을 사용하는 로거를 생성합니다."""
    log_file = _build_timestamped_log_file(file_prefix)
    return get_logger(
        name,
        log_file=log_file,
        level=level,
        console=console,
        file_logging=file_logging
    )


def get_run_logger(
    name: str,
    file_prefix: str,
    level: int = logging.INFO,
    console: bool = True,
    file_logging: bool = True,
    log_dir: Optional[str] = None,
    timestamp: Optional[str] = None
) -> logging.Logger:
    """실행 단위로 분리된 로거를 생성합니다."""
    resolved_timestamp = timestamp or datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    log_file = _build_timestamped_log_file(file_prefix, resolved_timestamp)
    return get_logger(
        name,
        log_file=log_file,
        level=level,
        console=console,
        file_logging=file_logging,
        log_dir=log_dir,
        rotate=False,
        force_new_handlers=True
    )


def get_collection_logger(collector_name: str, verbose: bool = True) -> logging.Logger:
    """
    수집기용 로거를 생성합니다.

    Args:
        collector_name: 수집기 이름 (예: 'AppStoreDetails', 'PlayStoreReviews')
        verbose: 상세 로깅 여부

    Returns:
        설정된 Logger 인스턴스
    """
    level = logging.DEBUG if verbose else logging.WARNING
    log_prefix = f"collector_{collector_name.lower()}"
    return get_timestamped_logger(collector_name, file_prefix=log_prefix, level=level)


def get_collection_run_logger(
    collector_name: str,
    verbose: bool = True,
    log_dir: Optional[str] = None,
    cleanup_prefixes: Optional[List[str]] = None,
    cleanup_max_age_days: int = 365,
    timestamp: Optional[str] = None
) -> logging.Logger:
    """실행 단위 로거를 생성하고 오래된 로그를 정리합니다."""
    level = logging.DEBUG if verbose else logging.WARNING
    log_prefix = f"collector_{collector_name.lower()}"
    if cleanup_prefixes:
        cleanup_old_logs(
            prefixes=cleanup_prefixes,
            max_age_days=cleanup_max_age_days,
            log_dir=log_dir
        )
    return get_run_logger(
        collector_name,
        file_prefix=log_prefix,
        level=level,
        log_dir=log_dir,
        timestamp=timestamp
    )


def get_test_logger(test_name: str = 'long_running_test') -> logging.Logger:
    """
    테스트용 로거를 생성합니다.

    Args:
        test_name: 테스트 이름

    Returns:
        설정된 Logger 인스턴스
    """
    return get_timestamped_logger(test_name, file_prefix=test_name, level=logging.DEBUG)


class CollectorLogger:
    """
    수집기용 로깅 래퍼 클래스
    기존 collector의 log() 메서드를 대체합니다.
    """

    def __init__(self, name: str, verbose: bool = True):
        self.name = name
        self.verbose = verbose
        self.logger = get_collection_logger(name, verbose)

    def log(self, message: str):
        """INFO 레벨 로그 (verbose 모드일 때만)"""
        if self.verbose:
            self.logger.info(f"[{self.name}] {message}")

    def debug(self, message: str):
        """DEBUG 레벨 로그"""
        self.logger.debug(f"[{self.name}] {message}")

    def info(self, message: str):
        """INFO 레벨 로그 (항상 출력)"""
        self.logger.info(f"[{self.name}] {message}")

    def warning(self, message: str):
        """WARNING 레벨 로그"""
        self.logger.warning(f"[{self.name}] {message}")

    def error(self, message: str):
        """ERROR 레벨 로그"""
        self.logger.error(f"[{self.name}] {message}")


def cleanup_old_logs(
    prefixes: List[str],
    max_age_days: int = 365,
    log_dir: Optional[str] = None
) -> int:
    """지정된 접두어 로그 중 보관 기간을 지난 파일을 삭제합니다."""
    target_dir = log_dir or LOG_DIR
    if not os.path.isdir(target_dir):
        return 0

    import time

    cutoff = time.time() - (max_age_days * 24 * 60 * 60)
    removed = 0
    for entry in os.scandir(target_dir):
        if not entry.is_file():
            continue
        if not any(entry.name.startswith(prefix) for prefix in prefixes):
            continue
        try:
            if entry.stat().st_mtime < cutoff:
                os.remove(entry.path)
                removed += 1
        except OSError:
            continue
    return removed


def close_logger_handlers(logger: logging.Logger) -> None:
    """로거 핸들러를 닫고 해제합니다."""
    for handler in list(logger.handlers):
        try:
            handler.flush()
        except Exception:
            pass
        try:
            handler.close()
        finally:
            logger.removeHandler(handler)


class ProgressLogger:
    """
    배치 작업 진행률 로깅 유틸리티.

    로그 정책:
    - 10% 간격으로만 INFO 로그 출력 (불필요한 반복 최소화)
    - 개별 항목은 DEBUG 레벨
    - 전체 요약은 INFO 레벨
    """

    def __init__(self, logger: logging.Logger, total: int, step_name: str = "batch",
                 interval_percent: int = 10):
        """
        Args:
            logger: 사용할 로거
            total: 전체 처리 대상 수
            step_name: 스텝 이름 (예: 'collect_batch', 'collect_reviews')
            interval_percent: 진행률 로그 출력 간격 (기본: 10%)
        """
        self.logger = logger
        self.total = total
        self.step_name = step_name
        self.interval = max(1, total * interval_percent // 100)
        self.last_logged = 0
        self.start_time = None
        self.stats = {}

    def start(self, **context):
        """스텝 시작 로깅"""
        import time
        self.start_time = time.perf_counter()
        self.stats = {}
        ctx = ' | '.join(f"{k}={v}" for k, v in context.items()) if context else ""
        self.logger.info(f"[STEP START] {self.step_name} | total={self.total}" + (f" | {ctx}" if ctx else ""))

    def tick(self, current: int, item_id: str = None):
        """
        진행률 체크 - 간격 도달 시에만 INFO 로그.
        개별 항목은 DEBUG 레벨로 기록.
        """
        if item_id:
            self.logger.debug(f"[ITEM] {current}/{self.total} | id={item_id}")

        # 10% 간격 체크
        if current - self.last_logged >= self.interval or current == self.total:
            pct = current * 100 // self.total if self.total > 0 else 100
            self.logger.info(f"[PROGRESS] {current}/{self.total} ({pct}%)")
            self.last_logged = current

    def end(self, status: str = "OK", **stats):
        """스텝 종료 로깅 (소요시간 + 요약 통계)"""
        import time
        elapsed = time.perf_counter() - self.start_time if self.start_time else 0
        self.stats.update(stats)

        stats_str = ' | '.join(f"{k}={v}" for k, v in self.stats.items())
        self.logger.info(
            f"[STEP END] {self.step_name} | elapsed={elapsed:.2f}s | status={status}"
            + (f" | {stats_str}" if stats_str else "")
        )

    def add_stat(self, key: str, value):
        """통계 추가"""
        self.stats[key] = value


def format_error_log(reason: str, target: str, action: str, detail: str = None) -> str:
    """
    ERROR 로그 포맷: 원인 + 영향 + 조치를 한 줄에 포함.

    Args:
        reason: 원인 (예외/조건)
        target: 영향 (어떤 스텝/대상)
        action: 조치 (retry/skip/abort)
        detail: 추가 상세 정보 (선택)

    Returns:
        포맷된 에러 메시지

    Example:
        format_error_log("NotFoundError", "app_id=com.example", "skip", "API returned 404")
        -> "[ERROR] reason=NotFoundError | target=app_id=com.example | action=skip | API returned 404"
    """
    msg = f"reason={reason} | target={target} | action={action}"
    if detail:
        msg += f" | {detail}"
    return msg


def format_warning_log(issue: str, target: str, detail: str = None) -> str:
    """
    WARNING 로그 포맷: 이상 징후 요약.

    Args:
        issue: 이상 징후 유형 (retry, fallback, rate_limit, missing_data 등)
        target: 대상 (앱ID, URL 등)
        detail: 상세 정보

    Returns:
        포맷된 경고 메시지

    Example:
        format_warning_log("rate_limit", "app_id=com.example", "429 Too Many Requests")
        -> "[WARN] issue=rate_limit | target=app_id=com.example | 429 Too Many Requests"
    """
    msg = f"issue={issue} | target={target}"
    if detail:
        msg += f" | {detail}"
    return msg
