"""
로깅 모듈
콘솔과 파일에 동시에 로그를 출력합니다.
"""
import os
import sys
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional

# 로그 디렉토리 설정
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')

# 기본 설정
DEFAULT_LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
DEFAULT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10MB
DEFAULT_BACKUP_COUNT = 5


def ensure_log_dir():
    """로그 디렉토리 생성"""
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)


def get_logger(
    name: str,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    console: bool = True,
    file_logging: bool = True
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
    if logger.handlers:
        return logger

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
        ensure_log_dir()

        if log_file is None:
            log_file = f"{name}.log"

        log_path = os.path.join(LOG_DIR, log_file)

        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=DEFAULT_MAX_BYTES,
            backupCount=DEFAULT_BACKUP_COUNT,
            encoding='utf-8'
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


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
    log_file = f"collector_{collector_name.lower()}.log"
    return get_logger(collector_name, log_file=log_file, level=level)


def get_test_logger(test_name: str = 'long_running_test') -> logging.Logger:
    """
    테스트용 로거를 생성합니다.

    Args:
        test_name: 테스트 이름

    Returns:
        설정된 Logger 인스턴스
    """
    # 테스트 시작 시간을 포함한 파일명
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f"{test_name}_{timestamp}.log"
    return get_logger(test_name, log_file=log_file, level=logging.DEBUG)


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
