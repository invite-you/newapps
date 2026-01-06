"""
에러 추적 모듈
에러 발생 시 상세 정보를 누적 기록하여 원인 분석을 용이하게 합니다.
"""
import os
import json
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from enum import Enum

from .logger import get_logger, LOG_DIR


class ErrorStep(Enum):
    """에러 발생 단계"""
    FETCH = "fetch"              # 데이터 가져오기
    PARSE = "parse"              # 파싱
    SAVE = "save"                # 저장
    COLLECT_APP = "collect_app"  # 앱 수집
    COLLECT_REVIEW = "collect_review"  # 리뷰 수집
    COLLECT_SITEMAP = "collect_sitemap"  # 사이트맵 수집
    DB_OPERATION = "db_operation"  # DB 작업
    ENCODING = "encoding"        # 인코딩
    UNKNOWN = "unknown"          # 알 수 없음


@dataclass
class ErrorRecord:
    """에러 레코드"""
    timestamp: str
    platform: str
    step: str
    app_id: Optional[str]
    error_type: str
    error_message: str
    traceback: Optional[str] = None
    extra_info: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """딕셔너리로 변환"""
        return asdict(self)

    def to_summary(self) -> str:
        """요약 문자열 생성"""
        app_info = f"app={self.app_id}" if self.app_id else "no_app"
        return f"[{self.platform}:{self.step}] {app_info} - {self.error_type}: {self.error_message[:100]}"


class ErrorTracker:
    """
    에러 추적기

    수집 과정에서 발생하는 에러를 상세하게 기록하고 분석할 수 있게 합니다.
    """

    def __init__(self, name: str = "default", max_errors: int = 10000):
        """
        Args:
            name: 추적기 이름 (로그 파일 구분용)
            max_errors: 최대 보관 에러 수 (메모리 관리)
        """
        self.name = name
        self.max_errors = max_errors
        self.errors: List[ErrorRecord] = []
        self.error_counts: Dict[str, int] = {}  # step별 에러 카운트
        self.logger = get_logger(
            f"error_tracker_{name}",
            log_file=f"errors_{name}.log"
        )

    def add_error(
        self,
        platform: str,
        step: ErrorStep,
        error: Exception,
        app_id: Optional[str] = None,
        include_traceback: bool = True,
        **extra_info
    ) -> ErrorRecord:
        """
        에러를 기록합니다.

        Args:
            platform: 플랫폼 ('app_store', 'play_store')
            step: 에러 발생 단계
            error: 발생한 예외
            app_id: 앱 ID (있는 경우)
            include_traceback: 스택트레이스 포함 여부
            **extra_info: 추가 정보 (country, page 등)

        Returns:
            생성된 ErrorRecord
        """
        tb = traceback.format_exc() if include_traceback else None
        step_name = step.value if isinstance(step, ErrorStep) else str(step)

        record = ErrorRecord(
            timestamp=datetime.now().isoformat(),
            platform=platform,
            step=step_name,
            app_id=app_id,
            error_type=type(error).__name__,
            error_message=str(error)[:500],  # 메시지 길이 제한
            traceback=tb,
            extra_info=extra_info
        )

        # 에러 목록에 추가 (최대 개수 유지)
        if len(self.errors) >= self.max_errors:
            self.errors.pop(0)  # 오래된 에러 제거
        self.errors.append(record)

        # 카운트 업데이트
        count_key = f"{platform}:{step_name}"
        self.error_counts[count_key] = self.error_counts.get(count_key, 0) + 1

        # 로그 기록
        self.logger.error(record.to_summary())
        if tb:
            self.logger.debug(f"Traceback:\n{tb}")

        return record

    def add_error_simple(
        self,
        context: str,
        error: str,
        app_id: Optional[str] = None
    ):
        """
        간단한 에러 기록 (기존 add_error와 호환)

        Args:
            context: 컨텍스트 문자열 (예: 'sitemap_app_store')
            error: 에러 메시지
            app_id: 앱 ID
        """
        # context에서 platform과 step 추출 시도
        parts = context.split('_', 1)
        if len(parts) >= 2:
            step_part = parts[0]
            platform_part = '_'.join(parts[1:])
            if platform_part in ['app_store', 'play_store']:
                platform = platform_part
                step = step_part
            else:
                platform = "unknown"
                step = context
        else:
            platform = "unknown"
            step = context

        record = ErrorRecord(
            timestamp=datetime.now().isoformat(),
            platform=platform,
            step=step,
            app_id=app_id,
            error_type="Error",
            error_message=str(error)[:500],
            traceback=None,
            extra_info={}
        )

        if len(self.errors) >= self.max_errors:
            self.errors.pop(0)
        self.errors.append(record)

        count_key = f"{platform}:{step}"
        self.error_counts[count_key] = self.error_counts.get(count_key, 0) + 1

        self.logger.error(record.to_summary())

    def get_errors_by_app(self, app_id: str) -> List[ErrorRecord]:
        """특정 앱의 에러 목록"""
        return [e for e in self.errors if e.app_id == app_id]

    def get_errors_by_step(self, step: ErrorStep) -> List[ErrorRecord]:
        """특정 단계의 에러 목록"""
        step_name = step.value if isinstance(step, ErrorStep) else str(step)
        return [e for e in self.errors if e.step == step_name]

    def get_errors_by_platform(self, platform: str) -> List[ErrorRecord]:
        """특정 플랫폼의 에러 목록"""
        return [e for e in self.errors if e.platform == platform]

    def get_summary(self) -> Dict[str, Any]:
        """에러 요약 정보"""
        return {
            'total_errors': len(self.errors),
            'errors_by_step': dict(self.error_counts),
            'unique_apps_with_errors': len(set(
                e.app_id for e in self.errors if e.app_id
            )),
            'recent_errors': [e.to_dict() for e in self.errors[-20:]],
            'error_types': self._count_by_field('error_type'),
            'errors_by_platform': {
                'app_store': len([e for e in self.errors if e.platform == 'app_store']),
                'play_store': len([e for e in self.errors if e.platform == 'play_store']),
                'unknown': len([e for e in self.errors if e.platform not in ['app_store', 'play_store']])
            }
        }

    def _count_by_field(self, field: str) -> Dict[str, int]:
        """필드별 카운트"""
        counts = {}
        for e in self.errors:
            value = getattr(e, field, 'unknown')
            counts[value] = counts.get(value, 0) + 1
        return counts

    def get_all_errors(self) -> List[Dict]:
        """모든 에러를 딕셔너리 리스트로 반환"""
        return [e.to_dict() for e in self.errors]

    def get_error_count(self) -> int:
        """총 에러 수"""
        return len(self.errors)

    def clear(self):
        """에러 목록 초기화"""
        self.errors.clear()
        self.error_counts.clear()

    def save_to_file(self, filename: Optional[str] = None) -> str:
        """
        에러 로그를 JSON 파일로 저장

        Args:
            filename: 파일 이름 (없으면 자동 생성)

        Returns:
            저장된 파일 경로
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"error_report_{self.name}_{timestamp}.json"

        filepath = os.path.join(LOG_DIR, filename)

        report = {
            'generated_at': datetime.now().isoformat(),
            'tracker_name': self.name,
            'summary': self.get_summary(),
            'all_errors': self.get_all_errors()
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)

        self.logger.info(f"Error report saved to: {filepath}")
        return filepath

    def print_summary(self):
        """콘솔에 요약 출력"""
        summary = self.get_summary()

        print("\n" + "=" * 60)
        print("에러 추적 요약")
        print("=" * 60)
        print(f"총 에러 수: {summary['total_errors']}")
        print(f"에러 발생 앱 수: {summary['unique_apps_with_errors']}")

        print("\n[플랫폼별 에러]")
        for platform, count in summary['errors_by_platform'].items():
            if count > 0:
                print(f"  {platform}: {count}건")

        print("\n[단계별 에러]")
        for step, count in sorted(summary['errors_by_step'].items(), key=lambda x: -x[1]):
            print(f"  {step}: {count}건")

        print("\n[에러 유형별]")
        for error_type, count in sorted(summary['error_types'].items(), key=lambda x: -x[1])[:10]:
            print(f"  {error_type}: {count}건")

        print("\n[최근 에러 (최대 10개)]")
        for err in summary['recent_errors'][-10:]:
            app_info = f"app={err['app_id']}" if err['app_id'] else "no_app"
            print(f"  [{err['platform']}:{err['step']}] {app_info}")
            print(f"    {err['error_type']}: {err['error_message'][:80]}")

        print("=" * 60 + "\n")


# 전역 에러 트래커 (싱글톤 패턴)
_global_tracker: Optional[ErrorTracker] = None


def get_global_tracker(name: str = "global") -> ErrorTracker:
    """전역 에러 트래커 가져오기"""
    global _global_tracker
    if _global_tracker is None:
        _global_tracker = ErrorTracker(name)
    return _global_tracker


def reset_global_tracker():
    """전역 에러 트래커 초기화"""
    global _global_tracker
    _global_tracker = None
