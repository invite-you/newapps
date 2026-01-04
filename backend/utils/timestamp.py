"""
타임스탬프 포맷 보조 모듈.
API 응답에서 받은 다양한 시각 표현을 ISO 8601(UTC, 초 단위)로 통일합니다.
"""
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

# 지원하는 파싱 포맷 (주요 API 반환값 중심)
_TIMESTAMP_FORMATS: Iterable[str] = (
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%b %d, %Y",
    "%B %d, %Y",
    "%m/%d/%Y",
)


def _finalize_datetime(dt: datetime) -> str:
    """datetime 객체를 UTC ISO 8601(초 단위) 문자열로 변환합니다."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat(timespec="seconds")


def normalize_timestamp(value: Any) -> Optional[str]:
    """
    다양한 타입의 시각 값을 ISO 8601(UTC, 초 단위) 문자열로 통일합니다.
    변환에 실패하면 None을 반환합니다.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return _finalize_datetime(value)

    if isinstance(value, (int, float)):
        return _finalize_datetime(datetime.fromtimestamp(value, tz=timezone.utc))

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None

        # Z 표기 처리
        normalized_text = text[:-1] + "+00:00" if text.endswith("Z") else text

        # 1차: ISO 파싱
        try:
            dt = datetime.fromisoformat(normalized_text)
            return _finalize_datetime(dt)
        except ValueError:
            pass

        # 2차: 준비된 포맷 반복 시도
        for fmt in _TIMESTAMP_FORMATS:
            try:
                dt = datetime.strptime(text, fmt)
                return _finalize_datetime(dt)
            except ValueError:
                continue

        return None

    return None

