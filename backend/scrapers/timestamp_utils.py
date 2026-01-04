"""
타임스탬프 정규화 유틸리티
API 응답에서 제공되는 다양한 형식의 시간을 ISO 8601 문자열로 통일합니다.
"""
from datetime import datetime, timezone
from typing import Optional, Union

# 지원할 타임스탬프 포맷 목록 (가장 빈번한 순서로 배치)
TIMESTAMP_FORMATS = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%b %d, %Y",   # Jan 02, 2024
    "%B %d, %Y",   # January 02, 2024
    "%d %b %Y",    # 02 Jan 2024
    "%d %B %Y",    # 02 January 2024
]


def normalize_timestamp(value: Union[str, int, float, datetime, None]) -> Optional[str]:
    """
    입력된 타임스탬프 값을 ISO 8601 문자열(UTC)로 변환합니다.
    - datetime 객체: UTC로 변환 후 isoformat 출력
    - 숫자: epoch 기준(초/밀리초)으로 간주
    - 문자열: 대표적인 날짜 포맷을 시도 후 isoformat 출력
    파싱이 불가능하면 None을 반환합니다.
    """
    if value is None:
        return None

    # datetime 객체 처리
    if isinstance(value, datetime):
        dt = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    # 숫자(epoch)
    if isinstance(value, (int, float)):
        # 13자리(밀리초) → 초 단위로 변환
        timestamp = value / 1000 if value > 1e11 else value
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return dt.isoformat()

    # 문자열 처리
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None

        # ISO 형태(+Z) 우선 파싱
        try:
            normalized = text.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            dt = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            pass

        # 사전 정의 포맷 시도
        for fmt in TIMESTAMP_FORMATS:
            try:
                dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except ValueError:
                continue

    return None
