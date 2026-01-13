"""
수집기 공통 유틸리티
앱 로컬라이제이션 쌍 처리 및 기본 선택 로직을 제공합니다.
"""
import os
from typing import List, Tuple, Optional

from database.sitemap_apps_db import (
    get_connection as get_sitemap_connection,
    release_connection as release_sitemap_connection,
)
from config.language_country_priority import (
    select_best_pairs_for_collection,
    sort_language_country_pairs,
)

DEFAULT_LANGUAGE = "en"
DEFAULT_COUNTRY = "US"
DEFAULT_MAX_PAIRS = 10
DEFAULT_MAX_LANGUAGES = 10
NETWORK_FAILURE_REASONS = frozenset({
    "timeout",
    "network_error",
    "request_error",
    "rate_limited",
    "server_error",
    "scraper_error",
    "http_error",
})


class CollectionErrorPolicy:
    """수집 실패 사유별 중단 여부를 결정합니다."""

    def __init__(self, network_reasons: Optional[set] = None):
        self.network_reasons = network_reasons or NETWORK_FAILURE_REASONS

    @staticmethod
    def _base_reason(reason: Optional[str]) -> Optional[str]:
        if not reason:
            return None
        return reason.split(":")[0] if ":" in reason else reason

    def should_abort(self, reason: Optional[str]) -> bool:
        base = self._base_reason(reason)
        if not base:
            return False
        return base in self.network_reasons


class LocalePairPolicy:
    """언어/국가 쌍 선택 정책을 관리합니다."""

    def __init__(self, max_languages: int, max_pairs: int):
        self.max_languages = max_languages
        self.max_pairs = max_pairs

    @classmethod
    def from_env(cls) -> "LocalePairPolicy":
        max_languages = int(os.getenv("APP_LOCALE_MAX_LANGUAGES", str(DEFAULT_MAX_LANGUAGES)))
        max_pairs = int(os.getenv("APP_LOCALE_MAX_PAIRS", str(DEFAULT_MAX_PAIRS)))
        return cls(max_languages=max_languages, max_pairs=max_pairs)

    def select_pairs(
        self,
        pairs: List[Tuple[str, str]],
        country_case: str = "upper",
        default_pair: Optional[Tuple[str, str]] = None,
    ) -> List[Tuple[str, str]]:
        if not pairs:
            return [default_pair] if default_pair else []

        normalized = [
            (lang.lower(), country.upper())
            for lang, country in pairs
            if lang and country
        ]
        if not normalized:
            return [default_pair] if default_pair else []

        languages = {lang.split("-")[0] for lang, _ in normalized}
        max_languages = self.max_languages if self.max_languages > 0 else len(languages)
        max_languages = min(max_languages, len(languages)) if languages else 0

        selected = select_best_pairs_for_collection(
            normalized,
            max_languages=max_languages or len(languages),
        )
        prioritized = sort_language_country_pairs(selected)
        if self.max_pairs > 0:
            prioritized = prioritized[: self.max_pairs]

        if country_case == "lower":
            return [(lang, country.lower()) for lang, country in prioritized]
        return [(lang, country.upper()) for lang, country in prioritized]


def _normalize_country(country: str, target_case: str) -> str:
    """국가 코드를 대소문자 규칙에 맞게 정규화합니다."""
    if target_case == "lower":
        return country.lower()
    return country.upper()


def get_app_language_country_pairs(
    app_id: str,
    platform: str,
    normalize_country_case: str = "upper",
    default_pair: Optional[Tuple[str, str]] = None,
) -> List[Tuple[str, str]]:
    """sitemap DB에서 앱의 (language, country) 쌍을 가져옵니다."""
    conn = get_sitemap_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT language, country FROM app_localizations
                WHERE app_id = %s AND platform = %s
                """,
                (app_id, platform),
            )
            pairs = [
                (row["language"], _normalize_country(row["country"], normalize_country_case))
                for row in cursor.fetchall()
            ]
    finally:
        release_sitemap_connection(conn)

    if pairs:
        return pairs
    if default_pair:
        return [default_pair]
    return []


def select_primary_country(
    optimized_pairs: List[Tuple[str, str]],
    preferred_country: str = DEFAULT_COUNTRY,
) -> str:
    """최적화된 쌍에서 기준 국가를 선택합니다."""
    preferred_upper = preferred_country.upper()
    for _, country in optimized_pairs:
        if country.upper() == preferred_upper:
            return preferred_upper
    if optimized_pairs:
        return optimized_pairs[0][1].upper()
    return preferred_upper


def select_primary_pair(
    optimized_pairs: List[Tuple[str, str]],
    preferred_language: str = DEFAULT_LANGUAGE,
    preferred_country: str = DEFAULT_COUNTRY,
) -> Tuple[str, str]:
    """최적화된 쌍에서 기준 (language, country)를 선택합니다."""
    preferred_upper = preferred_country.upper()
    for lang, country in optimized_pairs:
        if lang == preferred_language and country.upper() == preferred_upper:
            return (lang, country)
    for lang, country in optimized_pairs:
        if lang == preferred_language:
            return (lang, country)
    if optimized_pairs:
        return optimized_pairs[0]
    return (preferred_language, preferred_country)
