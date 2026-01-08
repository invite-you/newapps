"""
수집기 공통 유틸리티
앱 로컬라이제이션 쌍 처리 및 기본 선택 로직을 제공합니다.
"""
from typing import List, Tuple, Optional

from database.sitemap_apps_db import (
    get_connection as get_sitemap_connection,
    release_connection as release_sitemap_connection,
)

DEFAULT_LANGUAGE = "en"
DEFAULT_COUNTRY = "US"


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
