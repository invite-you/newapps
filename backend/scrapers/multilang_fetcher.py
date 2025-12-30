# -*- coding: utf-8 -*-
"""
다국어 앱 정보 수집기
- 5개 언어(ko, en, es, ja, zh)로 앱 상세 정보 수집
- Google Play와 App Store 지원
- title 기준으로 개발자 현지화 언어 판별
"""
import sys
import os
import json
import time
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import REQUEST_DELAY
from database.db import get_connection, log_step, SUPPORTED_LANGUAGES, MULTILANG_FIELDS

# Google Play Scraper
try:
    from google_play_scraper import app as google_app
    from google_play_scraper.exceptions import NotFoundError as GooglePlayNotFoundError
    GOOGLE_PLAY_AVAILABLE = True
except ImportError:
    GOOGLE_PLAY_AVAILABLE = False
    GooglePlayNotFoundError = None

# App Store - iTunes API
import requests

# 언어-국가 매핑 (Google Play용)
LANG_TO_COUNTRY = {
    'ko': 'kr',
    'en': 'us',
    'es': 'es',
    'ja': 'jp',
    'zh': 'cn',
}

# 다국어 필드 매핑 (API 응답 키 → DB 필드)
GOOGLE_PLAY_FIELD_MAP = {
    'title': 'title',
    'summary': 'summary',
    'description': 'description',
    'descriptionHTML': 'description_html',
    'recentChanges': 'release_notes',
    'genre': 'category',
    'contentRating': 'content_rating',
}

APP_STORE_FIELD_MAP = {
    'trackName': 'title',
    'description': 'description',
    'releaseNotes': 'release_notes',
    'primaryGenreName': 'category',
    'contentAdvisoryRating': 'content_rating',
}


def fetch_google_play_multilang(app_id: str) -> Dict[str, Any]:
    """
    Google Play 앱의 다국어 정보 수집

    Args:
        app_id: 앱 패키지명

    Returns:
        {
            'title_ko': '카카오톡',
            'title_en': 'KakaoTalk',
            ...
            'available_languages': ['ko', 'en', 'ja']
        }
    """
    if not GOOGLE_PLAY_AVAILABLE:
        return {}

    result = {}
    lang_titles = {}

    for lang in SUPPORTED_LANGUAGES:
        country = LANG_TO_COUNTRY[lang]
        try:
            data = google_app(app_id, lang=lang, country=country)

            # 다국어 필드 추출
            for api_key, db_field in GOOGLE_PLAY_FIELD_MAP.items():
                value = data.get(api_key)
                column_name = f"{db_field}_{lang}"
                # None과 빈값 구분: API가 None 반환 → None, 빈문자열 → ''
                result[column_name] = value

            # title 저장 (available_languages 계산용)
            lang_titles[lang] = data.get('title')

            time.sleep(REQUEST_DELAY)

        except GooglePlayNotFoundError:
            # 해당 국가에서 앱을 찾을 수 없음 - 모든 필드 None
            for db_field in GOOGLE_PLAY_FIELD_MAP.values():
                result[f"{db_field}_{lang}"] = None
        except Exception as e:
            log_step("다국어 수집", f"[오류] Google Play {app_id} ({lang}): {e}", "다국어 수집")
            for db_field in GOOGLE_PLAY_FIELD_MAP.values():
                result[f"{db_field}_{lang}"] = None

    # available_languages 계산 (title 기준)
    result['available_languages'] = json.dumps(
        detect_localized_languages(lang_titles)
    )

    return result


def fetch_app_store_multilang(app_id: str) -> Dict[str, Any]:
    """
    App Store 앱의 다국어 정보 수집

    Args:
        app_id: 앱 ID (숫자)

    Returns:
        {
            'title_ko': '카카오톡',
            'title_en': 'KakaoTalk',
            ...
            'available_languages': ['ko', 'en', 'ja']
        }
    """
    result = {}
    lang_titles = {}

    for lang in SUPPORTED_LANGUAGES:
        country = LANG_TO_COUNTRY[lang]
        try:
            url = f"https://itunes.apple.com/lookup?id={app_id}&country={country}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data.get('resultCount', 0) > 0:
                app_data = data['results'][0]

                # 다국어 필드 추출
                for api_key, db_field in APP_STORE_FIELD_MAP.items():
                    value = app_data.get(api_key)
                    column_name = f"{db_field}_{lang}"
                    result[column_name] = value

                # App Store는 summary가 없음
                result[f"summary_{lang}"] = None
                result[f"description_html_{lang}"] = None

                # title 저장
                lang_titles[lang] = app_data.get('trackName')
            else:
                # 해당 국가에서 앱 없음
                for db_field in set(APP_STORE_FIELD_MAP.values()):
                    result[f"{db_field}_{lang}"] = None
                result[f"summary_{lang}"] = None
                result[f"description_html_{lang}"] = None

            time.sleep(REQUEST_DELAY)

        except Exception as e:
            log_step("다국어 수집", f"[오류] App Store {app_id} ({lang}): {e}", "다국어 수집")
            for db_field in set(APP_STORE_FIELD_MAP.values()):
                result[f"{db_field}_{lang}"] = None
            result[f"summary_{lang}"] = None
            result[f"description_html_{lang}"] = None

    # available_languages 계산 (title 기준)
    result['available_languages'] = json.dumps(
        detect_localized_languages(lang_titles)
    )

    return result


def detect_localized_languages(lang_titles: Dict[str, Optional[str]]) -> List[str]:
    """
    title 기준으로 개발자가 설정한 고유 언어 판별

    규칙:
    - 모든 title이 동일하면 → 원본 언어 1개만 포함
    - title이 다르면 → 원본 언어 + 현지화된 언어 모두 포함

    Args:
        lang_titles: {'ko': '카카오톡', 'en': 'KakaoTalk', 'ja': 'カカオトーク', ...}

    Returns:
        개발자가 설정한 고유 언어 목록

    Examples:
        - 카카오톡 (모두 다름): ['en', 'es', 'ja', 'ko', 'zh']
        - TMAP (모두 동일): ['ko'] (원본 언어 1개)
        - 쿠팡 (en만 다름): ['en', 'ko'] (원본 + 현지화)
    """
    if not lang_titles:
        return []

    # None이 아닌 title만 추출
    valid_titles = {lang: title for lang, title in lang_titles.items() if title is not None}

    if not valid_titles:
        return []

    # 고유한 title 값들
    unique_titles = set(valid_titles.values())

    if len(unique_titles) == 1:
        # 모든 title이 동일 = 원본 언어 1개만 반환
        # 우선순위: en > ko > 나머지
        for priority_lang in ['en', 'ko', 'es', 'ja', 'zh']:
            if priority_lang in valid_titles:
                return [priority_lang]
        return [list(valid_titles.keys())[0]]

    # 각 title이 몇 번 등장하는지 카운트
    from collections import Counter
    title_counts = Counter(valid_titles.values())

    # 가장 많이 등장하는 title = 원본 (기본 언어)
    base_title = title_counts.most_common(1)[0][0]

    # 원본 언어 1개 선택 (우선순위: en > ko > 나머지)
    base_langs = [lang for lang, title in valid_titles.items() if title == base_title]
    base_lang = None
    for priority_lang in ['en', 'ko', 'es', 'ja', 'zh']:
        if priority_lang in base_langs:
            base_lang = priority_lang
            break
    if not base_lang:
        base_lang = base_langs[0]

    # 현지화된 언어들 (원본과 다른 title)
    localized = [lang for lang, title in valid_titles.items() if title != base_title]

    # 원본 + 현지화된 언어 반환
    return sorted([base_lang] + localized)


def update_app_multilang(app_id: str, platform: str, country_code: str) -> bool:
    """
    기존 앱 레코드에 다국어 정보 업데이트

    Args:
        app_id: 앱 ID
        platform: 'google_play' 또는 'app_store'
        country_code: 국가 코드

    Returns:
        성공 여부
    """
    try:
        # 다국어 데이터 수집
        if platform == 'google_play':
            multilang_data = fetch_google_play_multilang(app_id)
        elif platform == 'app_store':
            multilang_data = fetch_app_store_multilang(app_id)
        else:
            return False

        if not multilang_data:
            return False

        # DB 업데이트
        conn = get_connection()
        cursor = conn.cursor()

        # SET 절 생성
        set_clauses = []
        values = []
        for column, value in multilang_data.items():
            set_clauses.append(f"{column} = ?")
            values.append(value)

        values.extend([app_id, platform, country_code])

        cursor.execute(f"""
            UPDATE apps
            SET {', '.join(set_clauses)}, updated_at = CURRENT_TIMESTAMP
            WHERE app_id = ? AND platform = ? AND country_code = ?
        """, values)

        conn.commit()
        updated = cursor.rowcount > 0
        conn.close()

        return updated

    except Exception as e:
        log_step("다국어 업데이트", f"[오류] {app_id}: {e}", "다국어 업데이트")
        return False


def fetch_multilang_batch(app_ids: List[Tuple[str, str, str]],
                          batch_size: int = 10) -> Dict[str, int]:
    """
    여러 앱의 다국어 정보 일괄 수집

    Args:
        app_ids: [(app_id, platform, country_code), ...]
        batch_size: 배치당 처리할 앱 수

    Returns:
        {'success': 10, 'failed': 2, 'skipped': 0}
    """
    stats = {'success': 0, 'failed': 0, 'skipped': 0}
    total = len(app_ids)

    for i, (app_id, platform, country_code) in enumerate(app_ids):
        try:
            if update_app_multilang(app_id, platform, country_code):
                stats['success'] += 1
            else:
                stats['failed'] += 1

            # 진행 상황 출력
            if (i + 1) % batch_size == 0:
                log_step("다국어 수집",
                        f"진행: {i+1}/{total} (성공: {stats['success']}, 실패: {stats['failed']})",
                        "다국어 수집")

        except Exception as e:
            stats['failed'] += 1
            log_step("다국어 수집", f"[오류] {app_id}: {e}", "다국어 수집")

    return stats


def get_apps_without_multilang(platform: str = None, limit: int = 100) -> List[Tuple[str, str, str]]:
    """
    다국어 정보가 없는 앱 목록 조회

    Args:
        platform: 플랫폼 필터 (None이면 전체)
        limit: 최대 개수

    Returns:
        [(app_id, platform, country_code), ...]
    """
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT app_id, platform, country_code
        FROM apps
        WHERE available_languages IS NULL
    """
    params = []

    if platform:
        query += " AND platform = ?"
        params.append(platform)

    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return [(row['app_id'], row['platform'], row['country_code']) for row in rows]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='다국어 앱 정보 수집')
    parser.add_argument('--app-id', type=str, help='단일 앱 테스트')
    parser.add_argument('--platform', type=str, default='google_play',
                        choices=['google_play', 'app_store'])
    parser.add_argument('--batch', action='store_true', help='다국어 정보 없는 앱 일괄 수집')
    parser.add_argument('--limit', type=int, default=10, help='일괄 수집 시 최대 개수')

    args = parser.parse_args()

    if args.app_id:
        # 단일 앱 테스트
        print(f"앱 다국어 정보 수집: {args.app_id} ({args.platform})")

        if args.platform == 'google_play':
            result = fetch_google_play_multilang(args.app_id)
        else:
            result = fetch_app_store_multilang(args.app_id)

        print("\n수집 결과:")
        for key, value in sorted(result.items()):
            if value and isinstance(value, str) and len(value) > 50:
                print(f"  {key}: {value[:50]}...")
            else:
                print(f"  {key}: {value}")

    elif args.batch:
        # 일괄 수집
        print(f"다국어 정보 없는 앱 일괄 수집 (limit={args.limit})")
        apps = get_apps_without_multilang(args.platform, args.limit)
        print(f"대상 앱: {len(apps)}개")

        if apps:
            stats = fetch_multilang_batch(apps)
            print(f"\n완료: 성공={stats['success']}, 실패={stats['failed']}")
    else:
        # 기본: 테스트
        print("테스트: 카카오톡 다국어 정보 수집")
        result = fetch_google_play_multilang('com.kakao.talk')
        print(f"\navailable_languages: {result.get('available_languages')}")
        for lang in SUPPORTED_LANGUAGES:
            print(f"  title_{lang}: {result.get(f'title_{lang}', 'N/A')}")
