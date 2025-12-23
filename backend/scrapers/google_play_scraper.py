# -*- coding: utf-8 -*-
"""
Google Play Store 스크래퍼
- 다양한 검색어로 최대한 많은 앱 수집
- app() 함수로 모든 상세 정보 수집
- 최근 업데이트 앱 우선 수집
"""
import sys
import os
import json
import time
from datetime import datetime
from google_play_scraper import search, app, Sort

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import (
    COUNTRIES, FETCH_LIMIT_PER_COUNTRY, GOOGLE_PLAY_SEARCH_QUERIES,
    LOG_FORMAT, REQUEST_DELAY, SSL_VERIFY, get_proxies
)
from database.db import get_connection, log_step

# 프록시 설정이 있으면 requests 세션에 적용
proxies = get_proxies()
if proxies:
    import requests
    original_request = requests.Session.request

    def patched_request(self, method, url, **kwargs):
        """프록시 및 SSL 설정을 자동으로 추가하는 패치된 request 메서드"""
        if 'proxies' not in kwargs:
            kwargs['proxies'] = proxies
        if 'verify' not in kwargs:
            kwargs['verify'] = SSL_VERIFY
        return original_request(self, method, url, **kwargs)

    requests.Session.request = patched_request


def search_apps(query, country_code, lang='en', n_hits=30):
    """
    검색어로 앱 검색

    Args:
        query: 검색어
        country_code: 국가 코드
        lang: 언어 코드
        n_hits: 결과 개수 (최대 30)

    Returns:
        앱 ID 목록
    """
    try:
        results = search(
            query,
            lang=lang,
            country=country_code,
            n_hits=min(n_hits, 30)  # 최대 30개
        )
        return [r.get('appId') for r in results if r.get('appId')]
    except Exception as e:
        print(f"    검색 오류 [{query}]: {str(e)[:50]}")
        return []


def get_app_details(app_id, country_code, lang='en'):
    """
    앱 상세 정보 가져오기

    Args:
        app_id: 앱 ID (패키지명)
        country_code: 국가 코드
        lang: 언어 코드

    Returns:
        앱 상세 정보 딕셔너리 또는 None
    """
    try:
        return app(app_id, lang=lang, country=country_code)
    except Exception as e:
        print(f"    상세정보 오류 [{app_id}]: {str(e)[:50]}")
        return None


def parse_google_play_data(app_data, country_code):
    """
    Google Play Scraper 응답을 DB 저장 형식으로 변환

    Args:
        app_data: google-play-scraper app() 응답
        country_code: 국가 코드

    Returns:
        DB 저장용 딕셔너리
    """
    if not app_data:
        return None

    # 카테고리 정보
    categories = app_data.get('categories', [])
    category_names = [c.get('name') for c in categories if c.get('name')]
    category_ids = [c.get('id') for c in categories if c.get('id')]

    # 히스토그램 (별점별 리뷰 수)
    histogram = app_data.get('histogram')

    # 업데이트 날짜 파싱 (timestamp 또는 문자열)
    updated = app_data.get('updated')
    updated_date = None
    if updated:
        if isinstance(updated, (int, float)):
            # Unix timestamp인 경우
            try:
                updated_date = datetime.fromtimestamp(updated).isoformat()
            except (ValueError, OSError):
                updated_date = None
        else:
            updated_date = str(updated)

    # 출시일 파싱
    released = app_data.get('released')
    release_date = str(released) if released else None

    return {
        'app_id': app_data.get('appId'),
        'bundle_id': app_data.get('appId'),  # Android는 패키지명이 bundle_id
        'platform': 'google_play',
        'country_code': country_code,

        # 기본 정보
        'title': app_data.get('title'),
        'developer': app_data.get('developer'),
        'developer_id': str(app_data.get('developerId', '')),
        'developer_email': app_data.get('developerEmail'),
        'developer_website': app_data.get('developerWebsite'),
        'developer_address': app_data.get('developerAddress'),
        'seller_name': app_data.get('developer'),

        # 아이콘 및 이미지
        'icon_url': app_data.get('icon'),
        'icon_url_small': app_data.get('icon'),
        'icon_url_large': app_data.get('icon'),
        'header_image': app_data.get('headerImage'),
        'screenshots': json.dumps(app_data.get('screenshots', [])),

        # 평점
        'rating': app_data.get('score'),
        'rating_count': app_data.get('ratings'),
        'rating_count_current_version': None,
        'rating_current_version': None,
        'reviews_count': app_data.get('reviews'),
        'histogram': json.dumps(histogram) if histogram else None,

        # 설치 및 가격
        'installs': app_data.get('installs'),
        'installs_min': app_data.get('minInstalls'),
        'installs_exact': app_data.get('realInstalls'),
        'price': app_data.get('price'),
        'price_formatted': str(app_data.get('price', 0)) if app_data.get('price') else 'Free',
        'currency': app_data.get('currency'),
        'free': 1 if app_data.get('free', True) else 0,

        # 카테고리
        'category': app_data.get('genre'),
        'category_id': app_data.get('genreId'),
        'genres': json.dumps(category_names) if category_names else None,
        'genre_ids': json.dumps(category_ids) if category_ids else None,

        # 설명
        'description': app_data.get('description'),
        'description_html': app_data.get('descriptionHTML'),
        'summary': app_data.get('summary'),
        'release_notes': app_data.get('recentChanges'),

        # 날짜
        'release_date': release_date,
        'updated_date': updated_date,
        'current_version_release_date': updated_date,

        # 버전 및 기술 정보
        'version': app_data.get('version'),
        'minimum_os_version': app_data.get('androidVersion'),
        'file_size': None,
        'file_size_formatted': app_data.get('size'),
        'supported_devices': None,
        'languages': None,

        # 콘텐츠 등급
        'content_rating': app_data.get('contentRating'),
        'content_rating_description': app_data.get('contentRatingDescription'),
        'advisories': None,

        # 앱 내 구매 및 광고
        'has_iap': 1 if app_data.get('offersIAP') else 0,
        'iap_price_range': app_data.get('inAppProductPrice'),
        'contains_ads': 1 if app_data.get('containsAds') else 0,
        'ad_supported': 1 if app_data.get('adSupported') else 0,

        # URL
        'url': app_data.get('url'),
        'store_url': app_data.get('url'),
        'privacy_policy_url': app_data.get('privacyPolicy'),

        # 차트 정보 (검색에서는 없음)
        'chart_position': None,
        'chart_type': None,

        # 기타
        'features': None,
        'permissions': None,
    }


def save_apps_to_db(apps_data):
    """앱 데이터를 데이터베이스에 저장"""
    if not apps_data:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    saved_count = 0

    columns = [
        'app_id', 'bundle_id', 'platform', 'country_code',
        'title', 'developer', 'developer_id', 'developer_email',
        'developer_website', 'developer_address', 'seller_name',
        'icon_url', 'icon_url_small', 'icon_url_large', 'header_image', 'screenshots',
        'rating', 'rating_count', 'rating_count_current_version',
        'rating_current_version', 'reviews_count', 'histogram',
        'installs', 'installs_min', 'installs_exact', 'price', 'price_formatted',
        'currency', 'free',
        'category', 'category_id', 'genres', 'genre_ids',
        'description', 'description_html', 'summary', 'release_notes',
        'release_date', 'updated_date', 'current_version_release_date',
        'version', 'minimum_os_version', 'file_size', 'file_size_formatted',
        'supported_devices', 'languages',
        'content_rating', 'content_rating_description', 'advisories',
        'has_iap', 'iap_price_range', 'contains_ads', 'ad_supported',
        'url', 'store_url', 'privacy_policy_url',
        'chart_position', 'chart_type',
        'features', 'permissions',
    ]

    placeholders = ', '.join(['?' for _ in columns])
    columns_str = ', '.join(columns)
    update_columns = [col for col in columns if col not in ('app_id', 'platform', 'country_code')]
    update_assignments = ', '.join([f"{col} = excluded.{col}" for col in update_columns])

    for app_data in apps_data:
        if not app_data:
            continue
        try:
            values = tuple(app_data.get(col) for col in columns)
            cursor.execute(f"""
                INSERT INTO apps ({columns_str}, updated_at)
                VALUES ({placeholders}, CURRENT_TIMESTAMP)
                ON CONFLICT(app_id, platform, country_code) DO UPDATE SET
                    {update_assignments},
                    updated_at = CURRENT_TIMESTAMP
            """, values)
            saved_count += 1
        except Exception as e:
            print(f"  저장 실패 [{app_data.get('app_id')}]: {str(e)}")

    conn.commit()
    conn.close()
    return saved_count


def scrape_new_apps_by_country(country_code, limit=FETCH_LIMIT_PER_COUNTRY):
    """
    특정 국가의 Google Play Store에서 앱 수집
    여러 검색어로 최대한 많은 앱을 수집

    Args:
        country_code: 국가 코드 (예: 'kr', 'us')
        limit: 수집할 최대 앱 개수

    Returns:
        수집된 앱 개수
    """
    start_time = datetime.now()
    log_step(f"Google Play 수집 [{country_code.upper()}]", "시작", start_time)

    # 언어 설정 (국가별)
    lang_map = {
        'kr': 'ko', 'jp': 'ja', 'cn': 'zh', 'tw': 'zh',
        'de': 'de', 'fr': 'fr', 'es': 'es', 'it': 'it',
        'br': 'pt', 'ru': 'ru', 'th': 'th', 'vn': 'vi',
        'id': 'id', 'in': 'hi', 'ar': 'ar', 'sa': 'ar',
    }
    lang = lang_map.get(country_code, 'en')

    all_app_ids = set()

    # 1. 여러 검색어로 앱 ID 수집
    print(f"  검색어 {len(GOOGLE_PLAY_SEARCH_QUERIES)}개로 앱 검색 중...")
    for query in GOOGLE_PLAY_SEARCH_QUERIES:
        app_ids = search_apps(query, country_code, lang=lang)
        before_count = len(all_app_ids)
        all_app_ids.update(app_ids)
        new_count = len(all_app_ids) - before_count
        if new_count > 0:
            print(f"    [{query}]: +{new_count}개 (총 {len(all_app_ids)}개)")
        time.sleep(REQUEST_DELAY)

        # 목표 개수 도달 시 중단
        if len(all_app_ids) >= limit:
            break

    if not all_app_ids:
        log_step(f"Google Play 수집 [{country_code.upper()}]", "앱 없음", start_time)
        return 0

    print(f"  총 {len(all_app_ids)}개 고유 앱 ID 수집됨")

    # 2. 각 앱의 상세 정보 수집
    print(f"  상세 정보 수집 중...")
    apps_data = []
    collected = 0
    failed = 0

    for app_id in list(all_app_ids)[:limit]:
        details = get_app_details(app_id, country_code, lang=lang)
        if details:
            parsed = parse_google_play_data(details, country_code)
            if parsed:
                apps_data.append(parsed)
                collected += 1
        else:
            failed += 1

        # 진행상황 출력 (50개마다)
        if (collected + failed) % 50 == 0:
            print(f"    진행: {collected}개 수집, {failed}개 실패")

        time.sleep(REQUEST_DELAY)

    print(f"  -> {len(apps_data)}개 앱 상세 정보 수집됨")

    # 최근 업데이트 순으로 정렬
    apps_data.sort(
        key=lambda x: x.get('updated_date') or x.get('release_date') or '',
        reverse=True
    )

    # 3. 데이터베이스에 저장
    saved_count = save_apps_to_db(apps_data)
    log_step(f"Google Play 수집 [{country_code.upper()}]", f"완료 ({saved_count}개 저장)", start_time)

    return saved_count


def scrape_all_countries():
    """모든 국가의 Google Play Store에서 앱 수집"""
    total_start = datetime.now()
    log_step("Google Play 전체 수집", "시작", total_start)

    total_apps = 0
    for country in COUNTRIES:
        try:
            count = scrape_new_apps_by_country(country['code'])
            total_apps += count
            time.sleep(REQUEST_DELAY * 2)  # 국가 간 딜레이
        except Exception as e:
            print(f"  오류 발생 [{country['code']}]: {str(e)}")
            continue

    log_step("Google Play 전체 수집", f"완료 (총 {total_apps}개 앱)", total_start)
    return total_apps


if __name__ == "__main__":
    # 단일 국가 테스트
    print("=" * 60)
    print("Google Play 스크래퍼 테스트")
    print("=" * 60)
    scrape_new_apps_by_country('kr', limit=30)
