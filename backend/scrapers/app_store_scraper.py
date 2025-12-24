# -*- coding: utf-8 -*-
"""
Apple App Store 스크래퍼
- RSS API v2: 최신 차트에서 앱 목록 수집 (국가별 최대 200개)
- iTunes Lookup API: 앱 상세 정보 수집 (모든 필드)
"""
import sys
import os
import json
import time
import requests
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import (
    COUNTRIES, FETCH_LIMIT_PER_COUNTRY, APPLE_RSS_FEEDS,
    REQUEST_DELAY, get_request_kwargs
)
from database.db import get_connection, log_step

# API 엔드포인트
RSS_API_BASE = "https://rss.applemarketingtools.com/api/v2/{country}/apps/{feed}/{limit}/apps.json"
LOOKUP_API_BASE = "https://itunes.apple.com/lookup"


def fetch_chart_apps(country_code, feed_type, limit=200):
    """
    RSS API v2를 사용하여 차트 앱 목록 가져오기

    Args:
        country_code: 국가 코드 (예: 'kr', 'us')
        feed_type: 피드 유형 ('top-free', 'top-paid')
        limit: 가져올 앱 개수 (최대 200)

    Returns:
        (앱 ID 목록, 차트 정보 딕셔너리)
    """
    url = RSS_API_BASE.format(
        country=country_code.lower(),
        feed=feed_type,
        limit=min(limit, 200)
    )

    try:
        response = requests.get(url, **get_request_kwargs())
        if response.status_code != 200:
            print(f"  RSS API 오류 [{feed_type}]: HTTP {response.status_code}")
            return [], {}

        data = response.json()
        feed = data.get('feed', {})
        results = feed.get('results', [])

        app_ids = []
        chart_info = {}

        for position, app in enumerate(results, 1):
            app_id = app.get('id')
            if app_id:
                app_ids.append(app_id)
                chart_info[app_id] = {
                    'position': position,
                    'chart_type': feed_type,
                    'chart_name': app.get('name'),
                    'chart_artist': app.get('artistName'),
                }

        return app_ids, chart_info

    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"  RSS API 오류 [{feed_type}]: {str(e)}")
        return [], {}


def fetch_app_details(app_ids, country_code):
    """
    iTunes Lookup API를 사용하여 앱 상세 정보 가져오기

    Args:
        app_ids: 앱 ID 목록 (최대 200개씩 배치 처리)
        country_code: 국가 코드

    Returns:
        앱 상세 정보 딕셔너리 (app_id -> 정보)
    """
    all_details = {}
    batch_size = 200  # iTunes Lookup API 한 번에 최대 200개

    for i in range(0, len(app_ids), batch_size):
        batch_ids = app_ids[i:i + batch_size]
        ids_str = ','.join(batch_ids)

        try:
            params = {
                'id': ids_str,
                'country': country_code.upper(),
            }
            response = requests.get(LOOKUP_API_BASE, params=params, **get_request_kwargs())

            if response.status_code != 200:
                print(f"  Lookup API 오류: HTTP {response.status_code}")
                continue

            data = response.json()
            results = data.get('results', [])

            for app in results:
                app_id = str(app.get('trackId'))
                if app_id:
                    all_details[app_id] = app

            # 레이트 리밋 방지
            if i + batch_size < len(app_ids):
                time.sleep(REQUEST_DELAY)

        except (requests.RequestException, json.JSONDecodeError) as e:
            print(f"  Lookup API 오류: {str(e)}")
            continue

    return all_details


def parse_app_store_data(app_data, country_code, chart_info=None):
    """
    iTunes Lookup API 응답을 DB 저장 형식으로 변환

    Args:
        app_data: iTunes Lookup API 응답의 앱 정보
        country_code: 국가 코드
        chart_info: 차트 정보 (위치, 유형)

    Returns:
        DB 저장용 딕셔너리
    """
    app_id = str(app_data.get('trackId', ''))

    # 스크린샷 URL 목록
    screenshots = (
        app_data.get('screenshotUrls', []) +
        app_data.get('ipadScreenshotUrls', []) +
        app_data.get('appletvScreenshotUrls', [])
    )

    # 장르 정보
    genres = app_data.get('genres', [])
    genre_ids = app_data.get('genreIds', [])

    # 차트 정보
    chart_position = None
    chart_type = None
    if chart_info and app_id in chart_info:
        chart_position = chart_info[app_id].get('position')
        chart_type = chart_info[app_id].get('chart_type')

    # 앱 내 구매 정보
    has_iap = None
    if 'inAppPurchases' in app_data:
        has_iap = 1 if app_data.get('inAppPurchases') else 0
    elif 'hasInAppPurchases' in app_data:
        has_iap = 1 if app_data.get('hasInAppPurchases') else 0

    # Game Center 활성화 정보
    game_center_enabled = None
    if 'isGameCenterEnabled' in app_data:
        game_center_enabled = 1 if app_data.get('isGameCenterEnabled') else 0

    return {
        'app_id': app_id,
        'bundle_id': app_data.get('bundleId'),
        'platform': 'app_store',
        'country_code': country_code,

        # 기본 정보
        'title': app_data.get('trackName'),
        'developer': app_data.get('artistName'),
        'developer_id': str(app_data.get('artistId', '')),
        'developer_email': None,  # App Store에서 제공하지 않음
        'developer_website': app_data.get('sellerUrl'),
        'developer_address': None,
        'seller_name': app_data.get('sellerName'),

        # 아이콘 및 이미지
        'icon_url': app_data.get('artworkUrl100'),
        'icon_url_small': app_data.get('artworkUrl60'),
        'icon_url_large': app_data.get('artworkUrl512'),
        'header_image': None,
        'screenshots': json.dumps(screenshots) if screenshots else None,

        # 평점
        'rating': app_data.get('averageUserRating'),
        'rating_count': app_data.get('userRatingCount'),
        'rating_count_current_version': app_data.get('userRatingCountForCurrentVersion'),
        'rating_current_version': app_data.get('averageUserRatingForCurrentVersion'),
        'reviews_count': app_data.get('userRatingCount'),
        'histogram': None,

        # 가격
        'installs': None,  # App Store에서 제공하지 않음
        'installs_min': None,
        'installs_exact': None,
        'price': app_data.get('price'),
        'price_formatted': app_data.get('formattedPrice'),
        'currency': app_data.get('currency'),
        'free': 1 if app_data.get('price', 0) == 0 else 0,

        # 카테고리
        'category': app_data.get('primaryGenreName'),
        'category_id': str(app_data.get('primaryGenreId', '')),
        'genres': json.dumps(genres) if genres else None,
        'genre_ids': json.dumps(genre_ids) if genre_ids else None,

        # 설명
        'description': app_data.get('description'),
        'description_html': None,
        'summary': None,
        'release_notes': app_data.get('releaseNotes'),

        # 날짜
        'release_date': app_data.get('releaseDate'),
        'updated_date': app_data.get('currentVersionReleaseDate'),
        'current_version_release_date': app_data.get('currentVersionReleaseDate'),

        # 버전 및 기술 정보
        'version': app_data.get('version'),
        'minimum_os_version': app_data.get('minimumOsVersion'),
        'file_size': app_data.get('fileSizeBytes'),
        'file_size_formatted': None,
        'supported_devices': json.dumps(app_data.get('supportedDevices', [])),
        'languages': json.dumps(app_data.get('languageCodesISO2A', [])),

        # 콘텐츠 등급
        'content_rating': app_data.get('contentAdvisoryRating') or app_data.get('trackContentRating'),
        'content_rating_description': None,
        'advisories': json.dumps(app_data.get('advisories', [])),

        # 앱 내 구매
        'has_iap': has_iap,
        'iap_price_range': None,
        'contains_ads': None,
        'ad_supported': None,
        'game_center_enabled': game_center_enabled,

        # URL
        'url': app_data.get('trackViewUrl'),
        'store_url': app_data.get('artistViewUrl'),
        'privacy_policy_url': None,

        # 차트 정보
        'chart_position': chart_position,
        'chart_type': chart_type,

        # 기타
        'features': json.dumps(app_data.get('features', [])),
        'permissions': None,
    }


def save_apps_to_db(apps_data):
    """앱 데이터를 데이터베이스에 저장"""
    if not apps_data:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    saved_count = 0

    # 컬럼 목록 (순서 중요)
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
        'has_iap', 'iap_price_range', 'contains_ads', 'ad_supported', 'game_center_enabled',
        'url', 'store_url', 'privacy_policy_url',
        'chart_position', 'chart_type',
        'features', 'permissions',
    ]

    placeholders = ', '.join(['?' for _ in columns])
    columns_str = ', '.join(columns)
    update_columns = [col for col in columns if col not in ('app_id', 'platform', 'country_code')]
    update_assignments = ', '.join([f"{col} = excluded.{col}" for col in update_columns])

    for app_data in apps_data:
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
    특정 국가의 App Store에서 앱 수집
    RSS API로 차트 앱 목록을 가져오고, Lookup API로 상세 정보 수집

    Args:
        country_code: 국가 코드 (예: 'kr', 'us')
        limit: 피드당 가져올 최대 앱 개수 (최대 200)

    Returns:
        수집된 앱 개수
    """
    start_time = datetime.now()
    log_step(f"App Store 수집 [{country_code.upper()}]", "시작", start_time)

    all_app_ids = []
    all_chart_info = {}

    # 1. RSS API로 각 피드에서 앱 ID 수집
    for feed_type in APPLE_RSS_FEEDS:
        print(f"  피드 수집: {feed_type}")
        app_ids, chart_info = fetch_chart_apps(country_code, feed_type, limit)
        print(f"    -> {len(app_ids)}개 앱 발견")

        for app_id in app_ids:
            if app_id not in all_chart_info:
                all_app_ids.append(app_id)
                all_chart_info[app_id] = chart_info.get(app_id, {})

        time.sleep(REQUEST_DELAY)

    if not all_app_ids:
        log_step(f"App Store 수집 [{country_code.upper()}]", "앱 없음", start_time)
        return 0

    print(f"  총 {len(all_app_ids)}개 고유 앱 ID 수집됨")

    # 2. Lookup API로 상세 정보 수집
    print("  상세 정보 수집 중...")
    app_details = fetch_app_details(all_app_ids, country_code)
    print(f"  -> {len(app_details)}개 앱 상세 정보 수집됨")

    # 3. 데이터 파싱 및 저장
    apps_data = []
    for app_id, details in app_details.items():
        parsed = parse_app_store_data(details, country_code, all_chart_info)
        apps_data.append(parsed)

    # 최근 업데이트 순으로 정렬
    apps_data.sort(
        key=lambda x: x.get('updated_date') or x.get('release_date') or '',
        reverse=True
    )

    saved_count = save_apps_to_db(apps_data)
    log_step(f"App Store 수집 [{country_code.upper()}]", f"완료 ({saved_count}개 저장)", start_time)

    return saved_count


def scrape_all_countries():
    """모든 국가의 App Store에서 앱 수집"""
    total_start = datetime.now()
    log_step("App Store 전체 수집", "시작", total_start)

    total_apps = 0
    for country in COUNTRIES:
        try:
            count = scrape_new_apps_by_country(country['code'])
            total_apps += count
            time.sleep(REQUEST_DELAY * 2)  # 국가 간 딜레이
        except Exception as e:
            print(f"  오류 발생 [{country['code']}]: {str(e)}")
            continue

    log_step("App Store 전체 수집", f"완료 (총 {total_apps}개 앱)", total_start)
    return total_apps


if __name__ == "__main__":
    # 단일 국가 테스트
    print("=" * 60)
    print("App Store 스크래퍼 테스트")
    print("=" * 60)
    scrape_new_apps_by_country('kr', limit=50)
