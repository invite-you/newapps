# -*- coding: utf-8 -*-
"""
Sitemap에서 발견된 앱의 상세 정보 수집기
- sitemap_tracking DB에서 새로 발견된 앱 ID 조회
- 각 플랫폼의 API를 통해 상세 정보 수집
- apps DB에 저장
"""
import sys
import os
import json
import time
from datetime import datetime
from typing import List, Dict, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import (
    LOG_FORMAT, REQUEST_DELAY, COUNTRIES
)
from database.db import get_connection as get_apps_connection
from database.sitemap_db import (
    get_connection as get_sitemap_connection,
    get_recently_discovered_apps, get_discovery_stats
)

# Google Play Scraper
try:
    from google_play_scraper import app as google_app
    GOOGLE_PLAY_AVAILABLE = True
except ImportError:
    GOOGLE_PLAY_AVAILABLE = False
    print("경고: google-play-scraper 라이브러리가 설치되지 않았습니다.")

# App Store - iTunes API
import requests


def log_step(step: str, message: str, start_time: Optional[datetime] = None):
    """타임스탬프 로그 출력"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    duration = (datetime.now() - start_time).total_seconds() if start_time else 0
    print(LOG_FORMAT.format(
        timestamp=timestamp,
        step=step,
        message=message,
        duration=f"{duration:.2f}"
    ))


def get_unfetched_app_ids(platform: str, limit: int = 1000) -> List[str]:
    """
    상세 정보가 아직 수집되지 않은 앱 ID 목록 반환

    Args:
        platform: 'google_play' 또는 'app_store'
        limit: 최대 개수

    Returns:
        앱 ID 목록
    """
    # sitemap에서 발견된 앱 ID
    sitemap_conn = get_sitemap_connection()
    sitemap_cursor = sitemap_conn.cursor()

    sitemap_cursor.execute("""
        SELECT app_id FROM app_discovery
        WHERE platform = ?
        ORDER BY first_seen_at DESC
        LIMIT ?
    """, (platform, limit * 2))

    sitemap_app_ids = {row['app_id'] for row in sitemap_cursor.fetchall()}
    sitemap_conn.close()

    if not sitemap_app_ids:
        return []

    # apps DB에 이미 있는 앱 ID
    apps_conn = get_apps_connection()
    apps_cursor = apps_conn.cursor()

    # 플랫폼 이름 매핑
    db_platform = platform

    placeholders = ','.join(['?' for _ in sitemap_app_ids])
    apps_cursor.execute(f"""
        SELECT DISTINCT app_id FROM apps
        WHERE platform = ? AND app_id IN ({placeholders})
    """, (db_platform, *sitemap_app_ids))

    existing_app_ids = {row['app_id'] for row in apps_cursor.fetchall()}
    apps_conn.close()

    # 차집합: sitemap에는 있지만 apps에는 없는 ID
    unfetched = sitemap_app_ids - existing_app_ids

    return list(unfetched)[:limit]


def fetch_google_play_details(app_id: str, country_code: str = 'us', lang: str = 'en') -> Optional[Dict]:
    """Google Play 앱 상세 정보 가져오기"""
    if not GOOGLE_PLAY_AVAILABLE:
        return None

    try:
        data = google_app(app_id, lang=lang, country=country_code)
        return parse_google_play_data(data, country_code)
    except Exception as e:
        # 앱이 삭제되었거나 접근 불가
        return None


def parse_google_play_data(app_data: Dict, country_code: str) -> Optional[Dict]:
    """Google Play Scraper 응답을 DB 저장 형식으로 변환"""
    if not app_data:
        return None

    categories = app_data.get('categories', [])
    category_names = [c.get('name') for c in categories if c.get('name')]
    category_ids = [c.get('id') for c in categories if c.get('id')]

    histogram = app_data.get('histogram')

    updated = app_data.get('updated')
    updated_date = None
    if updated:
        if isinstance(updated, (int, float)):
            try:
                updated_date = datetime.fromtimestamp(updated).isoformat()
            except (ValueError, OSError):
                updated_date = None
        else:
            updated_date = str(updated)

    released = app_data.get('released')
    release_date = str(released) if released else None

    return {
        'app_id': app_data.get('appId'),
        'bundle_id': app_data.get('appId'),
        'platform': 'google_play',
        'country_code': country_code,
        'title': app_data.get('title'),
        'developer': app_data.get('developer'),
        'developer_id': str(app_data.get('developerId', '')),
        'developer_email': app_data.get('developerEmail'),
        'developer_website': app_data.get('developerWebsite'),
        'developer_address': app_data.get('developerAddress'),
        'seller_name': app_data.get('developer'),
        'icon_url': app_data.get('icon'),
        'icon_url_small': app_data.get('icon'),
        'icon_url_large': app_data.get('icon'),
        'header_image': app_data.get('headerImage'),
        'screenshots': json.dumps(app_data.get('screenshots', [])),
        'rating': app_data.get('score'),
        'rating_count': app_data.get('ratings'),
        'rating_count_current_version': None,
        'rating_current_version': None,
        'reviews_count': app_data.get('reviews'),
        'histogram': json.dumps(histogram) if histogram else None,
        'installs': app_data.get('installs'),
        'installs_min': app_data.get('minInstalls'),
        'installs_exact': app_data.get('realInstalls'),
        'price': app_data.get('price'),
        'price_formatted': str(app_data.get('price', 0)) if app_data.get('price') else 'Free',
        'currency': app_data.get('currency'),
        'free': 1 if app_data.get('free', True) else 0,
        'category': app_data.get('genre'),
        'category_id': app_data.get('genreId'),
        'genres': json.dumps(category_names) if category_names else None,
        'genre_ids': json.dumps(category_ids) if category_ids else None,
        'description': app_data.get('description'),
        'description_html': app_data.get('descriptionHTML'),
        'summary': app_data.get('summary'),
        'release_notes': app_data.get('recentChanges'),
        'release_date': release_date,
        'updated_date': updated_date,
        'current_version_release_date': updated_date,
        'version': app_data.get('version'),
        'minimum_os_version': app_data.get('androidVersion'),
        'file_size': None,
        'file_size_formatted': app_data.get('size'),
        'supported_devices': None,
        'languages': None,
        'content_rating': app_data.get('contentRating'),
        'content_rating_description': app_data.get('contentRatingDescription'),
        'advisories': None,
        'has_iap': 1 if app_data.get('offersIAP') else 0,
        'iap_price_range': app_data.get('inAppProductPrice'),
        'contains_ads': 1 if app_data.get('containsAds') else 0,
        'ad_supported': 1 if app_data.get('adSupported') else 0,
        'url': app_data.get('url'),
        'store_url': app_data.get('url'),
        'privacy_policy_url': app_data.get('privacyPolicy'),
        'chart_position': None,
        'chart_type': None,
        'features': None,
        'permissions': None,
    }


def fetch_app_store_details_batch(app_ids: List[str], country_code: str = 'us') -> List[Dict]:
    """
    App Store 앱 상세 정보 배치로 가져오기 (iTunes Lookup API)

    Args:
        app_ids: 앱 ID 목록 (최대 200개)
        country_code: 국가 코드

    Returns:
        앱 정보 딕셔너리 목록
    """
    if not app_ids:
        return []

    # 200개 제한
    ids_str = ','.join(app_ids[:200])
    url = f"https://itunes.apple.com/lookup?id={ids_str}&country={country_code}"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        results = []
        for item in data.get('results', []):
            parsed = parse_app_store_data(item, country_code)
            if parsed:
                results.append(parsed)

        return results

    except Exception as e:
        log_step("App Store", f"배치 조회 실패: {e}", datetime.now())
        return []


def parse_app_store_data(app_data: Dict, country_code: str) -> Optional[Dict]:
    """iTunes Lookup API 응답을 DB 저장 형식으로 변환"""
    if not app_data or app_data.get('wrapperType') != 'software':
        return None

    # 스크린샷 URL들
    screenshots = app_data.get('screenshotUrls', [])
    ipad_screenshots = app_data.get('ipadScreenshotUrls', [])

    # 장르
    genres = app_data.get('genres', [])
    genre_ids = app_data.get('genreIds', [])

    # 지원 기기
    supported_devices = app_data.get('supportedDevices', [])

    # 언어
    languages = app_data.get('languageCodesISO2A', [])

    # 날짜 파싱
    release_date = app_data.get('releaseDate')
    current_version_release_date = app_data.get('currentVersionReleaseDate')

    return {
        'app_id': str(app_data.get('trackId')),
        'bundle_id': app_data.get('bundleId'),
        'platform': 'app_store',
        'country_code': country_code,
        'title': app_data.get('trackName'),
        'developer': app_data.get('artistName'),
        'developer_id': str(app_data.get('artistId', '')),
        'developer_email': None,
        'developer_website': app_data.get('sellerUrl'),
        'developer_address': None,
        'seller_name': app_data.get('sellerName'),
        'icon_url': app_data.get('artworkUrl100'),
        'icon_url_small': app_data.get('artworkUrl60'),
        'icon_url_large': app_data.get('artworkUrl512'),
        'header_image': None,
        'screenshots': json.dumps(screenshots + ipad_screenshots),
        'rating': app_data.get('averageUserRating'),
        'rating_count': app_data.get('userRatingCount'),
        'rating_count_current_version': app_data.get('userRatingCountForCurrentVersion'),
        'rating_current_version': app_data.get('averageUserRatingForCurrentVersion'),
        'reviews_count': app_data.get('userRatingCount'),
        'histogram': None,
        'installs': None,
        'installs_min': None,
        'installs_exact': None,
        'price': app_data.get('price'),
        'price_formatted': app_data.get('formattedPrice'),
        'currency': app_data.get('currency'),
        'free': 1 if app_data.get('price', 0) == 0 else 0,
        'category': app_data.get('primaryGenreName'),
        'category_id': str(app_data.get('primaryGenreId', '')),
        'genres': json.dumps(genres),
        'genre_ids': json.dumps(genre_ids),
        'description': app_data.get('description'),
        'description_html': None,
        'summary': None,
        'release_notes': app_data.get('releaseNotes'),
        'release_date': release_date,
        'updated_date': current_version_release_date,
        'current_version_release_date': current_version_release_date,
        'version': app_data.get('version'),
        'minimum_os_version': app_data.get('minimumOsVersion'),
        'file_size': app_data.get('fileSizeBytes'),
        'file_size_formatted': None,
        'supported_devices': json.dumps(supported_devices),
        'languages': json.dumps(languages),
        'content_rating': app_data.get('contentAdvisoryRating'),
        'content_rating_description': app_data.get('trackContentRating'),
        'advisories': json.dumps(app_data.get('advisories', [])),
        'has_iap': 1 if app_data.get('isVppDeviceBasedLicensingEnabled') else 0,
        'iap_price_range': None,
        'contains_ads': 0,
        'ad_supported': 0,
        'url': app_data.get('trackViewUrl'),
        'store_url': app_data.get('trackViewUrl'),
        'privacy_policy_url': None,
        'chart_position': None,
        'chart_type': None,
        'features': json.dumps(app_data.get('features', [])),
        'permissions': None,
    }


def save_apps_to_db(apps_data: List[Dict]) -> int:
    """앱 데이터를 데이터베이스에 저장"""
    if not apps_data:
        return 0

    conn = get_apps_connection()
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

    for app_data in apps_data:
        if not app_data:
            continue
        try:
            values = tuple(app_data.get(col) for col in columns)
            cursor.execute(f"""
                INSERT OR REPLACE INTO apps ({columns_str}, updated_at)
                VALUES ({placeholders}, CURRENT_TIMESTAMP)
            """, values)
            saved_count += 1
        except Exception as e:
            print(f"  저장 실패 [{app_data.get('app_id')}]: {str(e)}")

    conn.commit()
    conn.close()
    return saved_count


def fetch_google_play_new_apps(limit: int = 100, country_code: str = 'us') -> Dict:
    """
    Sitemap에서 새로 발견된 Google Play 앱의 상세 정보 수집

    Args:
        limit: 수집할 최대 앱 수
        country_code: 기본 국가 코드

    Returns:
        수집 결과 통계
    """
    start_time = datetime.now()
    log_step("Google Play 상세정보", "수집 시작", start_time)

    # 아직 상세 정보가 없는 앱 ID 조회
    unfetched_ids = get_unfetched_app_ids('google_play', limit)
    log_step("Google Play", f"미수집 앱: {len(unfetched_ids)}개", datetime.now())

    if not unfetched_ids:
        log_step("Google Play 상세정보", "수집할 앱 없음", start_time)
        return {'fetched': 0, 'saved': 0, 'failed': 0}

    apps_data = []
    failed = 0

    for i, app_id in enumerate(unfetched_ids):
        data = fetch_google_play_details(app_id, country_code)
        if data:
            apps_data.append(data)
        else:
            failed += 1

        # 진행 상황 출력
        if (i + 1) % 50 == 0:
            log_step("Google Play", f"진행: {i+1}/{len(unfetched_ids)}, 성공: {len(apps_data)}", datetime.now())

        time.sleep(REQUEST_DELAY)

    # 저장
    saved = save_apps_to_db(apps_data)

    log_step("Google Play 상세정보", f"완료: {saved}개 저장, {failed}개 실패", start_time)

    return {
        'fetched': len(apps_data),
        'saved': saved,
        'failed': failed
    }


def fetch_app_store_new_apps(limit: int = 500, country_code: str = 'us') -> Dict:
    """
    Sitemap에서 새로 발견된 App Store 앱의 상세 정보 수집

    Args:
        limit: 수집할 최대 앱 수
        country_code: 기본 국가 코드

    Returns:
        수집 결과 통계
    """
    start_time = datetime.now()
    log_step("App Store 상세정보", "수집 시작", start_time)

    # 아직 상세 정보가 없는 앱 ID 조회
    unfetched_ids = get_unfetched_app_ids('app_store', limit)
    log_step("App Store", f"미수집 앱: {len(unfetched_ids)}개", datetime.now())

    if not unfetched_ids:
        log_step("App Store 상세정보", "수집할 앱 없음", start_time)
        return {'fetched': 0, 'saved': 0, 'failed': 0}

    apps_data = []

    # 200개씩 배치 처리
    batch_size = 200
    for i in range(0, len(unfetched_ids), batch_size):
        batch = unfetched_ids[i:i + batch_size]
        results = fetch_app_store_details_batch(batch, country_code)
        apps_data.extend(results)

        log_step("App Store", f"배치 {i//batch_size + 1}: {len(results)}개 수집", datetime.now())
        time.sleep(REQUEST_DELAY)

    # 저장
    saved = save_apps_to_db(apps_data)
    failed = len(unfetched_ids) - len(apps_data)

    log_step("App Store 상세정보", f"완료: {saved}개 저장, {failed}개 실패", start_time)

    return {
        'fetched': len(apps_data),
        'saved': saved,
        'failed': failed
    }


def fetch_all_new_app_details(google_limit: int = 100, appstore_limit: int = 500) -> Dict:
    """
    모든 플랫폼에서 새로 발견된 앱의 상세 정보 수집

    Args:
        google_limit: Google Play 앱 수집 제한
        appstore_limit: App Store 앱 수집 제한

    Returns:
        전체 수집 결과
    """
    start_time = datetime.now()
    log_step("전체 상세정보 수집", "시작", start_time)

    results = {
        'google_play': fetch_google_play_new_apps(google_limit),
        'app_store': fetch_app_store_new_apps(appstore_limit)
    }

    # 요약
    print("\n" + "=" * 60)
    print("상세 정보 수집 결과")
    print("=" * 60)
    print(f"Google Play: {results['google_play']['saved']}개 저장, "
          f"{results['google_play']['failed']}개 실패")
    print(f"App Store: {results['app_store']['saved']}개 저장, "
          f"{results['app_store']['failed']}개 실패")
    print("=" * 60)

    log_step("전체 상세정보 수집", "완료", start_time)

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Sitemap 기반 앱 상세 정보 수집')
    parser.add_argument('--google-limit', type=int, default=50,
                        help='Google Play 앱 수집 제한 (기본: 50)')
    parser.add_argument('--appstore-limit', type=int, default=200,
                        help='App Store 앱 수집 제한 (기본: 200)')
    parser.add_argument('--google-only', action='store_true',
                        help='Google Play만 수집')
    parser.add_argument('--appstore-only', action='store_true',
                        help='App Store만 수집')

    args = parser.parse_args()

    if args.google_only:
        result = fetch_google_play_new_apps(args.google_limit)
        print(f"\n결과: {result}")
    elif args.appstore_only:
        result = fetch_app_store_new_apps(args.appstore_limit)
        print(f"\n결과: {result}")
    else:
        results = fetch_all_new_app_details(args.google_limit, args.appstore_limit)
