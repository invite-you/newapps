# -*- coding: utf-8 -*-
"""
Sitemap에서 발견된 앱의 상세 정보 수집기
- sitemap_tracking DB에서 새로 발견된 앱 ID 조회
- 각 플랫폼의 API를 통해 상세 정보 수집
- apps DB에 저장 (빈 값은 기존 값 유지)
- 수집 타임스탬프 기록
"""
import sys
import os
import json
import time
from datetime import datetime
from typing import List, Dict, Optional, Set, Any, Tuple

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import REQUEST_DELAY, timing_tracker, get_request_kwargs
from database.db import get_connection as get_apps_connection, log_step
from database.sitemap_db import (
    get_connection as get_sitemap_connection,
    prioritize_for_retry,
    upsert_failed_app_detail,
    clear_failed_app_detail
)

EXISTING_APP_ID_BATCH_SIZE = 899  # 플랫폼 파라미터까지 포함해 변수 개수를 900 이하로 유지

# Google Play Scraper
try:
    from google_play_scraper import app as google_app
    from google_play_scraper.exceptions import NotFoundError as GooglePlayNotFoundError
    GOOGLE_PLAY_AVAILABLE = True
except ImportError:
    GOOGLE_PLAY_AVAILABLE = False
    GooglePlayNotFoundError = None
    print("경고: google-play-scraper 라이브러리가 설치되지 않았습니다.")

# 영구 제외 에러 (재시도 불필요)
PERMANENT_FAILURE_REASONS = frozenset(["not_found_404", "app_removed"])

# App Store - iTunes API
import requests


def get_unfetched_app_ids(platform: str, limit: int = 1000) -> List[Tuple[str, Optional[str]]]:
    """
    상세 정보가 아직 수집되지 않은 앱 ID 목록 반환

    Args:
        platform: 'google_play' 또는 'app_store'
        limit: 최대 개수

    Returns:
        (app_id, country_code) 목록
    """
    # sitemap에서 발견된 앱 ID
    sitemap_conn = get_sitemap_connection()
    sitemap_cursor = sitemap_conn.cursor()

    sitemap_cursor.execute("""
        SELECT app_id, country_code FROM app_discovery
        WHERE platform = ?
        ORDER BY first_seen_at DESC
        LIMIT ?
    """, (platform, limit * 2))

    sitemap_records = [(row['app_id'], row['country_code']) for row in sitemap_cursor.fetchall()]
    sitemap_conn.close()

    if not sitemap_records:
        return []

    # apps DB에 이미 있는 앱 ID (배치 단위 조회로 변수 개수 제한)
    apps_conn = get_apps_connection()
    apps_cursor = apps_conn.cursor()

    db_platform = platform
    # 버그 수정: sitemap_app_ids -> sitemap_records에서 app_id 추출
    sitemap_app_id_list = [app_id for app_id, country_code in sitemap_records]
    existing_app_ids: Set[str] = set()

    for start in range(0, len(sitemap_app_id_list), EXISTING_APP_ID_BATCH_SIZE):
        chunked_ids = sitemap_app_id_list[start:start + EXISTING_APP_ID_BATCH_SIZE]
        placeholders = ','.join(['?' for _ in chunked_ids])
        apps_cursor.execute(f"""
            SELECT DISTINCT app_id FROM apps
            WHERE platform = ? AND app_id IN ({placeholders})
        """, (db_platform, *chunked_ids))
        existing_app_ids.update(row['app_id'] for row in apps_cursor.fetchall())

    apps_conn.close()

    # 차집합: sitemap에는 있지만 apps에는 없는 ID
    unfetched = [
        (app_id, country_code)
        for app_id, country_code in sitemap_records
        if app_id not in existing_app_ids
    ]
    if not unfetched:
        return []

    return prioritize_for_retry(platform, unfetched, limit)


def get_existing_app_data(platform: str, app_id: str) -> Optional[Dict]:
    """기존 앱 데이터 조회 (부분 업데이트용)"""
    conn = get_apps_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM apps WHERE platform = ? AND app_id = ?
    """, (platform, app_id))

    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None


def merge_app_data(existing: Optional[Dict], new_data: Dict) -> Dict:
    """
    기존 데이터와 새 데이터 병합
    - 새 데이터가 None이거나 빈 값이면 기존 값 유지
    - 새 데이터가 유효하면 새 값으로 업데이트
    """
    if not existing:
        return new_data

    merged = existing.copy()

    for key, new_value in new_data.items():
        # 새 값이 유효한 경우에만 업데이트
        if new_value is not None and new_value != '' and new_value != []:
            # JSON 문자열인 경우 빈 배열/객체 체크
            if isinstance(new_value, str):
                try:
                    parsed = json.loads(new_value)
                    if parsed == [] or parsed == {} or parsed == '':
                        continue  # 빈 값이면 기존 값 유지
                except (json.JSONDecodeError, TypeError):
                    pass  # JSON이 아니면 그대로 사용

            merged[key] = new_value

    return merged


def has_significant_changes(existing: Dict, new_data: Dict) -> bool:
    """
    기존 데이터와 새 데이터를 비교하여 유의미한 변경이 있는지 확인
    - 변경이 없으면 False 반환 (DB 업데이트 스킵)
    - 변경이 있으면 True 반환
    """
    # 비교할 주요 필드들 (자주 변경되는 필드 위주)
    compare_fields = [
        'title', 'rating', 'rating_count', 'reviews_count',
        'installs', 'installs_min', 'installs_exact',
        'price', 'version', 'updated_date', 'release_notes',
        'description', 'summary', 'content_rating'
    ]

    for field in compare_fields:
        old_val = existing.get(field)
        new_val = new_data.get(field)

        # 둘 다 None/빈값이면 동일한 것으로 간주
        if (old_val is None or old_val == '') and (new_val is None or new_val == ''):
            continue

        # 숫자 비교 (부동소수점 오차 허용)
        if isinstance(old_val, (int, float)) and isinstance(new_val, (int, float)):
            if abs(old_val - new_val) > 0.001:
                return True
            continue

        # 문자열 비교
        if str(old_val) != str(new_val):
            return True

    return False


def fetch_google_play_details(app_id: str, country_code: str = 'us', lang: str = 'en') -> Tuple[Optional[Dict], Optional[str]]:
    """Google Play 앱 상세 정보 가져오기 (재시도 포함)"""
    if not GOOGLE_PLAY_AVAILABLE:
        return None, "google-play-scraper 라이브러리 미설치"

    attempts = 4  # 최초 요청 + 3회 재시도
    backoff_delays = [1, 3, 7]
    last_error: Optional[Exception] = None

    for attempt in range(attempts):
        try:
            data = google_app(app_id, lang=lang, country=country_code)
            return parse_google_play_data(data, country_code), None
        except GooglePlayNotFoundError:
            # 404 에러: 앱이 존재하지 않음 - 재시도 없이 즉시 영구 제외
            return None, "not_found_404"
        except Exception as e:
            last_error = e
            # 재시도 시에만 로그 출력
            if attempt == attempts - 1:
                log_step("Google Play", f"수집 실패: {app_id} ({e})", "Google Play 상세정보")
            if attempt < attempts - 1:
                delay_index = min(attempt, len(backoff_delays) - 1)
                time.sleep(backoff_delays[delay_index])

    return None, str(last_error) if last_error else None


def parse_google_play_data(app_data: Dict, country_code: str) -> Optional[Dict]:
    """Google Play Scraper 응답을 DB 저장 형식으로 변환"""
    if not app_data:
        return None

    # 수집 타임스탬프 추가
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
        'developer_id': str(app_data.get('developerId', '')) if app_data.get('developerId') else None,
        'developer_email': app_data.get('developerEmail'),
        'developer_website': app_data.get('developerWebsite'),
        'developer_address': app_data.get('developerAddress'),
        'seller_name': app_data.get('developer'),
        'icon_url': app_data.get('icon'),
        'icon_url_small': app_data.get('icon'),
        'icon_url_large': app_data.get('icon'),
        'header_image': app_data.get('headerImage'),
        'screenshots': json.dumps(app_data.get('screenshots', [])) if app_data.get('screenshots') else None,
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
        'price_formatted': str(app_data.get('price', 0)) if app_data.get('price') is not None else None,
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
        # 수집 타임스탬프
        '_collected_at': collected_at,
    }


def fetch_app_store_details_batch(
    app_ids: List[str],
    country_code: str = 'us',
    allow_split: bool = True,
    request_kwargs: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    App Store 앱 상세 정보 배치로 가져오기 (iTunes Lookup API)

    Args:
        app_ids: 앱 ID 목록 (최대 200개)
        country_code: 국가 코드
        allow_split: 재시도 실패 시 50개 단위 분할 재시도 허용 여부

    Returns:
        {
            'results': 앱 정보 딕셔너리 목록,
            'failed_ids': 조회 실패 앱 ID 목록
        }
    """
    if not app_ids:
        return {'results': [], 'failed_ids': []}

    # 200개 제한
    ids_str = ','.join(app_ids[:200])
    url = f"https://itunes.apple.com/lookup?id={ids_str}&country={country_code}"
    max_attempts = 3
    backoff_seconds = 2
    last_error: Optional[str] = None
    prepared_request_kwargs = request_kwargs or get_request_kwargs()

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url, **prepared_request_kwargs)
            response.raise_for_status()
            data = response.json()

            results = []
            fetched_ids: Set[str] = set()
            for item in data.get('results', []):
                parsed = parse_app_store_data(item, country_code)
                if parsed:
                    results.append(parsed)
                    fetched_ids.add(parsed['app_id'])

            failed_ids = [app_id for app_id in app_ids[:200] if str(app_id) not in fetched_ids]
            failure_reasons = {str(app_id): "lookup_not_returned" for app_id in failed_ids}
            return {'results': results, 'failed_ids': failed_ids, 'failure_reasons': failure_reasons}
        except requests.Timeout:
            last_error = "timeout"
        except requests.RequestException as e:
            last_error = str(e)
        except Exception as e:
            last_error = str(e)

        if attempt < max_attempts:
            wait_seconds = backoff_seconds ** (attempt - 1)
            time.sleep(wait_seconds)

    # 최종 실패 시에만 로그
    if allow_split and len(app_ids) > 50:
        aggregated_results: List[Dict] = []
        aggregated_failed: Set[str] = set()
        aggregated_reasons: Dict[str, str] = {}
        for start in range(0, len(app_ids), 50):
            sub_ids = app_ids[start:start + 50]
            sub_result = fetch_app_store_details_batch(
                sub_ids,
                country_code,
                allow_split=False,
                request_kwargs=prepared_request_kwargs
            )
            aggregated_results.extend(sub_result['results'])
            aggregated_failed.update(sub_result['failed_ids'])
            aggregated_reasons.update(sub_result.get('failure_reasons', {}))

        return {
            'results': aggregated_results,
            'failed_ids': list(aggregated_failed),
            'failure_reasons': aggregated_reasons
        }

    failure_reasons = {str(app_id): last_error or "lookup_failed" for app_id in app_ids[:200]}
    return {'results': [], 'failed_ids': app_ids[:200], 'failure_reasons': failure_reasons}


def parse_app_store_data(app_data: Dict, country_code: str) -> Optional[Dict]:
    """iTunes Lookup API 응답을 DB 저장 형식으로 변환"""
    if not app_data or app_data.get('wrapperType') != 'software':
        return None

    # 수집 타임스탬프 추가
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

    all_screenshots = screenshots + ipad_screenshots

    return {
        'app_id': str(app_data.get('trackId')),
        'bundle_id': app_data.get('bundleId'),
        'platform': 'app_store',
        'country_code': country_code,
        'title': app_data.get('trackName'),
        'developer': app_data.get('artistName'),
        'developer_id': str(app_data.get('artistId', '')) if app_data.get('artistId') else None,
        'developer_email': None,
        'developer_website': app_data.get('sellerUrl'),
        'developer_address': None,
        'seller_name': app_data.get('sellerName'),
        'icon_url': app_data.get('artworkUrl100'),
        'icon_url_small': app_data.get('artworkUrl60'),
        'icon_url_large': app_data.get('artworkUrl512'),
        'header_image': None,
        'screenshots': json.dumps(all_screenshots) if all_screenshots else None,
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
        'category_id': str(app_data.get('primaryGenreId', '')) if app_data.get('primaryGenreId') else None,
        'genres': json.dumps(genres) if genres else None,
        'genre_ids': json.dumps(genre_ids) if genre_ids else None,
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
        'supported_devices': json.dumps(supported_devices) if supported_devices else None,
        'languages': json.dumps(languages) if languages else None,
        'content_rating': app_data.get('contentAdvisoryRating'),
        'content_rating_description': app_data.get('trackContentRating'),
        'advisories': json.dumps(app_data.get('advisories', [])) if app_data.get('advisories') else None,
        'has_iap': 1 if app_data.get('isVppDeviceBasedLicensingEnabled') else 0,
        'iap_price_range': None,
        'contains_ads': 0,
        'ad_supported': 0,
        'url': app_data.get('trackViewUrl'),
        'store_url': app_data.get('trackViewUrl'),
        'privacy_policy_url': None,
        'chart_position': None,
        'chart_type': None,
        'features': json.dumps(app_data.get('features', [])) if app_data.get('features') else None,
        'permissions': None,
        # 수집 타임스탬프
        '_collected_at': collected_at,
    }


def save_apps_to_db(apps_data: List[Dict], merge_existing: bool = True) -> Tuple[int, int]:
    """
    앱 데이터를 데이터베이스에 저장 (변경 없으면 스킵, 배치 커밋으로 lock 방지)

    Args:
        apps_data: 앱 데이터 목록
        merge_existing: True면 기존 데이터와 병합 (빈 값 유지)

    Returns:
        (저장된 앱 수, 스킵된 앱 수)
    """
    if not apps_data:
        return 0, 0

    conn = get_apps_connection()
    cursor = conn.cursor()
    saved_count = 0
    skipped_count = 0

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

    # 배치 크기 설정 (50개마다 커밋하여 lock 시간 단축)
    batch_size = 50
    batch_count = 0

    try:
        for app_data in apps_data:
            if not app_data:
                continue

            try:
                # _collected_at 제거 (DB 컬럼에 없음, updated_at으로 대체)
                app_data.pop('_collected_at', None)

                # 기존 데이터 조회
                existing = get_existing_app_data(
                    app_data.get('platform'),
                    app_data.get('app_id')
                )

                if existing:
                    # 변경사항이 없으면 스킵
                    if not has_significant_changes(existing, app_data):
                        skipped_count += 1
                        continue

                    # 기존 데이터와 병합 (빈 값 보존)
                    if merge_existing:
                        app_data = merge_app_data(existing, app_data)

                values = tuple(app_data.get(col) for col in columns)
                cursor.execute(f"""
                    INSERT OR REPLACE INTO apps ({columns_str}, updated_at)
                    VALUES ({placeholders}, CURRENT_TIMESTAMP)
                """, values)
                saved_count += 1
                batch_count += 1

                # 배치 크기마다 중간 커밋 (lock 시간 단축)
                if batch_count >= batch_size:
                    conn.commit()
                    batch_count = 0

            except Exception as e:
                print(f"  저장 실패 [{app_data.get('app_id')}]: {str(e)}")

        # 남은 데이터 커밋
        conn.commit()

    except Exception as e:
        print(f"  [오류] 배치 저장 실패: {e}")
        conn.rollback()
    finally:
        conn.close()

    return saved_count, skipped_count


def fetch_google_play_new_apps(limit: int = 100, country_code: str = 'us') -> Dict:
    """
    Sitemap에서 새로 발견된 Google Play 앱의 상세 정보 수집

    Args:
        limit: 수집할 최대 앱 수
        country_code: 기본 국가 코드

    Returns:
        수집 결과 통계
    """
    timing_tracker.start_task("Google Play 상세정보")
    start_time = datetime.now()

    # 아직 상세 정보가 없는 앱 ID 조회
    unfetched_ids = get_unfetched_app_ids('google_play', limit)
    if not unfetched_ids:
        return {'fetched': 0, 'saved': 0, 'failed': 0}

    log_step("Google Play", f"수집 대상: {len(unfetched_ids)}개", "Google Play 상세정보")

    apps_data = []
    failed = 0

    for i, (app_id, discovered_country) in enumerate(unfetched_ids):
        target_country = discovered_country or country_code
        target_lang = discovered_country or 'en'
        data, error_message = fetch_google_play_details(app_id, target_country, target_lang)
        if data:
            apps_data.append(data)
            clear_failed_app_detail(app_id, 'google_play', target_country)
        else:
            failed += 1
            upsert_failed_app_detail(app_id, 'google_play', target_country, error_message or "unknown_error")

        # 진행 상황 출력 (100개마다)
        if (i + 1) % 100 == 0:
            log_step("Google Play", f"진행: {i+1}/{len(unfetched_ids)}", "Google Play 상세정보")

        time.sleep(REQUEST_DELAY)

    # 저장 (기존 데이터와 병합, 변경 없으면 스킵)
    saved, skipped = save_apps_to_db(apps_data, merge_existing=True)

    log_step("Google Play", f"완료: 저장={saved}, 스킵={skipped}, 실패={failed}", "Google Play 상세정보")

    return {
        'fetched': len(apps_data),
        'saved': saved,
        'skipped': skipped,
        'failed': failed,
        'collected_at': start_time.strftime('%Y-%m-%d %H:%M:%S')
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
    timing_tracker.start_task("App Store 상세정보")
    start_time = datetime.now()

    # 아직 상세 정보가 없는 앱 ID 조회
    unfetched_ids = get_unfetched_app_ids('app_store', limit)
    if not unfetched_ids:
        return {'fetched': 0, 'saved': 0, 'failed': 0}

    log_step("App Store", f"수집 대상: {len(unfetched_ids)}개", "App Store 상세정보")

    apps_data = []
    failed_ids: Set[str] = set()

    # 200개씩 배치 처리
    batch_size = 200
    country_grouped_ids: Dict[str, List[str]] = {}
    for app_id, discovered_country in unfetched_ids:
        target_country = discovered_country or country_code
        country_grouped_ids.setdefault(target_country, []).append(app_id)

    for target_country, grouped_ids in country_grouped_ids.items():
        for i in range(0, len(grouped_ids), batch_size):
            batch = grouped_ids[i:i + batch_size]
            batch_result = fetch_app_store_details_batch(batch, target_country)
            for app in batch_result['results']:
                apps_data.append(app)
                clear_failed_app_detail(app['app_id'], 'app_store', target_country)

            failure_reasons = batch_result.get('failure_reasons', {})
            for failed_id in batch_result['failed_ids']:
                failed_id_str = str(failed_id)
                failed_ids.add(failed_id_str)
                upsert_failed_app_detail(failed_id_str, 'app_store', target_country, failure_reasons.get(failed_id_str, "lookup_failed"))

            time.sleep(REQUEST_DELAY)

    # 저장 (기존 데이터와 병합, 변경 없으면 스킵)
    saved, skipped = save_apps_to_db(apps_data, merge_existing=True)
    failed = len(failed_ids)

    log_step("App Store", f"완료: 저장={saved}, 스킵={skipped}, 실패={failed}", "App Store 상세정보")

    return {
        'fetched': len(apps_data),
        'saved': saved,
        'skipped': skipped,
        'failed': failed,
        'failed_ids': list(failed_ids),
        'collected_at': start_time.strftime('%Y-%m-%d %H:%M:%S')
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
    timing_tracker.start_task("전체 상세정보 수집")
    start_time = datetime.now()

    results = {
        'google_play': fetch_google_play_new_apps(google_limit),
        'app_store': fetch_app_store_new_apps(appstore_limit),
        'collected_at': start_time.strftime('%Y-%m-%d %H:%M:%S')
    }

    # 최종 요약만 출력
    gp = results['google_play']
    ap = results['app_store']
    print(f"\n[수집 완료] Google Play: {gp['saved']}/{gp.get('skipped',0)}/{gp['failed']} | "
          f"App Store: {ap['saved']}/{ap.get('skipped',0)}/{ap['failed']} (저장/스킵/실패)")

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
