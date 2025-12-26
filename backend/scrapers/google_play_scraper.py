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
from google_play_scraper import search, app

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import (
    COUNTRIES, FETCH_LIMIT_PER_COUNTRY, GOOGLE_PLAY_SEARCH_QUERIES,
    REQUEST_DELAY, SSL_VERIFY, get_proxies, timing_tracker,
    normalize_date, normalize_price, normalize_rating, normalize_count, normalize_file_size
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
        app_ids = [r.get('appId') for r in results if r.get('appId')]
        return app_ids
    except Exception as e:
        log_step("Google Play 검색", f"[오류] 검색 실패 (query='{query}', country={country_code}): {str(e)}", "Google Play 검색")
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
        log_step("Google Play 상세", f"[오류] 앱 상세정보 수집 실패 (app_id={app_id}, country={country_code}): {str(e)}", "Google Play 상세")
        return None


def parse_google_play_data(app_data, country_code):
    """
    Google Play Scraper 응답을 DB 저장 형식으로 변환 (정규화 적용)

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

    # 날짜 정규화: updated는 Unix timestamp, released는 지역화된 문자열
    updated_date = normalize_date(app_data.get('updated'))
    release_date = normalize_date(app_data.get('released'))

    # 평점 및 숫자 정규화
    rating = normalize_rating(app_data.get('score'))
    rating_count = normalize_count(app_data.get('ratings'))
    reviews_count = normalize_count(app_data.get('reviews'))
    installs_min = normalize_count(app_data.get('minInstalls'))
    installs_exact = normalize_count(app_data.get('realInstalls'))
    price = normalize_price(app_data.get('price'))
    file_size = normalize_file_size(app_data.get('size'))

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
        'rating': rating,
        'rating_count': rating_count,
        'rating_count_current_version': None,
        'rating_current_version': None,
        'reviews_count': reviews_count,
        'histogram': json.dumps(histogram) if histogram else None,

        # 설치 및 가격
        'installs': app_data.get('installs'),  # 원본 문자열 유지 (예: "100,000,000+")
        'installs_min': installs_min,
        'installs_exact': installs_exact,
        'price': price,
        'price_formatted': str(price) if price is not None else 'Free',
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
        'file_size': file_size,
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
            log_step("Google Play DB", f"[오류] 앱 저장 실패 (app_id={app_data.get('app_id')}): {str(e)}", "Google Play DB")

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
    task_name = f"Google Play 수집 [{country_code.upper()}]"
    start_time = datetime.now()
    timing_tracker.start_task(task_name)
    log_step(task_name, f"수집 시작 (타임스탬프: {start_time.strftime('%Y-%m-%d %H:%M:%S')}, limit={limit})", task_name)

    # 언어 설정 (국가별)
    lang_map = {
        'kr': 'ko', 'jp': 'ja', 'cn': 'zh', 'tw': 'zh',
        'de': 'de', 'fr': 'fr', 'es': 'es', 'it': 'it',
        'br': 'pt', 'ru': 'ru', 'th': 'th', 'vn': 'vi',
        'id': 'id', 'in': 'hi', 'ar': 'ar', 'sa': 'ar',
    }
    lang = lang_map.get(country_code, 'en')
    log_step(task_name, f"  언어 설정: {lang}", task_name)

    all_app_ids = set()

    # 1. 여러 검색어로 앱 ID 수집
    log_step(task_name, f"[1단계] 검색어 {len(GOOGLE_PLAY_SEARCH_QUERIES)}개로 앱 검색 시작", task_name)
    for query in GOOGLE_PLAY_SEARCH_QUERIES:
        app_ids = search_apps(query, country_code, lang=lang)
        before_count = len(all_app_ids)
        all_app_ids.update(app_ids)
        new_count = len(all_app_ids) - before_count
        if new_count > 0:
            log_step(task_name, f"  검색어 '{query}': +{new_count}개 (누적: {len(all_app_ids)}개)", task_name)
        time.sleep(REQUEST_DELAY)

        # 목표 개수 도달 시 중단
        if len(all_app_ids) >= limit:
            log_step(task_name, f"  목표 수량 {limit}개 달성, 검색 종료", task_name)
            break

    if not all_app_ids:
        log_step(task_name, "[결과] 앱 없음 - 검색 결과가 비어있습니다", task_name)
        return 0

    log_step(task_name, f"[1단계 완료] 총 {len(all_app_ids)}개 고유 앱 ID 수집됨", task_name)

    # 2. 각 앱의 상세 정보 수집
    log_step(task_name, f"[2단계] 앱 상세 정보 수집 시작 (최대 {min(limit, len(all_app_ids))}개)", task_name)
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
            log_step(task_name, f"  진행: {collected}개 수집, {failed}개 실패", task_name)

        time.sleep(REQUEST_DELAY)

    log_step(task_name, f"[2단계 완료] {len(apps_data)}개 앱 상세 정보 수집 성공, {failed}개 실패", task_name)

    # 최근 업데이트 순으로 정렬
    apps_data.sort(
        key=lambda x: x.get('updated_date') or x.get('release_date') or '',
        reverse=True
    )

    # 3. 데이터베이스에 저장
    log_step(task_name, f"[3단계] DB에 앱 정보 저장 중...", task_name)
    saved_count = save_apps_to_db(apps_data)
    elapsed_seconds = (datetime.now() - start_time).total_seconds()
    log_step(
        task_name,
        f"[완료] 저장: {saved_count}개 | 수집: {collected}개 | 실패: {failed}개 | 소요시간: {elapsed_seconds:.1f}초",
        task_name
    )

    return saved_count


def scrape_all_countries():
    """모든 국가의 Google Play Store에서 앱 수집"""
    task_name = "Google Play 전체 수집"
    start_time = datetime.now()
    timing_tracker.start_task(task_name)
    log_step(task_name, f"전체 수집 시작 (국가 수: {len(COUNTRIES)}개, 타임스탬프: {start_time.strftime('%Y-%m-%d %H:%M:%S')})", task_name)

    total_apps = 0
    success_countries = 0
    failed_countries = []

    for i, country in enumerate(COUNTRIES, 1):
        try:
            log_step(task_name, f"[{i}/{len(COUNTRIES)}] {country['name']} ({country['code']}) 수집 시작", task_name)
            count = scrape_new_apps_by_country(country['code'])
            total_apps += count
            success_countries += 1
            log_step(task_name, f"[{i}/{len(COUNTRIES)}] {country['name']} 완료: {count}개 앱 저장", task_name)
            time.sleep(REQUEST_DELAY * 2)  # 국가 간 딜레이
        except Exception as e:
            failed_countries.append(country['code'])
            log_step(task_name, f"[오류] {country['name']} ({country['code']}) 수집 실패: {str(e)}", task_name)
            continue

    elapsed_seconds = (datetime.now() - start_time).total_seconds()
    log_step(
        task_name,
        f"[완료] 총 {total_apps}개 앱 저장 | 성공: {success_countries}개국 | 실패: {len(failed_countries)}개국 | 소요시간: {elapsed_seconds:.1f}초",
        task_name
    )
    if failed_countries:
        log_step(task_name, f"  실패 국가 목록: {', '.join(failed_countries)}", task_name)

    return total_apps


if __name__ == "__main__":
    # 단일 국가 테스트
    print("=" * 60)
    print("Google Play 스크래퍼 테스트")
    print("=" * 60)
    scrape_new_apps_by_country('kr', limit=30)
