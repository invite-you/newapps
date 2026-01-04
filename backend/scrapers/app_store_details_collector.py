"""
App Store Details Collector
iTunes Lookup API를 사용하여 앱 상세정보를 수집합니다.
"""
import sys
import os
import time
import requests
import json
from typing import List, Dict, Any, Optional, Set

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.app_details_db import (
    init_database, insert_app, insert_app_localized, insert_app_metrics,
    is_failed_app, mark_app_failed, update_collection_status
)
from database.sitemap_apps_db import get_connection as get_sitemap_connection
from config.language_country_priority import (
    select_best_pairs_for_collection,
    get_primary_country,
    PRIORITY_LANGUAGES
)

PLATFORM = 'app_store'
API_BASE_URL = 'https://itunes.apple.com/lookup'
REQUEST_DELAY = 0.01  # 10ms


class AppStoreDetailsCollector:
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.stats = {
            'apps_processed': 0,
            'apps_skipped_failed': 0,
            'apps_not_found': 0,
            'new_records': 0,
            'unchanged_records': 0,
            'errors': 0
        }

    def log(self, message: str):
        if self.verbose:
            print(f"[AppStore Details] {message}")

    def get_app_language_country_pairs(self, app_id: str) -> List[tuple]:
        """sitemap에서 앱의 (language, country) 쌍을 가져옵니다."""
        conn = get_sitemap_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT language, country FROM app_localizations
            WHERE app_id = ? AND platform = ?
        """, (app_id, PLATFORM))
        pairs = [(row['language'], row['country'].upper()) for row in cursor.fetchall()]
        conn.close()
        return pairs

    def fetch_app_info(self, app_id: str, country: str = 'US') -> Optional[Dict]:
        """iTunes Lookup API로 앱 정보를 가져옵니다."""
        url = f"{API_BASE_URL}?id={app_id}&country={country}"

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data.get('resultCount', 0) > 0:
                return data['results'][0]
            return None

        except requests.exceptions.RequestException as e:
            self.log(f"Error fetching app {app_id}: {e}")
            return None

    def parse_app_metadata(self, data: Dict, app_id: str) -> Dict:
        """API 응답을 apps 테이블 형식으로 변환합니다."""
        features = data.get('features') or []
        has_iap_feature = False
        if isinstance(features, list):
            has_iap_feature = any(
                isinstance(feature, str) and feature.lower() == 'in-app purchases'
                for feature in features
            )

        in_app_purchases = data.get('inAppPurchases')
        if isinstance(in_app_purchases, list):
            has_in_app_purchases_value = len(in_app_purchases) > 0
        else:
            has_in_app_purchases_value = bool(in_app_purchases)

        has_in_app_purchases_flag = data.get('hasInAppPurchases')
        if isinstance(has_in_app_purchases_flag, list):
            has_in_app_purchases_flag_value = len(has_in_app_purchases_flag) > 0
        else:
            has_in_app_purchases_flag_value = bool(has_in_app_purchases_flag)

        has_iap = 1 if any([
            has_iap_feature,
            has_in_app_purchases_value,
            has_in_app_purchases_flag_value
        ]) else 0

        return {
            'app_id': app_id,
            'platform': PLATFORM,
            'bundle_id': data.get('bundleId'),
            'version': data.get('version'),
            'developer': data.get('artistName'),
            'developer_id': str(data.get('artistId', '')),
            'developer_email': None,  # App Store API에서 제공 안 함
            'developer_website': data.get('sellerUrl'),
            'icon_url': data.get('artworkUrl512') or data.get('artworkUrl100'),
            'header_image': None,
            'screenshots': json.dumps(data.get('screenshotUrls', []), ensure_ascii=False),
            'price': data.get('price', 0),
            'currency': data.get('currency'),
            'free': 1 if data.get('price', 0) == 0 else 0,
            'has_iap': has_iap,
            'category_id': str(data.get('primaryGenreId', '')),
            'genre_id': str(data.get('primaryGenreId', '')),
            'content_rating': data.get('contentAdvisoryRating'),
            'content_rating_description': data.get('trackContentRating'),
            'min_os_version': data.get('minimumOsVersion'),
            'file_size': int(data.get('fileSizeBytes', 0)) if data.get('fileSizeBytes') else None,
            'supported_devices': json.dumps(data.get('supportedDevices', [])[:20], ensure_ascii=False),  # 최대 20개
            'release_date': data.get('releaseDate'),
            'updated_date': data.get('currentVersionReleaseDate'),
            'privacy_policy_url': None
        }

    def parse_app_localized(self, data: Dict, app_id: str, language: str) -> Dict:
        """API 응답을 apps_localized 테이블 형식으로 변환합니다."""
        genres = data.get('genres', [])
        return {
            'app_id': app_id,
            'platform': PLATFORM,
            'language': language.lower(),
            'title': data.get('trackName'),
            'summary': None,  # App Store에는 summary 없음
            'description': data.get('description'),
            'release_notes': data.get('releaseNotes'),
            'genre_name': genres[0] if genres else data.get('primaryGenreName')
        }

    def parse_app_metrics(self, data: Dict, app_id: str) -> Dict:
        """API 응답을 apps_metrics 테이블 형식으로 변환합니다."""
        return {
            'app_id': app_id,
            'platform': PLATFORM,
            'score': data.get('averageUserRating'),
            'ratings': data.get('userRatingCount'),
            'reviews_count': data.get('userRatingCount'),  # App Store는 ratings = reviews
            'installs': None,  # App Store에서 제공 안 함
            'installs_exact': None,
            'histogram': None  # App Store에서 제공 안 함
        }

    def collect_app(self, app_id: str) -> bool:
        """단일 앱의 상세정보를 수집합니다."""
        # 실패한 앱인지 확인
        if is_failed_app(app_id, PLATFORM):
            self.stats['apps_skipped_failed'] += 1
            return False

        # 앱의 (language, country) 쌍 가져오기
        pairs = self.get_app_language_country_pairs(app_id)
        if not pairs:
            pairs = [('en', 'US')]  # 기본값

        # 우선순위에 따라 최적의 (language, country) 쌍 선택
        # 각 언어당 가장 적합한 국가를 선택 (예: fr-FR > fr-CA)
        optimized_pairs = select_best_pairs_for_collection(pairs, max_languages=10)

        # 기본 정보 수집용 국가 결정 (US 우선)
        primary_country = 'US'
        for lang, country in optimized_pairs:
            if country.upper() == 'US':
                primary_country = 'US'
                break
        else:
            # US가 없으면 첫 번째 최적화된 쌍의 국가 사용
            if optimized_pairs:
                primary_country = optimized_pairs[0][1].upper()

        data = self.fetch_app_info(app_id, primary_country)

        if not data:
            # 다른 국가로 재시도 (우선순위 순서대로)
            for lang, country in optimized_pairs:
                if country.upper() != primary_country:
                    data = self.fetch_app_info(app_id, country.upper())
                    if data:
                        primary_country = country.upper()
                        break
                    time.sleep(REQUEST_DELAY)

        if not data:
            mark_app_failed(app_id, PLATFORM, 'not_found')
            self.stats['apps_not_found'] += 1
            return False

        # 앱 메타데이터 저장
        app_meta = self.parse_app_metadata(data, app_id)
        is_new, _ = insert_app(app_meta)
        if is_new:
            self.stats['new_records'] += 1
        else:
            self.stats['unchanged_records'] += 1

        # 수치 데이터 저장
        metrics = self.parse_app_metrics(data, app_id)
        insert_app_metrics(metrics)

        # 다국어 데이터 수집 - 우선순위 기반 최적화된 쌍 사용
        # 이제 각 언어별로 최적의 국가가 이미 선택됨
        # (예: 프랑스어는 FR, 스페인어는 MX, 포르투갈어는 BR)
        languages_collected = set()
        fetched_countries = {primary_country: data}  # 이미 가져온 데이터 캐시

        for language, country in optimized_pairs:
            if language in languages_collected:
                continue

            country_upper = country.upper()

            # 해당 국가의 데이터 가져오기 (캐시 활용)
            if country_upper in fetched_countries:
                country_data = fetched_countries[country_upper]
            else:
                country_data = self.fetch_app_info(app_id, country_upper)
                fetched_countries[country_upper] = country_data
                time.sleep(REQUEST_DELAY)

            if country_data:
                localized = self.parse_app_localized(country_data, app_id, language)
                insert_app_localized(localized)
                languages_collected.add(language)

        # 수집 상태 업데이트
        update_collection_status(app_id, PLATFORM, details_collected=True)
        self.stats['apps_processed'] += 1

        return True

    def collect_batch(self, app_ids: List[str]) -> Dict[str, Any]:
        """배치로 앱 상세정보를 수집합니다."""
        self.log(f"Starting batch collection for {len(app_ids)} apps...")

        for i, app_id in enumerate(app_ids, 1):
            if i % 100 == 0:
                self.log(f"Progress: {i}/{len(app_ids)}")

            try:
                self.collect_app(app_id)
            except Exception as e:
                self.log(f"Error processing app {app_id}: {e}")
                self.stats['errors'] += 1

            time.sleep(REQUEST_DELAY)

        self.log(f"Batch collection completed. Stats: {self.stats}")
        return self.stats


def get_apps_to_collect(limit: int = 1000) -> List[str]:
    """수집할 앱 ID 목록을 가져옵니다 (최근 발견 순)."""
    from database.app_details_db import get_connection as get_details_connection

    # 1. 이미 수집된 앱 ID 가져오기 (app_details.db)
    details_conn = get_details_connection()
    cursor = details_conn.cursor()

    cursor.execute("""
        SELECT app_id FROM collection_status
        WHERE platform = 'app_store' AND details_collected_at IS NOT NULL
    """)
    collected_ids = {row['app_id'] for row in cursor.fetchall()}

    cursor.execute("""
        SELECT app_id FROM failed_apps WHERE platform = 'app_store'
    """)
    failed_ids = {row['app_id'] for row in cursor.fetchall()}

    details_conn.close()

    exclude_ids = collected_ids | failed_ids

    # 2. sitemap에서 수집할 앱 목록 가져오기 (sitemap_apps.db)
    sitemap_conn = get_sitemap_connection()
    cursor = sitemap_conn.cursor()

    cursor.execute("""
        SELECT DISTINCT app_id
        FROM app_localizations
        WHERE platform = 'app_store'
        ORDER BY first_seen_at DESC
    """)

    app_ids = []
    for row in cursor.fetchall():
        if row['app_id'] not in exclude_ids:
            app_ids.append(row['app_id'])
            if len(app_ids) >= limit:
                break

    sitemap_conn.close()
    return app_ids


def main():
    init_database()

    # 수집할 앱 목록 가져오기
    app_ids = get_apps_to_collect(limit=10)  # 테스트용 10개
    print(f"Found {len(app_ids)} apps to collect")

    if app_ids:
        collector = AppStoreDetailsCollector(verbose=True)
        stats = collector.collect_batch(app_ids)
        print(f"\nFinal Stats: {stats}")


if __name__ == '__main__':
    main()
