"""
Play Store Details Collector
google-play-scraper 라이브러리를 사용하여 앱 상세정보를 수집합니다.
"""
import sys
import os
import time
import json
from typing import List, Dict, Any, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google_play_scraper import app
from google_play_scraper.exceptions import NotFoundError

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

PLATFORM = 'play_store'
REQUEST_DELAY = 0.01  # 10ms


class PlayStoreDetailsCollector:
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
            print(f"[PlayStore Details] {message}")

    def get_app_language_country_pairs(self, app_id: str) -> List[tuple]:
        """sitemap에서 앱의 (language, country) 쌍을 가져옵니다."""
        conn = get_sitemap_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT language, country FROM app_localizations
            WHERE app_id = ? AND platform = ?
        """, (app_id, PLATFORM))
        pairs = [(row['language'], row['country'].lower()) for row in cursor.fetchall()]
        conn.close()
        return pairs if pairs else [('en', 'us')]

    def fetch_app_info(self, app_id: str, lang: str = 'en', country: str = 'us') -> Optional[Dict]:
        """google-play-scraper로 앱 정보를 가져옵니다."""
        try:
            result = app(app_id, lang=lang, country=country)
            return result
        except NotFoundError:
            return None
        except Exception as e:
            self.log(f"Error fetching app {app_id}: {e}")
            return None

    def parse_app_metadata(self, data: Dict, app_id: str) -> Dict:
        """API 응답을 apps 테이블 형식으로 변환합니다."""
        return {
            'app_id': app_id,
            'platform': PLATFORM,
            'bundle_id': data.get('appId'),
            'version': data.get('version'),
            'developer': data.get('developer'),
            'developer_id': data.get('developerId'),
            'developer_email': data.get('developerEmail'),
            'developer_website': data.get('developerWebsite'),
            'icon_url': data.get('icon'),
            'header_image': data.get('headerImage'),
            'screenshots': json.dumps(data.get('screenshots', [])[:10], ensure_ascii=False),  # 최대 10개
            'price': data.get('price', 0),
            'currency': data.get('currency'),
            'free': 1 if data.get('free', True) else 0,
            'has_iap': 1 if data.get('offersIAP') else 0,
            'category_id': data.get('genreId'),
            'genre_id': data.get('genreId'),
            'content_rating': data.get('contentRating'),
            'content_rating_description': data.get('contentRatingDescription'),
            'min_os_version': None,  # Play Store API에서 직접 제공 안 함
            'file_size': None,
            'supported_devices': None,
            'release_date': data.get('released'),
            'updated_date': data.get('lastUpdatedOn'),
            'privacy_policy_url': data.get('privacyPolicy')
        }

    def parse_app_localized(self, data: Dict, app_id: str, language: str) -> Dict:
        """API 응답을 apps_localized 테이블 형식으로 변환합니다."""
        return {
            'app_id': app_id,
            'platform': PLATFORM,
            'language': language.lower(),
            'title': data.get('title'),
            'summary': data.get('summary'),
            'description': data.get('description'),
            'release_notes': data.get('recentChanges'),
            'genre_name': data.get('genre')
        }

    def parse_app_metrics(self, data: Dict, app_id: str) -> Dict:
        """API 응답을 apps_metrics 테이블 형식으로 변환합니다."""
        histogram = data.get('histogram')
        return {
            'app_id': app_id,
            'platform': PLATFORM,
            'score': data.get('score'),
            'ratings': data.get('ratings'),
            'reviews_count': data.get('reviews'),
            'installs': data.get('installs'),
            'installs_exact': data.get('realInstalls'),
            'histogram': json.dumps(histogram, ensure_ascii=False) if histogram else None
        }

    def collect_app(self, app_id: str) -> bool:
        """단일 앱의 상세정보를 수집합니다."""
        # 실패한 앱인지 확인
        if is_failed_app(app_id, PLATFORM):
            self.stats['apps_skipped_failed'] += 1
            return False

        # sitemap에서 (language, country) 쌍 가져오기
        pairs = self.get_app_language_country_pairs(app_id)
        if not pairs:
            pairs = [('en', 'us')]

        # 우선순위에 따라 최적의 (language, country) 쌍 선택
        # 각 언어당 가장 적합한 국가를 선택 (예: fr-FR > fr-CA)
        optimized_pairs = select_best_pairs_for_collection(pairs, max_languages=10)

        # 기본 정보 수집용 쌍 결정 (영어 US 우선)
        primary_pair = None
        for lang, country in optimized_pairs:
            if lang == 'en' and country.upper() == 'US':
                primary_pair = (lang, country)
                break
        if not primary_pair:
            for lang, country in optimized_pairs:
                if lang == 'en':
                    primary_pair = (lang, country)
                    break
        if not primary_pair:
            primary_pair = optimized_pairs[0] if optimized_pairs else ('en', 'us')

        primary_lang, primary_country = primary_pair
        data = self.fetch_app_info(app_id, lang=primary_lang, country=primary_country.lower())

        if not data:
            # 다른 쌍으로 재시도 (우선순위 순서대로)
            for lang, country in optimized_pairs:
                if (lang, country) != primary_pair:
                    data = self.fetch_app_info(app_id, lang=lang, country=country.lower())
                    if data:
                        primary_pair = (lang, country)
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
        fetched_pairs = {primary_pair: data}  # 이미 가져온 데이터 캐시

        for lang, country in optimized_pairs:
            if lang in languages_collected:
                continue

            # 해당 쌍의 데이터 가져오기 (캐시 활용)
            if (lang, country) in fetched_pairs:
                pair_data = fetched_pairs[(lang, country)]
            else:
                pair_data = self.fetch_app_info(app_id, lang=lang, country=country.lower())
                fetched_pairs[(lang, country)] = pair_data
                time.sleep(REQUEST_DELAY)

            if pair_data:
                localized = self.parse_app_localized(pair_data, app_id, lang)
                insert_app_localized(localized)
                languages_collected.add(lang)

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

    # 1. 이미 수집된 앱 ID 가져오기
    details_conn = get_details_connection()
    cursor = details_conn.cursor()

    cursor.execute("""
        SELECT app_id FROM collection_status
        WHERE platform = 'play_store' AND details_collected_at IS NOT NULL
    """)
    collected_ids = {row['app_id'] for row in cursor.fetchall()}

    cursor.execute("""
        SELECT app_id FROM failed_apps WHERE platform = 'play_store'
    """)
    failed_ids = {row['app_id'] for row in cursor.fetchall()}

    details_conn.close()

    exclude_ids = collected_ids | failed_ids

    # 2. sitemap에서 수집할 앱 목록 가져오기
    sitemap_conn = get_sitemap_connection()
    cursor = sitemap_conn.cursor()

    cursor.execute("""
        SELECT DISTINCT app_id
        FROM app_localizations
        WHERE platform = 'play_store'
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

    # 수집할 앱 목록
    app_ids = get_apps_to_collect(limit=10)
    print(f"Found {len(app_ids)} apps to collect")

    if app_ids:
        collector = PlayStoreDetailsCollector(verbose=True)
        stats = collector.collect_batch(app_ids)
        print(f"\nFinal Stats: {stats}")


if __name__ == '__main__':
    main()
