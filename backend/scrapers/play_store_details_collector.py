"""
Play Store Details Collector
google-play-scraper 라이브러리를 사용하여 앱 상세정보를 수집합니다.
"""
import sys
import os
import time
from datetime import datetime
import json
from typing import List, Dict, Any, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google_play_scraper import app
from google_play_scraper.exceptions import NotFoundError
from requests.exceptions import Timeout, ConnectionError as RequestsConnectionError

from database.app_details_db import (
    init_database, insert_app, insert_app_localized, insert_app_metrics,
    is_failed_app, mark_app_failed, update_collection_status,
    get_failed_app_ids, get_abandoned_apps_to_skip, normalize_date_format
)
from database.sitemap_apps_db import (
    get_connection as get_sitemap_connection,
    release_connection as release_sitemap_connection,
)
from config.language_country_priority import select_best_pairs_for_collection
from scrapers.collection_utils import (
    get_app_language_country_pairs,
    select_primary_pair
)
from utils.logger import get_collection_logger, get_timestamped_logger
from utils.error_tracker import ErrorTracker, ErrorStep

PLATFORM = 'play_store'
REQUEST_DELAY = 0.01  # 10ms


class PlayStoreDetailsCollector:
    def __init__(self, verbose: bool = True, error_tracker: Optional[ErrorTracker] = None):
        self.verbose = verbose
        self.logger = get_collection_logger('PlayStoreDetails', verbose)
        self.error_tracker = error_tracker or ErrorTracker('play_store_details')
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
            self.logger.info(message)

    def fetch_app_info(self, app_id: str, lang: str = 'en', country: str = 'us') -> Tuple[Optional[Dict], Optional[str]]:
        """google-play-scraper로 앱 정보를 가져옵니다.

        Returns:
            Tuple of (data, error_reason)
            - data: 앱 정보 또는 None
            - error_reason: 실패 시 사유 (not_found, timeout, network_error, rate_limited, server_error, scraper_error)
        """
        try:
            result = app(app_id, lang=lang, country=country)
            return (result, None)
        except NotFoundError:
            return (None, 'not_found')
        except Timeout:
            self.log(f"Timeout fetching app {app_id}")
            return (None, 'timeout')
        except RequestsConnectionError as e:
            self.log(f"Network error fetching app {app_id}: {e}")
            return (None, 'network_error')
        except Exception as e:
            error_str = str(e).lower()
            # rate limit 감지
            if '429' in error_str or 'too many' in error_str or 'rate' in error_str:
                self.log(f"Rate limited fetching app {app_id}: {e}")
                return (None, 'rate_limited')
            # 서버 오류 감지
            if any(code in error_str for code in ['500', '502', '503', '504']):
                self.log(f"Server error fetching app {app_id}: {e}")
                return (None, 'server_error')
            # 기타 스크래퍼 오류
            self.log(f"Scraper error fetching app {app_id}: {e}")
            return (None, f'scraper_error:{type(e).__name__}')

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
            'genre_name': data.get('genre'),
            'content_rating': data.get('contentRating'),
            'content_rating_description': data.get('contentRatingDescription'),
            'min_os_version': None,  # Play Store API에서 직접 제공 안 함
            'file_size': None,
            'supported_devices': None,
            'release_date': normalize_date_format(data.get('released')),
            'updated_date': normalize_date_format(data.get('lastUpdatedOn')),
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
            'release_notes': data.get('recentChanges')
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
        self.logger.info(f"[APP START] app_id={app_id} | {datetime.now().isoformat()}")
        # 실패한 앱인지 확인
        if is_failed_app(app_id, PLATFORM):
            self.stats['apps_skipped_failed'] += 1
            self.logger.info(f"[APP SKIP] app_id={app_id} | status=failed_app")
            return False

        # sitemap에서 (language, country) 쌍 가져오기
        pairs = get_app_language_country_pairs(
            app_id,
            PLATFORM,
            normalize_country_case="lower",
            default_pair=("en", "us")
        )

        # 우선순위에 따라 최적의 (language, country) 쌍 선택
        # 각 언어당 가장 적합한 국가를 선택 (예: fr-FR > fr-CA)
        optimized_pairs = select_best_pairs_for_collection(pairs, max_languages=10)

        # 기본 정보 수집용 쌍 결정 (영어 US 우선)
        primary_pair = select_primary_pair(
            optimized_pairs,
            preferred_language="en",
            preferred_country="us"
        )

        primary_lang, primary_country = primary_pair
        data, last_error = self.fetch_app_info(app_id, lang=primary_lang, country=primary_country.lower())

        if not data:
            # 다른 쌍으로 재시도 (우선순위 순서대로)
            for lang, country in optimized_pairs:
                if (lang, country) != primary_pair:
                    data, error = self.fetch_app_info(app_id, lang=lang, country=country.lower())
                    if data:
                        primary_pair = (lang, country)
                        last_error = None
                        break
                    last_error = error  # 마지막 에러 사유 유지
                    time.sleep(REQUEST_DELAY)

        if not data:
            # 상세한 에러 사유로 기록
            reason = last_error or 'unknown'
            mark_app_failed(app_id, PLATFORM, reason)
            self.stats['apps_not_found'] += 1
            self.logger.info(f"[APP FAIL] app_id={app_id} | reason={reason}")
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
        # 최적화: title+description이 기준 언어와 동일하면 저장하지 않음
        languages_collected = set()
        fetched_pairs = {primary_pair: data}  # 이미 가져온 데이터 캐시

        # 기준 언어 데이터 먼저 저장
        base_localized = self.parse_app_localized(data, app_id, primary_pair[0])
        base_title = base_localized.get('title')
        base_description = base_localized.get('description')
        insert_app_localized(base_localized)
        languages_collected.add(primary_pair[0])

        for lang, country in optimized_pairs:
            if lang in languages_collected:
                continue

            # 해당 쌍의 데이터 가져오기 (캐시 활용)
            if (lang, country) in fetched_pairs:
                pair_data = fetched_pairs[(lang, country)]
            else:
                pair_data, _ = self.fetch_app_info(app_id, lang=lang, country=country.lower())
                fetched_pairs[(lang, country)] = pair_data
                time.sleep(REQUEST_DELAY)

            if pair_data:
                localized = self.parse_app_localized(pair_data, app_id, lang)
                # 중복 체크: title과 description이 기준 언어와 다를 때만 저장
                if localized.get('title') != base_title or localized.get('description') != base_description:
                    insert_app_localized(localized)
                languages_collected.add(lang)

        # 수집 상태 업데이트
        update_collection_status(app_id, PLATFORM, details_collected=True)
        self.stats['apps_processed'] += 1
        self.logger.info(f"[APP END] app_id={app_id} | status=OK")

        return True

    def collect_batch(self, app_ids: List[str]) -> Dict[str, Any]:
        """배치로 앱 상세정보를 수집합니다."""
        start_ts = datetime.now().isoformat()
        start_perf = time.perf_counter()
        self.log(f"Starting batch collection for {len(app_ids)} apps...")
        self.logger.info(f"[STEP START] collect_batch | {start_ts}")

        for i, app_id in enumerate(app_ids, 1):
            self.logger.info(f"[PROGRESS] {i}/{len(app_ids)} | app_id={app_id}")

            try:
                self.collect_app(app_id)
            except Exception as e:
                self.log(f"Error processing app {app_id}: {e}")
                self.stats['errors'] += 1
                # 상세 에러 추적
                self.error_tracker.add_error(
                    platform=PLATFORM,
                    step=ErrorStep.COLLECT_APP,
                    error=e,
                    app_id=app_id,
                    include_traceback=True
                )
                self.logger.exception(f"[APP ERROR] app_id={app_id}")

            time.sleep(REQUEST_DELAY)

        self.log(f"Batch collection completed. Stats: {self.stats}")
        elapsed = time.perf_counter() - start_perf
        self.logger.info(
            f"[STEP END] collect_batch | {datetime.now().isoformat()} | "
            f"elapsed={elapsed:.2f}s | status=OK"
        )
        return self.stats

    def get_error_tracker(self) -> ErrorTracker:
        """에러 트래커 반환"""
        return self.error_tracker


def get_apps_to_collect(limit: Optional[int] = None) -> List[str]:
    """수집할 앱 ID 목록을 가져옵니다.

    수집 정책:
    - 활성 앱: 매번 수집 (crontab으로 매일 실행)
    - 버려진 앱 (2년 이상 업데이트 안 됨): 7일에 1번 수집
    - 실패한 앱: 제외
    """
    # 제외할 앱 ID: 실패한 앱 + 7일 이내 수집된 버려진 앱
    exclude_ids = get_failed_app_ids(PLATFORM) | get_abandoned_apps_to_skip(PLATFORM, 'details_collected_at')

    # sitemap에서 앱 목록 가져오기 (최근 발견 순)
    sitemap_conn = get_sitemap_connection()
    try:
        with sitemap_conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT app_id FROM app_localizations
                WHERE platform = 'play_store'
                ORDER BY first_seen_at DESC
            """)

            result = []
            for row in cursor.fetchall():
                if row['app_id'] not in exclude_ids:
                    result.append(row['app_id'])
                    if limit is not None and len(result) >= limit:
                        break
    finally:
        release_sitemap_connection(sitemap_conn)
    return result


def main():
    init_database()

    # 수집할 앱 목록
    app_ids = get_apps_to_collect(limit=10)
    logger = get_timestamped_logger("play_store_details_main", file_prefix="play_store_details_main")
    logger.info(f"Found {len(app_ids)} apps to collect")

    if app_ids:
        collector = PlayStoreDetailsCollector(verbose=True)
        stats = collector.collect_batch(app_ids)
        logger.info(f"\nFinal Stats: {stats}")


if __name__ == '__main__':
    main()
