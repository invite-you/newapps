"""
App Store Details Collector
iTunes Lookup API를 사용하여 앱 상세정보를 수집합니다.
"""
import sys
import os
import time
from datetime import datetime
import requests
import json
from typing import List, Dict, Any, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.app_details_db import (
    init_database, save_app_details_atomic,
    is_app_blocked, record_app_failure, clear_app_failure,
    get_blocked_app_ids, get_abandoned_apps_to_skip, normalize_date_format,
    generate_session_id
)
from database.sitemap_apps_db import (
    get_connection as get_sitemap_connection,
    release_connection as release_sitemap_connection,
)
from config.language_country_priority import select_best_pairs_for_collection
from scrapers.collection_utils import (
    get_app_language_country_pairs,
    select_primary_country
)
from utils.logger import get_collection_logger, get_timestamped_logger, ProgressLogger, format_warning_log, format_error_log
from utils.network_binding import configure_network_binding
from utils.network_binding import get_requests_session
from utils.error_tracker import ErrorTracker, ErrorStep

PLATFORM = 'app_store'
API_BASE_URL = 'https://itunes.apple.com/lookup'
REQUEST_DELAY = 0.01  # 10ms


class AppStoreDetailsCollector:
    def __init__(self, verbose: bool = True, error_tracker: Optional[ErrorTracker] = None,
                 session_id: Optional[str] = None):
        self.verbose = verbose
        self.logger = get_collection_logger('AppStoreDetails', verbose)
        configure_network_binding(logger=self.logger)
        self.error_tracker = error_tracker or ErrorTracker('app_store_details')
        # 세션 ID: 프로그램 실행 단위로 실패 관리에 사용
        self.session_id = session_id or generate_session_id()
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

    def fetch_app_info(self, app_id: str, country: str = 'US') -> Tuple[Optional[Dict], Optional[str]]:
        """iTunes Lookup API로 앱 정보를 가져옵니다.

        Returns:
            Tuple of (data, error_reason)
            - data: 앱 정보 또는 None
            - error_reason: 실패 시 사유 (not_found, timeout, network_error, rate_limited, server_error, api_error)
        """
        url = f"{API_BASE_URL}?id={app_id}&country={country}"

        try:
            response = get_requests_session().get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data.get('resultCount', 0) > 0:
                return (data['results'][0], None)
            return (None, 'not_found')

        except requests.exceptions.Timeout:
            self.logger.warning(format_warning_log("timeout", f"app_id={app_id}", "request timed out"))
            return (None, 'timeout')
        except requests.exceptions.ConnectionError as e:
            self.logger.warning(format_warning_log("network_error", f"app_id={app_id}", str(e)))
            return (None, 'network_error')
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else 0
            if status_code == 429:
                self.logger.warning(format_warning_log("rate_limit", f"app_id={app_id}", "HTTP 429"))
                return (None, 'rate_limited')
            elif status_code >= 500:
                self.logger.warning(format_warning_log("server_error", f"app_id={app_id}", f"HTTP {status_code}"))
                return (None, f'server_error:{status_code}')
            else:
                self.logger.warning(format_warning_log("http_error", f"app_id={app_id}", f"HTTP {status_code}: {e}"))
                return (None, f'http_error:{status_code}')
        except requests.exceptions.JSONDecodeError as e:
            self.logger.warning(format_warning_log("invalid_response", f"app_id={app_id}", str(e)))
            return (None, 'invalid_response')
        except requests.exceptions.RequestException as e:
            self.logger.warning(format_warning_log("request_error", f"app_id={app_id}", f"{type(e).__name__}: {e}"))
            return (None, f'request_error:{type(e).__name__}')

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

        has_iap = any([
            has_iap_feature,
            has_in_app_purchases_value,
            has_in_app_purchases_flag_value
        ])

        genres = data.get('genres', [])
        genre_name = genres[0] if genres else data.get('primaryGenreName')

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
            'free': data.get('price', 0) == 0,
            'has_iap': has_iap,
            'category_id': str(data.get('primaryGenreId', '')),
            'genre_id': str(data.get('primaryGenreId', '')),
            'genre_name': genre_name,
            'content_rating': data.get('contentAdvisoryRating'),
            'content_rating_description': data.get('trackContentRating'),
            'min_os_version': data.get('minimumOsVersion'),
            'file_size': int(data.get('fileSizeBytes', 0)) if data.get('fileSizeBytes') else None,
            'supported_devices': json.dumps(data.get('supportedDevices', [])[:20], ensure_ascii=False),  # 최대 20개
            'release_date': normalize_date_format(data.get('releaseDate')),
            'updated_date': normalize_date_format(data.get('currentVersionReleaseDate')),
            'privacy_policy_url': None
        }

    def parse_app_localized(self, data: Dict, app_id: str, language: str) -> Dict:
        """API 응답을 apps_localized 테이블 형식으로 변환합니다."""
        return {
            'app_id': app_id,
            'platform': PLATFORM,
            'language': language.lower(),
            'title': data.get('trackName'),
            'summary': None,  # App Store에는 summary 없음
            'description': data.get('description'),
            'release_notes': data.get('releaseNotes')
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
        self.logger.debug(f"[APP START] app_id={app_id}")
        # 차단된 앱인지 확인 (영구 실패 또는 이번 세션에서 실패)
        if is_app_blocked(app_id, PLATFORM, session_id=self.session_id):
            self.stats['apps_skipped_failed'] += 1
            self.logger.debug(f"[APP SKIP] app_id={app_id} | status=blocked")
            return False

        # 앱의 (language, country) 쌍 가져오기
        pairs = get_app_language_country_pairs(
            app_id,
            PLATFORM,
            normalize_country_case="upper",
            default_pair=("en", "US")
        )

        # 우선순위에 따라 최적의 (language, country) 쌍 선택
        # 각 언어당 가장 적합한 국가를 선택 (예: fr-FR > fr-CA)
        optimized_pairs = select_best_pairs_for_collection(pairs, max_languages=10)

        # 기본 정보 수집용 국가 결정 (US 우선)
        primary_country = select_primary_country(optimized_pairs, preferred_country="US")

        data, last_error = self.fetch_app_info(app_id, primary_country)

        if not data:
            # 다른 국가로 재시도 (우선순위 순서대로)
            for lang, country in optimized_pairs:
                if country.upper() != primary_country:
                    data, error = self.fetch_app_info(app_id, country.upper())
                    if data:
                        primary_country = country.upper()
                        last_error = None
                        break
                    last_error = error  # 마지막 에러 사유 유지
                    time.sleep(REQUEST_DELAY)

        if not data:
            # 실패 기록 (실행 횟수 기반 영구 실패 판정)
            reason = last_error or 'unknown'
            failure_info = record_app_failure(app_id, PLATFORM, reason, session_id=self.session_id)
            self.stats['apps_not_found'] += 1
            self.logger.warning(format_warning_log(
                "fetch_failed", f"app_id={app_id}",
                f"reason={reason} | permanent={failure_info['is_permanent']} | fail_count={failure_info['consecutive_fail_count']}"
            ))
            return False

        # 앱 메타데이터 준비
        app_meta = self.parse_app_metadata(data, app_id)

        # 수치 데이터 준비
        metrics = self.parse_app_metrics(data, app_id)

        # 다국어 데이터 수집 - 우선순위 기반 최적화된 쌍 사용
        # 최적화: title+description이 기준 언어와 동일하면 저장하지 않음
        languages_collected = set()
        fetched_countries = {primary_country: data}  # 이미 가져온 데이터 캐시
        localized_list = []

        # 기준 언어 데이터 먼저 추가
        base_localized = self.parse_app_localized(data, app_id, optimized_pairs[0][0] if optimized_pairs else 'en')
        base_title = base_localized.get('title')
        base_description = base_localized.get('description')
        localized_list.append(base_localized)
        languages_collected.add(base_localized['language'])

        for language, country in optimized_pairs:
            if language in languages_collected:
                continue

            country_upper = country.upper()

            # 해당 국가의 데이터 가져오기 (캐시 활용)
            if country_upper in fetched_countries:
                country_data = fetched_countries[country_upper]
            else:
                country_data, _ = self.fetch_app_info(app_id, country_upper)
                fetched_countries[country_upper] = country_data
                time.sleep(REQUEST_DELAY)

            if country_data:
                localized = self.parse_app_localized(country_data, app_id, language)
                # 중복 체크: title과 description이 기준 언어와 다를 때만 추가
                if localized.get('title') != base_title or localized.get('description') != base_description:
                    localized_list.append(localized)
                languages_collected.add(language)

        # 원자적 저장: 모든 데이터를 단일 트랜잭션으로 저장
        result = save_app_details_atomic(
            app_id=app_id,
            platform=PLATFORM,
            app_meta=app_meta,
            metrics_data=metrics,
            localized_list=localized_list
        )

        # 성공 시 실패 기록 삭제 (재시도로 성공한 경우)
        clear_app_failure(app_id, PLATFORM)

        if result['app_inserted']:
            self.stats['new_records'] += 1
        else:
            self.stats['unchanged_records'] += 1

        self.stats['apps_processed'] += 1
        self.logger.debug(
            f"[APP END] app_id={app_id} | status=OK | "
            f"app_new={result['app_inserted']} | localized={result['localized_inserted']}"
        )

        return True

    def collect_batch(self, app_ids: List[str]) -> Dict[str, Any]:
        """배치로 앱 상세정보를 수집합니다."""
        progress = ProgressLogger(self.logger, len(app_ids), "collect_batch")
        progress.start()

        for i, app_id in enumerate(app_ids, 1):
            progress.tick(i, app_id)

            try:
                self.collect_app(app_id)
            except Exception as e:
                self.stats['errors'] += 1
                # 상세 에러 추적
                self.error_tracker.add_error(
                    platform=PLATFORM,
                    step=ErrorStep.COLLECT_APP,
                    error=e,
                    app_id=app_id,
                    include_traceback=True
                )
                self.logger.error(format_error_log(
                    reason=type(e).__name__,
                    target=f"app_id={app_id}",
                    action="skip",
                    detail=str(e)
                ))

            time.sleep(REQUEST_DELAY)

        progress.end(
            status="OK",
            processed=self.stats['apps_processed'],
            skipped=self.stats['apps_skipped_failed'],
            not_found=self.stats['apps_not_found'],
            new=self.stats['new_records'],
            errors=self.stats['errors']
        )
        return self.stats

    def get_error_tracker(self) -> ErrorTracker:
        """에러 트래커 반환"""
        return self.error_tracker


def get_apps_to_collect(limit: Optional[int] = None, session_id: Optional[str] = None) -> List[str]:
    """수집할 앱 ID 목록을 가져옵니다.

    수집 정책:
    - 활성 앱: 매번 수집 (crontab으로 매일 실행)
    - 버려진 앱 (2년 이상 업데이트 안 됨): 7일에 1번 수집
    - 차단된 앱: 제외 (영구 실패 또는 이번 세션에서 실패)
    """
    # 제외할 앱 ID: 차단된 앱 + 7일 이내 수집된 버려진 앱
    exclude_ids = get_blocked_app_ids(PLATFORM, session_id=session_id) | get_abandoned_apps_to_skip(PLATFORM, 'details_collected_at')

    # sitemap에서 앱 목록 가져오기 (최근 발견 순)
    sitemap_conn = get_sitemap_connection()
    try:
        with sitemap_conn.cursor() as cursor:
            cursor.execute("""
                SELECT app_id
                FROM app_localizations
                WHERE platform = 'app_store'
                GROUP BY app_id
                ORDER BY MAX(first_seen_at) DESC NULLS LAST, app_id ASC
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

    # 세션 ID 생성 (전체 실행에서 동일한 ID 사용)
    session_id = generate_session_id()
    logger = get_timestamped_logger("app_store_details_main", file_prefix="app_store_details_main")
    logger.info(f"Session ID: {session_id}")

    # 수집할 앱 목록 가져오기 (이번 세션에서 실패한 앱 제외)
    app_ids = get_apps_to_collect(limit=10, session_id=session_id)  # 테스트용 10개
    logger.info(f"Found {len(app_ids)} apps to collect")

    if app_ids:
        collector = AppStoreDetailsCollector(verbose=True, session_id=session_id)
        stats = collector.collect_batch(app_ids)
        logger.info(f"\nFinal Stats: {stats}")


if __name__ == '__main__':
    main()
