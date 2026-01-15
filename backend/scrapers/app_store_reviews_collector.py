"""
App Store Reviews Collector
RSS를 통해 앱 리뷰를 수집합니다.
"""
import sys
import os
import time
import requests
import traceback
from typing import List, Dict, Any, Optional, Set

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.app_details_db import (
    init_database, insert_reviews_batch, get_all_review_ids,
    get_collection_status, update_collection_status, get_review_count,
    is_app_blocked, get_blocked_app_ids, get_abandoned_apps_to_skip, normalize_date_format,
    generate_session_id
)
from utils.logger import get_collection_logger, get_timestamped_logger, ProgressLogger, format_warning_log, format_error_log
from utils.network_binding import configure_network_binding
from utils.network_binding import get_requests_session
from utils.error_tracker import ErrorTracker, ErrorStep
from scrapers.collection_utils import (
    get_app_language_country_pairs,
    LocalePairPolicy,
    CollectionErrorPolicy,
    collect_app_ids_from_cursor,
)

PLATFORM = 'app_store'
RSS_BASE_URL = 'https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortBy=mostRecent/json'
REQUEST_DELAY = 0.01  # 10ms
REQUEST_TIMEOUT = int(os.getenv("APP_STORE_REVIEW_TIMEOUT", "60"))
MAX_REVIEWS_TOTAL = int(os.getenv("APP_REVIEWS_MAX_PER_RUN", "50000"))  # 실행당 최대 수집 리뷰 수


class AppStoreReviewsCollector:
    def __init__(self, verbose: bool = True, error_tracker: Optional[ErrorTracker] = None,
                 session_id: Optional[str] = None):
        self.verbose = verbose
        self.logger = get_collection_logger('AppStoreReviews', verbose)
        configure_network_binding(logger=self.logger)
        self.error_tracker = error_tracker or ErrorTracker('app_store_reviews')
        # 세션 ID: 프로그램 실행 단위로 실패 관리에 사용
        self.session_id = session_id or generate_session_id()
        self.locale_policy = LocalePairPolicy.from_env()
        self.error_policy = CollectionErrorPolicy()
        self.stats = {
            'apps_processed': 0,
            'apps_skipped': 0,
            'reviews_collected': 0,
            'reviews_duplicates': 0,
            'errors': 0
        }

    def log(self, message: str):
        if self.verbose:
            self.logger.info(message)

    def get_app_language_country_pairs(self, app_id: str) -> List[tuple]:
        """sitemap에서 앱의 (language, country) 쌍을 가져옵니다."""
        pairs = get_app_language_country_pairs(
            app_id,
            PLATFORM,
            normalize_country_case="upper",
            default_pair=("en", "US"),
        )
        return self.locale_policy.select_pairs(
            pairs,
            country_case="lower",
            default_pair=("en", "us"),
        )

    def fetch_reviews_page(self, app_id: str, country: str, page: int) -> tuple:
        """RSS에서 리뷰 페이지를 가져옵니다."""
        url = RSS_BASE_URL.format(country=country, page=page, app_id=app_id)

        try:
            response = get_requests_session().get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200:
                return [], None

            data = response.json()
            entries = data.get('feed', {}).get('entry', [])

            if isinstance(entries, dict):
                entries = [entries]
            elif not isinstance(entries, list):
                return [], None

            # 첫 번째는 앱 정보, 나머지가 리뷰
            if len(entries) <= 1:
                return [], None

            reviews = []
            for entry in entries[1:]:  # 첫 번째 제외
                review = self.parse_review(entry, app_id, country)
                if review:
                    reviews.append(review)

            return reviews, None

        except (requests.exceptions.RequestException, ValueError) as e:
            self.logger.warning(format_warning_log("fetch_error", f"app_id={app_id} country={country} page={page}", str(e)))
            return [], "network_error"

    def parse_review(self, entry: Dict, app_id: str, country: str) -> Optional[Dict]:
        """RSS 엔트리를 리뷰 데이터로 변환합니다."""
        try:
            review_id = entry.get('id', {}).get('label', '')
            if not review_id:
                return None

            return {
                'app_id': app_id,
                'platform': PLATFORM,
                'review_id': review_id,
                'country': country,
                'language': None,  # RSS에서 언어 정보 없음
                'user_name': entry.get('author', {}).get('name', {}).get('label', ''),
                'user_image': None,
                'score': int(entry.get('im:rating', {}).get('label', 0)),
                'title': entry.get('title', {}).get('label', ''),
                'content': entry.get('content', {}).get('label', ''),
                'thumbs_up_count': int(entry.get('im:voteCount', {}).get('label', 0)),
                'app_version': entry.get('im:version', {}).get('label', ''),
                'reviewed_at': normalize_date_format(entry.get('updated', {}).get('label', '')),
                'reply_content': None,
                'replied_at': None
            }
        except Exception:
            return None

    def collect_reviews_for_country(self, app_id: str, country: str, quota: int,
                                      existing_ids: Set[str], stop_on_existing: bool) -> tuple:
        """
        특정 국가에서 리뷰를 수집합니다.
        Returns: (collected_count, hit_existing, has_more)
        """
        collected = 0
        hit_existing = False
        has_more = False
        page = 0

        while collected < quota:
            page += 1
            reviews, error_reason = self.fetch_reviews_page(app_id, country, page)
            if error_reason and self.error_policy.should_abort(error_reason):
                self.stats['errors'] += 1
                return collected, hit_existing, has_more, True
            if not reviews:
                break  # 더 이상 리뷰 없음

            new_reviews = []
            for review in reviews:
                if review['review_id'] in existing_ids:
                    if stop_on_existing:
                        hit_existing = True
                        break
                    continue  # 중복 건너뛰기

                new_reviews.append(review)
                existing_ids.add(review['review_id'])

                if collected + len(new_reviews) >= quota:
                    has_more = True  # 할당량 도달, 더 있을 수 있음
                    break

            if new_reviews:
                to_save = new_reviews[:quota - collected]
                inserted = insert_reviews_batch(to_save)
                collected += inserted
                self.stats['reviews_collected'] += inserted

            if hit_existing:
                break

            time.sleep(REQUEST_DELAY)

        return collected, hit_existing, has_more, False

    def collect_reviews_for_app(self, app_id: str) -> int:
        """단일 앱의 리뷰를 수집합니다. 국가별 균등 분배 + 잔여 분배."""
        # 차단된 앱인지 확인 (영구 실패 또는 이번 세션에서 실패)
        if is_app_blocked(app_id, PLATFORM, session_id=self.session_id):
            self.logger.debug(f"[APP SKIP] app_id={app_id} | status=blocked")
            self.stats['apps_skipped'] += 1
            return 0

        # 수집 상태 확인
        status = get_collection_status(app_id, PLATFORM)
        current_count = get_review_count(app_id, PLATFORM)
        initial_done = status.get('initial_review_done', 0) if status else 0

        # 이미 수집된 리뷰 ID 세트
        existing_ids = get_all_review_ids(app_id, PLATFORM)

        # sitemap에서 (language, country) 쌍 가져오기
        pairs = self.get_app_language_country_pairs(app_id)
        # RSS는 country만 사용하므로 고유한 국가 목록 추출
        countries = list({country for _, country in pairs})

        # 수집할 수 있는 리뷰 수 계산 (실행당 최대 5만 건)
        remaining = MAX_REVIEWS_TOTAL
        has_existing_reviews = current_count > 0
        incremental_mode = bool(initial_done or has_existing_reviews)
        mode = "incremental" if incremental_mode else "initial"

        if remaining <= 0:
            self.logger.debug(f"[APP SKIP] app_id={app_id} | quota exhausted")
            self.stats['apps_skipped'] += 1
            return 0

        # 국가별 할당량 계산
        per_country_quota = remaining // len(countries)
        if per_country_quota < 1:
            per_country_quota = 1

        self.logger.debug(
            f"[APP START] app_id={app_id} | mode={mode} | countries={len(countries)} | existing={current_count}"
        )

        collected_total = 0
        hit_existing_any = False
        countries_with_more = []  # 추가 수집 가능한 국가
        country_results = {}

        # === 1차: 국가별 균등 분배 ===
        for country in countries:
            collected, hit_existing, has_more, aborted = self.collect_reviews_for_country(
                app_id, country, per_country_quota, existing_ids,
                stop_on_existing=incremental_mode  # 추가 수집시에만 기존 리뷰에서 중단
            )
            if aborted:
                self.logger.warning(format_warning_log(
                    "network_abort", f"app_id={app_id}", "network_error"
                ))
                return 0
            collected_total += collected
            country_results[country] = collected

            if hit_existing:
                hit_existing_any = True
                self.logger.debug(f"[COUNTRY] {country} | {collected} reviews | hit_existing")
            elif has_more:
                countries_with_more.append(country)
                self.logger.debug(f"[COUNTRY] {country} | {collected} reviews | has_more")
            elif collected > 0:
                self.logger.debug(f"[COUNTRY] {country} | {collected} reviews")

        # === 2차: 잔여 분배 (리뷰가 더 있는 국가에서 추가 수집) ===
        remaining_after_first = remaining - collected_total

        if remaining_after_first > 0 and countries_with_more and not hit_existing_any:
            extra_per_country = remaining_after_first // len(countries_with_more)
            if extra_per_country < 1:
                extra_per_country = remaining_after_first

            self.logger.debug(f"[2ND PASS] remaining={remaining_after_first} | countries={len(countries_with_more)}")

            for country in countries_with_more:
                if collected_total >= remaining:
                    break

                extra_quota = min(extra_per_country, remaining - collected_total)
                collected, hit_existing, _, aborted = self.collect_reviews_for_country(
                    app_id, country, extra_quota, existing_ids,
                    stop_on_existing=incremental_mode
                )
                if aborted:
                    self.logger.warning(format_warning_log(
                        "network_abort", f"app_id={app_id}", "network_error"
                    ))
                    return collected_total
                collected_total += collected
                country_results[country] = country_results.get(country, 0) + collected

                if collected > 0:
                    self.logger.debug(f"[COUNTRY 2ND] {country} | +{collected} reviews")

                if hit_existing:
                    break

        # 수집 상태 업데이트
        new_total = current_count + collected_total
        update_collection_status(
            app_id, PLATFORM,
            reviews_collected=True,
            reviews_count=new_total,
            initial_review_done=bool(
                initial_done or has_existing_reviews or collected_total > 0 or hit_existing_any
            )
        )

        self.stats['apps_processed'] += 1

        # 최종 결과 로그 (DEBUG - 배치 요약에서 INFO로 출력)
        self.logger.debug(f"[APP END] app_id={app_id} | +{collected_total} reviews | total={new_total}")

        return collected_total

    def collect_batch(self, app_ids: List[str]) -> Dict[str, Any]:
        """배치로 리뷰를 수집합니다."""
        progress = ProgressLogger(self.logger, len(app_ids), "collect_reviews")
        progress.start(max_reviews_per_app=MAX_REVIEWS_TOTAL)

        for i, app_id in enumerate(app_ids, 1):
            progress.tick(i, app_id)

            try:
                self.collect_reviews_for_app(app_id)
            except Exception as e:
                self.stats['errors'] += 1
                # 상세 에러 추적
                self.error_tracker.add_error(
                    platform=PLATFORM,
                    step=ErrorStep.COLLECT_REVIEW,
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
            skipped=self.stats['apps_skipped'],
            reviews=self.stats['reviews_collected'],
            errors=self.stats['errors']
        )
        return self.stats

    def get_error_tracker(self) -> ErrorTracker:
        """에러 트래커 반환"""
        return self.error_tracker


def get_apps_for_review_collection(limit: Optional[int] = None, session_id: Optional[str] = None) -> List[str]:
    """리뷰 수집할 앱 ID 목록을 가져옵니다.

    수집 정책:
    - 상세정보가 수집된 앱만 대상
    - 상세정보 최신 지표(apps_metrics.reviews_count)에서 0으로 표시된 앱은 실제 리뷰가 없다고 판단해 제외
    - 활성 앱: 매번 수집 (crontab으로 매일 실행)
    - 버려진 앱 (2년 이상 업데이트 안 됨): 7일에 1번 수집
    - 차단된 앱: 제외 (영구 실패 또는 이번 세션에서 실패)
    """
    from database.app_details_db import (
        get_connection as get_details_connection,
        release_connection as release_details_connection,
    )

    # 제외할 앱 ID: 차단된 앱 + 7일 이내 수집된 버려진 앱
    exclude_ids = get_blocked_app_ids(PLATFORM, session_id=session_id) | get_abandoned_apps_to_skip(PLATFORM, 'reviews_collected_at')

    # 상세정보가 수집된 앱 목록
    details_conn = get_details_connection()
    try:
        with details_conn.cursor() as cursor:
            cursor.execute("""
                SELECT cs.app_id
                FROM collection_status cs
                LEFT JOIN LATERAL (
                    SELECT reviews_count
                    FROM apps_metrics am
                    WHERE am.app_id = cs.app_id
                      AND am.platform = cs.platform
                    ORDER BY am.recorded_at DESC
                    LIMIT 1
                ) latest_metrics ON true
                WHERE cs.platform = 'app_store'
                  AND cs.details_collected_at IS NOT NULL
                  AND (latest_metrics.reviews_count IS NULL OR latest_metrics.reviews_count > 0)
                ORDER BY cs.details_collected_at DESC
            """)

            result = collect_app_ids_from_cursor(cursor, exclude_ids, limit)
    finally:
        release_details_connection(details_conn)
    return result


def main():
    init_database()

    # 세션 ID 생성 (전체 실행에서 동일한 ID 사용)
    session_id = generate_session_id()
    logger = get_timestamped_logger("app_store_reviews_main", file_prefix="app_store_reviews_main")
    logger.info(f"Session ID: {session_id}")

    # 수집할 앱 목록 (이번 세션에서 실패한 앱 제외)
    app_ids = get_apps_for_review_collection(limit=10, session_id=session_id)
    logger.info(f"Found {len(app_ids)} apps for review collection")

    if app_ids:
        collector = AppStoreReviewsCollector(verbose=True, session_id=session_id)
        stats = collector.collect_batch(app_ids)
        logger.info(f"\nFinal Stats: {stats}")


if __name__ == '__main__':
    main()
