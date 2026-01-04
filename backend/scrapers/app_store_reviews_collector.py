"""
App Store Reviews Collector
RSS를 통해 앱 리뷰를 수집합니다.
"""
import sys
import os
import time
import requests
from typing import List, Dict, Any, Optional, Set

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.app_details_db import (
    init_database, insert_reviews_batch, get_all_review_ids,
    get_collection_status, update_collection_status, get_review_count,
    is_failed_app
)
from database.sitemap_apps_db import get_connection as get_sitemap_connection

PLATFORM = 'app_store'
RSS_BASE_URL = 'https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortBy=mostRecent/json'
REQUEST_DELAY = 0.01  # 10ms
MAX_REVIEWS_TOTAL = 20000  # 실행당 최대 수집 리뷰 수 (무한루프 방지)


class AppStoreReviewsCollector:
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.stats = {
            'apps_processed': 0,
            'apps_skipped': 0,
            'reviews_collected': 0,
            'reviews_duplicates': 0,
            'errors': 0
        }

    def log(self, message: str):
        if self.verbose:
            print(f"[AppStore Reviews] {message}")

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

    def fetch_reviews_page(self, app_id: str, country: str, page: int) -> List[Dict]:
        """RSS에서 리뷰 페이지를 가져옵니다."""
        url = RSS_BASE_URL.format(country=country, page=page, app_id=app_id)

        try:
            response = requests.get(url, timeout=30)
            if response.status_code != 200:
                return []

            data = response.json()
            entries = data.get('feed', {}).get('entry', [])

            if isinstance(entries, dict):
                entries = [entries]
            elif not isinstance(entries, list):
                return []

            # 첫 번째는 앱 정보, 나머지가 리뷰
            if len(entries) <= 1:
                return []

            reviews = []
            for entry in entries[1:]:  # 첫 번째 제외
                review = self.parse_review(entry, app_id, country)
                if review:
                    reviews.append(review)

            return reviews

        except (requests.exceptions.RequestException, ValueError) as e:
            self.log(f"Error fetching reviews page {page} for {app_id}/{country}: {e}")
            return []

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
                'reviewed_at': entry.get('updated', {}).get('label', ''),
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
            reviews = self.fetch_reviews_page(app_id, country, page)
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

        return collected, hit_existing, has_more

    def collect_reviews_for_app(self, app_id: str) -> int:
        """단일 앱의 리뷰를 수집합니다. 국가별 균등 분배 + 잔여 분배."""
        # 실패한 앱인지 확인
        if is_failed_app(app_id, PLATFORM):
            self.log(f"  [{app_id}] 건너뜀: 실패 목록에 있음")
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

        # 수집할 수 있는 리뷰 수 계산
        if initial_done:
            remaining = MAX_REVIEWS_TOTAL
            mode = "추가 수집"
        else:
            remaining = MAX_REVIEWS_TOTAL - current_count
            mode = "초기 수집"

        if remaining <= 0:
            self.log(f"  [{app_id}] 건너뜀: 이번 실행 할당량 소진")
            self.stats['apps_skipped'] += 1
            return 0

        # 국가별 할당량 계산
        per_country_quota = remaining // len(countries)
        if per_country_quota < 1:
            per_country_quota = 1

        self.log(f"  [{app_id}] {mode} 시작 | 국가: {len(countries)}개 | "
                 f"기존: {current_count}건 | 목표: {remaining}건 (국가당 {per_country_quota}건)")

        collected_total = 0
        hit_existing_any = False
        countries_with_more = []  # 추가 수집 가능한 국가
        country_results = {}

        # === 1차: 국가별 균등 분배 ===
        for country in countries:
            collected, hit_existing, has_more = self.collect_reviews_for_country(
                app_id, country, per_country_quota, existing_ids,
                stop_on_existing=initial_done  # 추가 수집시에만 기존 리뷰에서 중단
            )
            collected_total += collected
            country_results[country] = collected

            if hit_existing:
                hit_existing_any = True
                self.log(f"    [{country.upper()}] {collected}건 (기존 리뷰 발견, 중단)")
            elif has_more:
                countries_with_more.append(country)
                self.log(f"    [{country.upper()}] {collected}건 (더 있음)")
            elif collected > 0:
                self.log(f"    [{country.upper()}] {collected}건")

        # === 2차: 잔여 분배 (리뷰가 더 있는 국가에서 추가 수집) ===
        remaining_after_first = remaining - collected_total

        if remaining_after_first > 0 and countries_with_more and not hit_existing_any:
            extra_per_country = remaining_after_first // len(countries_with_more)
            if extra_per_country < 1:
                extra_per_country = remaining_after_first

            self.log(f"    [2차 분배] 잔여 {remaining_after_first}건 → {len(countries_with_more)}개 국가")

            for country in countries_with_more:
                if collected_total >= remaining:
                    break

                extra_quota = min(extra_per_country, remaining - collected_total)
                collected, hit_existing, _ = self.collect_reviews_for_country(
                    app_id, country, extra_quota, existing_ids,
                    stop_on_existing=initial_done
                )
                collected_total += collected
                country_results[country] = country_results.get(country, 0) + collected

                if collected > 0:
                    self.log(f"    [{country.upper()}] +{collected}건 (2차)")

                if hit_existing:
                    break

        # 수집 상태 업데이트
        new_total = current_count + collected_total
        update_collection_status(
            app_id, PLATFORM,
            reviews_collected=True,
            reviews_count=new_total,
            initial_review_done=(not initial_done and (collected_total > 0 or hit_existing_any))
        )

        self.stats['apps_processed'] += 1

        # 최종 결과 로그
        if collected_total > 0:
            self.log(f"  [{app_id}] 완료: +{collected_total}건 (누적 {new_total}건)")
        else:
            self.log(f"  [{app_id}] 완료: 신규 리뷰 없음 (누적 {new_total}건)")

        return collected_total

    def collect_batch(self, app_ids: List[str]) -> Dict[str, Any]:
        """배치로 리뷰를 수집합니다."""
        self.log(f"=== App Store 리뷰 수집 시작 ===")
        self.log(f"대상 앱: {len(app_ids)}개 | 실행당 최대: {MAX_REVIEWS_TOTAL}건/앱")
        self.log("")

        for i, app_id in enumerate(app_ids, 1):
            self.log(f"[{i}/{len(app_ids)}] 앱 처리 중...")

            try:
                self.collect_reviews_for_app(app_id)
            except Exception as e:
                self.log(f"  [{app_id}] 오류 발생: {e}")
                self.stats['errors'] += 1

            time.sleep(REQUEST_DELAY)

        self.log("")
        self.log(f"=== App Store 리뷰 수집 완료 ===")
        self.log(f"처리: {self.stats['apps_processed']}개 | "
                 f"건너뜀: {self.stats['apps_skipped']}개 | "
                 f"수집: {self.stats['reviews_collected']}건 | "
                 f"오류: {self.stats['errors']}개")
        return self.stats


def get_apps_for_review_collection(limit: int = 1000) -> List[str]:
    """리뷰 수집할 앱 ID 목록을 가져옵니다.

    상세정보가 수집된 모든 앱에서 리뷰를 수집합니다.
    리뷰 수집 시 중복은 review_id로 자동 필터링되므로,
    initial_review_done 여부와 관계없이 모든 앱을 재수집 대상으로 포함합니다.
    """
    from database.app_details_db import get_connection as get_details_connection

    details_conn = get_details_connection()
    cursor = details_conn.cursor()

    # 상세정보가 수집된 모든 앱에서 리뷰 수집 (재수집 허용)
    cursor.execute("""
        SELECT app_id FROM collection_status
        WHERE platform = 'app_store'
          AND details_collected_at IS NOT NULL
        ORDER BY reviews_collected_at ASC NULLS FIRST, details_collected_at DESC
        LIMIT ?
    """, (limit,))

    app_ids = [row['app_id'] for row in cursor.fetchall()]
    details_conn.close()
    return app_ids


def main():
    init_database()

    # 수집할 앱 목록
    app_ids = get_apps_for_review_collection(limit=10)
    print(f"Found {len(app_ids)} apps for review collection")

    if app_ids:
        collector = AppStoreReviewsCollector(verbose=True)
        stats = collector.collect_batch(app_ids)
        print(f"\nFinal Stats: {stats}")


if __name__ == '__main__':
    main()
