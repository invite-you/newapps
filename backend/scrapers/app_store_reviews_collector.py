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
MAX_REVIEWS_TOTAL = 10000  # 앱당 최대 리뷰 수
MAX_PAGES_PER_COUNTRY = 10  # 국가당 최대 페이지 (약 500건)


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

    def get_app_countries(self, app_id: str) -> List[str]:
        """sitemap에서 앱의 국가 목록을 가져옵니다."""
        conn = get_sitemap_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT country FROM app_localizations
            WHERE app_id = ? AND platform = ?
        """, (app_id, PLATFORM))
        countries = [row['country'].lower() for row in cursor.fetchall()]
        conn.close()
        return countries if countries else ['us']

    def fetch_reviews_page(self, app_id: str, country: str, page: int) -> List[Dict]:
        """RSS에서 리뷰 페이지를 가져옵니다."""
        url = RSS_BASE_URL.format(country=country, page=page, app_id=app_id)

        try:
            response = requests.get(url, timeout=30)
            if response.status_code != 200:
                return []

            data = response.json()
            entries = data.get('feed', {}).get('entry', [])

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

    def collect_reviews_for_app(self, app_id: str) -> int:
        """단일 앱의 리뷰를 수집합니다."""
        # 실패한 앱인지 확인
        if is_failed_app(app_id, PLATFORM):
            self.stats['apps_skipped'] += 1
            return 0

        # 수집 상태 확인
        status = get_collection_status(app_id, PLATFORM)
        current_count = get_review_count(app_id, PLATFORM)
        initial_done = status.get('initial_review_done', 0) if status else 0

        # 이미 수집된 리뷰 ID 세트
        existing_ids = get_all_review_ids(app_id, PLATFORM)

        # 국가 목록
        countries = self.get_app_countries(app_id)

        # 수집할 수 있는 리뷰 수 계산
        if initial_done:
            # 추가 수집: 제한 없이 새 리뷰만
            remaining = MAX_REVIEWS_TOTAL  # 실질적으로 제한 없음
        else:
            # 최초 수집: 최대 10000건
            remaining = MAX_REVIEWS_TOTAL - current_count

        if remaining <= 0:
            self.stats['apps_skipped'] += 1
            return 0

        collected_total = 0
        hit_existing = False

        # 각 국가에서 순차적으로 수집 (전체 합산 도달 시 중단)
        for country in countries:
            if collected_total >= remaining or hit_existing:
                break

            for page in range(1, MAX_PAGES_PER_COUNTRY + 1):
                if collected_total >= remaining or hit_existing:
                    break

                reviews = self.fetch_reviews_page(app_id, country, page)
                if not reviews:
                    break

                # 새 리뷰만 필터링
                new_reviews = []
                for review in reviews:
                    if review['review_id'] in existing_ids:
                        # 이미 수집된 리뷰 발견 - 중단
                        hit_existing = True
                        break
                    new_reviews.append(review)
                    existing_ids.add(review['review_id'])

                if new_reviews:
                    # 남은 개수만큼만 저장
                    to_save = new_reviews[:remaining - collected_total]
                    inserted = insert_reviews_batch(to_save)
                    collected_total += inserted
                    self.stats['reviews_collected'] += inserted

                time.sleep(REQUEST_DELAY)

        # 수집 상태 업데이트
        new_total = current_count + collected_total
        update_collection_status(
            app_id, PLATFORM,
            reviews_collected=True,
            reviews_count=new_total,
            initial_review_done=(not initial_done and (collected_total > 0 or hit_existing))
        )

        self.stats['apps_processed'] += 1
        self.log(f"App {app_id}: collected {collected_total} reviews (total: {new_total})")

        return collected_total

    def collect_batch(self, app_ids: List[str]) -> Dict[str, Any]:
        """배치로 리뷰를 수집합니다."""
        self.log(f"Starting batch review collection for {len(app_ids)} apps...")

        for i, app_id in enumerate(app_ids, 1):
            if i % 10 == 0:
                self.log(f"Progress: {i}/{len(app_ids)}")

            try:
                self.collect_reviews_for_app(app_id)
            except Exception as e:
                self.log(f"Error processing app {app_id}: {e}")
                self.stats['errors'] += 1

            time.sleep(REQUEST_DELAY)

        self.log(f"Batch review collection completed. Stats: {self.stats}")
        return self.stats


def get_apps_for_review_collection(limit: int = 1000) -> List[str]:
    """리뷰 수집할 앱 ID 목록을 가져옵니다."""
    from database.app_details_db import get_connection as get_details_connection

    # 상세정보가 수집된 앱 중 리뷰 수집이 안 된 것들
    details_conn = get_details_connection()
    cursor = details_conn.cursor()

    cursor.execute("""
        SELECT app_id FROM collection_status
        WHERE platform = 'app_store'
          AND details_collected_at IS NOT NULL
          AND (reviews_collected_at IS NULL OR initial_review_done = 0)
        ORDER BY details_collected_at DESC
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
