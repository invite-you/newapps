"""
Play Store Reviews Collector
google-play-scraper 라이브러리를 사용하여 앱 리뷰를 수집합니다.
"""
import sys
import os
import time
from typing import List, Dict, Any, Optional, Set

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google_play_scraper import reviews, Sort
from google_play_scraper.exceptions import NotFoundError

from database.app_details_db import (
    init_database, insert_reviews_batch, get_all_review_ids,
    get_collection_status, update_collection_status, get_review_count,
    is_failed_app
)
from database.sitemap_apps_db import get_connection as get_sitemap_connection

PLATFORM = 'play_store'
REQUEST_DELAY = 0.01  # 10ms
MAX_REVIEWS_TOTAL = 10000  # 앱당 최대 리뷰 수
BATCH_SIZE = 100  # 한 번에 가져올 리뷰 수


class PlayStoreReviewsCollector:
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
            print(f"[PlayStore Reviews] {message}")

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

    def fetch_reviews(self, app_id: str, lang: str = 'en', country: str = 'us',
                      count: int = BATCH_SIZE, continuation_token: str = None) -> tuple:
        """리뷰를 가져옵니다."""
        try:
            result, token = reviews(
                app_id,
                lang=lang,
                country=country,
                sort=Sort.NEWEST,
                count=count,
                continuation_token=continuation_token
            )
            return result, token
        except NotFoundError:
            return [], None
        except Exception as e:
            self.log(f"Error fetching reviews for {app_id}: {e}")
            return [], None

    def parse_review(self, data: Dict, app_id: str, country: str, language: str) -> Dict:
        """리뷰 데이터를 DB 형식으로 변환합니다."""
        reviewed_at = data.get('at')
        replied_at = data.get('repliedAt')

        return {
            'app_id': app_id,
            'platform': PLATFORM,
            'review_id': data.get('reviewId', ''),
            'country': country,
            'language': language,
            'user_name': data.get('userName', ''),
            'user_image': data.get('userImage'),
            'score': data.get('score'),
            'title': None,  # Play Store에는 title 없음
            'content': data.get('content', ''),
            'thumbs_up_count': data.get('thumbsUpCount', 0),
            'app_version': data.get('reviewCreatedVersion'),
            'reviewed_at': reviewed_at.isoformat() if reviewed_at else None,
            'reply_content': data.get('replyContent'),
            'replied_at': replied_at.isoformat() if replied_at else None
        }

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

        # sitemap에서 (language, country) 쌍 가져오기
        pairs = self.get_app_language_country_pairs(app_id)
        if not pairs:
            pairs = [('en', 'us')]

        # 수집할 수 있는 리뷰 수 계산
        if initial_done:
            remaining = MAX_REVIEWS_TOTAL
        else:
            remaining = MAX_REVIEWS_TOTAL - current_count

        if remaining <= 0:
            self.stats['apps_skipped'] += 1
            return 0

        collected_total = 0
        hit_existing = False

        # 각 (language, country) 쌍에서 순차적으로 수집
        for lang, country in pairs:
            if collected_total >= remaining or hit_existing:
                break

            continuation_token = None

            while collected_total < remaining and not hit_existing:
                result, continuation_token = self.fetch_reviews(
                    app_id, lang=lang, country=country,
                    count=min(BATCH_SIZE, remaining - collected_total),
                    continuation_token=continuation_token
                )

                if not result:
                    break

                new_reviews = []
                for review_data in result:
                    review_id = review_data.get('reviewId', '')
                    if review_id in existing_ids:
                        hit_existing = True
                        break

                    review = self.parse_review(review_data, app_id, country, lang)
                    new_reviews.append(review)
                    existing_ids.add(review_id)

                if new_reviews:
                    inserted = insert_reviews_batch(new_reviews)
                    collected_total += inserted
                    self.stats['reviews_collected'] += inserted

                if not continuation_token:
                    break

                time.sleep(REQUEST_DELAY)

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

    details_conn = get_details_connection()
    cursor = details_conn.cursor()

    cursor.execute("""
        SELECT app_id FROM collection_status
        WHERE platform = 'play_store'
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
        collector = PlayStoreReviewsCollector(verbose=True)
        stats = collector.collect_batch(app_ids)
        print(f"\nFinal Stats: {stats}")


if __name__ == '__main__':
    main()
