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
    is_failed_app, get_failed_app_ids, get_abandoned_apps_to_skip
)
from database.sitemap_apps_db import get_connection as get_sitemap_connection

PLATFORM = 'play_store'
REQUEST_DELAY = 0.01  # 10ms
MAX_REVIEWS_TOTAL = 20000  # 실행당 최대 수집 리뷰 수 (무한루프 방지)
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

    def collect_reviews_for_pair(self, app_id: str, lang: str, country: str, quota: int,
                                   existing_ids: Set[str], stop_on_existing: bool) -> tuple:
        """
        특정 (language, country) 쌍에서 리뷰를 수집합니다.
        Returns: (collected_count, hit_existing, has_more)
        """
        collected = 0
        hit_existing = False
        has_more = False
        continuation_token = None

        while collected < quota:
            result, continuation_token = self.fetch_reviews(
                app_id, lang=lang, country=country,
                count=min(BATCH_SIZE, quota - collected),
                continuation_token=continuation_token
            )

            if not result:
                break  # 더 이상 리뷰 없음

            new_reviews = []
            for review_data in result:
                review_id = review_data.get('reviewId', '')
                if review_id in existing_ids:
                    if stop_on_existing:
                        hit_existing = True
                        break
                    continue  # 중복 건너뛰기

                review = self.parse_review(review_data, app_id, country, lang)
                new_reviews.append(review)
                existing_ids.add(review_id)

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

            if not continuation_token:
                break  # 더 이상 페이지 없음

            # 할당량 도달했는데 continuation_token 있으면 더 있음
            if collected >= quota and continuation_token:
                has_more = True
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
        if not pairs:
            pairs = [('en', 'us')]

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

        # 쌍별 할당량 계산
        per_pair_quota = remaining // len(pairs)
        if per_pair_quota < 1:
            per_pair_quota = 1

        self.log(f"  [{app_id}] {mode} 시작 | (lang,country) 쌍: {len(pairs)}개 | "
                 f"기존: {current_count}건 | 목표: {remaining}건 (쌍당 {per_pair_quota}건)")

        collected_total = 0
        hit_existing_any = False
        pairs_with_more = []  # 추가 수집 가능한 쌍
        pair_results = {}

        # === 1차: 쌍별 균등 분배 ===
        for lang, country in pairs:
            collected, hit_existing, has_more = self.collect_reviews_for_pair(
                app_id, lang, country, per_pair_quota, existing_ids,
                stop_on_existing=initial_done  # 추가 수집시에만 기존 리뷰에서 중단
            )
            collected_total += collected
            pair_key = f"{lang}-{country}"
            pair_results[pair_key] = collected

            if hit_existing:
                hit_existing_any = True
                self.log(f"    [{pair_key.upper()}] {collected}건 (기존 리뷰 발견, 중단)")
            elif has_more:
                pairs_with_more.append((lang, country))
                self.log(f"    [{pair_key.upper()}] {collected}건 (더 있음)")
            elif collected > 0:
                self.log(f"    [{pair_key.upper()}] {collected}건")

        # === 2차: 잔여 분배 (리뷰가 더 있는 쌍에서 추가 수집) ===
        remaining_after_first = remaining - collected_total

        if remaining_after_first > 0 and pairs_with_more and not hit_existing_any:
            extra_per_pair = remaining_after_first // len(pairs_with_more)
            if extra_per_pair < 1:
                extra_per_pair = remaining_after_first

            self.log(f"    [2차 분배] 잔여 {remaining_after_first}건 → {len(pairs_with_more)}개 쌍")

            for lang, country in pairs_with_more:
                if collected_total >= remaining:
                    break

                extra_quota = min(extra_per_pair, remaining - collected_total)
                collected, hit_existing, _ = self.collect_reviews_for_pair(
                    app_id, lang, country, extra_quota, existing_ids,
                    stop_on_existing=initial_done
                )
                collected_total += collected
                pair_key = f"{lang}-{country}"
                pair_results[pair_key] = pair_results.get(pair_key, 0) + collected

                if collected > 0:
                    self.log(f"    [{pair_key.upper()}] +{collected}건 (2차)")

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
        self.log(f"=== Play Store 리뷰 수집 시작 ===")
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
        self.log(f"=== Play Store 리뷰 수집 완료 ===")
        self.log(f"처리: {self.stats['apps_processed']}개 | "
                 f"건너뜀: {self.stats['apps_skipped']}개 | "
                 f"수집: {self.stats['reviews_collected']}건 | "
                 f"오류: {self.stats['errors']}개")
        return self.stats


def get_apps_for_review_collection(limit: int = 1000) -> List[str]:
    """리뷰 수집할 앱 ID 목록을 가져옵니다.

    수집 정책:
    - 상세정보가 수집된 앱만 대상
    - 활성 앱: 매번 수집 (crontab으로 매일 실행)
    - 버려진 앱 (2년 이상 업데이트 안 됨): 7일에 1번 수집
    - 실패한 앱: 제외
    """
    from database.app_details_db import get_connection as get_details_connection

    # 제외할 앱 ID: 실패한 앱 + 7일 이내 수집된 버려진 앱
    exclude_ids = get_failed_app_ids(PLATFORM) | get_abandoned_apps_to_skip(PLATFORM, 'reviews_collected_at')

    # 상세정보가 수집된 앱 목록
    details_conn = get_details_connection()
    cursor = details_conn.cursor()
    cursor.execute("""
        SELECT app_id FROM collection_status
        WHERE platform = 'play_store' AND details_collected_at IS NOT NULL
        ORDER BY details_collected_at DESC
    """)

    result = []
    for row in cursor.fetchall():
        if row['app_id'] not in exclude_ids:
            result.append(row['app_id'])
            if len(result) >= limit:
                break

    details_conn.close()
    return result


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
