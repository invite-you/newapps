"""
Play Store Sitemap Collector
Play Store sitemap에서 앱 로컬라이제이션 정보를 수집합니다.

최적화: 언어당 최적의 국가 1개만 저장하여 DB 용량 절감
"""
import sys
import os
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List, Dict, Any, Tuple
from scrapers.sitemap_utils import (
    fetch_url, fetch_and_hash, parse_sitemap_index, parse_sitemap_urlset,
    extract_play_store_app_id, parse_hreflang, get_filename_from_url,
    is_play_store_app_url, filter_best_country_per_language, log_sitemap_step_end
)
from database.sitemap_apps_db import (
    get_sitemap_file_hash, update_sitemap_file, upsert_app_localizations_batch
)
from utils.logger import get_timestamped_logger
from utils.network_binding import configure_network_binding
PLATFORM = 'play_store'
LOG_FILE_PREFIX = "sitemap_play_store"

# Play Store sitemap index URLs
SITEMAP_INDEX_URLS = [
    'https://play.google.com/sitemaps/sitemaps-index-0.xml',
    'https://play.google.com/sitemaps/sitemaps-index-1.xml',
]


class PlayStoreSitemapCollector:
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.logger = get_timestamped_logger("play_store_sitemap", file_prefix=LOG_FILE_PREFIX)
        configure_network_binding(logger=self.logger)
        self.stats = {
            'sitemap_indexes_processed': 0,   # 처리한 index 수
            'sitemap_indexes_unchanged': 0,   # 변경 없는 index 수
            'sitemap_files_processed': 0,
            'sitemap_files_skipped': 0,
            'new_localizations': 0,
            'total_localizations': 0,
            'raw_localizations': 0,  # 필터링 전 원본 수
            'filtered_out': 0,       # 필터링으로 제외된 수
            'skipped_non_apps': 0,
            'errors': 0
        }

    def log(self, message: str):
        if self.verbose:
            self.logger.info(f"[PlayStore] {message}")

    def collect_sitemap_index(self, index_url: str) -> Tuple[List[str], bool, bool]:
        """sitemap index에서 개별 sitemap URL들을 가져옵니다.

        Returns: (sitemap_urls, index_changed, success)
            - sitemap_urls: 개별 sitemap URL 리스트
            - index_changed: index가 변경되었는지 여부
            - success: 다운로드 성공 여부 (False면 에러 발생)
        """
        self.log(f"Fetching sitemap index: {index_url}")
        content, content_hash = fetch_and_hash(index_url, logger=self.logger)
        if not content or not content_hash:
            self.log(f"Failed to fetch sitemap index: {index_url}")
            return [], False, False  # 실패

        # 기존 해시와 비교하여 변경 여부 확인
        existing_hash = get_sitemap_file_hash(index_url)
        index_changed = existing_hash != content_hash

        if not index_changed:
            self.log(f"Sitemap index unchanged (hash={content_hash[:8]}...): {index_url}")
            # index가 변경되지 않았어도 URL 목록은 반환 (개별 파일 해시 검사는 별도로 수행)
            sitemap_urls = parse_sitemap_index(content, logger=self.logger)
            return sitemap_urls, False, True  # 성공, 변경 없음

        # index가 변경된 경우 해시 업데이트
        sitemap_urls = parse_sitemap_index(content, logger=self.logger)
        update_sitemap_file(PLATFORM, index_url, content_hash, len(sitemap_urls))
        self.log(f"Sitemap index updated (hash={content_hash[:8]}...): Found {len(sitemap_urls)} sitemap files")
        return sitemap_urls, True, True  # 성공, 변경됨

    def process_sitemap_file(self, sitemap_url: str) -> int:
        """개별 sitemap 파일을 처리합니다. 새로 추가된 로컬라이제이션 수를 반환."""
        start_ts = datetime.now().isoformat()
        start_perf = time.perf_counter()
        filename = get_filename_from_url(sitemap_url)
        self.logger.info(f"[STEP START] sitemap_file={filename} | {start_ts}")

        # 파일 다운로드 및 해시 계산
        content, content_hash = fetch_and_hash(sitemap_url, logger=self.logger)
        if not content or not content_hash:
            self.log(f"Failed to fetch: {filename}")
            self.stats['errors'] += 1
            log_sitemap_step_end(self.logger, filename, start_perf, "FAIL")
            return 0

        # 기존 해시와 비교
        existing_hash = get_sitemap_file_hash(sitemap_url)
        if existing_hash == content_hash:
            self.log(f"Skipping (unchanged): {filename}")
            self.stats['sitemap_files_skipped'] += 1
            log_sitemap_step_end(self.logger, filename, start_perf, "SKIP")
            return 0

        self.log(f"Processing: {filename}")

        # sitemap 파싱
        url_entries = parse_sitemap_urlset(content, logger=self.logger)
        if not url_entries:
            self.log(f"No entries found in: {filename}")
            log_sitemap_step_end(self.logger, filename, start_perf, "EMPTY")
            return 0

        # 1단계: 모든 로컬라이제이션 정보 추출 (필터링 전)
        raw_localizations = []
        skipped = 0

        for entry in url_entries:
            for hreflang_info in entry.get('hreflangs', []):
                hreflang = hreflang_info.get('hreflang', '')
                href = hreflang_info.get('href', '')

                if not hreflang or not href:
                    continue

                # 앱 URL이 아닌 경우 건너뛰기 (book, movie 등)
                if not is_play_store_app_url(href):
                    skipped += 1
                    continue

                # 앱 ID 추출
                app_id = extract_play_store_app_id(href)
                if not app_id:
                    continue

                # hreflang 파싱
                language, country = parse_hreflang(hreflang)
                if not language or not country:
                    continue

                raw_localizations.append({
                    'platform': PLATFORM,
                    'app_id': app_id,
                    'language': language,
                    'country': country,
                    'source_file': filename
                })

        self.stats['skipped_non_apps'] += skipped

        # 2단계: 언어당 최적 국가 1개만 필터링
        localizations = filter_best_country_per_language(raw_localizations)
        filtered_out = len(raw_localizations) - len(localizations)

        # DB에 저장
        new_count = upsert_app_localizations_batch(localizations)

        # sitemap 파일 정보 업데이트
        update_sitemap_file(PLATFORM, sitemap_url, content_hash, len(localizations))

        self.stats['sitemap_files_processed'] += 1
        self.stats['new_localizations'] += new_count
        self.stats['total_localizations'] += len(localizations)
        self.stats['raw_localizations'] += len(raw_localizations)
        self.stats['filtered_out'] += filtered_out

        self.log(f"Processed {filename}: {len(raw_localizations)} raw -> {len(localizations)} filtered ({new_count} new)")
        log_sitemap_step_end(self.logger, filename, start_perf, "OK")
        return new_count

    def collect_all(self) -> Dict[str, Any]:
        """모든 sitemap index에서 앱 정보를 수집합니다."""
        start_perf = time.perf_counter()
        start_ts = datetime.now().isoformat()
        self.log("Starting Play Store sitemap collection...")
        self.logger.info(f"[STEP START] collect_all | {start_ts}")

        all_sitemap_urls = []
        for index_url in SITEMAP_INDEX_URLS:
            sitemap_urls, index_changed, success = self.collect_sitemap_index(index_url)
            self.stats['sitemap_indexes_processed'] += 1
            if not success:
                self.stats['errors'] += 1
            elif not index_changed:
                self.stats['sitemap_indexes_unchanged'] += 1
            all_sitemap_urls.extend(sitemap_urls)

        before_dedup_count = len(all_sitemap_urls)
        unique_sitemap_urls = list(dict.fromkeys(all_sitemap_urls))
        after_dedup_count = len(unique_sitemap_urls)
        self.log(
            "Sitemap URLs deduped (order preserved): "
            f"{before_dedup_count} -> {after_dedup_count}"
        )
        all_sitemap_urls = unique_sitemap_urls

        self.log(f"Total sitemap files to process: {len(all_sitemap_urls)}")

        for i, sitemap_url in enumerate(all_sitemap_urls, 1):
            self.log(f"Progress: {i}/{len(all_sitemap_urls)}")
            self.process_sitemap_file(sitemap_url)

        self.log("Collection completed!")
        self.log(f"Stats: {self.stats}")
        elapsed = time.perf_counter() - start_perf
        self.logger.info(
            f"[STEP END] collect_all | {datetime.now().isoformat()} | "
            f"elapsed={elapsed:.2f}s | status=OK"
        )
        return self.stats


def main():
    from database.sitemap_apps_db import init_database
    init_database()

    collector = PlayStoreSitemapCollector(verbose=True)
    stats = collector.collect_all()
    collector.logger.info(f"\nFinal Stats: {stats}")


if __name__ == '__main__':
    main()
