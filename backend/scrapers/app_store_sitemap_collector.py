"""
App Store Sitemap Collector
App Store sitemap에서 앱 로컬라이제이션 정보를 수집합니다.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List, Dict, Any
from scrapers.sitemap_utils import (
    fetch_url, fetch_and_hash, parse_sitemap_index, parse_sitemap_urlset,
    extract_app_store_app_id, parse_hreflang, get_filename_from_url
)
from database.sitemap_apps_db import (
    get_sitemap_file_hash, update_sitemap_file, upsert_app_localizations_batch
)

PLATFORM = 'app_store'

# App Store sitemap index URLs
SITEMAP_INDEX_URLS = [
    'https://apps.apple.com/sitemaps_apps_index_app_1.xml',
    'https://apps.apple.com/sitemaps_apps_index_new-app_1.xml',
]


class AppStoreSitemapCollector:
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.stats = {
            'sitemap_files_processed': 0,
            'sitemap_files_skipped': 0,
            'new_localizations': 0,
            'total_localizations': 0,
            'errors': 0
        }

    def log(self, message: str):
        if self.verbose:
            print(f"[AppStore] {message}")

    def collect_sitemap_index(self, index_url: str) -> List[str]:
        """sitemap index에서 개별 sitemap URL들을 가져옵니다."""
        self.log(f"Fetching sitemap index: {index_url}")
        content = fetch_url(index_url)
        if not content:
            self.log(f"Failed to fetch sitemap index: {index_url}")
            return []

        sitemap_urls = parse_sitemap_index(content)
        self.log(f"Found {len(sitemap_urls)} sitemap files in index")
        return sitemap_urls

    def process_sitemap_file(self, sitemap_url: str) -> int:
        """개별 sitemap 파일을 처리합니다. 새로 추가된 로컬라이제이션 수를 반환."""
        filename = get_filename_from_url(sitemap_url)

        # 파일 다운로드 및 해시 계산
        content, content_hash = fetch_and_hash(sitemap_url)
        if not content or not content_hash:
            self.log(f"Failed to fetch: {filename}")
            self.stats['errors'] += 1
            return 0

        # 기존 해시와 비교
        existing_hash = get_sitemap_file_hash(sitemap_url)
        if existing_hash == content_hash:
            self.log(f"Skipping (unchanged): {filename}")
            self.stats['sitemap_files_skipped'] += 1
            return 0

        self.log(f"Processing: {filename}")

        # sitemap 파싱
        url_entries = parse_sitemap_urlset(content)
        if not url_entries:
            self.log(f"No entries found in: {filename}")
            return 0

        # 로컬라이제이션 정보 추출
        localizations = []
        for entry in url_entries:
            for hreflang_info in entry.get('hreflangs', []):
                hreflang = hreflang_info.get('hreflang', '')
                href = hreflang_info.get('href', '')

                if not hreflang or not href:
                    continue

                # 앱 ID 추출
                app_id = extract_app_store_app_id(href)
                if not app_id:
                    continue

                # hreflang 파싱
                language, country = parse_hreflang(hreflang)
                if not language or not country:
                    continue

                localizations.append({
                    'platform': PLATFORM,
                    'app_id': app_id,
                    'language': language,
                    'country': country,
                    'href': href,
                    'source_file': filename
                })

        # DB에 저장
        new_count = upsert_app_localizations_batch(localizations)

        # sitemap 파일 정보 업데이트
        update_sitemap_file(PLATFORM, sitemap_url, content_hash, len(localizations))

        self.stats['sitemap_files_processed'] += 1
        self.stats['new_localizations'] += new_count
        self.stats['total_localizations'] += len(localizations)

        self.log(f"Processed {filename}: {len(localizations)} localizations ({new_count} new)")
        return new_count

    def collect_all(self) -> Dict[str, Any]:
        """모든 sitemap index에서 앱 정보를 수집합니다."""
        self.log("Starting App Store sitemap collection...")

        all_sitemap_urls = []
        for index_url in SITEMAP_INDEX_URLS:
            sitemap_urls = self.collect_sitemap_index(index_url)
            all_sitemap_urls.extend(sitemap_urls)

        self.log(f"Total sitemap files to process: {len(all_sitemap_urls)}")

        for i, sitemap_url in enumerate(all_sitemap_urls, 1):
            self.log(f"Progress: {i}/{len(all_sitemap_urls)}")
            self.process_sitemap_file(sitemap_url)

        self.log("Collection completed!")
        self.log(f"Stats: {self.stats}")
        return self.stats


def main():
    from database.sitemap_apps_db import init_database
    init_database()

    collector = AppStoreSitemapCollector(verbose=True)
    stats = collector.collect_all()
    print(f"\nFinal Stats: {stats}")


if __name__ == '__main__':
    main()
