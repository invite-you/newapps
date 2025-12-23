# -*- coding: utf-8 -*-
"""
Sitemap 기반 앱 ID 수집기
- Google Play Store sitemap
- Apple App Store sitemap
- 앱 ID 추출 및 delta tracking
"""
import gzip
import io
import re
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Set, Dict, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import (
    LOG_FORMAT, REQUEST_TIMEOUT, REQUEST_DELAY,
    get_request_kwargs, SSL_VERIFY
)
from database.sitemap_db import (
    get_known_app_ids, save_discovered_apps,
    save_sitemap_snapshot, init_sitemap_database
)


def log_step(step: str, message: str, start_time: Optional[datetime] = None):
    """타임스탬프 로그 출력"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    duration = (datetime.now() - start_time).total_seconds() if start_time else 0
    print(LOG_FORMAT.format(
        timestamp=timestamp,
        step=step,
        message=message,
        duration=f"{duration:.2f}"
    ))


class GooglePlaySitemapCollector:
    """Google Play Store Sitemap 수집기"""

    ROBOTS_URL = "https://play.google.com/robots.txt"
    APP_URL_PATTERN = re.compile(r'/store/apps/details\?id=([a-zA-Z0-9._]+)')

    def __init__(self):
        self.request_kwargs = get_request_kwargs()

    def get_sitemap_index_urls(self) -> List[str]:
        """robots.txt에서 sitemap index URL 추출"""
        log_step("Google Play", "robots.txt에서 sitemap 확인", datetime.now())

        try:
            response = requests.get(self.ROBOTS_URL, **self.request_kwargs)
            response.raise_for_status()

            sitemap_urls = []
            for line in response.text.split('\n'):
                if line.startswith('Sitemap:'):
                    url = line.split(':', 1)[1].strip()
                    sitemap_urls.append(url)

            log_step("Google Play", f"Sitemap index 발견: {len(sitemap_urls)}개", datetime.now())
            return sitemap_urls

        except Exception as e:
            log_step("Google Play", f"robots.txt 읽기 실패: {e}", datetime.now())
            return []

    def parse_sitemap_index(self, index_url: str) -> List[str]:
        """Sitemap index XML에서 개별 sitemap URL 추출"""
        try:
            response = requests.get(index_url, **self.request_kwargs)
            response.raise_for_status()

            # XML 파싱
            root = ET.fromstring(response.content)
            ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            sitemap_urls = []
            for sitemap in root.findall('.//sm:sitemap/sm:loc', ns):
                sitemap_urls.append(sitemap.text)

            return sitemap_urls

        except Exception as e:
            log_step("Google Play", f"Sitemap index 파싱 실패 ({index_url}): {e}", datetime.now())
            return []

    def fetch_and_parse_sitemap(self, sitemap_url: str) -> Set[str]:
        """개별 sitemap.gz 파일에서 앱 ID 추출"""
        app_ids = set()

        try:
            response = requests.get(sitemap_url, **self.request_kwargs)
            response.raise_for_status()

            # gzip 압축 해제
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                content = f.read().decode('utf-8')

            # 앱 URL에서 package ID 추출
            matches = self.APP_URL_PATTERN.findall(content)
            app_ids.update(matches)

        except Exception as e:
            # 개별 sitemap 실패는 조용히 넘김 (전체 수집 중)
            pass

        return app_ids

    def collect_all_app_ids(self, limit: int = None) -> Tuple[Set[str], int]:
        """
        모든 sitemap에서 앱 ID 수집

        Args:
            limit: 처리할 sitemap 수 제한 (None이면 전체 처리, 테스트 목적)

        Returns:
            (app_ids, sitemap_count): 수집된 앱 ID 집합, 처리한 sitemap 수
        """
        start_time = datetime.now()
        log_step("Google Play Sitemap", "전체 앱 ID 수집 시작", start_time)

        all_app_ids = set()
        sitemap_count = 0

        # sitemap index URLs 가져오기
        index_urls = self.get_sitemap_index_urls()

        for index_url in index_urls:
            log_step("Google Play", f"Sitemap index 처리: {index_url}", datetime.now())

            sitemap_urls = self.parse_sitemap_index(index_url)
            log_step("Google Play", f"개별 sitemap 발견: {len(sitemap_urls)}개", datetime.now())

            # 테스트 목적으로 제한 적용
            if limit is not None:
                sitemap_urls = sitemap_urls[:limit]

            # 병렬 처리
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(self.fetch_and_parse_sitemap, url): url
                          for url in sitemap_urls}

                for future in as_completed(futures):
                    try:
                        app_ids = future.result()
                        all_app_ids.update(app_ids)
                        sitemap_count += 1

                        if sitemap_count % 100 == 0:
                            log_step("Google Play", f"진행: {sitemap_count}개 sitemap, {len(all_app_ids)}개 앱 ID", datetime.now())

                    except Exception as e:
                        continue

        log_step("Google Play Sitemap", f"수집 완료: {len(all_app_ids)}개 앱 ID", start_time)
        return all_app_ids, sitemap_count


class AppStoreSitemapCollector:
    """Apple App Store Sitemap 수집기"""

    ROBOTS_URL = "https://apps.apple.com/robots.txt"
    APP_ID_PATTERN = re.compile(r'/id(\d+)')

    # 관심 sitemap 타입 (new-app도 동일하게 처리)
    SITEMAP_TYPES = ['app', 'new-app']

    def __init__(self):
        self.request_kwargs = get_request_kwargs()

    def get_sitemap_index_urls(self) -> Dict[str, List[str]]:
        """robots.txt에서 sitemap index URL 추출 (타입별)"""
        log_step("App Store", "robots.txt에서 sitemap 확인", datetime.now())

        try:
            response = requests.get(self.ROBOTS_URL, **self.request_kwargs)
            response.raise_for_status()

            result = {t: [] for t in self.SITEMAP_TYPES}

            for line in response.text.split('\n'):
                if line.startswith('Sitemap:'):
                    url = line.split(':', 1)[1].strip()

                    for sitemap_type in self.SITEMAP_TYPES:
                        if f'index_{sitemap_type}_' in url:
                            result[sitemap_type].append(url)

            for t, urls in result.items():
                log_step("App Store", f"{t} sitemap index: {len(urls)}개", datetime.now())

            return result

        except Exception as e:
            log_step("App Store", f"robots.txt 읽기 실패: {e}", datetime.now())
            return {t: [] for t in self.SITEMAP_TYPES}

    def parse_sitemap_index(self, index_url: str) -> List[str]:
        """Sitemap index XML에서 개별 sitemap URL 추출"""
        try:
            response = requests.get(index_url, **self.request_kwargs)
            response.raise_for_status()

            root = ET.fromstring(response.content)
            ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            sitemap_urls = []
            for sitemap in root.findall('.//sm:sitemap/sm:loc', ns):
                sitemap_urls.append(sitemap.text)

            return sitemap_urls

        except Exception as e:
            log_step("App Store", f"Sitemap index 파싱 실패: {e}", datetime.now())
            return []

    def fetch_and_parse_sitemap(self, sitemap_url: str) -> Tuple[Set[str], str]:
        """
        개별 sitemap.gz에서 앱 ID 추출

        Returns:
            (app_ids, country_code): 앱 ID 집합, 국가 코드
        """
        app_ids = set()
        country_code = None

        try:
            response = requests.get(sitemap_url, **self.request_kwargs)
            response.raise_for_status()

            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                content = f.read().decode('utf-8')

            # 앱 ID 추출
            matches = self.APP_ID_PATTERN.findall(content)
            app_ids.update(matches)

            # 국가 코드 추출 (URL에서)
            # 예: https://apps.apple.com/kr/app/... -> kr
            country_match = re.search(r'apple\.com/([a-z]{2})/', content)
            if country_match:
                country_code = country_match.group(1)

        except Exception as e:
            pass

        return app_ids, country_code

    def collect_all_app_ids(self, limit: int = None) -> Dict[str, Tuple[Set[str], int]]:
        """
        모든 sitemap에서 앱 ID 수집 (타입별)

        Args:
            limit: 타입별 처리할 sitemap 수 제한 (None이면 전체 처리)

        Returns:
            {sitemap_type: (app_ids, sitemap_count)}
        """
        start_time = datetime.now()
        log_step("App Store Sitemap", "전체 앱 ID 수집 시작", start_time)

        result = {}

        # 타입별 sitemap index URL
        index_urls_by_type = self.get_sitemap_index_urls()

        for sitemap_type, index_urls in index_urls_by_type.items():
            all_app_ids = set()
            sitemap_count = 0

            log_step("App Store", f"{sitemap_type} 타입 처리 시작", datetime.now())

            for index_url in index_urls:
                sitemap_urls = self.parse_sitemap_index(index_url)

                # 테스트 목적으로 제한 적용
                if limit is not None:
                    sitemap_urls = sitemap_urls[:limit]

                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = {executor.submit(self.fetch_and_parse_sitemap, url): url
                              for url in sitemap_urls}

                    for future in as_completed(futures):
                        try:
                            app_ids, _ = future.result()
                            all_app_ids.update(app_ids)
                            sitemap_count += 1

                            if sitemap_count % 50 == 0:
                                log_step("App Store", f"{sitemap_type} 진행: {sitemap_count}개, {len(all_app_ids)}개 앱", datetime.now())

                        except Exception as e:
                            continue

            result[sitemap_type] = (all_app_ids, sitemap_count)
            log_step("App Store", f"{sitemap_type} 완료: {len(all_app_ids)}개 앱 ID", datetime.now())

        log_step("App Store Sitemap", "수집 완료", start_time)
        return result


def collect_and_save_google_play_apps(limit: int = None) -> Dict:
    """
    Google Play sitemap에서 앱 수집 및 저장

    Args:
        limit: 처리할 sitemap 수 제한 (None이면 전체 처리)

    Returns:
        수집 결과 통계
    """
    start_time = datetime.now()
    log_step("Google Play 수집", "시작", start_time)

    # DB 초기화
    init_sitemap_database()

    # 기존에 알려진 앱 ID
    known_ids = get_known_app_ids('google_play')
    log_step("Google Play", f"기존 앱 ID: {len(known_ids)}개", datetime.now())

    # 새로 수집
    collector = GooglePlaySitemapCollector()
    all_app_ids, sitemap_count = collector.collect_all_app_ids(limit)

    # 신규 앱만 필터링
    new_app_ids = all_app_ids - known_ids

    log_step("Google Play", f"신규 앱 ID: {len(new_app_ids)}개 (전체: {len(all_app_ids)}개)", datetime.now())

    # 저장
    if all_app_ids:
        new_count, updated_count = save_discovered_apps(
            list(all_app_ids),
            'google_play',
            sitemap_source='sitemap'
        )

        save_sitemap_snapshot(
            'google_play',
            'sitemaps-index',
            len(all_app_ids),
            new_count
        )

        log_step("Google Play", f"저장 완료: 신규 {new_count}개, 업데이트 {updated_count}개", datetime.now())

    log_step("Google Play 수집", "완료", start_time)

    return {
        'platform': 'google_play',
        'total_collected': len(all_app_ids),
        'new_apps': len(new_app_ids),
        'sitemaps_processed': sitemap_count,
        'duration_seconds': (datetime.now() - start_time).total_seconds()
    }


def collect_and_save_app_store_apps(limit: int = None) -> Dict:
    """
    App Store sitemap에서 앱 수집 및 저장

    Args:
        limit: 타입별 처리할 sitemap 수 제한 (None이면 전체 처리)

    Returns:
        수집 결과 통계
    """
    start_time = datetime.now()
    log_step("App Store 수집", "시작", start_time)

    # DB 초기화
    init_sitemap_database()

    # 기존에 알려진 앱 ID
    known_ids = get_known_app_ids('app_store')
    log_step("App Store", f"기존 앱 ID: {len(known_ids)}개", datetime.now())

    # 새로 수집
    collector = AppStoreSitemapCollector()
    results = collector.collect_all_app_ids(limit)

    stats = {
        'platform': 'app_store',
        'by_type': {},
        'total_collected': 0,
        'new_apps': 0,
        'duration_seconds': 0
    }

    for sitemap_type, (app_ids, sitemap_count) in results.items():
        new_app_ids = app_ids - known_ids

        log_step("App Store", f"{sitemap_type}: {len(new_app_ids)}개 신규 (전체: {len(app_ids)}개)", datetime.now())

        if app_ids:
            # 모든 sitemap 타입을 동일하게 처리 (is_new_app 플래그 제거)
            new_count, updated_count = save_discovered_apps(
                list(app_ids),
                'app_store',
                sitemap_source=sitemap_type
            )

            save_sitemap_snapshot(
                'app_store',
                sitemap_type,
                len(app_ids),
                new_count
            )

        stats['by_type'][sitemap_type] = {
            'total': len(app_ids),
            'new': len(new_app_ids),
            'sitemaps': sitemap_count
        }
        stats['total_collected'] += len(app_ids)
        stats['new_apps'] += len(new_app_ids)

        # known_ids 업데이트 (다음 타입 처리 시 중복 방지)
        known_ids.update(app_ids)

    stats['duration_seconds'] = (datetime.now() - start_time).total_seconds()
    log_step("App Store 수집", "완료", start_time)

    return stats


def collect_all_sitemaps(google_limit: int = None, appstore_limit: int = None) -> Dict:
    """
    모든 sitemap에서 앱 수집

    Args:
        google_limit: Google Play sitemap 처리 수 제한 (None이면 전체)
        appstore_limit: App Store sitemap 타입별 처리 수 제한 (None이면 전체)

    Returns:
        전체 수집 결과
    """
    start_time = datetime.now()
    log_step("전체 Sitemap 수집", "시작", start_time)

    results = {
        'google_play': collect_and_save_google_play_apps(google_limit),
        'app_store': collect_and_save_app_store_apps(appstore_limit),
        'total_duration_seconds': 0
    }

    results['total_duration_seconds'] = (datetime.now() - start_time).total_seconds()

    # 요약 출력
    print("\n" + "=" * 60)
    print("Sitemap 수집 결과 요약")
    print("=" * 60)
    print(f"Google Play: {results['google_play']['total_collected']:,}개 앱 "
          f"(신규: {results['google_play']['new_apps']:,}개)")

    if 'by_type' in results['app_store']:
        for t, s in results['app_store']['by_type'].items():
            print(f"App Store ({t}): {s['total']:,}개 앱 (신규: {s['new']:,}개)")

    print(f"총 소요 시간: {results['total_duration_seconds']:.1f}초")
    print("=" * 60 + "\n")

    log_step("전체 Sitemap 수집", "완료", start_time)

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Sitemap 기반 앱 ID 수집')
    parser.add_argument('--google-limit', type=int, default=None,
                        help='Google Play sitemap 처리 수 제한 (기본: 전체)')
    parser.add_argument('--appstore-limit', type=int, default=None,
                        help='App Store sitemap 타입별 처리 수 제한 (기본: 전체)')
    parser.add_argument('--google-only', action='store_true',
                        help='Google Play만 수집')
    parser.add_argument('--appstore-only', action='store_true',
                        help='App Store만 수집')

    args = parser.parse_args()

    if args.google_only:
        result = collect_and_save_google_play_apps(args.google_limit)
        print(f"\n결과: {result}")
    elif args.appstore_only:
        result = collect_and_save_app_store_apps(args.appstore_limit)
        print(f"\n결과: {result}")
    else:
        results = collect_all_sitemaps(args.google_limit, args.appstore_limit)
