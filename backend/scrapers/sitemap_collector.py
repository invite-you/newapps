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
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import get_request_kwargs, timing_tracker
from database.db import log_step
from database.sitemap_db import (
    get_known_app_ids, save_discovered_apps,
    save_sitemap_snapshot, init_sitemap_database,
    upsert_failed_sitemap_url, clear_failed_sitemap_url,
    get_failed_sitemap_urls, get_sitemap_retry_stats
)


class GooglePlaySitemapCollector:
    """Google Play Store Sitemap 수집기"""

    ROBOTS_URL = "https://play.google.com/robots.txt"
    APP_URL_PATTERN = re.compile(r'/store/apps/details\?id=([a-zA-Z0-9._]+)')

    def __init__(self):
        self.request_kwargs = get_request_kwargs()

    def get_sitemap_index_urls(self) -> List[str]:
        """robots.txt에서 sitemap index URL 추출"""
        log_step("Google Play", "robots.txt에서 sitemap 확인", "Google Play Sitemap")

        try:
            response = requests.get(self.ROBOTS_URL, **self.request_kwargs)
            response.raise_for_status()

            sitemap_urls = []
            for line in response.text.split('\n'):
                if line.startswith('Sitemap:'):
                    url = line.split(':', 1)[1].strip()
                    sitemap_urls.append(url)

            log_step("Google Play", f"Sitemap index 발견: {len(sitemap_urls)}개", "Google Play Sitemap")
            return sitemap_urls

        except Exception as e:
            log_step("Google Play", f"robots.txt 읽기 실패: {e}", "Google Play Sitemap")
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
            log_step("Google Play", f"Sitemap index 파싱 실패 ({index_url}): {e}", "Google Play Sitemap")
            return []

    def fetch_and_parse_sitemap(self, sitemap_url: str) -> Tuple[Dict[str, Dict], str, bool]:
        """
        개별 sitemap.gz 파일에서 앱 ID 및 메타데이터 추출

        Returns:
            (app_metadata, sitemap_filename, success):
                app_metadata: {app_id: {url, lastmod, changefreq, priority}}
                sitemap_filename: 파일명
                success: 성공 여부 (True/False)
        """
        app_metadata = {}
        sitemap_filename = sitemap_url.split('/')[-1]  # 파일명 추출
        success = False
        error_reason = None

        try:
            response = requests.get(sitemap_url, **self.request_kwargs)
            response.raise_for_status()

            # gzip 압축 해제
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                content = f.read().decode('utf-8')

            # XML 파싱하여 상세 정보 추출
            try:
                root = ET.fromstring(content)
                ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

                for url_elem in root.findall('.//sm:url', ns):
                    loc = url_elem.find('sm:loc', ns)
                    if loc is None:
                        continue

                    url_text = loc.text
                    match = self.APP_URL_PATTERN.search(url_text)
                    if match:
                        app_id = match.group(1)
                        meta = {'url': url_text}

                        # 추가 메타데이터 추출
                        lastmod = url_elem.find('sm:lastmod', ns)
                        if lastmod is not None and lastmod.text:
                            meta['lastmod'] = lastmod.text

                        changefreq = url_elem.find('sm:changefreq', ns)
                        if changefreq is not None and changefreq.text:
                            meta['changefreq'] = changefreq.text

                        priority = url_elem.find('sm:priority', ns)
                        if priority is not None and priority.text:
                            try:
                                meta['priority'] = float(priority.text)
                            except ValueError:
                                pass

                        app_metadata[app_id] = meta

            except ET.ParseError:
                # XML 파싱 실패 시 정규식으로 fallback
                matches = self.APP_URL_PATTERN.findall(content)
                for app_id in matches:
                    app_metadata[app_id] = {}

            # 성공 - 실패 기록 제거
            success = True
            clear_failed_sitemap_url(sitemap_url, 'google_play')

        except requests.RequestException as e:
            error_reason = f"request_error: {str(e)}"
            log_step("Google Play Sitemap", f"[오류] sitemap 요청 실패 ({sitemap_filename}): {str(e)}", "Google Play Sitemap")
        except gzip.BadGzipFile as e:
            error_reason = f"gzip_error: {str(e)}"
            log_step("Google Play Sitemap", f"[오류] gzip 압축 해제 실패 ({sitemap_filename}): {str(e)}", "Google Play Sitemap")
        except Exception as e:
            error_reason = f"{type(e).__name__}: {str(e)}"
            log_step("Google Play Sitemap", f"[오류] sitemap 처리 중 예외 ({sitemap_filename}): {type(e).__name__}: {str(e)}", "Google Play Sitemap")

        # 실패 시 기록
        if not success and error_reason:
            upsert_failed_sitemap_url(sitemap_url, 'google_play', error_reason)

        return app_metadata, sitemap_filename, success

    def collect_all_app_ids(self, limit: int = None, retry_failed: bool = True) -> Tuple[Dict[str, Dict], Dict[str, str], int]:
        """
        모든 sitemap에서 앱 ID 및 메타데이터 수집

        Args:
            limit: 처리할 sitemap 수 제한 (None이면 전체 처리, 테스트 목적)
            retry_failed: 이전에 실패한 sitemap URL도 재시도할지 여부

        Returns:
            (app_metadata, app_to_sitemap, sitemap_count):
                app_metadata: {app_id: {url, lastmod, changefreq, priority}}
                app_to_sitemap: {app_id: sitemap_filename}
                sitemap_count: 처리한 sitemap 수
        """
        timing_tracker.start_task("Google Play Sitemap")
        log_step("Google Play Sitemap", "전체 앱 ID 수집 시작", "Google Play Sitemap")

        all_app_metadata = {}
        app_to_sitemap = {}
        sitemap_count = 0
        failed_count = 0

        # sitemap index URLs 가져오기
        index_urls = self.get_sitemap_index_urls()

        for index_url in index_urls:
            log_step("Google Play", f"Sitemap index 처리: {index_url}", "Google Play Sitemap")

            sitemap_urls = self.parse_sitemap_index(index_url)
            log_step("Google Play", f"개별 sitemap 발견: {len(sitemap_urls)}개", "Google Play Sitemap")

            # 테스트 목적으로 제한 적용
            if limit is not None:
                sitemap_urls = sitemap_urls[:limit]

            # sitemap을 역순으로 처리 (최신 sitemap 먼저)
            sitemap_urls = list(reversed(sitemap_urls))

            # 병렬 처리 (4배 증가: 10 → 40 workers)
            with ThreadPoolExecutor(max_workers=40) as executor:
                futures = {executor.submit(self.fetch_and_parse_sitemap, url): url
                          for url in sitemap_urls}

                for future in as_completed(futures):
                    try:
                        app_metadata, sitemap_filename, success = future.result()
                        if success:
                            all_app_metadata.update(app_metadata)
                            # 각 앱에 대해 sitemap 파일명 매핑
                            for app_id in app_metadata:
                                app_to_sitemap[app_id] = sitemap_filename
                            sitemap_count += 1
                        else:
                            failed_count += 1

                        if (sitemap_count + failed_count) % 100 == 0:
                            log_step("Google Play", f"진행: {sitemap_count}개 성공, {failed_count}개 실패, {len(all_app_metadata)}개 앱 ID", "Google Play Sitemap")

                    except Exception as e:
                        sitemap_url = futures.get(future, "unknown")
                        log_step("Google Play Sitemap", f"[오류] 병렬 처리 중 예외 ({sitemap_url}): {type(e).__name__}: {str(e)}", "Google Play Sitemap")
                        upsert_failed_sitemap_url(sitemap_url, 'google_play', f"parallel_error: {str(e)}")
                        failed_count += 1
                        continue

        # 이전에 실패한 sitemap URL 재시도
        if retry_failed:
            retry_urls = get_failed_sitemap_urls('google_play')
            if retry_urls:
                log_step("Google Play Sitemap", f"실패한 sitemap {len(retry_urls)}개 재시도 시작", "Google Play Sitemap")
                retry_success = 0
                for retry_info in retry_urls:
                    sitemap_url = retry_info['sitemap_url']
                    app_metadata, sitemap_filename, success = self.fetch_and_parse_sitemap(sitemap_url)
                    if success:
                        all_app_metadata.update(app_metadata)
                        for app_id in app_metadata:
                            app_to_sitemap[app_id] = sitemap_filename
                        retry_success += 1
                        sitemap_count += 1

                if retry_success > 0:
                    log_step("Google Play Sitemap", f"재시도 성공: {retry_success}/{len(retry_urls)}개", "Google Play Sitemap")

        # 최종 통계 출력
        retry_stats = get_sitemap_retry_stats('google_play')
        if retry_stats['total_failed'] > 0:
            log_step("Google Play Sitemap", f"실패 통계: 총 {retry_stats['total_failed']}개, 임계치 초과 {retry_stats['exceeded_threshold']}개, 재시도 대기 {retry_stats['retryable']}개", "Google Play Sitemap")

        log_step("Google Play Sitemap", f"수집 완료: {len(all_app_metadata)}개 앱 ID (성공 {sitemap_count}개, 실패 {failed_count}개)", "Google Play Sitemap")
        return all_app_metadata, app_to_sitemap, sitemap_count


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
        log_step("App Store", "robots.txt에서 sitemap 확인", "App Store Sitemap")

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
                log_step("App Store", f"{t} sitemap index: {len(urls)}개", "App Store Sitemap")

            return result

        except Exception as e:
            log_step("App Store", f"robots.txt 읽기 실패: {e}", "App Store Sitemap")
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
            log_step("App Store", f"Sitemap index 파싱 실패: {e}", "App Store Sitemap")
            return []

    def fetch_and_parse_sitemap(self, sitemap_url: str) -> Tuple[Dict[str, Dict], str, str, bool]:
        """
        개별 sitemap.gz에서 앱 ID 및 메타데이터 추출

        Returns:
            (app_metadata, country_code, sitemap_filename, success):
                app_metadata: {app_id: {url, lastmod, changefreq, priority}}
                country_code: 국가 코드
                sitemap_filename: sitemap 파일명
                success: 성공 여부 (True/False)
        """
        app_metadata = {}
        country_code = None
        sitemap_filename = sitemap_url.split('/')[-1]  # 파일명 추출
        success = False
        error_reason = None

        try:
            response = requests.get(sitemap_url, **self.request_kwargs)
            response.raise_for_status()

            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                content = f.read().decode('utf-8')

            # XML 파싱하여 상세 정보 추출
            try:
                root = ET.fromstring(content)
                ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

                for url_elem in root.findall('.//sm:url', ns):
                    loc = url_elem.find('sm:loc', ns)
                    if loc is None:
                        continue

                    url_text = loc.text
                    match = self.APP_ID_PATTERN.search(url_text)
                    if match:
                        app_id = match.group(1)
                        meta = {'url': url_text}
                        # 국가 코드 저장 (후속 상세 수집에서 활용)
                        if country_code:
                            meta['country_code'] = country_code

                        # 국가 코드 추출
                        if country_code is None:
                            country_match = re.search(r'apple\.com/([a-z]{2})/', url_text)
                            if country_match:
                                country_code = country_match.group(1)

                        # 추가 메타데이터 추출
                        lastmod = url_elem.find('sm:lastmod', ns)
                        if lastmod is not None and lastmod.text:
                            meta['lastmod'] = lastmod.text

                        changefreq = url_elem.find('sm:changefreq', ns)
                        if changefreq is not None and changefreq.text:
                            meta['changefreq'] = changefreq.text

                        priority = url_elem.find('sm:priority', ns)
                        if priority is not None and priority.text:
                            try:
                                meta['priority'] = float(priority.text)
                            except ValueError:
                                pass

                        app_metadata[app_id] = meta

            except ET.ParseError:
                # XML 파싱 실패 시 정규식으로 fallback
                matches = self.APP_ID_PATTERN.findall(content)
                for app_id in matches:
                    meta: Dict[str, Dict] = {}
                    if country_code:
                        meta['country_code'] = country_code
                    app_metadata[app_id] = meta

                # 국가 코드 추출 (fallback)
                country_match = re.search(r'apple\.com/([a-z]{2})/', content)
                if country_match:
                    country_code = country_match.group(1)

            # 성공 - 실패 기록 제거
            success = True
            clear_failed_sitemap_url(sitemap_url, 'app_store')

        except requests.RequestException as e:
            error_reason = f"request_error: {str(e)}"
            log_step("App Store Sitemap", f"[오류] sitemap 요청 실패 ({sitemap_filename}): {str(e)}", "App Store Sitemap")
        except gzip.BadGzipFile as e:
            error_reason = f"gzip_error: {str(e)}"
            log_step("App Store Sitemap", f"[오류] gzip 압축 해제 실패 ({sitemap_filename}): {str(e)}", "App Store Sitemap")
        except Exception as e:
            error_reason = f"{type(e).__name__}: {str(e)}"
            log_step("App Store Sitemap", f"[오류] sitemap 처리 중 예외 ({sitemap_filename}): {type(e).__name__}: {str(e)}", "App Store Sitemap")

        # 실패 시 기록
        if not success and error_reason:
            upsert_failed_sitemap_url(sitemap_url, 'app_store', error_reason)

        return app_metadata, country_code, sitemap_filename, success

    def collect_all_app_ids(self, limit: int = None, retry_failed: bool = True) -> Dict[str, Tuple[Dict[str, Dict], Dict[str, str], int]]:
        """
        모든 sitemap에서 앱 ID 및 메타데이터 수집 (타입별)

        Args:
            limit: 타입별 처리할 sitemap 수 제한 (None이면 전체 처리)
            retry_failed: 이전에 실패한 sitemap URL도 재시도할지 여부

        Returns:
            {sitemap_type: (app_metadata, app_to_sitemap, sitemap_count)}
                app_metadata: {app_id: {url, lastmod, changefreq, priority}}
                app_to_sitemap: {app_id: sitemap_filename}
        """
        timing_tracker.start_task("App Store Sitemap")
        log_step("App Store Sitemap", "전체 앱 ID 수집 시작", "App Store Sitemap")

        result = {}

        # 타입별 sitemap index URL
        index_urls_by_type = self.get_sitemap_index_urls()

        for sitemap_type, index_urls in index_urls_by_type.items():
            all_app_metadata = {}
            app_to_sitemap = {}
            sitemap_count = 0
            failed_count = 0

            log_step("App Store", f"{sitemap_type} 타입 처리 시작", "App Store Sitemap")

            for index_url in index_urls:
                sitemap_urls = self.parse_sitemap_index(index_url)

                # 테스트 목적으로 제한 적용
                if limit is not None:
                    sitemap_urls = sitemap_urls[:limit]

                # sitemap을 역순으로 처리 (최신 sitemap 먼저)
                sitemap_urls = list(reversed(sitemap_urls))

                with ThreadPoolExecutor(max_workers=40) as executor:
                    futures = {executor.submit(self.fetch_and_parse_sitemap, url): url
                              for url in sitemap_urls}

                    for future in as_completed(futures):
                        try:
                            app_metadata, _, sitemap_filename, success = future.result()
                            if success:
                                all_app_metadata.update(app_metadata)
                                # 각 앱에 대해 sitemap 파일명 매핑
                                for app_id in app_metadata:
                                    app_to_sitemap[app_id] = sitemap_filename
                                sitemap_count += 1
                            else:
                                failed_count += 1

                            if (sitemap_count + failed_count) % 50 == 0:
                                log_step("App Store", f"{sitemap_type} 진행: {sitemap_count}개 성공, {failed_count}개 실패, {len(all_app_metadata)}개 앱", "App Store Sitemap")

                        except Exception as e:
                            sitemap_url = futures.get(future, "unknown")
                            log_step("App Store Sitemap", f"[오류] 병렬 처리 중 예외 ({sitemap_url}): {type(e).__name__}: {str(e)}", "App Store Sitemap")
                            upsert_failed_sitemap_url(sitemap_url, 'app_store', f"parallel_error: {str(e)}")
                            failed_count += 1
                            continue

            result[sitemap_type] = (all_app_metadata, app_to_sitemap, sitemap_count)
            log_step("App Store", f"{sitemap_type} 완료: {len(all_app_metadata)}개 앱 ID (성공 {sitemap_count}개, 실패 {failed_count}개)", "App Store Sitemap")

        # 이전에 실패한 sitemap URL 재시도
        if retry_failed:
            retry_urls = get_failed_sitemap_urls('app_store')
            if retry_urls:
                log_step("App Store Sitemap", f"실패한 sitemap {len(retry_urls)}개 재시도 시작", "App Store Sitemap")
                retry_success = 0
                for retry_info in retry_urls:
                    sitemap_url = retry_info['sitemap_url']
                    app_metadata, _, sitemap_filename, success = self.fetch_and_parse_sitemap(sitemap_url)
                    if success:
                        # 결과를 적절한 타입에 추가 (기본적으로 'app' 타입에 추가)
                        if 'app' in result:
                            result['app'][0].update(app_metadata)
                            for app_id in app_metadata:
                                result['app'][1][app_id] = sitemap_filename
                        retry_success += 1

                if retry_success > 0:
                    log_step("App Store Sitemap", f"재시도 성공: {retry_success}/{len(retry_urls)}개", "App Store Sitemap")

        # 최종 통계 출력
        retry_stats = get_sitemap_retry_stats('app_store')
        if retry_stats['total_failed'] > 0:
            log_step("App Store Sitemap", f"실패 통계: 총 {retry_stats['total_failed']}개, 임계치 초과 {retry_stats['exceeded_threshold']}개, 재시도 대기 {retry_stats['retryable']}개", "App Store Sitemap")

        log_step("App Store Sitemap", "수집 완료", "App Store Sitemap")
        return result


def collect_and_save_google_play_apps(limit: int = None) -> Dict:
    """
    Google Play sitemap에서 앱 수집 및 저장

    Args:
        limit: 처리할 sitemap 수 제한 (None이면 전체 처리)

    Returns:
        수집 결과 통계
    """
    from datetime import datetime
    start_time = datetime.now()

    timing_tracker.start_task("Google Play 수집")
    log_step("Google Play 수집", f"수집 시작 (타임스탬프: {start_time.strftime('%Y-%m-%d %H:%M:%S')}, limit={limit})", "Google Play 수집")

    # DB 초기화
    log_step("Google Play 수집", "[1단계] Sitemap DB 초기화 중...", "Google Play 수집")
    init_sitemap_database()
    log_step("Google Play 수집", "[1단계 완료] Sitemap DB 초기화 완료", "Google Play 수집")

    # 기존에 알려진 앱 ID
    log_step("Google Play 수집", "[2단계] 기존 앱 ID 조회 중...", "Google Play 수집")
    known_ids = get_known_app_ids('google_play')
    log_step("Google Play 수집", f"[2단계 완료] 기존 앱 ID: {len(known_ids):,}개", "Google Play 수집")

    # 새로 수집
    log_step("Google Play 수집", "[3단계] Sitemap에서 앱 ID 수집 중...", "Google Play 수집")
    collector = GooglePlaySitemapCollector()
    all_app_metadata, app_to_sitemap, sitemap_count = collector.collect_all_app_ids(limit)
    log_step("Google Play 수집", f"[3단계 완료] {sitemap_count}개 sitemap 처리 완료", "Google Play 수집")

    all_app_ids = set(all_app_metadata.keys())
    log_step("Google Play 수집", f"  수집된 앱 ID: {len(all_app_ids):,}개", "Google Play 수집")

    # 신규 앱만 필터링
    new_app_ids = all_app_ids - known_ids

    log_step("Google Play 수집", f"[4단계] 신규 앱 분석 결과: 신규 {len(new_app_ids):,}개 / 기존 {len(known_ids):,}개 / 전체 {len(all_app_ids):,}개", "Google Play 수집")

    # 신규 앱 ID 최대 10개만 출력
    if new_app_ids:
        new_app_list = sorted(list(new_app_ids))[:10]
        log_step("Google Play 수집", f"  신규 앱 예시 (최대 10개):", "Google Play 수집")
        for app_id in new_app_list:
            log_step("Google Play 수집", f"    - {app_id}", "Google Play 수집")
        if len(new_app_ids) > 10:
            log_step("Google Play 수집", f"    ... 외 {len(new_app_ids) - 10}개", "Google Play 수집")

    # 저장 (앱별 sitemap 파일명 저장)
    log_step("Google Play 수집", "[5단계] DB에 앱 정보 저장 중...", "Google Play 수집")
    new_count = 0
    updated_count = 0
    if all_app_ids:
        # sitemap 파일명별로 그룹화하여 저장
        sitemap_groups = {}
        for app_id in all_app_ids:
            sitemap_name = app_to_sitemap.get(app_id, 'unknown')
            if sitemap_name not in sitemap_groups:
                sitemap_groups[sitemap_name] = []
            sitemap_groups[sitemap_name].append(app_id)

        for sitemap_name, app_ids_in_sitemap in sitemap_groups.items():
            # 해당 sitemap의 앱들에 대한 메타데이터 추출
            app_meta_subset = {app_id: all_app_metadata.get(app_id, {})
                               for app_id in app_ids_in_sitemap}
            nc, uc = save_discovered_apps(
                app_ids_in_sitemap,
                'google_play',
                sitemap_source=sitemap_name,
                app_metadata=app_meta_subset
            )
            new_count += nc
            updated_count += uc

        save_sitemap_snapshot(
            'google_play',
            'sitemaps-index',
            len(all_app_ids),
            new_count
        )

        log_step("Google Play 수집", f"[5단계 완료] 저장 결과: 신규 {new_count:,}개, 업데이트 {updated_count:,}개", "Google Play 수집")

    elapsed_seconds = (datetime.now() - start_time).total_seconds()
    log_step(
        "Google Play 수집",
        f"[수집 완료] 총 {len(all_app_ids):,}개 앱 | 신규 {len(new_app_ids):,}개 | sitemap {sitemap_count}개 처리 | 소요시간: {elapsed_seconds:.1f}초",
        "Google Play 수집"
    )

    return {
        'platform': 'google_play',
        'total_collected': len(all_app_ids),
        'new_apps': len(new_app_ids),
        'sitemaps_processed': sitemap_count,
        'duration_seconds': timing_tracker.get_timing("Google Play 수집")['task_duration']
    }


def collect_and_save_app_store_apps(limit: int = None) -> Dict:
    """
    App Store sitemap에서 앱 수집 및 저장

    Args:
        limit: 타입별 처리할 sitemap 수 제한 (None이면 전체 처리)

    Returns:
        수집 결과 통계
    """
    from datetime import datetime
    start_time = datetime.now()

    timing_tracker.start_task("App Store 수집")
    log_step("App Store 수집", f"수집 시작 (타임스탬프: {start_time.strftime('%Y-%m-%d %H:%M:%S')}, limit={limit})", "App Store 수집")

    # DB 초기화
    log_step("App Store 수집", "[1단계] Sitemap DB 초기화 중...", "App Store 수집")
    init_sitemap_database()
    log_step("App Store 수집", "[1단계 완료] Sitemap DB 초기화 완료", "App Store 수집")

    # 기존에 알려진 앱 ID
    log_step("App Store 수집", "[2단계] 기존 앱 ID 조회 중...", "App Store 수집")
    known_ids = get_known_app_ids('app_store')
    log_step("App Store 수집", f"[2단계 완료] 기존 앱 ID: {len(known_ids):,}개", "App Store 수집")

    # 새로 수집
    log_step("App Store 수집", "[3단계] Sitemap에서 앱 ID 수집 중...", "App Store 수집")
    collector = AppStoreSitemapCollector()
    results = collector.collect_all_app_ids(limit)
    log_step("App Store 수집", "[3단계 완료] Sitemap 수집 완료", "App Store 수집")

    stats = {
        'platform': 'app_store',
        'by_type': {},
        'total_collected': 0,
        'new_apps': 0,
        'duration_seconds': 0
    }

    log_step("App Store 수집", "[4단계] 타입별 결과 분석 및 저장 중...", "App Store 수집")
    for sitemap_type, (all_app_metadata, app_to_sitemap, sitemap_count) in results.items():
        all_app_ids = set(all_app_metadata.keys())
        new_app_ids = all_app_ids - known_ids

        log_step("App Store 수집", f"  [{sitemap_type}] 신규: {len(new_app_ids):,}개 | 전체: {len(all_app_ids):,}개 | sitemap: {sitemap_count}개", "App Store 수집")

        # 신규 앱 ID 최대 10개만 출력
        if new_app_ids:
            new_app_list = sorted(list(new_app_ids))[:10]
            log_step("App Store 수집", f"    신규 앱 예시 (최대 10개):", "App Store 수집")
            for app_id in new_app_list:
                log_step("App Store 수집", f"      - {app_id}", "App Store 수집")
            if len(new_app_ids) > 10:
                log_step("App Store 수집", f"      ... 외 {len(new_app_ids) - 10}개", "App Store 수집")

        if all_app_ids:
            # sitemap 파일명별로 그룹화하여 저장
            sitemap_groups = {}
            for app_id in all_app_ids:
                sitemap_name = app_to_sitemap.get(app_id, sitemap_type)
                if sitemap_name not in sitemap_groups:
                    sitemap_groups[sitemap_name] = []
                sitemap_groups[sitemap_name].append(app_id)

            total_new = 0
            for sitemap_name, app_ids_in_sitemap in sitemap_groups.items():
                app_meta_subset = {app_id: all_app_metadata.get(app_id, {})
                                   for app_id in app_ids_in_sitemap}
                new_count, updated_count = save_discovered_apps(
                    app_ids_in_sitemap,
                    'app_store',
                    sitemap_source=sitemap_name,
                    app_metadata=app_meta_subset
                )
                total_new += new_count

            save_sitemap_snapshot(
                'app_store',
                sitemap_type,
                len(all_app_ids),
                total_new
            )

        stats['by_type'][sitemap_type] = {
            'total': len(all_app_ids),
            'new': len(new_app_ids),
            'sitemaps': sitemap_count
        }
        stats['total_collected'] += len(all_app_ids)
        stats['new_apps'] += len(new_app_ids)

        # known_ids 업데이트 (다음 타입 처리 시 중복 방지)
        known_ids.update(all_app_ids)

    elapsed_seconds = (datetime.now() - start_time).total_seconds()
    stats['duration_seconds'] = timing_tracker.get_timing("App Store 수집")['task_duration']
    log_step(
        "App Store 수집",
        f"[수집 완료] 총 {stats['total_collected']:,}개 앱 | 신규 {stats['new_apps']:,}개 | 소요시간: {elapsed_seconds:.1f}초",
        "App Store 수집"
    )

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
    from datetime import datetime
    start_time = datetime.now()

    timing_tracker.start_task("전체 Sitemap 수집")
    log_step(
        "전체 Sitemap 수집",
        f"========== 전체 Sitemap 수집 시작 (타임스탬프: {start_time.strftime('%Y-%m-%d %H:%M:%S')}) ==========",
        "전체 Sitemap 수집"
    )
    log_step("전체 Sitemap 수집", f"  Google Play limit: {google_limit}", "전체 Sitemap 수집")
    log_step("전체 Sitemap 수집", f"  App Store limit: {appstore_limit}", "전체 Sitemap 수집")

    log_step("전체 Sitemap 수집", ">>> Google Play 수집 시작 <<<", "전체 Sitemap 수집")
    google_results = collect_and_save_google_play_apps(google_limit)

    log_step("전체 Sitemap 수집", ">>> App Store 수집 시작 <<<", "전체 Sitemap 수집")
    appstore_results = collect_and_save_app_store_apps(appstore_limit)

    results = {
        'google_play': google_results,
        'app_store': appstore_results,
        'total_duration_seconds': 0
    }

    elapsed_seconds = (datetime.now() - start_time).total_seconds()
    results['total_duration_seconds'] = timing_tracker.get_timing("전체 Sitemap 수집")['task_duration']

    # 요약 출력
    log_step("전체 Sitemap 수집", "", "전체 Sitemap 수집")
    log_step("전체 Sitemap 수집", "=" * 60, "전체 Sitemap 수집")
    log_step("전체 Sitemap 수집", "       Sitemap 수집 결과 요약", "전체 Sitemap 수집")
    log_step("전체 Sitemap 수집", "=" * 60, "전체 Sitemap 수집")
    log_step(
        "전체 Sitemap 수집",
        f"Google Play: {results['google_play']['total_collected']:,}개 앱 (신규: {results['google_play']['new_apps']:,}개)",
        "전체 Sitemap 수집"
    )

    if 'by_type' in results['app_store']:
        for t, s in results['app_store']['by_type'].items():
            log_step(
                "전체 Sitemap 수집",
                f"App Store ({t}): {s['total']:,}개 앱 (신규: {s['new']:,}개)",
                "전체 Sitemap 수집"
            )

    log_step("전체 Sitemap 수집", f"총 소요 시간: {elapsed_seconds:.1f}초", "전체 Sitemap 수집")
    log_step("전체 Sitemap 수집", "=" * 60, "전체 Sitemap 수집")

    log_step(
        "전체 Sitemap 수집",
        f"========== 전체 Sitemap 수집 완료 ==========",
        "전체 Sitemap 수집"
    )

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
