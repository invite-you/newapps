"""
Sitemap 수집 공통 유틸리티
MD5 해시, HTTP 요청, XML 파싱 등
"""
import hashlib
import gzip
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, List, Dict, Tuple
from urllib.parse import urlparse, parse_qs
import re
import time

from config.language_country_priority import get_best_country_for_language
from utils.logger import get_timestamped_logger

# User-Agent 설정
USER_AGENT = "Mozilla/5.0 (compatible; SitemapBot/1.0)"
REQUEST_TIMEOUT = 60
LOG_FILE_PREFIX = "sitemap_utils"
DEFAULT_LOGGER = get_timestamped_logger("sitemap_utils", file_prefix=LOG_FILE_PREFIX, level=logging.INFO)


def calculate_md5(data: bytes) -> str:
    """바이트 데이터의 MD5 해시를 계산합니다."""
    return hashlib.md5(data).hexdigest()


def calculate_content_hash(normalized_content: str) -> str:
    """정규화된 콘텐츠 문자열의 MD5 해시를 계산합니다."""
    return hashlib.md5(normalized_content.encode('utf-8')).hexdigest()


def _resolve_logger(logger: Optional[logging.Logger]) -> logging.Logger:
    return logger or DEFAULT_LOGGER


def fetch_url(
    url: str,
    max_retries: int = 3,
    retry_delay: float = 2.0,
    logger: Optional[logging.Logger] = None
) -> Optional[bytes]:
    """URL에서 데이터를 가져옵니다. gzip 압축된 경우 자동 해제."""
    resolved_logger = _resolve_logger(logger)
    headers = {
        'User-Agent': USER_AGENT,
        'Accept-Encoding': 'gzip, deflate'
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            content = response.content

            # .xml.gz 파일인 경우 압축 해제
            if url.endswith('.gz'):
                try:
                    content = gzip.decompress(content)
                except gzip.BadGzipFile:
                    # 이미 압축 해제되어 있거나 압축되지 않은 경우
                    pass

            return content

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))  # 지수 백오프
            else:
                resolved_logger.error(f"Error fetching {url}: {e}")
                return None

    return None


def fetch_and_hash(
    url: str,
    logger: Optional[logging.Logger] = None
) -> Tuple[Optional[bytes], Optional[str]]:
    """URL에서 데이터를 가져오고 MD5 해시를 계산합니다.

    gzip 파일의 경우 압축 해제된 실제 데이터의 해시를 반환합니다.
    gzip 헤더에는 mtime(압축 시간), OS 정보 등 메타데이터가 포함되어
    동일 내용이라도 압축 시간이 다르면 해시가 달라지는 문제가 있습니다.
    따라서 압축 해제된 데이터의 해시를 계산하여 진정한 변경 여부를 감지합니다.

    Returns: (decompressed_content, hash_of_decompressed_data)
    """
    resolved_logger = _resolve_logger(logger)
    headers = {
        'User-Agent': USER_AGENT,
    }

    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        raw_content = response.content

        # .xml.gz 파일인 경우 압축 해제 후 해시 계산
        if url.endswith('.gz'):
            try:
                decompressed = gzip.decompress(raw_content)
                # 압축 해제된 실제 데이터의 해시 계산 (gzip 헤더 제외)
                content_hash = calculate_md5(decompressed)
                return decompressed, content_hash
            except gzip.BadGzipFile:
                # 압축되지 않은 파일인 경우 원본 데이터 해시
                content_hash = calculate_md5(raw_content)
                return raw_content, content_hash

        # 비압축 파일인 경우 원본 데이터 해시
        content_hash = calculate_md5(raw_content)
        return raw_content, content_hash

    except requests.exceptions.RequestException as e:
        resolved_logger.error(f"Error fetching {url}: {e}")
        return None, None


def parse_sitemap_index(
    xml_content: bytes,
    logger: Optional[logging.Logger] = None
) -> List[str]:
    """sitemap index XML에서 개별 sitemap URL들을 추출합니다."""
    resolved_logger = _resolve_logger(logger)
    try:
        root = ET.fromstring(xml_content)
        namespace = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        sitemap_urls = []
        for sitemap in root.findall('.//sm:sitemap/sm:loc', namespace):
            if sitemap.text:
                sitemap_urls.append(sitemap.text.strip())

        # namespace 없이도 시도
        if not sitemap_urls:
            for sitemap in root.findall('.//sitemap/loc'):
                if sitemap.text:
                    sitemap_urls.append(sitemap.text.strip())

        return sitemap_urls

    except ET.ParseError as e:
        resolved_logger.error(f"Error parsing sitemap index: {e}")
        return []


def extract_file_id_from_sitemap_url(url: str) -> str:
    """sitemap URL에서 파일 식별자만 추출합니다.

    URL에서 날짜/타임스탬프 등 가변 부분을 제거하고 파일 번호만 추출합니다.
    예: play_sitemaps_2026-01-09_1767978176-00000-of-77447.xml.gz -> 00000-of-77447
        sitemaps_apps_app_61_5.xml.gz -> app_61_5
        sitemaps_apps_new-app_1_1.xml.gz -> new-app_1_1
    """
    filename = url.split('/')[-1]

    # Play Store: play_sitemaps_날짜_타임스탬프-번호-of-총수.xml.gz
    play_match = re.search(r'-(\d+-of-\d+)\.xml', filename)
    if play_match:
        return play_match.group(1)

    # App Store: sitemaps_apps_new-app_XX_Y.xml.gz (new-app 먼저 체크)
    new_app_match = re.search(r'(new-app_\d+_\d+)\.xml', filename)
    if new_app_match:
        return new_app_match.group(1)

    # App Store: sitemaps_apps_app_XX_Y.xml.gz
    app_match = re.search(r'(app_\d+_\d+)\.xml', filename)
    if app_match:
        return app_match.group(1)

    # 기타: 파일명 전체 사용 (확장자 제외)
    return re.sub(r'\.xml(\.gz)?$', '', filename)


def calculate_sitemap_index_content_hash(
    xml_content: bytes,
    logger: Optional[logging.Logger] = None
) -> str:
    """sitemap index의 콘텐츠 해시를 계산합니다.

    각 sitemap URL에서 파일 번호만 추출하여 해시합니다.
    날짜/타임스탬프가 바뀌어도 파일 구성이 같으면 같은 해시가 됩니다.
    """
    resolved_logger = _resolve_logger(logger)
    try:
        root = ET.fromstring(xml_content)
        namespace = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        file_ids = []
        for sitemap in root.findall('.//sm:sitemap/sm:loc', namespace):
            if sitemap.text:
                file_id = extract_file_id_from_sitemap_url(sitemap.text.strip())
                file_ids.append(file_id)

        # namespace 없이 시도
        if not file_ids:
            for sitemap in root.findall('.//sitemap/loc'):
                if sitemap.text:
                    file_id = extract_file_id_from_sitemap_url(sitemap.text.strip())
                    file_ids.append(file_id)

        # 정렬하여 순서에 관계없이 동일한 해시 생성
        sorted_ids = sorted(file_ids)
        normalized = '\n'.join(sorted_ids)
        return calculate_content_hash(normalized)

    except ET.ParseError as e:
        resolved_logger.error(f"Error parsing sitemap index for hash: {e}")
        return ""


def parse_sitemap_urlset(
    xml_content: bytes,
    logger: Optional[logging.Logger] = None
) -> List[Dict]:
    """sitemap urlset XML에서 URL 정보를 추출합니다.
    Returns: List of {loc, hreflangs: [{hreflang, href}, ...]}
    """
    resolved_logger = _resolve_logger(logger)
    try:
        root = ET.fromstring(xml_content)
        namespace = {
            'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
            'xhtml': 'http://www.w3.org/1999/xhtml'
        }

        results = []
        for url in root.findall('.//sm:url', namespace):
            loc_elem = url.find('sm:loc', namespace)
            loc = loc_elem.text.strip() if loc_elem is not None and loc_elem.text else None

            hreflangs = []
            for link in url.findall('xhtml:link', namespace):
                rel = link.get('rel')
                hreflang = link.get('hreflang')
                href = link.get('href')

                if rel == 'alternate' and hreflang and href:
                    hreflangs.append({
                        'hreflang': hreflang,
                        'href': href
                    })

            if hreflangs:  # hreflang이 있는 항목만 수집
                results.append({
                    'loc': loc,
                    'hreflangs': hreflangs
                })

        return results

    except ET.ParseError as e:
        resolved_logger.error(f"Error parsing sitemap urlset: {e}")
        return []


def calculate_sitemap_urlset_content_hash(
    xml_content: bytes,
    platform: str,
    logger: Optional[logging.Logger] = None
) -> str:
    """sitemap urlset의 콘텐츠 해시를 계산합니다.

    앱 ID + hreflang 정보만 추출하여 정렬 후 해시합니다.
    앱이 아닌 데이터(books, movies 등)는 무시합니다.

    Args:
        xml_content: sitemap XML 바이트
        platform: 'app_store' 또는 'play_store'
    """
    resolved_logger = _resolve_logger(logger)
    try:
        root = ET.fromstring(xml_content)
        namespace = {
            'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
            'xhtml': 'http://www.w3.org/1999/xhtml'
        }

        app_data = []
        for url_elem in root.findall('.//sm:url', namespace):
            # <xhtml:link> 에서 hreflang과 href 추출
            hreflangs = []
            first_href = None

            for link in url_elem.findall('xhtml:link', namespace):
                rel = link.get('rel')
                hreflang = link.get('hreflang', '')
                href = link.get('href', '')

                if rel == 'alternate' and hreflang and href:
                    if first_href is None:
                        first_href = href
                    hreflangs.append(hreflang.lower())

            if not first_href or not hreflangs:
                continue

            # 앱 ID 추출
            if platform == 'app_store':
                app_id = extract_app_store_app_id(first_href)
            else:
                # Play Store: 앱 URL이 아니면 무시 (books, movies 등)
                if not is_play_store_app_url(first_href):
                    continue
                app_id = extract_play_store_app_id(first_href)

            if not app_id:
                continue

            # "앱ID:lang1,lang2,lang3" 형태로 정규화
            sorted_langs = sorted(hreflangs)
            app_data.append(f"{app_id}:{','.join(sorted_langs)}")

        # 앱 ID 기준 정렬
        sorted_data = sorted(app_data)
        normalized = '\n'.join(sorted_data)
        return calculate_content_hash(normalized)

    except ET.ParseError as e:
        resolved_logger.error(f"Error parsing sitemap urlset for hash: {e}")
        return ""


def extract_app_store_app_id(url: str) -> Optional[str]:
    """App Store URL에서 앱 ID를 추출합니다.
    예: https://apps.apple.com/kr/app/example/id1234567890 -> 1234567890
    """
    match = re.search(r'/id(\d+)', url)
    return match.group(1) if match else None


def extract_play_store_app_id(url: str) -> Optional[str]:
    """Play Store URL에서 앱 ID를 추출합니다.
    예: https://play.google.com/store/apps/details?id=com.example.app -> com.example.app
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    app_id = params.get('id', [None])[0]
    return app_id


def parse_hreflang(hreflang: str) -> Tuple[str, str]:
    """hreflang 문자열을 language와 country로 분리합니다.
    예: ko-KR -> (ko, kr), en-us -> (en, us)
    """
    parts = hreflang.lower().split('-')
    if len(parts) >= 2:
        return parts[0], parts[1]
    return parts[0], ''


def is_play_store_app_url(url: str) -> bool:
    """Play Store URL이 앱 URL인지 확인합니다 (book, movie 등 제외)."""
    return '/store/apps/' in url


def get_filename_from_url(url: str) -> str:
    """URL에서 파일명을 추출합니다."""
    parsed = urlparse(url)
    return parsed.path.split('/')[-1]


def filter_best_country_per_language(raw_localizations: List[Dict]) -> List[Dict]:
    """각 앱의 각 언어에 대해 최적의 국가 1개만 선택합니다.

    예: 영어 116개 국가 → 영어 1개 국가 (US 우선)
    이를 통해 DB 용량을 약 50% 절감합니다.
    """
    app_lang_countries = {}

    for loc in raw_localizations:
        app_id = loc['app_id']
        language = loc['language']
        country = loc['country']
        app_lang_countries.setdefault(app_id, {}).setdefault(language, []).append((country, loc))

    filtered = []
    for app_id, lang_data in app_lang_countries.items():
        for language, country_list in lang_data.items():
            available_countries = [c for c, _ in country_list]
            best_country = get_best_country_for_language(language, available_countries)

            for country, loc_data in country_list:
                if country.upper() == best_country.upper():
                    filtered.append(loc_data)
                    break
            else:
                filtered.append(country_list[0][1])

    return filtered


def log_sitemap_step_end(
    logger: Optional[logging.Logger],
    filename: str,
    start_perf: float,
    status: str
) -> None:
    """sitemap 처리 단계 종료 로그를 기록합니다."""
    resolved_logger = _resolve_logger(logger)
    elapsed = time.perf_counter() - start_perf
    resolved_logger.info(
        f"[STEP END] sitemap_file={filename} | {datetime.now().isoformat()} | "
        f"elapsed={elapsed:.2f}s | status={status}"
    )
