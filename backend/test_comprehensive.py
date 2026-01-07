#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
앱 발견 시스템 종합 테스트 스크립트
- 실제 데이터로 테스트
- 신규 앱 발견 테스트
- 시계열 분석 기능 검증
- DB 데이터 무결성 검사
- 인코딩 문제 파악
- 성공/실패 횟수 분석
"""
import sys
import os
import json
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.logger import get_test_logger

SESSION_ID = None

# 테스트 결과 저장용
class TestResults:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.start_time = datetime.now()
        self.logger = get_test_logger('comprehensive_test', session_id=session_id)
        self.results = {
            'sitemap': {'app_store': {}, 'play_store': {}},
            'details': {'app_store': {}, 'play_store': {}},
            'reviews': {'app_store': {}, 'play_store': {}},
            'new_app_discovery': {},
            'time_series': {},
            'db_integrity': {},
            'encoding': {},
            'errors': []
        }
        self.iterations = 0

    def add_error(self, context: str, error: str):
        self.results['errors'].append({
            'time': datetime.now().isoformat(),
            'context': context,
            'error': error
        })
        self.logger.error(f"[ERROR] {context}: {error}")

    def summary(self):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return {
            'elapsed_seconds': elapsed,
            'iterations': self.iterations,
            **self.results
        }


def check_encoding(text: str, source: str, results: TestResults) -> bool:
    """문자열 인코딩 문제를 체크합니다."""
    try:
        if text is None:
            return True
        # 한글 확인
        has_korean = any('\uac00' <= c <= '\ud7a3' for c in text)
        # 깨진 문자 확인
        broken_patterns = ['�', '\ufffd', '\\x', '\\u']
        has_broken = any(p in text for p in broken_patterns)

        if has_broken:
            results.results['encoding']['broken_chars'] = results.results['encoding'].get('broken_chars', [])
            results.results['encoding']['broken_chars'].append({
                'source': source,
                'sample': text[:100] if len(text) > 100 else text
            })
            return False
        return True
    except Exception as e:
        results.add_error('check_encoding', str(e))
        return False


def test_sitemap_collection(results: TestResults, limit_files: int = 2):
    """Sitemap 수집 테스트 (소량)"""
    results.logger.info("\n" + "=" * 60)
    results.logger.info("1. SITEMAP 수집 테스트")
    results.logger.info("=" * 60)

    from database.sitemap_apps_db import init_database, get_stats
    from scrapers.sitemap_utils import (
        fetch_url, fetch_and_hash, parse_sitemap_index, parse_sitemap_urlset,
        extract_app_store_app_id, extract_play_store_app_id, parse_hreflang
    )

    init_database()

    # App Store 테스트
    results.logger.info("\n[App Store Sitemap 테스트]")
    app_store_index = 'https://apps.apple.com/sitemaps_apps_index_app_1.xml'

    try:
        content = fetch_url(app_store_index, logger=results.logger)
        if content:
            sitemap_urls = parse_sitemap_index(content, logger=results.logger)
            results.results['sitemap']['app_store']['index_urls'] = len(sitemap_urls)
            results.logger.info(f"  Sitemap index 파일 수: {len(sitemap_urls)}")

            if sitemap_urls:
                # 첫 번째 sitemap 파일만 테스트
                test_url = sitemap_urls[0]
                results.logger.info(f"  테스트 파일: {test_url.split('/')[-1]}")

                content, hash_val = fetch_and_hash(test_url, logger=results.logger)
                if content:
                    url_entries = parse_sitemap_urlset(content, logger=results.logger)
                    results.results['sitemap']['app_store']['entries_in_first_file'] = len(url_entries)
                    results.logger.info(f"  첫 파일 엔트리 수: {len(url_entries)}")

                    # 샘플 데이터 확인
                    if url_entries:
                        sample = url_entries[0]
                        hreflangs = sample.get('hreflangs', [])
                        results.results['sitemap']['app_store']['sample_hreflangs'] = len(hreflangs)
                        results.logger.info(f"  첫 엔트리 hreflang 수: {len(hreflangs)}")

                        if hreflangs:
                            first_href = hreflangs[0]
                            app_id = extract_app_store_app_id(first_href['href'])
                            lang, country = parse_hreflang(first_href['hreflang'])
                            results.logger.info(f"  샘플 앱 ID: {app_id}, 언어: {lang}, 국가: {country}")
                            results.results['sitemap']['app_store']['success'] = True
                else:
                    results.add_error('app_store_sitemap', 'Failed to fetch sitemap file')
        else:
            results.add_error('app_store_sitemap', 'Failed to fetch sitemap index')
    except Exception as e:
        results.add_error('app_store_sitemap', str(e))
        results.results['sitemap']['app_store']['success'] = False

    # Play Store 테스트
    results.logger.info("\n[Play Store Sitemap 테스트]")
    play_store_index = 'https://play.google.com/sitemaps/sitemaps-index-0.xml'

    try:
        content = fetch_url(play_store_index, logger=results.logger)
        if content:
            sitemap_urls = parse_sitemap_index(content, logger=results.logger)
            results.results['sitemap']['play_store']['index_urls'] = len(sitemap_urls)
            results.logger.info(f"  Sitemap index 파일 수: {len(sitemap_urls)}")

            if sitemap_urls:
                # 첫 번째 sitemap 파일만 테스트
                test_url = sitemap_urls[0]
                results.logger.info(f"  테스트 파일: {test_url.split('/')[-1]}")

                content, hash_val = fetch_and_hash(test_url, logger=results.logger)
                if content:
                    url_entries = parse_sitemap_urlset(content, logger=results.logger)
                    results.results['sitemap']['play_store']['entries_in_first_file'] = len(url_entries)
                    results.logger.info(f"  첫 파일 엔트리 수: {len(url_entries)}")

                    if url_entries:
                        sample = url_entries[0]
                        hreflangs = sample.get('hreflangs', [])
                        results.results['sitemap']['play_store']['sample_hreflangs'] = len(hreflangs)
                        results.logger.info(f"  첫 엔트리 hreflang 수: {len(hreflangs)}")

                        if hreflangs:
                            first_href = hreflangs[0]
                            app_id = extract_play_store_app_id(first_href['href'])
                            lang, country = parse_hreflang(first_href['hreflang'])
                            results.logger.info(f"  샘플 앱 ID: {app_id}, 언어: {lang}, 국가: {country}")
                            results.results['sitemap']['play_store']['success'] = True
                else:
                    results.add_error('play_store_sitemap', 'Failed to fetch sitemap file')
        else:
            results.add_error('play_store_sitemap', 'Failed to fetch sitemap index')
    except Exception as e:
        results.add_error('play_store_sitemap', str(e))
        results.results['sitemap']['play_store']['success'] = False


def run_sitemap_collection(results: TestResults):
    """실제 sitemap 수집 (소량)"""
    results.logger.info("\n" + "=" * 60)
    results.logger.info("2. 실제 SITEMAP 수집 (소량)")
    results.logger.info("=" * 60)

    from database.sitemap_apps_db import init_database, get_stats
    from scrapers.app_store_sitemap_collector import AppStoreSitemapCollector, SITEMAP_INDEX_URLS as APP_STORE_URLS
    from scrapers.play_store_sitemap_collector import PlayStoreSitemapCollector, SITEMAP_INDEX_URLS as PLAY_STORE_URLS
    from scrapers.sitemap_utils import fetch_url, fetch_and_hash, parse_sitemap_index

    init_database()

    # App Store - 첫 번째 sitemap 파일만 수집
    results.logger.info("\n[App Store 소량 수집]")
    try:
        collector = AppStoreSitemapCollector(verbose=True, session_id=results.session_id)
        content = fetch_url(APP_STORE_URLS[0], logger=results.logger)
        if content:
            sitemap_urls = parse_sitemap_index(content, logger=results.logger)
            if sitemap_urls and len(sitemap_urls) > 0:
                # 첫 2개 파일만 처리
                for url in sitemap_urls[:2]:
                    collector.process_sitemap_file(url)
                results.results['sitemap']['app_store']['collection_stats'] = collector.stats
                results.logger.info(f"  수집 결과: {collector.stats}")
    except Exception as e:
        results.add_error('app_store_collection', str(e))

    # Play Store - 첫 번째 sitemap 파일만 수집
    results.logger.info("\n[Play Store 소량 수집]")
    try:
        collector = PlayStoreSitemapCollector(verbose=True, session_id=results.session_id)
        content = fetch_url(PLAY_STORE_URLS[0], logger=results.logger)
        if content:
            sitemap_urls = parse_sitemap_index(content, logger=results.logger)
            if sitemap_urls and len(sitemap_urls) > 0:
                for url in sitemap_urls[:2]:
                    collector.process_sitemap_file(url)
                results.results['sitemap']['play_store']['collection_stats'] = collector.stats
                results.logger.info(f"  수집 결과: {collector.stats}")
    except Exception as e:
        results.add_error('play_store_collection', str(e))

    # 통계 확인
    stats = get_stats()
    results.logger.info(f"\n현재 DB 통계: {stats}")
    results.results['sitemap']['db_stats'] = stats


def test_details_collection(results: TestResults, limit: int = 5):
    """상세정보 수집 테스트"""
    results.logger.info("\n" + "=" * 60)
    results.logger.info("3. 앱 상세정보 수집 테스트")
    results.logger.info("=" * 60)

    from database.app_details_db import init_database, get_stats
    from scrapers.app_store_details_collector import AppStoreDetailsCollector, get_apps_to_collect as get_app_store_apps
    from scrapers.play_store_details_collector import PlayStoreDetailsCollector, get_apps_to_collect as get_play_store_apps

    init_database()

    # App Store 상세정보 수집
    results.logger.info("\n[App Store 상세정보 수집]")
    try:
        app_ids = get_app_store_apps(limit=limit)
        results.logger.info(f"  수집 대상: {len(app_ids)}개")
        if app_ids:
            results.logger.info(f"  앱 ID 샘플: {app_ids[:3]}")
            collector = AppStoreDetailsCollector(verbose=True, session_id=results.session_id)
            stats = collector.collect_batch(app_ids)
            results.results['details']['app_store'] = stats
    except Exception as e:
        results.add_error('app_store_details', str(e))

    # Play Store 상세정보 수집
    results.logger.info("\n[Play Store 상세정보 수집]")
    try:
        app_ids = get_play_store_apps(limit=limit)
        results.logger.info(f"  수집 대상: {len(app_ids)}개")
        if app_ids:
            results.logger.info(f"  앱 ID 샘플: {app_ids[:3]}")
            collector = PlayStoreDetailsCollector(verbose=True, session_id=results.session_id)
            stats = collector.collect_batch(app_ids)
            results.results['details']['play_store'] = stats
    except Exception as e:
        results.add_error('play_store_details', str(e))

    # 통계 확인
    stats = get_stats()
    results.logger.info(f"\n현재 App Details DB 통계: {stats}")
    results.results['details']['db_stats'] = stats


def test_reviews_collection(results: TestResults, limit: int = 3):
    """리뷰 수집 테스트"""
    results.logger.info("\n" + "=" * 60)
    results.logger.info("4. 앱 리뷰 수집 테스트")
    results.logger.info("=" * 60)

    from database.app_details_db import init_database, get_stats
    from scrapers.app_store_reviews_collector import AppStoreReviewsCollector, get_apps_for_review_collection as get_app_store_review_apps
    from scrapers.play_store_reviews_collector import PlayStoreReviewsCollector, get_apps_for_review_collection as get_play_store_review_apps

    init_database()

    # App Store 리뷰 수집
    results.logger.info("\n[App Store 리뷰 수집]")
    try:
        app_ids = get_app_store_review_apps(limit=limit)
        results.logger.info(f"  수집 대상: {len(app_ids)}개")
        if app_ids:
            collector = AppStoreReviewsCollector(verbose=True, session_id=results.session_id)
            stats = collector.collect_batch(app_ids)
            results.results['reviews']['app_store'] = stats
    except Exception as e:
        results.add_error('app_store_reviews', str(e))

    # Play Store 리뷰 수집
    results.logger.info("\n[Play Store 리뷰 수집]")
    try:
        app_ids = get_play_store_review_apps(limit=limit)
        results.logger.info(f"  수집 대상: {len(app_ids)}개")
        if app_ids:
            collector = PlayStoreReviewsCollector(verbose=True, session_id=results.session_id)
            stats = collector.collect_batch(app_ids)
            results.results['reviews']['play_store'] = stats
    except Exception as e:
        results.add_error('play_store_reviews', str(e))

    # 통계 확인
    stats = get_stats()
    results.logger.info(f"\n현재 App Details DB 통계: {stats}")
    results.results['reviews']['db_stats'] = stats


def test_new_app_discovery(results: TestResults):
    """신규 앱 발견 테스트"""
    results.logger.info("\n" + "=" * 60)
    results.logger.info("5. 신규 앱 발견 테스트")
    results.logger.info("=" * 60)

    from database.sitemap_apps_db import (
        get_connection, upsert_app_localizations_batch, get_stats
    )

    # 현재 상태 확인
    before_stats = get_stats()
    results.logger.info(f"  현재 상태: {before_stats}")

    # 가상의 신규 앱 추가
    test_apps = [
        {
            'platform': 'app_store',
            'app_id': 'test_new_app_001',
            'language': 'ko',
            'country': 'kr',
            'href': 'https://apps.apple.com/kr/app/test/id999999001',
            'source_file': 'test_discovery.xml'
        },
        {
            'platform': 'play_store',
            'app_id': 'com.test.newapp001',
            'language': 'ko',
            'country': 'kr',
            'href': 'https://play.google.com/store/apps/details?id=com.test.newapp001&hl=ko&gl=kr',
            'source_file': 'test_discovery.xml'
        }
    ]

    new_count = upsert_app_localizations_batch(test_apps)
    results.logger.info(f"  신규 앱 추가: {new_count}개")

    # 중복 추가 테스트
    duplicate_count = upsert_app_localizations_batch(test_apps)
    results.logger.info(f"  중복 추가 시도: {duplicate_count}개 (0이어야 정상)")

    after_stats = get_stats()
    results.logger.info(f"  추가 후 상태: {after_stats}")

    results.results['new_app_discovery'] = {
        'before': before_stats,
        'after': after_stats,
        'new_apps_added': new_count,
        'duplicate_check': duplicate_count == 0
    }


def test_time_series(results: TestResults):
    """시계열 분석 기능 테스트"""
    results.logger.info("\n" + "=" * 60)
    results.logger.info("6. 시계열 분석 기능 테스트")
    results.logger.info("=" * 60)

    from database.app_details_db import (
        get_connection, insert_app, insert_app_metrics,
        get_latest_app, get_latest_app_metrics
    )

    conn = get_connection()
    cursor = conn.cursor()

    # 테스트용 앱 생성
    test_app_id = 'test_time_series_001'

    # 첫 번째 레코드 삽입
    app_data_v1 = {
        'app_id': test_app_id,
        'platform': 'test_platform',
        'version': '1.0.0',
        'developer': 'Test Developer',
        'price': 0,
        'free': 1
    }
    is_new_v1, id_v1 = insert_app(app_data_v1)
    results.logger.info(f"  첫 번째 삽입 (v1.0.0): is_new={is_new_v1}, id={id_v1}")

    time.sleep(0.1)  # 시간 차이를 위해

    # 동일 데이터 삽입 시도 (변경 없음)
    is_new_dup, id_dup = insert_app(app_data_v1)
    results.logger.info(f"  동일 데이터 삽입 시도: is_new={is_new_dup}, id={id_dup}")

    time.sleep(0.1)

    # 변경된 데이터 삽입
    app_data_v2 = app_data_v1.copy()
    app_data_v2['version'] = '2.0.0'
    is_new_v2, id_v2 = insert_app(app_data_v2)
    results.logger.info(f"  두 번째 삽입 (v2.0.0): is_new={is_new_v2}, id={id_v2}")

    # 시계열 데이터 확인
    cursor.execute("""
        SELECT id, app_id, version, recorded_at
        FROM apps
        WHERE app_id = %s
        ORDER BY recorded_at
    """, (test_app_id,))

    records = cursor.fetchall()
    results.logger.info(f"\n  시계열 레코드 수: {len(records)}")
    for r in records:
        results.logger.info(f"    ID: {r['id']}, Version: {r['version']}, Recorded: {r['recorded_at']}")

    # 수치 데이터 시계열 테스트
    metrics_v1 = {
        'app_id': test_app_id,
        'platform': 'test_platform',
        'score': 4.5,
        'ratings': 1000
    }
    is_new_m1, _ = insert_app_metrics(metrics_v1)

    metrics_v2 = metrics_v1.copy()
    metrics_v2['score'] = 4.6  # 점수 변경
    metrics_v2['ratings'] = 1100  # 평가 수 변경
    time.sleep(0.1)
    is_new_m2, _ = insert_app_metrics(metrics_v2)

    cursor.execute("""
        SELECT id, score, ratings, recorded_at
        FROM apps_metrics
        WHERE app_id = %s
        ORDER BY recorded_at
    """, (test_app_id,))

    metric_records = cursor.fetchall()
    results.logger.info(f"\n  수치 시계열 레코드 수: {len(metric_records)}")
    for r in metric_records:
        results.logger.info(f"    ID: {r['id']}, Score: {r['score']}, Ratings: {r['ratings']}, Recorded: {r['recorded_at']}")

    conn.close()

    results.results['time_series'] = {
        'app_history_count': len(records),
        'metrics_history_count': len(metric_records),
        'duplicate_prevention': not is_new_dup,
        'change_detection': is_new_v2
    }


def test_db_integrity(results: TestResults):
    """DB 데이터 무결성 검사"""
    results.logger.info("\n" + "=" * 60)
    results.logger.info("7. DB 데이터 무결성 검사")
    results.logger.info("=" * 60)

    integrity_issues = []

    # sitemap_apps.db 검사
    from database.sitemap_apps_db import get_connection as get_sitemap_conn

    try:
        conn = get_sitemap_conn()
        cursor = conn.cursor()

        # 1. NULL 값 체크
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM app_localizations
            WHERE app_id IS NULL OR platform IS NULL
        """)
        null_count = cursor.fetchone()['cnt']
        if null_count > 0:
            integrity_issues.append(f"sitemap: {null_count} NULL app_id/platform")
        results.logger.info(f"  Sitemap NULL 값: {null_count}")

        # 2. 중복 체크
        cursor.execute("""
            SELECT platform, app_id, language, country, COUNT(*) as cnt
            FROM app_localizations
            GROUP BY platform, app_id, language, country
            HAVING cnt > 1
        """)
        duplicates = cursor.fetchall()
        if duplicates:
            integrity_issues.append(f"sitemap: {len(duplicates)} duplicates")
        results.logger.info(f"  Sitemap 중복: {len(duplicates)}")

        # 3. 인코딩 문제 체크
        cursor.execute("SELECT app_id, language, country, source_file FROM app_localizations LIMIT 100")
        encoding_issues = 0
        for row in cursor.fetchall():
            if not check_encoding(row['source_file'], 'sitemap_source_file', results):
                encoding_issues += 1
        results.logger.info(f"  Sitemap 인코딩 문제: {encoding_issues}")

        conn.close()
    except Exception as e:
        results.add_error('sitemap_integrity', str(e))

    # app_details.db 검사
    from database.app_details_db import get_connection as get_details_conn

    try:
        conn = get_details_conn()
        cursor = conn.cursor()

        # 1. 리뷰 중복 체크
        cursor.execute("""
            SELECT app_id, platform, review_id, COUNT(*) as cnt
            FROM app_reviews
            GROUP BY app_id, platform, review_id
            HAVING cnt > 1
        """)
        review_duplicates = cursor.fetchall()
        if review_duplicates:
            integrity_issues.append(f"reviews: {len(review_duplicates)} duplicates")
        results.logger.info(f"  리뷰 중복: {len(review_duplicates)}")

        # 2. 한글 리뷰 인코딩 체크
        cursor.execute("""
            SELECT content, user_name, title FROM app_reviews
            WHERE content LIKE '%가%' OR content LIKE '%의%' OR title LIKE '%가%'
            LIMIT 10
        """)
        korean_reviews = cursor.fetchall()
        korean_encoding_ok = 0
        for row in cursor.fetchall():
            if row['content'] and check_encoding(row['content'], 'review_content', results):
                korean_encoding_ok += 1
        results.logger.info(f"  한글 리뷰 샘플: {len(korean_reviews)}개")

        # 3. 상태 불일치 체크 (상세정보 있는데 collection_status 없는 경우)
        cursor.execute("""
            SELECT DISTINCT a.app_id, a.platform FROM apps a
            LEFT JOIN collection_status cs ON a.app_id = cs.app_id AND a.platform = cs.platform
            WHERE cs.id IS NULL AND a.platform != 'test_platform'
        """)
        orphan_apps = cursor.fetchall()
        if orphan_apps:
            integrity_issues.append(f"apps without status: {len(orphan_apps)}")
        results.logger.info(f"  상태 정보 없는 앱: {len(orphan_apps)}")

        conn.close()
    except Exception as e:
        results.add_error('details_integrity', str(e))

    results.results['db_integrity'] = {
        'issues': integrity_issues,
        'passed': len(integrity_issues) == 0
    }


def test_repeated_execution(results: TestResults, iterations: int = 3):
    """반복 실행 테스트 (안정성 확인)"""
    results.logger.info("\n" + "=" * 60)
    results.logger.info("8. 반복 실행 안정성 테스트")
    results.logger.info("=" * 60)

    from database.app_details_db import get_stats as get_details_stats
    from database.sitemap_apps_db import get_stats as get_sitemap_stats

    all_stats = []

    for i in range(iterations):
        results.logger.info(f"\n  [반복 {i+1}/{iterations}]")
        results.iterations = i + 1

        try:
            # 소량의 상세정보 수집
            from scrapers.app_store_details_collector import AppStoreDetailsCollector, get_apps_to_collect
            app_ids = get_apps_to_collect(limit=2)
            if app_ids:
                collector = AppStoreDetailsCollector(verbose=False, session_id=results.session_id)
                stats = collector.collect_batch(app_ids)
                all_stats.append({
                    'iteration': i + 1,
                    'app_store_details': stats
                })
                results.logger.info(f"    App Store: {stats}")

            # 소량의 리뷰 수집
            from scrapers.app_store_reviews_collector import AppStoreReviewsCollector, get_apps_for_review_collection
            review_apps = get_apps_for_review_collection(limit=1)
            if review_apps:
                collector = AppStoreReviewsCollector(verbose=False, session_id=results.session_id)
                stats = collector.collect_batch(review_apps)
                results.logger.info(f"    Reviews: {stats}")

            time.sleep(1)  # 요청 간격

        except Exception as e:
            results.add_error(f'iteration_{i+1}', str(e))

    # 최종 통계
    details_stats = get_details_stats()
    sitemap_stats = get_sitemap_stats()

    results.logger.info(f"\n  최종 상태:")
    results.logger.info(f"    Details DB: {details_stats}")
    results.logger.info(f"    Sitemap DB: {sitemap_stats}")


def generate_final_report(results: TestResults):
    """최종 분석 리포트 생성"""
    results.logger.info("\n" + "=" * 70)
    results.logger.info("최종 분석 리포트")
    results.logger.info("=" * 70)

    summary = results.summary()
    elapsed = summary['elapsed_seconds']

    results.logger.info(f"\n총 테스트 시간: {elapsed:.1f}초")
    results.logger.info(f"반복 실행 횟수: {summary['iterations']}")

    # Sitemap 수집 결과
    results.logger.info("\n[Sitemap 수집 결과]")
    for platform, data in summary['sitemap'].items():
        if platform in ['app_store', 'play_store']:
            stats = data.get('collection_stats', {})
            if stats:
                results.logger.info(f"  {platform}:")
                results.logger.info(f"    - 처리된 파일: {stats.get('sitemap_files_processed', 0)}")
                results.logger.info(f"    - 스킵된 파일: {stats.get('sitemap_files_skipped', 0)}")
                results.logger.info(f"    - 신규 로컬라이제이션: {stats.get('new_localizations', 0)}")
                results.logger.info(f"    - 에러: {stats.get('errors', 0)}")

    # 상세정보 수집 결과
    results.logger.info("\n[상세정보 수집 결과]")
    for platform, stats in summary['details'].items():
        if platform in ['app_store', 'play_store'] and stats:
            results.logger.info(f"  {platform}:")
            results.logger.info(f"    - 처리: {stats.get('apps_processed', 0)}")
            results.logger.info(f"    - 신규: {stats.get('new_records', 0)}")
            results.logger.info(f"    - 변경없음: {stats.get('unchanged_records', 0)}")
            results.logger.info(f"    - 미발견: {stats.get('apps_not_found', 0)}")
            results.logger.info(f"    - 에러: {stats.get('errors', 0)}")

    # 리뷰 수집 결과
    results.logger.info("\n[리뷰 수집 결과]")
    for platform, stats in summary['reviews'].items():
        if platform in ['app_store', 'play_store'] and stats:
            results.logger.info(f"  {platform}:")
            results.logger.info(f"    - 처리 앱: {stats.get('apps_processed', 0)}")
            results.logger.info(f"    - 수집 리뷰: {stats.get('reviews_collected', 0)}")
            results.logger.info(f"    - 에러: {stats.get('errors', 0)}")

    # 신규 앱 발견 테스트 결과
    results.logger.info("\n[신규 앱 발견 테스트]")
    new_app = summary['new_app_discovery']
    if new_app:
        results.logger.info(f"  신규 앱 추가: {new_app.get('new_apps_added', 0)}")
        results.logger.info(f"  중복 방지: {'OK' if new_app.get('duplicate_check') else 'FAIL'}")

    # 시계열 분석 테스트 결과
    results.logger.info("\n[시계열 분석 테스트]")
    time_series = summary['time_series']
    if time_series:
        results.logger.info(f"  앱 이력 레코드: {time_series.get('app_history_count', 0)}")
        results.logger.info(f"  수치 이력 레코드: {time_series.get('metrics_history_count', 0)}")
        results.logger.info(f"  중복 방지: {'OK' if time_series.get('duplicate_prevention') else 'FAIL'}")
        results.logger.info(f"  변경 감지: {'OK' if time_series.get('change_detection') else 'FAIL'}")

    # DB 무결성 검사 결과
    results.logger.info("\n[DB 무결성 검사]")
    integrity = summary['db_integrity']
    if integrity:
        results.logger.info(f"  통과: {'OK' if integrity.get('passed') else 'FAIL'}")
        for issue in integrity.get('issues', []):
            results.logger.info(f"    - {issue}")

    # 인코딩 문제
    results.logger.info("\n[인코딩 문제]")
    encoding = summary['encoding']
    broken = encoding.get('broken_chars', [])
    results.logger.info(f"  발견된 문제: {len(broken)}건")

    # 에러 요약
    results.logger.info("\n[에러 요약]")
    errors = summary['errors']
    results.logger.info(f"  총 에러: {len(errors)}건")
    for err in errors[:5]:  # 처음 5개만
        results.logger.info(f"    - [{err['context']}] {err['error'][:50]}")

    # 성공률 계산
    results.logger.info("\n[성공/실패 분석]")
    total_requests = 0
    total_errors = 0

    for platform in ['app_store', 'play_store']:
        details = summary['details'].get(platform, {})
        reviews = summary['reviews'].get(platform, {})

        processed = details.get('apps_processed', 0) + reviews.get('apps_processed', 0)
        errors = details.get('errors', 0) + reviews.get('errors', 0)

        total_requests += processed
        total_errors += errors

        if processed > 0:
            success_rate = ((processed - errors) / processed) * 100
            results.logger.info(f"  {platform}: {processed}건 처리, {errors}건 에러 ({success_rate:.1f}% 성공)")

    if total_requests > 0:
        overall_success = ((total_requests - total_errors) / total_requests) * 100
        results.logger.info(f"\n  전체: {total_requests}건 처리, {total_errors}건 에러 ({overall_success:.1f}% 성공)")

    results.logger.info("\n" + "=" * 70)

    return summary


def main():
    global SESSION_ID
    SESSION_ID = datetime.now().strftime('%Y%m%d_%H%M%S')
    results = TestResults(SESSION_ID)
    results.logger.info("=" * 70)
    results.logger.info("앱 발견 시스템 종합 테스트")
    results.logger.info(f"시작 시간: {datetime.now().isoformat()}")
    results.logger.info("=" * 70)

    try:
        # 1. Sitemap 수집 기본 테스트
        test_sitemap_collection(results)

        # 2. 실제 sitemap 수집 (소량)
        run_sitemap_collection(results)

        # 3. 상세정보 수집 테스트
        test_details_collection(results, limit=5)

        # 4. 리뷰 수집 테스트
        test_reviews_collection(results, limit=3)

        # 5. 신규 앱 발견 테스트
        test_new_app_discovery(results)

        # 6. 시계열 분석 기능 테스트
        test_time_series(results)

        # 7. DB 무결성 검사
        test_db_integrity(results)

        # 8. 반복 실행 테스트
        test_repeated_execution(results, iterations=2)

    except KeyboardInterrupt:
        results.logger.warning("\n테스트 중단됨")
    except Exception as e:
        results.add_error('main', str(e))
        results.logger.exception("[main] 상세 예외")

    # 최종 리포트
    summary = generate_final_report(results)

    # JSON으로 저장
    report_path = os.path.join(os.path.dirname(__file__), 'test_report.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    results.logger.info(f"\n상세 리포트 저장: {report_path}")

    return summary


if __name__ == '__main__':
    main()
