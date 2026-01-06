#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
장시간 반복 테스트 스크립트
- 약 1시간 동안 반복 실행
- 신규 앱 발견 테스트
- 시계열 데이터 누적 테스트
- 성공/실패 통계 수집
- 상세 에러 추적 및 파일 로깅
"""
import sys
import os
import json
import time
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.logger import get_test_logger, LOG_DIR
from utils.error_tracker import ErrorTracker, ErrorStep

# 테스트 설정
TEST_DURATION_MINUTES = 20  # 테스트 시간 (분)
ITERATION_INTERVAL_SECONDS = 15  # 반복 간격 (초)

class LongRunningTest:
    def __init__(self):
        self.start_time = datetime.now()
        self.logger = get_test_logger('long_running_test')
        self.error_tracker = ErrorTracker('long_running_test')
        self.stats = {
            'iterations': 0,
            'sitemap': {
                'app_store': {'files_processed': 0, 'files_skipped': 0, 'new_localizations': 0, 'errors': 0},
                'play_store': {'files_processed': 0, 'files_skipped': 0, 'new_localizations': 0, 'errors': 0}
            },
            'details': {
                'app_store': {'processed': 0, 'success': 0, 'not_found': 0, 'skipped': 0, 'errors': 0},
                'play_store': {'processed': 0, 'success': 0, 'not_found': 0, 'skipped': 0, 'errors': 0}
            },
            'reviews': {
                'app_store': {'apps_processed': 0, 'reviews_collected': 0, 'errors': 0},
                'play_store': {'apps_processed': 0, 'reviews_collected': 0, 'errors': 0}
            },
            'time_series': {
                'unchanged_skipped': 0,
                'changed_recorded': 0
            },
            'new_app_discovery': {
                'total_new_apps': 0,
                'by_platform': {'app_store': 0, 'play_store': 0}
            },
            'encoding': {
                'korean_samples': 0,
                'encoding_errors': 0
            },
            'errors': []  # 기존 호환성을 위해 유지
        }

    def log(self, message: str):
        """콘솔과 파일에 동시 로깅"""
        self.logger.info(message)

    def add_error(self, context: str, error: str, app_id: str = None):
        """에러 기록 (상세 추적 포함)"""
        # 기존 형식 유지 (호환성)
        self.stats['errors'].append({
            'time': datetime.now().isoformat(),
            'context': context,
            'error': str(error)[:200],
            'app_id': app_id
        })
        # 상세 에러 추적
        self.error_tracker.add_error_simple(context, error, app_id)
        self.logger.error(f"[{context}] {error}")

    def elapsed_minutes(self) -> float:
        return (datetime.now() - self.start_time).total_seconds() / 60

    def run_sitemap_collection(self, limit_files: int = 2):
        """Sitemap 수집 (소량)"""
        from database.sitemap_apps_db import init_database, get_stats
        from scrapers.sitemap_utils import fetch_url, parse_sitemap_index

        init_database()

        # App Store
        try:
            from scrapers.app_store_sitemap_collector import AppStoreSitemapCollector, SITEMAP_INDEX_URLS
            collector = AppStoreSitemapCollector(verbose=False)
            content = fetch_url(SITEMAP_INDEX_URLS[0])
            if content:
                sitemap_urls = parse_sitemap_index(content)
                for url in sitemap_urls[:limit_files]:
                    collector.process_sitemap_file(url)

                self.stats['sitemap']['app_store']['files_processed'] += collector.stats['sitemap_files_processed']
                self.stats['sitemap']['app_store']['files_skipped'] += collector.stats['sitemap_files_skipped']
                self.stats['sitemap']['app_store']['new_localizations'] += collector.stats['new_localizations']
                self.stats['sitemap']['app_store']['errors'] += collector.stats['errors']

                # 신규 앱 발견 업데이트
                self.stats['new_app_discovery']['by_platform']['app_store'] = collector.stats['new_localizations']
        except Exception as e:
            self.add_error('sitemap_app_store', str(e))

        # Play Store
        try:
            from scrapers.play_store_sitemap_collector import PlayStoreSitemapCollector, SITEMAP_INDEX_URLS
            collector = PlayStoreSitemapCollector(verbose=False)
            content = fetch_url(SITEMAP_INDEX_URLS[0])
            if content:
                sitemap_urls = parse_sitemap_index(content)
                for url in sitemap_urls[:limit_files]:
                    collector.process_sitemap_file(url)

                self.stats['sitemap']['play_store']['files_processed'] += collector.stats['sitemap_files_processed']
                self.stats['sitemap']['play_store']['files_skipped'] += collector.stats['sitemap_files_skipped']
                self.stats['sitemap']['play_store']['new_localizations'] += collector.stats['new_localizations']
                self.stats['sitemap']['play_store']['errors'] += collector.stats['errors']

                self.stats['new_app_discovery']['by_platform']['play_store'] = collector.stats['new_localizations']
        except Exception as e:
            self.add_error('sitemap_play_store', str(e))

    def run_details_collection(self, limit: int = 5):
        """앱 상세정보 수집"""
        from database.app_details_db import init_database

        init_database()

        # App Store
        try:
            from scrapers.app_store_details_collector import AppStoreDetailsCollector, get_apps_to_collect
            app_ids = get_apps_to_collect(limit=limit)
            if app_ids:
                # 에러 트래커 공유
                collector = AppStoreDetailsCollector(verbose=False, error_tracker=self.error_tracker)
                stats = collector.collect_batch(app_ids)

                self.stats['details']['app_store']['processed'] += stats['apps_processed']
                self.stats['details']['app_store']['success'] += stats['new_records']
                self.stats['details']['app_store']['skipped'] += stats['apps_skipped_failed']
                self.stats['details']['app_store']['not_found'] += stats['apps_not_found']
                self.stats['details']['app_store']['errors'] += stats['errors']

                # 시계열 통계
                self.stats['time_series']['unchanged_skipped'] += stats['unchanged_records']
                self.stats['time_series']['changed_recorded'] += stats['new_records']
        except Exception as e:
            self.add_error('details_app_store', str(e))

        # Play Store
        try:
            from scrapers.play_store_details_collector import PlayStoreDetailsCollector, get_apps_to_collect
            app_ids = get_apps_to_collect(limit=limit)
            if app_ids:
                # 에러 트래커 공유
                collector = PlayStoreDetailsCollector(verbose=False, error_tracker=self.error_tracker)
                stats = collector.collect_batch(app_ids)

                self.stats['details']['play_store']['processed'] += stats['apps_processed']
                self.stats['details']['play_store']['success'] += stats['new_records']
                self.stats['details']['play_store']['skipped'] += stats['apps_skipped_failed']
                self.stats['details']['play_store']['not_found'] += stats['apps_not_found']
                self.stats['details']['play_store']['errors'] += stats['errors']

                self.stats['time_series']['unchanged_skipped'] += stats['unchanged_records']
                self.stats['time_series']['changed_recorded'] += stats['new_records']
        except Exception as e:
            self.add_error('details_play_store', str(e))

    def run_reviews_collection(self, limit: int = 2):
        """리뷰 수집"""
        from database.app_details_db import init_database

        init_database()

        # App Store
        try:
            from scrapers.app_store_reviews_collector import AppStoreReviewsCollector, get_apps_for_review_collection
            app_ids = get_apps_for_review_collection(limit=limit)
            if app_ids:
                # 에러 트래커 공유
                collector = AppStoreReviewsCollector(verbose=False, error_tracker=self.error_tracker)
                stats = collector.collect_batch(app_ids)

                self.stats['reviews']['app_store']['apps_processed'] += stats['apps_processed']
                self.stats['reviews']['app_store']['reviews_collected'] += stats['reviews_collected']
                self.stats['reviews']['app_store']['errors'] += stats['errors']
        except Exception as e:
            self.add_error('reviews_app_store', str(e))

        # Play Store
        try:
            from scrapers.play_store_reviews_collector import PlayStoreReviewsCollector, get_apps_for_review_collection
            app_ids = get_apps_for_review_collection(limit=limit)
            if app_ids:
                # 에러 트래커 공유
                collector = PlayStoreReviewsCollector(verbose=False, error_tracker=self.error_tracker)
                stats = collector.collect_batch(app_ids)

                self.stats['reviews']['play_store']['apps_processed'] += stats['apps_processed']
                self.stats['reviews']['play_store']['reviews_collected'] += stats['reviews_collected']
                self.stats['reviews']['play_store']['errors'] += stats['errors']
        except Exception as e:
            self.add_error('reviews_play_store', str(e))

    def check_korean_encoding(self):
        """한글 데이터 인코딩 확인"""
        from database.app_details_db import get_connection

        try:
            conn = get_connection()
            cursor = conn.cursor()

            # 한글이 포함된 리뷰 확인
            cursor.execute("""
                SELECT content, user_name FROM app_reviews
                WHERE content LIKE '%가%' OR content LIKE '%를%' OR content LIKE '%이%'
                LIMIT 10
            """)
            korean_reviews = cursor.fetchall()

            # 한글이 포함된 앱 정보 확인
            cursor.execute("""
                SELECT title, description FROM apps_localized
                WHERE title LIKE '%가%' OR title LIKE '%를%' OR description LIKE '%가%'
                LIMIT 10
            """)
            korean_apps = cursor.fetchall()

            korean_count = len(korean_reviews) + len(korean_apps)
            self.stats['encoding']['korean_samples'] = korean_count

            # 깨진 문자 확인
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM app_reviews
                WHERE content LIKE '%�%' OR user_name LIKE '%�%'
            """)
            broken = cursor.fetchone()['cnt']
            self.stats['encoding']['encoding_errors'] = broken

            conn.close()

            if korean_count > 0:
                self.log(f"  한글 데이터 샘플: {korean_count}개")
            if broken > 0:
                self.log(f"  WARNING: 인코딩 깨진 데이터: {broken}개")

        except Exception as e:
            self.add_error('encoding_check', str(e))

    def inject_new_app(self, iteration: int):
        """신규 앱 주입 테스트 (반복 실행 중 새 앱 추가)"""
        from database.sitemap_apps_db import upsert_app_localizations_batch

        try:
            # 반복마다 다른 앱 ID 사용
            new_apps = [
                {
                    'platform': 'app_store',
                    'app_id': f'test_inject_{iteration}_appstore',
                    'language': 'ko',
                    'country': 'kr',
                    'href': f'https://apps.apple.com/kr/app/test/id{1000000000 + iteration}',
                    'source_file': 'test_inject.xml'
                },
                {
                    'platform': 'play_store',
                    'app_id': f'com.test.inject_{iteration}',
                    'language': 'ko',
                    'country': 'kr',
                    'href': f'https://play.google.com/store/apps/details?id=com.test.inject_{iteration}',
                    'source_file': 'test_inject.xml'
                }
            ]

            new_count = upsert_app_localizations_batch(new_apps)
            self.stats['new_app_discovery']['total_new_apps'] += new_count

            if new_count > 0:
                self.log(f"  신규 앱 주입: {new_count}개")

        except Exception as e:
            self.add_error('inject_new_app', str(e))

    def get_db_stats(self):
        """현재 DB 통계 가져오기"""
        from database.sitemap_apps_db import get_stats as get_sitemap_stats
        from database.app_details_db import get_stats as get_details_stats

        try:
            sitemap = get_sitemap_stats()
            details = get_details_stats()
            return {'sitemap': sitemap, 'details': details}
        except Exception as e:
            self.add_error('get_db_stats', str(e))
            return {}

    def run_iteration(self, iteration: int):
        """단일 반복 실행"""
        self.log(f"=== 반복 #{iteration} 시작 ===")

        # 1. Sitemap 수집 (처음에만 또는 10번마다)
        if iteration == 1 or iteration % 10 == 0:
            self.log("  Sitemap 수집 중...")
            self.run_sitemap_collection(limit_files=1)

        # 2. 상세정보 수집
        self.log("  상세정보 수집 중...")
        self.run_details_collection(limit=3)

        # 3. 리뷰 수집
        self.log("  리뷰 수집 중...")
        self.run_reviews_collection(limit=1)

        # 4. 신규 앱 주입 테스트 (3번마다)
        if iteration % 3 == 0:
            self.inject_new_app(iteration)

        # 5. 인코딩 확인 (5번마다)
        if iteration % 5 == 0:
            self.check_korean_encoding()

        # 6. 현재 상태 출력
        db_stats = self.get_db_stats()
        if db_stats:
            sitemap = db_stats.get('sitemap', {})
            details = db_stats.get('details', {})

            total_apps = sum(p.get('apps', 0) for p in sitemap.get('platform_stats', {}).values())
            total_reviews = sum(details.get('reviews_by_platform', {}).values())

            self.log(f"  현재 상태: 앱 {total_apps}개, 리뷰 {total_reviews}개")

        self.stats['iterations'] = iteration

    def generate_report(self):
        """최종 리포트 생성"""
        elapsed = self.elapsed_minutes()

        print("\n" + "=" * 70)
        print("장시간 테스트 최종 리포트")
        print("=" * 70)
        print(f"\n테스트 시간: {elapsed:.1f}분")
        print(f"반복 횟수: {self.stats['iterations']}")

        print("\n[Sitemap 수집 통계]")
        for platform in ['app_store', 'play_store']:
            s = self.stats['sitemap'][platform]
            print(f"  {platform}:")
            print(f"    - 처리된 파일: {s['files_processed']}")
            print(f"    - 스킵된 파일: {s['files_skipped']}")
            print(f"    - 신규 로컬라이제이션: {s['new_localizations']}")
            print(f"    - 에러: {s['errors']}")

        print("\n[상세정보 수집 통계]")
        for platform in ['app_store', 'play_store']:
            d = self.stats['details'][platform]
            print(f"  {platform}:")
            print(f"    - 처리: {d['processed']}")
            print(f"    - 성공: {d['success']}")
            print(f"    - 미발견: {d['not_found']}")
            print(f"    - 에러: {d['errors']}")
            if d['processed'] > 0:
                success_rate = (d['success'] / d['processed']) * 100
                print(f"    - 성공률: {success_rate:.1f}%")

        print("\n[리뷰 수집 통계]")
        for platform in ['app_store', 'play_store']:
            r = self.stats['reviews'][platform]
            print(f"  {platform}:")
            print(f"    - 처리 앱: {r['apps_processed']}")
            print(f"    - 수집 리뷰: {r['reviews_collected']}")
            print(f"    - 에러: {r['errors']}")

        print("\n[시계열 분석]")
        ts = self.stats['time_series']
        print(f"  변경 없음 (스킵): {ts['unchanged_skipped']}")
        print(f"  변경 감지 (기록): {ts['changed_recorded']}")

        print("\n[신규 앱 발견]")
        nad = self.stats['new_app_discovery']
        print(f"  총 신규 앱: {nad['total_new_apps']}")

        print("\n[인코딩 검사]")
        enc = self.stats['encoding']
        print(f"  한글 샘플: {enc['korean_samples']}")
        print(f"  인코딩 에러: {enc['encoding_errors']}")

        print("\n[에러 요약]")
        error_summary = self.error_tracker.get_summary()
        print(f"  총 에러: {error_summary['total_errors']}건")
        print(f"  에러 발생 앱 수: {error_summary['unique_apps_with_errors']}개")

        if error_summary['errors_by_step']:
            print("\n  [단계별 에러]")
            for step, count in sorted(error_summary['errors_by_step'].items(), key=lambda x: -x[1]):
                print(f"    - {step}: {count}건")

        print("\n  [최근 에러 (최대 10개)]")
        for err in error_summary['recent_errors'][-10:]:
            app_info = f"app={err['app_id']}" if err.get('app_id') else ""
            print(f"    - [{err['platform']}:{err['step']}] {app_info}")
            print(f"      {err['error_type']}: {err['error_message'][:60]}")

        # 전체 성공률 계산
        print("\n[전체 성공/실패 분석]")
        total_processed = 0
        total_errors = 0

        for platform in ['app_store', 'play_store']:
            d = self.stats['details'][platform]
            r = self.stats['reviews'][platform]

            platform_processed = d['processed'] + r['apps_processed']
            platform_errors = d['errors'] + r['errors']

            total_processed += platform_processed
            total_errors += platform_errors

        if total_processed > 0:
            overall_success = ((total_processed - total_errors) / total_processed) * 100
            print(f"  전체 요청: {total_processed}건")
            print(f"  전체 에러: {total_errors}건")
            print(f"  성공률: {overall_success:.1f}%")

        print("\n" + "=" * 70)

        return self.stats

    def run(self):
        """메인 테스트 루프"""
        self.log("장시간 테스트 시작")
        self.log(f"예정 시간: {TEST_DURATION_MINUTES}분")
        self.log(f"반복 간격: {ITERATION_INTERVAL_SECONDS}초")

        iteration = 0

        try:
            while self.elapsed_minutes() < TEST_DURATION_MINUTES:
                iteration += 1

                self.run_iteration(iteration)

                remaining = TEST_DURATION_MINUTES - self.elapsed_minutes()
                self.log(f"  남은 시간: {remaining:.1f}분")

                if remaining > 0:
                    time.sleep(ITERATION_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            self.log("테스트 중단됨 (Ctrl+C)")
        except Exception as e:
            self.add_error('main_loop', str(e))
            traceback.print_exc()

        # 최종 리포트
        final_stats = self.generate_report()

        # 상세 에러 정보를 stats에 추가
        final_stats['error_details'] = self.error_tracker.get_summary()
        final_stats['all_errors'] = self.error_tracker.get_all_errors()

        # JSON 저장
        report_path = os.path.join(os.path.dirname(__file__), 'long_test_report.json')
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(final_stats, f, ensure_ascii=False, indent=2, default=str)
        self.log(f"상세 리포트 저장: {report_path}")

        # 에러 리포트 별도 저장 (상세 정보 포함)
        error_report_path = self.error_tracker.save_to_file()
        self.log(f"에러 리포트 저장: {error_report_path}")

        self.log(f"로그 디렉토리: {LOG_DIR}")

        return final_stats


def main():
    test = LongRunningTest()
    return test.run()


if __name__ == '__main__':
    main()
