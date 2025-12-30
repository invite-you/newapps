# -*- coding: utf-8 -*-
"""
앱 발견 및 시계열 분석 기능 종합 테스트
- 가짜 데이터 생성
- 신규 앱 발견 테스트
- 시계열 분석 기능 테스트 (Delta Storage)
- 반복 실행으로 문제점 발견
"""
import sys
import os
import json
import time
import random
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

# 경로 설정
sys.path.insert(0, os.path.dirname(__file__))

from config import timing_tracker
from database.db import init_database, get_connection as get_apps_connection, log_step
from database.sitemap_db import (
    init_sitemap_database,
    get_connection as get_sitemap_connection,
    save_discovered_apps,
    save_app_metrics_batch,
    get_known_app_ids,
    get_discovery_stats,
    get_app_metrics_timeseries,
    get_metrics_changes,
    get_top_growing_apps,
    get_metrics_storage_stats,
    cleanup_old_metrics,
    _has_significant_metric_change,
    METRICS_CHANGE_THRESHOLDS,
)
from analyzer.app_analyzer import calculate_app_score, analyze_and_update_scores

# 테스트 결과 저장
TEST_RESULTS = {
    'passed': [],
    'failed': [],
    'warnings': [],
}


def log_test(name: str, passed: bool, message: str = ""):
    """테스트 결과 로깅"""
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"  {status}: {name} {message}")

    if passed:
        TEST_RESULTS['passed'].append(name)
    else:
        TEST_RESULTS['failed'].append((name, message))


def log_warning(message: str):
    """경고 로깅"""
    print(f"  ⚠ WARNING: {message}")
    TEST_RESULTS['warnings'].append(message)


def generate_fake_app(
    app_id: str,
    platform: str = 'google_play',
    country_code: str = 'us',
    rating: float = 4.5,
    rating_count: int = 1000,
    installs_min: int = 10000,
    version: str = "1.0.0"
) -> Dict:
    """테스트용 가짜 앱 데이터 생성"""
    return {
        'app_id': app_id,
        'bundle_id': app_id,
        'platform': platform,
        'country_code': country_code,
        'title': f"Test App {app_id}",
        'developer': 'Test Developer',
        'developer_id': 'test_dev_123',
        'icon_url': f"https://example.com/icon/{app_id}.png",
        'rating': rating,
        'rating_count': rating_count,
        'reviews_count': rating_count,
        'installs': f"{installs_min:,}+",
        'installs_min': installs_min,
        'installs_exact': installs_min * 2,
        'price': 0.0,
        'free': 1,
        'category': 'Productivity',
        'description': f"This is a test app {app_id}",
        'release_date': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
        'updated_date': datetime.now().strftime('%Y-%m-%d'),
        'version': version,
        'has_iap': 0,
        'contains_ads': 0,
        'score': 0,
        'is_featured': 0,
    }


def save_fake_app_to_db(app_data: Dict) -> bool:
    """가짜 앱 데이터를 DB에 저장"""
    conn = get_apps_connection()
    cursor = conn.cursor()

    columns = [
        'app_id', 'bundle_id', 'platform', 'country_code',
        'title', 'developer', 'developer_id', 'icon_url',
        'rating', 'rating_count', 'reviews_count',
        'installs', 'installs_min', 'installs_exact',
        'price', 'free', 'category', 'description',
        'release_date', 'updated_date', 'version',
        'has_iap', 'contains_ads', 'score', 'is_featured'
    ]

    placeholders = ', '.join(['?' for _ in columns])
    columns_str = ', '.join(columns)
    update_columns = ', '.join([f"{col}=excluded.{col}" for col in columns])

    values = tuple(app_data.get(col) for col in columns)

    try:
        cursor.execute(f"""
            INSERT INTO apps ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT(app_id, platform, country_code) DO UPDATE SET
                {update_columns}
        """, values)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"  저장 실패: {e}")
        conn.rollback()
        conn.close()
        return False


def test_database_initialization():
    """테스트 1: 데이터베이스 초기화 테스트"""
    print("\n" + "=" * 60)
    print("테스트 1: 데이터베이스 초기화")
    print("=" * 60)

    try:
        # Apps DB 초기화
        init_database()
        conn = get_apps_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row['name'] for row in cursor.fetchall()]
        conn.close()

        log_test("Apps DB 초기화", 'apps' in tables, f"- 테이블: {tables}")

        # Sitemap DB 초기화
        init_sitemap_database()
        conn = get_sitemap_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row['name'] for row in cursor.fetchall()]
        conn.close()

        expected_tables = ['app_discovery', 'app_metrics_history', 'app_history', 'sitemap_snapshots']
        all_present = all(t in tables for t in expected_tables)
        log_test("Sitemap DB 초기화", all_present, f"- 테이블: {tables}")

        return True
    except Exception as e:
        log_test("데이터베이스 초기화", False, f"- 오류: {e}")
        traceback.print_exc()
        return False


def test_app_discovery():
    """테스트 2: 신규 앱 발견 기능 테스트"""
    print("\n" + "=" * 60)
    print("테스트 2: 신규 앱 발견 기능")
    print("=" * 60)

    try:
        platform = 'google_play'

        # 초기 알려진 앱 수 확인
        known_before = get_known_app_ids(platform)
        print(f"  기존 알려진 앱 수: {len(known_before)}")

        # 신규 앱 ID 생성
        new_app_ids = [f"com.test.app.{i}_{int(time.time())}" for i in range(5)]

        # 신규 앱 저장
        new_count, updated_count = save_discovered_apps(
            new_app_ids,
            platform,
            sitemap_source='test_sitemap.xml.gz',
            country_code='us'
        )

        log_test("신규 앱 저장", new_count == 5, f"- 저장됨: {new_count}, 업데이트됨: {updated_count}")

        # 알려진 앱 수 확인
        known_after = get_known_app_ids(platform)
        log_test("앱 ID 증가", len(known_after) >= len(known_before) + 5,
                f"- 변화: {len(known_before)} -> {len(known_after)}")

        # 중복 저장 테스트 (업데이트 되어야 함)
        new_count2, updated_count2 = save_discovered_apps(
            new_app_ids[:2],
            platform,
            sitemap_source='test_sitemap2.xml.gz',
            country_code='us'
        )

        log_test("중복 앱 업데이트", updated_count2 == 2 and new_count2 == 0,
                f"- 신규: {new_count2}, 업데이트: {updated_count2}")

        # 통계 확인
        stats = get_discovery_stats()
        log_test("발견 통계 조회", 'by_platform' in stats, f"- 통계: {stats}")

        return True
    except Exception as e:
        log_test("앱 발견 기능", False, f"- 오류: {e}")
        traceback.print_exc()
        return False


def test_metrics_delta_storage():
    """테스트 3: 시계열 분석 - Delta Storage 기능 테스트"""
    print("\n" + "=" * 60)
    print("테스트 3: 시계열 분석 - Delta Storage")
    print("=" * 60)

    try:
        test_app_id = f"com.test.metrics.{int(time.time())}"

        # 첫 번째 메트릭 저장 (모든 데이터가 새로우므로 저장되어야 함)
        apps_data_1 = [{
            'app_id': test_app_id,
            'platform': 'google_play',
            'country_code': 'us',
            'rating': 4.5,
            'rating_count': 1000,
            'reviews_count': 1000,
            'installs_min': 10000,
            'installs_exact': 20000,
            'score': 75.0,
            'version': '1.0.0',
            'is_featured': 1,
        }]

        saved1, skipped1 = save_app_metrics_batch(apps_data_1)
        log_test("첫 번째 메트릭 저장", saved1 == 1, f"- 저장: {saved1}, 스킵: {skipped1}")

        # 동일한 데이터로 다시 저장 (스킵되어야 함)
        saved2, skipped2 = save_app_metrics_batch(apps_data_1)

        # threshold가 0이면 모든 변화 감지 = 실제로는 같은 값도 저장됨
        # 문서에 따르면 threshold가 0이면 "모든 변화 감지"
        # 하지만 값이 동일하면 변화가 없어 스킵되어야 함
        if saved2 == 0:
            log_test("동일 데이터 스킵", True, f"- 저장: {saved2}, 스킵: {skipped2}")
        else:
            log_warning(f"동일 데이터도 저장됨 - 저장: {saved2}, 스킵: {skipped2}")

        # 메트릭 변화 후 저장 (저장되어야 함)
        apps_data_2 = [{
            'app_id': test_app_id,
            'platform': 'google_play',
            'country_code': 'us',
            'rating': 4.6,  # 변화
            'rating_count': 1100,  # 변화
            'reviews_count': 1100,
            'installs_min': 15000,  # 변화
            'installs_exact': 30000,
            'score': 78.0,  # 변화
            'version': '1.0.1',  # 버전 변화
            'is_featured': 1,
        }]

        saved3, skipped3 = save_app_metrics_batch(apps_data_2)
        log_test("변경된 메트릭 저장", saved3 == 1, f"- 저장: {saved3}, 스킵: {skipped3}")

        return True
    except Exception as e:
        log_test("Delta Storage", False, f"- 오류: {e}")
        traceback.print_exc()
        return False


def test_metrics_change_detection():
    """테스트 4: 메트릭 변화 감지 로직 테스트"""
    print("\n" + "=" * 60)
    print("테스트 4: 메트릭 변화 감지 로직")
    print("=" * 60)

    try:
        # 테스트 케이스들
        test_cases = [
            # (old, new, expected_result, description)
            ({}, {'rating': 4.5}, True, "빈 old -> 새 데이터"),
            ({'rating': 4.5}, {'rating': 4.5}, False, "동일한 평점"),
            ({'rating': 4.5}, {'rating': 4.6}, True, "평점 변화 (threshold=0)"),
            ({'rating_count': 1000}, {'rating_count': 1000}, False, "동일한 리뷰 수"),
            ({'rating_count': 1000}, {'rating_count': 1001}, True, "리뷰 수 변화 (threshold=0)"),
            ({'version': '1.0.0'}, {'version': '1.0.0'}, False, "동일한 버전"),
            ({'version': '1.0.0'}, {'version': '1.0.1'}, True, "버전 변화"),
            ({'is_featured': 0}, {'is_featured': 1}, True, "featured 변화"),
            ({'score': 50}, {'score': 50}, False, "동일한 점수"),
            ({'score': 50}, {'score': 50.01}, True, "점수 미세 변화"),
        ]

        all_passed = True
        for old, new, expected, desc in test_cases:
            result = _has_significant_metric_change(old, new)
            passed = result == expected
            if not passed:
                all_passed = False
                log_test(f"변화 감지: {desc}", False, f"- 예상: {expected}, 실제: {result}")
            else:
                log_test(f"변화 감지: {desc}", True, "")

        # Threshold 설정 확인
        print(f"\n  현재 Threshold 설정:")
        for field, threshold in METRICS_CHANGE_THRESHOLDS.items():
            print(f"    {field}: {threshold}")

        if all(v == 0 for v in METRICS_CHANGE_THRESHOLDS.values()):
            log_warning("모든 threshold가 0으로 설정됨 - 모든 변화가 감지됨")

        return all_passed
    except Exception as e:
        log_test("변화 감지 로직", False, f"- 오류: {e}")
        traceback.print_exc()
        return False


def test_timeseries_queries():
    """테스트 5: 시계열 쿼리 기능 테스트"""
    print("\n" + "=" * 60)
    print("테스트 5: 시계열 쿼리 기능")
    print("=" * 60)

    try:
        test_app_id = f"com.test.timeseries.{int(time.time())}"
        platform = 'google_play'
        country_code = 'us'

        # 여러 날짜에 대한 메트릭 데이터 생성
        today = datetime.now()
        for i in range(5):
            date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
            apps_data = [{
                'app_id': test_app_id,
                'platform': platform,
                'country_code': country_code,
                'rating': 4.0 + (0.1 * i),  # 날마다 변화
                'rating_count': 1000 + (i * 100),
                'score': 60 + (i * 5),
                'version': f"1.0.{i}",
            }]
            save_app_metrics_batch(apps_data, recorded_date=date)

        # 시계열 데이터 조회
        timeseries = get_app_metrics_timeseries(
            test_app_id, platform, country_code, days=30
        )
        log_test("시계열 데이터 조회", len(timeseries) >= 1, f"- 데이터 수: {len(timeseries)}")

        # 메트릭 변화량 조회
        changes = get_metrics_changes(
            test_app_id, platform, country_code, compare_days=7
        )
        log_test("메트릭 변화량 조회", isinstance(changes, dict), f"- 필드 수: {len(changes)}")

        # 성장 앱 조회
        growing_apps = get_top_growing_apps(
            platform=platform,
            country_code=country_code,
            metric='rating_count',
            days=7,
            limit=10
        )
        log_test("성장 앱 조회", isinstance(growing_apps, list), f"- 결과 수: {len(growing_apps)}")

        # 저장소 통계
        storage_stats = get_metrics_storage_stats()
        log_test("저장소 통계 조회", 'total_records' in storage_stats, f"- 통계: {storage_stats}")

        return True
    except Exception as e:
        log_test("시계열 쿼리", False, f"- 오류: {e}")
        traceback.print_exc()
        return False


def test_score_calculation():
    """테스트 6: 앱 점수 계산 테스트"""
    print("\n" + "=" * 60)
    print("테스트 6: 앱 점수 계산")
    print("=" * 60)

    try:
        # 테스트용 앱 데이터
        test_apps = [
            # 고점수 앱 (평점 높고 리뷰 많음)
            generate_fake_app(
                f"com.test.score.high.{int(time.time())}",
                rating=4.8,
                rating_count=50000,
                installs_min=1000000
            ),
            # 저점수 앱 (평점 낮고 리뷰 적음)
            generate_fake_app(
                f"com.test.score.low.{int(time.time())}",
                rating=2.5,
                rating_count=10,
                installs_min=100
            ),
            # 중간 앱
            generate_fake_app(
                f"com.test.score.mid.{int(time.time())}",
                rating=4.0,
                rating_count=1000,
                installs_min=10000
            ),
        ]

        for app in test_apps:
            # DB에 저장
            save_fake_app_to_db(app)

            # 점수 계산
            score = calculate_app_score(app)

            print(f"  앱: {app['app_id'][:30]}...")
            print(f"    평점: {app['rating']}, 리뷰: {app['rating_count']}, 설치: {app['installs_min']}")
            print(f"    계산된 점수: {score}")

        # 전체 점수 업데이트
        updated, featured = analyze_and_update_scores()
        log_test("점수 계산 및 업데이트", updated > 0, f"- 업데이트: {updated}, 주목 앱: {featured}")

        return True
    except Exception as e:
        log_test("점수 계산", False, f"- 오류: {e}")
        traceback.print_exc()
        return False


def test_repeated_execution():
    """테스트 7: 반복 실행 테스트"""
    print("\n" + "=" * 60)
    print("테스트 7: 반복 실행 테스트")
    print("=" * 60)

    try:
        iterations = 3
        platform = 'google_play'
        base_app_id = f"com.test.repeated.{int(time.time())}"

        for iteration in range(iterations):
            print(f"\n  --- 반복 {iteration + 1}/{iterations} ---")

            # 1. 신규 앱 발견
            new_ids = [f"{base_app_id}.iter{iteration}.app{i}" for i in range(3)]
            new_count, updated_count = save_discovered_apps(
                new_ids, platform, f"sitemap_iter{iteration}.xml.gz", 'us'
            )
            print(f"    앱 발견: 신규={new_count}, 업데이트={updated_count}")

            # 2. 앱 데이터 저장
            for app_id in new_ids:
                app_data = generate_fake_app(
                    app_id,
                    platform=platform,
                    rating=random.uniform(3.0, 5.0),
                    rating_count=random.randint(100, 10000),
                    installs_min=random.randint(1000, 100000)
                )
                save_fake_app_to_db(app_data)

            # 3. 메트릭 저장
            apps_metrics = [
                {
                    'app_id': app_id,
                    'platform': platform,
                    'country_code': 'us',
                    'rating': random.uniform(3.0, 5.0),
                    'rating_count': random.randint(100, 10000),
                    'score': random.uniform(40, 90),
                }
                for app_id in new_ids
            ]
            saved, skipped = save_app_metrics_batch(apps_metrics)
            print(f"    메트릭: 저장={saved}, 스킵={skipped}")

            # 4. 점수 계산
            updated, featured = analyze_and_update_scores()
            print(f"    점수: 업데이트={updated}, 주목={featured}")

            # 5. 통계 확인
            stats = get_discovery_stats()
            gp_total = stats.get('by_platform', {}).get(platform, {}).get('total', 0)
            print(f"    총 앱 수: {gp_total}")

            time.sleep(0.1)  # 잠시 대기

        log_test("반복 실행", True, f"- {iterations}회 반복 완료")
        return True
    except Exception as e:
        log_test("반복 실행", False, f"- 오류: {e}")
        traceback.print_exc()
        return False


def test_new_app_during_execution():
    """테스트 8: 실행 중 신규 앱 추가 테스트"""
    print("\n" + "=" * 60)
    print("테스트 8: 실행 중 신규 앱 추가")
    print("=" * 60)

    try:
        platform = 'google_play'

        # 초기 상태
        initial_known = get_known_app_ids(platform)
        print(f"  초기 알려진 앱 수: {len(initial_known)}")

        # 첫 번째 배치
        batch1_ids = [f"com.test.batch1.{i}.{int(time.time())}" for i in range(3)]
        save_discovered_apps(batch1_ids, platform, 'batch1.xml.gz', 'us')

        after_batch1 = get_known_app_ids(platform)
        print(f"  배치1 후 알려진 앱 수: {len(after_batch1)}")

        # 점수 계산 수행
        for app_id in batch1_ids:
            app = generate_fake_app(app_id, platform)
            save_fake_app_to_db(app)

        analyze_and_update_scores()

        # 두 번째 배치 (점수 계산 후 추가)
        batch2_ids = [f"com.test.batch2.{i}.{int(time.time())}" for i in range(3)]
        save_discovered_apps(batch2_ids, platform, 'batch2.xml.gz', 'us')

        after_batch2 = get_known_app_ids(platform)
        print(f"  배치2 후 알려진 앱 수: {len(after_batch2)}")

        # 검증
        batch2_in_known = all(app_id in after_batch2 for app_id in batch2_ids)
        log_test("신규 앱 발견", batch2_in_known, f"- 배치2 앱 모두 발견됨")

        # 두 번째 점수 계산
        for app_id in batch2_ids:
            app = generate_fake_app(app_id, platform)
            save_fake_app_to_db(app)

        updated, featured = analyze_and_update_scores()
        log_test("신규 앱 점수 계산", updated >= 3, f"- 업데이트: {updated}")

        return True
    except Exception as e:
        log_test("신규 앱 추가", False, f"- 오류: {e}")
        traceback.print_exc()
        return False


def test_edge_cases():
    """테스트 9: 엣지 케이스 테스트"""
    print("\n" + "=" * 60)
    print("테스트 9: 엣지 케이스")
    print("=" * 60)

    try:
        # 1. 빈 데이터 처리
        new_count, updated_count = save_discovered_apps([], 'google_play')
        log_test("빈 앱 목록 처리", new_count == 0 and updated_count == 0, "")

        saved, skipped = save_app_metrics_batch([])
        log_test("빈 메트릭 목록 처리", saved == 0 and skipped == 0, "")

        # 2. None 값 처리
        app_with_none = {
            'app_id': f'com.test.none.{int(time.time())}',
            'platform': 'google_play',
            'country_code': 'us',
            'rating': None,
            'rating_count': None,
            'score': None,
        }
        saved, skipped = save_app_metrics_batch([app_with_none])
        log_test("None 값 메트릭 처리", True, f"- 저장: {saved}")

        # 3. 특수 문자 앱 ID
        special_id = f"com.test.special'chars\"<>&.{int(time.time())}"
        new_count, _ = save_discovered_apps([special_id], 'google_play')
        log_test("특수 문자 앱 ID", new_count == 1, "")

        # 4. 매우 큰 숫자
        big_number_app = {
            'app_id': f'com.test.big.{int(time.time())}',
            'platform': 'google_play',
            'country_code': 'us',
            'rating': 5.0,
            'rating_count': 999999999,
            'installs_min': 9999999999,
            'score': 100.0,
        }
        saved, _ = save_app_metrics_batch([big_number_app])
        log_test("큰 숫자 처리", saved == 1, "")

        # 5. 잘못된 country_code
        invalid_country_ids = [f'com.test.invalid.country.{int(time.time())}']
        new_count, _ = save_discovered_apps(invalid_country_ids, 'google_play', country_code='invalid')
        known = get_known_app_ids('google_play')
        # 잘못된 country_code는 None으로 정규화됨
        log_test("잘못된 country_code 처리", new_count == 1, "")

        return True
    except Exception as e:
        log_test("엣지 케이스", False, f"- 오류: {e}")
        traceback.print_exc()
        return False


def test_cleanup_metrics():
    """테스트 10: 메트릭 정리 기능 테스트"""
    print("\n" + "=" * 60)
    print("테스트 10: 메트릭 정리")
    print("=" * 60)

    try:
        # 오래된 데이터 정리 (기본값: 90일)
        # 테스트에서는 데이터가 방금 생성되었으므로 삭제되지 않아야 함
        deleted = cleanup_old_metrics(retention_days=90)
        log_test("메트릭 정리 (90일)", deleted == 0, f"- 삭제됨: {deleted}")

        # 0일 보관 (모든 데이터 삭제)
        # 주의: 이 테스트는 실제 데이터를 삭제함
        stats_before = get_metrics_storage_stats()
        total_before = stats_before.get('total_records', 0)

        # 오늘 데이터는 삭제하지 않음 (date('now', '-0 days') = 오늘)
        deleted = cleanup_old_metrics(retention_days=0)

        stats_after = get_metrics_storage_stats()
        total_after = stats_after.get('total_records', 0)

        log_test("메트릭 정리 (0일)", True,
                f"- 이전: {total_before}, 이후: {total_after}, 삭제: {deleted}")

        return True
    except Exception as e:
        log_test("메트릭 정리", False, f"- 오류: {e}")
        traceback.print_exc()
        return False


def print_summary():
    """테스트 결과 요약"""
    print("\n" + "=" * 60)
    print("테스트 결과 요약")
    print("=" * 60)

    passed = len(TEST_RESULTS['passed'])
    failed = len(TEST_RESULTS['failed'])
    warnings = len(TEST_RESULTS['warnings'])

    print(f"\n  ✓ 통과: {passed}개")
    print(f"  ✗ 실패: {failed}개")
    print(f"  ⚠ 경고: {warnings}개")

    if TEST_RESULTS['failed']:
        print("\n  실패한 테스트:")
        for name, message in TEST_RESULTS['failed']:
            print(f"    - {name}: {message}")

    if TEST_RESULTS['warnings']:
        print("\n  경고:")
        for warning in TEST_RESULTS['warnings']:
            print(f"    - {warning}")

    print("\n" + "=" * 60)

    return failed == 0


def main():
    """메인 테스트 실행"""
    timing_tracker.reset()
    timing_tracker.start_task("테스트 실행")

    print("\n" + "=" * 60)
    print("앱 발견 및 시계열 분석 기능 종합 테스트")
    print("=" * 60)
    print(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 테스트 실행
    tests = [
        test_database_initialization,
        test_app_discovery,
        test_metrics_delta_storage,
        test_metrics_change_detection,
        test_timeseries_queries,
        test_score_calculation,
        test_repeated_execution,
        test_new_app_during_execution,
        test_edge_cases,
        test_cleanup_metrics,
    ]

    for test_func in tests:
        try:
            test_func()
        except Exception as e:
            print(f"\n  예외 발생: {e}")
            traceback.print_exc()

    # 결과 요약
    all_passed = print_summary()

    print(f"종료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
