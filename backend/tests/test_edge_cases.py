# -*- coding: utf-8 -*-
"""
엣지 케이스 및 스트레스 테스트
- 대량 데이터 처리
- 빈 데이터 처리
- 특수 문자 앱 ID
- 동시 실행 시뮬레이션
- 실패 후 재시도
"""
import os
import sys
import sqlite3
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# 상위 디렉토리 import
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.db import init_database, get_connection as get_apps_db, get_app_columns
from database.sitemap_db import (
    init_sitemap_database,
    get_connection as get_sitemap_db,
    save_discovered_apps,
    get_known_app_ids,
    get_discovery_stats,
    upsert_failed_app_detail,
    clear_failed_app_detail,
    prioritize_for_retry,
)
from analyzer.app_analyzer import analyze_and_update_scores
from config import timing_tracker

# 테스트용 함수들
from tests.test_repeated_execution import (
    generate_fake_app_ids,
    generate_fake_app_metadata,
    save_fake_app_to_db,
    clear_test_databases,
    simulate_sitemap_collection,
    simulate_details_fetch,
)


def test_large_batch():
    """대량 데이터 배치 처리 테스트"""
    print("\n" + "="*60)
    print("[테스트] 대량 배치 처리 (1000개 앱)")
    print("="*60)

    issues = []

    # 1000개 앱 ID 생성
    app_ids = generate_fake_app_ids("google_play", 1000, prefix="large_batch_")

    start_time = time.time()
    new_count, updated_count = simulate_sitemap_collection(app_ids, "google_play", "kr")
    duration = time.time() - start_time

    print(f"  처리 시간: {duration:.2f}초")
    print(f"  신규: {new_count}개, 업데이트: {updated_count}개")

    if new_count != 1000:
        issues.append(f"대량 배치 신규 앱 수 불일치: 예상 1000, 실제 {new_count}")
        print(f"  [✗] 신규 앱 수 불일치: 예상 1000, 실제 {new_count}")
    else:
        print(f"  [✓] 1000개 앱 정상 처리")

    # 중복 실행
    new_count2, updated_count2 = simulate_sitemap_collection(app_ids, "google_play", "kr")

    if new_count2 != 0:
        issues.append(f"중복 실행 시 신규 앱 발생: {new_count2}개")
        print(f"  [✗] 중복 실행 시 신규 앱 발생: {new_count2}개")
    else:
        print(f"  [✓] 중복 실행 시 신규 앱 0개 확인")

    return issues


def test_empty_data():
    """빈 데이터 처리 테스트"""
    print("\n" + "="*60)
    print("[테스트] 빈 데이터 처리")
    print("="*60)

    issues = []

    # 빈 리스트
    new_count, updated_count = save_discovered_apps([], "google_play")

    if new_count != 0 or updated_count != 0:
        issues.append(f"빈 리스트 처리 오류: new={new_count}, updated={updated_count}")
        print(f"  [✗] 빈 리스트 처리 오류")
    else:
        print(f"  [✓] 빈 리스트 정상 처리")

    return issues


def test_special_characters():
    """특수 문자 앱 ID 테스트"""
    print("\n" + "="*60)
    print("[테스트] 특수 문자 앱 ID")
    print("="*60)

    issues = []

    # 특수 문자가 포함된 앱 ID
    special_ids = [
        "com.test.app_with_underscore",
        "com.test.app-with-dash",
        "com.test.app.with.dots",
        "com.test.App123Numbers",
        "com.test.UPPERCASE",
    ]

    new_count, updated_count = simulate_sitemap_collection(special_ids, "google_play", "kr")

    if new_count != len(special_ids):
        issues.append(f"특수 문자 앱 ID 처리 오류: 예상 {len(special_ids)}, 실제 {new_count}")
        print(f"  [✗] 처리 오류: 예상 {len(special_ids)}, 실제 {new_count}")
    else:
        print(f"  [✓] 특수 문자 앱 ID 정상 처리")

    # 조회 확인
    known_ids = get_known_app_ids("google_play")
    for app_id in special_ids:
        if app_id not in known_ids:
            issues.append(f"특수 문자 앱 ID 누락: {app_id}")
            print(f"  [✗] 앱 ID 누락: {app_id}")

    if not any(app_id not in known_ids for app_id in special_ids):
        print(f"  [✓] 모든 특수 문자 앱 ID 조회 확인")

    return issues


def test_failed_app_retry():
    """실패한 앱 재시도 로직 테스트"""
    print("\n" + "="*60)
    print("[테스트] 실패한 앱 재시도 로직")
    print("="*60)

    issues = []

    # 실패 기록 추가
    test_apps = [
        ("com.failed.app1", "google_play", "kr", "network_error"),
        ("com.failed.app2", "google_play", "kr", "timeout"),
        ("com.failed.app3", "google_play", "kr", "not_found_404"),  # 영구 제외 대상
    ]

    for app_id, platform, country, reason in test_apps:
        upsert_failed_app_detail(app_id, platform, country, reason)

    # 재시도 대상 조회
    candidates = [(app_id, country) for app_id, _, country, _ in test_apps]
    retryable = prioritize_for_retry("google_play", candidates, 10)

    # not_found_404는 영구 제외되어야 함
    if ("com.failed.app3", "kr") in retryable:
        issues.append("영구 제외 대상(404)이 재시도 목록에 포함됨")
        print(f"  [✗] 404 앱이 재시도 목록에 포함됨")
    else:
        print(f"  [✓] 404 앱 영구 제외 확인")

    # 성공 후 기록 삭제
    clear_failed_app_detail("com.failed.app1", "google_play", "kr")

    # 다시 조회
    retryable2 = prioritize_for_retry("google_play", candidates, 10)
    if ("com.failed.app1", "kr") in retryable2:
        # 삭제 후에는 재시도 대상이 되어야 함 (실패 기록 없음)
        print(f"  [✓] 성공 후 실패 기록 삭제 확인")
    else:
        print(f"  [!] 성공 후 실패 기록 상태 확인")

    return issues


def test_multi_country():
    """다중 국가 테스트"""
    print("\n" + "="*60)
    print("[테스트] 다중 국가 데이터")
    print("="*60)

    issues = []

    countries = ['kr', 'us', 'jp', 'cn', 'gb']
    apps_per_country = 20

    for country in countries:
        app_ids = generate_fake_app_ids("app_store", apps_per_country, prefix=f"{country}_")
        simulate_sitemap_collection(app_ids, "app_store", country)

        # 상세정보 저장
        for app_id in app_ids:
            app_data = generate_fake_app_metadata(app_id, "app_store", country)
            save_fake_app_to_db(app_data)

    # 국가별 앱 수 확인
    conn = get_apps_db()
    cursor = conn.cursor()

    for country in countries:
        cursor.execute("SELECT COUNT(*) as cnt FROM apps WHERE country_code = ? AND platform = 'app_store'", (country,))
        count = cursor.fetchone()['cnt']

        if count < apps_per_country:  # 이전 테스트에서 추가된 앱들이 있을 수 있으므로 >= 로 체크
            issues.append(f"국가별 앱 수 부족: {country} = {count} (예상 >= {apps_per_country})")
            print(f"  [✗] {country}: {count}개 (예상 >= {apps_per_country})")
        else:
            print(f"  [✓] {country}: {count}개")

    conn.close()

    return issues


def test_repeated_analysis():
    """반복 분석 테스트"""
    print("\n" + "="*60)
    print("[테스트] 반복 분석 일관성")
    print("="*60)

    issues = []

    # 첫 번째 분석
    updated1, featured1 = analyze_and_update_scores()

    # 두 번째 분석
    updated2, featured2 = analyze_and_update_scores()

    # 점수와 주목 앱 수는 동일해야 함 (데이터 변경 없으므로)
    if updated1 != updated2:
        issues.append(f"반복 분석 시 처리 수 변경: {updated1} -> {updated2}")
        print(f"  [✗] 처리 수 변경: {updated1} -> {updated2}")
    else:
        print(f"  [✓] 처리 수 일관성: {updated1}개")

    if featured1 != featured2:
        issues.append(f"반복 분석 시 주목 앱 수 변경: {featured1} -> {featured2}")
        print(f"  [✗] 주목 앱 수 변경: {featured1} -> {featured2}")
    else:
        print(f"  [✓] 주목 앱 수 일관성: {featured1}개")

    return issues


def test_concurrent_writes():
    """동시 쓰기 테스트 (WAL 모드 검증)"""
    print("\n" + "="*60)
    print("[테스트] 동시 쓰기 (WAL 모드)")
    print("="*60)

    issues = []

    def write_batch(batch_id: int) -> Tuple[int, int]:
        app_ids = generate_fake_app_ids("google_play", 50, prefix=f"concurrent_{batch_id}_")
        return simulate_sitemap_collection(app_ids, "google_play", "kr")

    # 5개 스레드로 동시 쓰기
    total_new = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(write_batch, i) for i in range(5)]

        for future in as_completed(futures):
            try:
                new_count, updated_count = future.result()
                total_new += new_count
            except Exception as e:
                issues.append(f"동시 쓰기 오류: {str(e)}")
                print(f"  [✗] 동시 쓰기 오류: {str(e)}")

    expected_total = 50 * 5
    if total_new != expected_total:
        issues.append(f"동시 쓰기 결과 불일치: 예상 {expected_total}, 실제 {total_new}")
        print(f"  [✗] 동시 쓰기 결과 불일치: 예상 {expected_total}, 실제 {total_new}")
    else:
        print(f"  [✓] 동시 쓰기 정상: {total_new}개")

    return issues


def test_many_repeated_runs():
    """여러 번 반복 실행 테스트"""
    print("\n" + "="*60)
    print("[테스트] 10회 반복 실행")
    print("="*60)

    issues = []

    # 초기 데이터
    base_apps = generate_fake_app_ids("google_play", 100, prefix="repeat_base_")
    simulate_sitemap_collection(base_apps, "google_play", "kr")

    # 10회 반복
    for run in range(1, 11):
        # 매 실행마다 10개 신규 앱 추가
        new_apps = generate_fake_app_ids("google_play", 10, prefix=f"repeat_run{run}_")
        all_apps = base_apps + new_apps

        new_count, updated_count = simulate_sitemap_collection(all_apps, "google_play", "kr")

        if new_count != 10:
            issues.append(f"반복 실행 {run}: 신규 앱 수 불일치 (예상 10, 실제 {new_count})")
            print(f"  [✗] 실행 {run}: 신규={new_count}, 업데이트={updated_count}")
        else:
            print(f"  [✓] 실행 {run}: 신규={new_count}, 업데이트={updated_count}")

        # 다음 실행을 위해 base_apps에 추가
        base_apps.extend(new_apps)

    # 최종 확인
    known_ids = get_known_app_ids("google_play")
    expected_total = 100 + (10 * 10)  # 초기 100 + 10회 * 10개

    # 이전 테스트에서 추가된 앱들이 있을 수 있으므로 count만 확인
    actual_repeat_apps = len([aid for aid in known_ids if aid.startswith("com.repeat_")])

    if actual_repeat_apps < expected_total:
        issues.append(f"반복 실행 후 총 앱 수 부족: {actual_repeat_apps} < {expected_total}")
        print(f"  [✗] 최종 앱 수 부족: {actual_repeat_apps}")
    else:
        print(f"  [✓] 10회 반복 완료, 총 {actual_repeat_apps}개 앱")

    return issues


def main():
    """메인 테스트 실행"""
    timing_tracker.reset()
    all_issues = []

    print("\n" + "#"*60)
    print("#  엣지 케이스 및 스트레스 테스트")
    print("#"*60)

    # DB 초기화
    clear_test_databases()

    # 테스트 실행
    all_issues.extend(test_empty_data())
    all_issues.extend(test_special_characters())
    all_issues.extend(test_large_batch())
    all_issues.extend(test_failed_app_retry())
    all_issues.extend(test_multi_country())
    all_issues.extend(test_repeated_analysis())
    all_issues.extend(test_concurrent_writes())
    all_issues.extend(test_many_repeated_runs())

    # 최종 결과
    print("\n\n" + "#"*60)
    print("#  엣지 케이스 테스트 결과")
    print("#"*60)

    # 최종 통계
    stats = get_discovery_stats()
    print("\n[최종 통계]")
    for platform, data in stats.get('by_platform', {}).items():
        print(f"  {platform}: {data['total']}개")

    if all_issues:
        print(f"\n[✗] 발견된 문제점: {len(all_issues)}개")
        for i, issue in enumerate(all_issues, 1):
            print(f"  {i}. {issue}")
        return 1
    else:
        print("\n[✓] 모든 엣지 케이스 테스트 통과!")
        return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
