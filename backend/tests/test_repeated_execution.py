# -*- coding: utf-8 -*-
"""
반복 실행 테스트 스크립트
- 가짜 데이터로 앱 발견 및 상세정보 수집 시뮬레이션
- 반복 실행 시 데이터 일관성 검증
- 신규 앱 추가 후 발견 기능 테스트
"""
import os
import sys
import sqlite3
import json
import random
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple

# 상위 디렉토리 import
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.db import init_database, get_connection as get_apps_db, get_app_columns
from database.sitemap_db import (
    init_sitemap_database,
    get_connection as get_sitemap_db,
    save_discovered_apps,
    get_known_app_ids,
    get_discovery_stats,
    get_new_apps_since,
)
from analyzer.app_analyzer import analyze_and_update_scores
from config import timing_tracker


# ============ 가짜 데이터 생성 ============

def generate_fake_app_ids(platform: str, count: int, prefix: str = "") -> List[str]:
    """가짜 앱 ID 생성"""
    if platform == "google_play":
        return [f"com.{prefix}test.app{i:04d}" for i in range(count)]
    else:  # app_store
        return [f"{1000000000 + i + (hash(prefix) % 1000000)}" for i in range(count)]


def generate_fake_app_metadata(app_id: str, platform: str, country_code: str = "kr") -> Dict:
    """가짜 앱 상세정보 생성"""
    random.seed(hash(app_id) % 2**32)  # 동일한 app_id면 동일한 데이터

    titles = ["테스트앱", "MyApp", "SuperGame", "UtilityPro", "PhotoEditor", "MusicPlayer"]
    developers = ["TestDev", "AppsInc", "GameStudio", "UtilityLab"]
    categories = ["GAME", "PRODUCTIVITY", "SOCIAL", "ENTERTAINMENT", "TOOLS"]

    base_data = {
        'app_id': app_id,
        'platform': platform,
        'country_code': country_code,
        'title': f"{random.choice(titles)} {app_id[-4:]}",
        'developer': random.choice(developers),
        'developer_id': f"dev_{hash(app_id) % 10000}",
        'rating': round(random.uniform(3.0, 5.0), 1),
        'rating_count': random.randint(10, 100000),
        'reviews_count': random.randint(5, 50000),
        'category': random.choice(categories),
        'description': f"테스트 앱 설명 - {app_id}",
        'version': f"{random.randint(1, 10)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
        'release_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d"),
        'updated_date': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
        'free': 1 if random.random() > 0.3 else 0,
        'price': 0 if random.random() > 0.3 else round(random.uniform(0.99, 9.99), 2),
        'icon_url': f"https://example.com/icon/{app_id}.png",
        'url': f"https://play.google.com/store/apps/details?id={app_id}" if platform == "google_play"
               else f"https://apps.apple.com/app/id{app_id}",
    }

    if platform == "google_play":
        base_data['installs'] = f"{random.choice([100, 1000, 10000, 100000, 1000000])}+"
        base_data['installs_min'] = random.randint(100, 1000000)

    return base_data


def save_fake_app_to_db(app_data: Dict):
    """가짜 앱 데이터를 apps DB에 저장"""
    conn = get_apps_db()
    cursor = conn.cursor()

    columns = get_app_columns()
    values = []
    for col in columns:
        values.append(app_data.get(col))

    placeholders = ', '.join(['?' for _ in columns])
    column_str = ', '.join(columns)

    try:
        cursor.execute(f"""
            INSERT OR REPLACE INTO apps ({column_str})
            VALUES ({placeholders})
        """, values)
        conn.commit()
    except Exception as e:
        print(f"[오류] 앱 저장 실패: {app_data.get('app_id')} - {e}")
        conn.rollback()
    finally:
        conn.close()


# ============ 테스트 함수들 ============

def clear_test_databases():
    """테스트 DB 초기화 (모든 데이터 삭제)"""
    print("\n" + "="*60)
    print("[준비] 테스트 데이터베이스 초기화 중...")

    # apps.db 초기화
    init_database(force_reset=True)
    init_database()  # 테이블 재생성

    # sitemap_tracking.db 초기화
    sitemap_db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "sitemap_tracking.db")
    if os.path.exists(sitemap_db_path):
        os.remove(sitemap_db_path)
    init_sitemap_database()

    print("[완료] 테스트 데이터베이스 초기화 완료")


def verify_db_state(expected_discovery_count: Dict[str, int], expected_apps_count: int,
                    label: str = "") -> List[str]:
    """DB 상태 검증 및 문제점 반환"""
    issues = []
    print(f"\n--- {label} DB 상태 검증 ---")

    # sitemap_tracking.db 검증
    sitemap_conn = get_sitemap_db()
    cursor = sitemap_conn.cursor()

    for platform, expected in expected_discovery_count.items():
        cursor.execute("SELECT COUNT(*) as cnt FROM app_discovery WHERE platform = ?", (platform,))
        actual = cursor.fetchone()['cnt']
        status = "✓" if actual == expected else "✗"
        print(f"  [{status}] app_discovery ({platform}): 예상 {expected}, 실제 {actual}")
        if actual != expected:
            issues.append(f"app_discovery ({platform}) 불일치: 예상 {expected}, 실제 {actual}")

    sitemap_conn.close()

    # apps.db 검증
    apps_conn = get_apps_db()
    cursor = apps_conn.cursor()

    cursor.execute("SELECT COUNT(*) as cnt FROM apps")
    actual_apps = cursor.fetchone()['cnt']
    status = "✓" if actual_apps == expected_apps_count else "✗"
    print(f"  [{status}] apps: 예상 {expected_apps_count}, 실제 {actual_apps}")
    if actual_apps != expected_apps_count:
        issues.append(f"apps 테이블 불일치: 예상 {expected_apps_count}, 실제 {actual_apps}")

    # 중복 검사
    cursor.execute("""
        SELECT app_id, platform, country_code, COUNT(*) as cnt
        FROM apps
        GROUP BY app_id, platform, country_code
        HAVING cnt > 1
    """)
    duplicates = cursor.fetchall()
    if duplicates:
        print(f"  [✗] 중복 발견: {len(duplicates)}개")
        for dup in duplicates[:5]:
            print(f"      - {dup['app_id']} ({dup['platform']}/{dup['country_code']}): {dup['cnt']}개")
        issues.append(f"apps 테이블에 {len(duplicates)}개 중복 발견")
    else:
        print("  [✓] 중복 없음")

    apps_conn.close()

    return issues


def simulate_sitemap_collection(app_ids: List[str], platform: str, country_code: str = None) -> Tuple[int, int]:
    """Sitemap 수집 시뮬레이션"""
    print(f"\n[Sitemap 수집] {platform}: {len(app_ids)}개 앱 ID 처리 중...")

    # 메타데이터 생성
    app_metadata = {}
    for app_id in app_ids:
        app_metadata[app_id] = {
            'lastmod': datetime.now().strftime("%Y-%m-%d"),
            'priority': 0.8,
            'url': f"https://example.com/app/{app_id}",
            'country_code': country_code,
        }

    new_count, updated_count = save_discovered_apps(
        app_ids=app_ids,
        platform=platform,
        sitemap_source="test_sitemap.xml",
        country_code=country_code,
        app_metadata=app_metadata
    )

    print(f"  -> 신규: {new_count}개, 업데이트: {updated_count}개")
    return new_count, updated_count


def simulate_details_fetch(platform: str, limit: int = 100, country_code: str = "kr") -> int:
    """앱 상세정보 수집 시뮬레이션"""
    print(f"\n[상세정보 수집] {platform}: 최대 {limit}개 처리 중...")

    # 아직 상세정보가 없는 앱 조회
    sitemap_conn = get_sitemap_db()
    cursor = sitemap_conn.cursor()

    cursor.execute("""
        SELECT app_id, country_code FROM app_discovery
        WHERE platform = ?
        ORDER BY first_seen_at DESC
        LIMIT ?
    """, (platform, limit))

    discovered_apps = cursor.fetchall()
    sitemap_conn.close()

    # apps.db에서 이미 있는 앱 확인
    apps_conn = get_apps_db()
    cursor = apps_conn.cursor()

    existing_ids = set()
    for app in discovered_apps:
        cursor.execute("""
            SELECT app_id FROM apps
            WHERE app_id = ? AND platform = ? AND country_code = ?
        """, (app['app_id'], platform, app['country_code'] or country_code))
        if cursor.fetchone():
            existing_ids.add(app['app_id'])

    apps_conn.close()

    # 새 앱만 저장
    saved_count = 0
    for app in discovered_apps:
        if app['app_id'] not in existing_ids:
            app_data = generate_fake_app_metadata(
                app['app_id'],
                platform,
                app['country_code'] or country_code
            )
            save_fake_app_to_db(app_data)
            saved_count += 1

    print(f"  -> 저장: {saved_count}개 (기존: {len(existing_ids)}개)")
    return saved_count


def run_full_cycle(google_app_ids: List[str], appstore_app_ids: List[str],
                   cycle_name: str, country_code: str = "kr"):
    """전체 수집 사이클 실행"""
    print(f"\n{'='*60}")
    print(f"[{cycle_name}] 전체 수집 사이클 시작")
    print(f"{'='*60}")

    # 1. Sitemap 수집
    print("\n[단계 1] Sitemap 수집")
    gp_new, gp_updated = simulate_sitemap_collection(google_app_ids, "google_play", country_code)
    as_new, as_updated = simulate_sitemap_collection(appstore_app_ids, "app_store", country_code)

    # 2. 상세정보 수집
    print("\n[단계 2] 상세정보 수집")
    gp_details = simulate_details_fetch("google_play", limit=len(google_app_ids), country_code=country_code)
    as_details = simulate_details_fetch("app_store", limit=len(appstore_app_ids), country_code=country_code)

    # 3. 분석 및 점수 계산
    print("\n[단계 3] 분석 및 점수 계산")
    updated, featured = analyze_and_update_scores()
    print(f"  -> 처리: {updated}개, 주목 앱: {featured}개")

    return {
        'sitemap': {'google_play': (gp_new, gp_updated), 'app_store': (as_new, as_updated)},
        'details': {'google_play': gp_details, 'app_store': as_details},
        'analysis': {'updated': updated, 'featured': featured}
    }


def check_new_app_discovery(platform: str, expected_new_ids: Set[str]) -> List[str]:
    """신규 앱 발견 확인"""
    issues = []

    # 오늘 발견된 앱 조회
    today = datetime.now().strftime("%Y-%m-%d")
    new_apps = get_new_apps_since(platform, today)
    found_ids = {app['app_id'] for app in new_apps}

    # 검증
    missing = expected_new_ids - found_ids
    extra = found_ids - expected_new_ids

    if missing:
        issues.append(f"[{platform}] 미발견 앱: {missing}")
        print(f"  [✗] 미발견 앱: {missing}")

    if extra:
        # extra는 이전 실행에서 추가된 것일 수 있으므로 경고만
        print(f"  [!] 추가 발견 앱 (이전 실행?): {len(extra)}개")

    if not missing:
        print(f"  [✓] {platform}: 모든 신규 앱 발견 완료 ({len(found_ids)}개)")

    return issues


def test_country_update():
    """국가 코드 업데이트 테스트"""
    print("\n" + "="*60)
    print("[테스트] 국가 코드 업데이트 검증")
    print("="*60)

    issues = []

    # 같은 앱을 다른 국가로 발견
    app_id = "com.test.country.app001"

    # 먼저 kr로 발견
    save_discovered_apps([app_id], "google_play", country_code="kr")

    # 다시 us로 업데이트 시도
    save_discovered_apps([app_id], "google_play", country_code="us",
                         app_metadata={app_id: {'country_code': 'us'}})

    # 결과 확인
    conn = get_sitemap_db()
    cursor = conn.cursor()
    cursor.execute("SELECT country_code FROM app_discovery WHERE app_id = ? AND platform = ?",
                   (app_id, "google_play"))
    result = cursor.fetchone()
    conn.close()

    # COALESCE 로직에 따라 기존 값 유지되어야 함 (kr)
    if result['country_code'] == 'kr':
        print("  [✓] 국가 코드 유지 확인 (기존 값 보존)")
    else:
        issues.append(f"국가 코드 변경됨: kr -> {result['country_code']}")
        print(f"  [✗] 국가 코드 변경됨: kr -> {result['country_code']}")

    return issues


def test_duplicate_handling():
    """중복 처리 테스트"""
    print("\n" + "="*60)
    print("[테스트] 중복 데이터 처리 검증")
    print("="*60)

    issues = []

    # 같은 앱을 여러 번 저장
    app_id = "com.test.duplicate.app001"

    for i in range(3):
        save_discovered_apps([app_id], "google_play", country_code="kr")
        app_data = generate_fake_app_metadata(app_id, "google_play", "kr")
        save_fake_app_to_db(app_data)

    # 중복 확인
    conn = get_sitemap_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) as cnt FROM app_discovery
        WHERE app_id = ? AND platform = ?
    """, (app_id, "google_play"))
    discovery_count = cursor.fetchone()['cnt']
    conn.close()

    apps_conn = get_apps_db()
    cursor = apps_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) as cnt FROM apps
        WHERE app_id = ? AND platform = ? AND country_code = ?
    """, (app_id, "google_play", "kr"))
    apps_count = cursor.fetchone()['cnt']
    apps_conn.close()

    if discovery_count == 1:
        print("  [✓] app_discovery 중복 방지 확인")
    else:
        issues.append(f"app_discovery 중복: {discovery_count}개")
        print(f"  [✗] app_discovery 중복: {discovery_count}개")

    if apps_count == 1:
        print("  [✓] apps 테이블 중복 방지 확인 (REPLACE)")
    else:
        issues.append(f"apps 테이블 중복: {apps_count}개")
        print(f"  [✗] apps 테이블 중복: {apps_count}개")

    return issues


# ============ 메인 테스트 실행 ============

def main():
    """메인 테스트 실행"""
    timing_tracker.reset()
    all_issues = []

    print("\n" + "#"*60)
    print("#  반복 실행 테스트 시작")
    print("#"*60)

    # 1. DB 초기화
    clear_test_databases()

    # 2. 1차 실행 - 초기 데이터
    print("\n\n" + "="*60)
    print("  [1차 실행] 초기 데이터 수집")
    print("="*60)

    gp_apps_1 = generate_fake_app_ids("google_play", 50, prefix="batch1_")
    as_apps_1 = generate_fake_app_ids("app_store", 30, prefix="batch1_")

    result_1 = run_full_cycle(gp_apps_1, as_apps_1, "1차 실행")

    # 1차 검증
    issues_1 = verify_db_state(
        expected_discovery_count={'google_play': 50, 'app_store': 30},
        expected_apps_count=80,
        label="1차 실행 후"
    )
    all_issues.extend(issues_1)

    # 3. 2차 실행 - 동일 데이터 반복 (중복 테스트)
    print("\n\n" + "="*60)
    print("  [2차 실행] 동일 데이터 반복 (중복 처리 테스트)")
    print("="*60)

    result_2 = run_full_cycle(gp_apps_1, as_apps_1, "2차 실행")

    # 2차 검증 - 데이터 수 변경 없어야 함
    issues_2 = verify_db_state(
        expected_discovery_count={'google_play': 50, 'app_store': 30},
        expected_apps_count=80,
        label="2차 실행 후 (변경 없어야 함)"
    )
    all_issues.extend(issues_2)

    # 신규 앱 발견 수 검증 (0이어야 함)
    if result_2['sitemap']['google_play'][0] > 0:
        issue = f"2차 실행에서 Google Play 신규 앱 발견됨: {result_2['sitemap']['google_play'][0]}개"
        all_issues.append(issue)
        print(f"  [✗] {issue}")
    else:
        print("  [✓] Google Play 중복 실행 시 신규 앱 0개 확인")

    # 4. 3차 실행 - 신규 앱 추가
    print("\n\n" + "="*60)
    print("  [3차 실행] 신규 앱 추가 테스트")
    print("="*60)

    gp_apps_new = generate_fake_app_ids("google_play", 20, prefix="batch2_")
    as_apps_new = generate_fake_app_ids("app_store", 15, prefix="batch2_")

    # 기존 앱 + 신규 앱 함께 전달 (실제 sitemap처럼)
    gp_apps_3 = gp_apps_1 + gp_apps_new
    as_apps_3 = as_apps_1 + as_apps_new

    result_3 = run_full_cycle(gp_apps_3, as_apps_3, "3차 실행 (신규 앱 추가)")

    # 3차 검증
    issues_3 = verify_db_state(
        expected_discovery_count={'google_play': 70, 'app_store': 45},
        expected_apps_count=115,
        label="3차 실행 후"
    )
    all_issues.extend(issues_3)

    # 신규 앱 발견 검증
    print("\n[신규 앱 발견 검증]")
    gp_discovery_issues = check_new_app_discovery("google_play", set(gp_apps_new))
    as_discovery_issues = check_new_app_discovery("app_store", set(as_apps_new))
    all_issues.extend(gp_discovery_issues)
    all_issues.extend(as_discovery_issues)

    # 5. 추가 테스트
    country_issues = test_country_update()
    all_issues.extend(country_issues)

    dup_issues = test_duplicate_handling()
    all_issues.extend(dup_issues)

    # 6. 4차 실행 - 다른 국가 데이터
    print("\n\n" + "="*60)
    print("  [4차 실행] 다른 국가 데이터 테스트")
    print("="*60)

    gp_apps_us = generate_fake_app_ids("google_play", 10, prefix="us_")
    as_apps_us = generate_fake_app_ids("app_store", 10, prefix="us_")

    result_4 = run_full_cycle(gp_apps_us, as_apps_us, "4차 실행 (US)", country_code="us")

    # 4차 검증 - 국가별로 별도 카운트되어야 함
    # app_discovery는 platform별로만 unique (국가 무관)
    # apps 테이블은 (app_id, platform, country_code)로 unique

    apps_conn = get_apps_db()
    cursor = apps_conn.cursor()
    cursor.execute("SELECT COUNT(*) as cnt FROM apps WHERE country_code = 'us'")
    us_count = cursor.fetchone()['cnt']
    cursor.execute("SELECT COUNT(*) as cnt FROM apps WHERE country_code = 'kr'")
    kr_count = cursor.fetchone()['cnt']
    cursor.execute("SELECT COUNT(*) as cnt FROM apps")
    total_count = cursor.fetchone()['cnt']
    apps_conn.close()

    print(f"\n[국가별 앱 수] KR: {kr_count}, US: {us_count}, 전체: {total_count}")

    # 최종 통계
    print("\n\n" + "#"*60)
    print("#  최종 통계")
    print("#"*60)

    stats = get_discovery_stats()
    print("\n[발견 통계]")
    for platform, data in stats.get('by_platform', {}).items():
        today = stats.get('today', {}).get(platform, 0)
        week = stats.get('last_7_days', {}).get(platform, 0)
        print(f"  {platform}: 전체 {data['total']}개, 오늘 {today}개, 7일간 {week}개")

    # 최종 결과
    print("\n\n" + "#"*60)
    print("#  테스트 결과 요약")
    print("#"*60)

    if all_issues:
        print(f"\n[✗] 발견된 문제점: {len(all_issues)}개")
        for i, issue in enumerate(all_issues, 1):
            print(f"  {i}. {issue}")
        return 1
    else:
        print("\n[✓] 모든 테스트 통과!")
        return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
