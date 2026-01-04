#!/usr/bin/env python3
"""
RSS vs app-store-scraper 비교 테스트
실제 앱을 사용하여 두 방식의 수집 결과를 비교합니다.
"""
import sys
import os
import time
import json
from datetime import datetime
from typing import Dict, List, Any

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from app_store_scraper import AppStore

# ===== 설정 =====
# 테스트할 앱 목록 (id, country)
TEST_APPS = [
    {"id": "333903271", "name": "Twitter/X", "country": "us"},
    {"id": "310633997", "name": "WhatsApp", "country": "us"},
    {"id": "284882215", "name": "Facebook", "country": "us"},
    {"id": "389801252", "name": "Instagram", "country": "us"},
    {"id": "544007664", "name": "YouTube", "country": "us"},
]

# 각 방식으로 수집할 리뷰 수
REVIEW_COUNT = 100

# ===== RSS 방식 =====
RSS_BASE_URL = 'https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortBy=mostRecent/json'


def fetch_rss_reviews(app_id: str, country: str, max_reviews: int = 100) -> Dict[str, Any]:
    """RSS로 리뷰 수집"""
    start_time = time.time()
    reviews = []
    page = 0
    errors = []
    requests_made = 0

    while len(reviews) < max_reviews:
        page += 1
        url = RSS_BASE_URL.format(country=country, page=page, app_id=app_id)

        try:
            requests_made += 1
            response = requests.get(url, timeout=30)

            if response.status_code != 200:
                errors.append(f"Page {page}: HTTP {response.status_code}")
                break

            data = response.json()
            entries = data.get('feed', {}).get('entry', [])

            # 첫 번째는 앱 정보
            if len(entries) <= 1:
                break

            for entry in entries[1:]:
                if len(reviews) >= max_reviews:
                    break

                review = {
                    'review_id': entry.get('id', {}).get('label', ''),
                    'user_name': entry.get('author', {}).get('name', {}).get('label', ''),
                    'user_image': None,  # RSS에서 제공 안됨
                    'score': int(entry.get('im:rating', {}).get('label', 0)),
                    'title': entry.get('title', {}).get('label', ''),
                    'content': entry.get('content', {}).get('label', ''),
                    'thumbs_up_count': int(entry.get('im:voteCount', {}).get('label', 0)),
                    'app_version': entry.get('im:version', {}).get('label', ''),
                    'reviewed_at': entry.get('updated', {}).get('label', ''),
                    'developer_reply': None,  # RSS에서 제공 안됨
                    'developer_reply_date': None,  # RSS에서 제공 안됨
                }
                reviews.append(review)

            time.sleep(0.01)  # Rate limiting

        except Exception as e:
            errors.append(f"Page {page}: {str(e)}")
            break

    elapsed_time = time.time() - start_time

    return {
        'method': 'RSS',
        'reviews': reviews,
        'count': len(reviews),
        'time_seconds': elapsed_time,
        'requests_made': requests_made,
        'errors': errors,
        'fields_available': list(reviews[0].keys()) if reviews else []
    }


def fetch_scraper_reviews(app_id: str, country: str, app_name: str = "Unknown", max_reviews: int = 100) -> Dict[str, Any]:
    """app-store-scraper로 리뷰 수집"""
    start_time = time.time()
    reviews = []
    errors = []

    try:
        app = AppStore(country=country, app_name=app_name, app_id=app_id)
        app.review(how_many=max_reviews)

        for review in app.reviews:
            review_data = {
                'review_id': review.get('id', '') if isinstance(review.get('id'), str) else str(review.get('id', '')),
                'user_name': review.get('userName', ''),
                'user_image': None,  # 라이브러리에서 제공 안됨
                'score': review.get('rating', 0),
                'title': review.get('title', ''),
                'content': review.get('review', ''),
                'thumbs_up_count': None,  # 라이브러리에서 제공 안됨
                'app_version': review.get('version', ''),
                'reviewed_at': review.get('date').isoformat() if review.get('date') else '',
                'developer_reply': review.get('developerResponse', {}).get('body') if review.get('developerResponse') else None,
                'developer_reply_date': review.get('developerResponse', {}).get('modified').isoformat() if review.get('developerResponse') and review.get('developerResponse').get('modified') else None,
            }
            reviews.append(review_data)

    except Exception as e:
        errors.append(str(e))

    elapsed_time = time.time() - start_time

    return {
        'method': 'app-store-scraper',
        'reviews': reviews,
        'count': len(reviews),
        'time_seconds': elapsed_time,
        'requests_made': None,  # 라이브러리 내부에서 관리
        'errors': errors,
        'fields_available': list(reviews[0].keys()) if reviews else []
    }


def compare_fields(rss_result: Dict, scraper_result: Dict) -> Dict:
    """두 방식의 필드 비교"""
    rss_fields = set(rss_result['fields_available'])
    scraper_fields = set(scraper_result['fields_available'])

    # 각 필드별 데이터 존재 여부 확인
    field_comparison = {}

    all_fields = [
        'review_id', 'user_name', 'user_image', 'score', 'title',
        'content', 'thumbs_up_count', 'app_version', 'reviewed_at',
        'developer_reply', 'developer_reply_date'
    ]

    for field in all_fields:
        rss_has_data = False
        scraper_has_data = False

        # RSS 데이터 확인
        if rss_result['reviews']:
            values = [r.get(field) for r in rss_result['reviews'][:10]]
            rss_has_data = any(v is not None and v != '' and v != 0 for v in values)

        # Scraper 데이터 확인
        if scraper_result['reviews']:
            values = [r.get(field) for r in scraper_result['reviews'][:10]]
            scraper_has_data = any(v is not None and v != '' and v != 0 for v in values)

        field_comparison[field] = {
            'rss': '✅' if rss_has_data else '❌',
            'scraper': '✅' if scraper_has_data else '❌'
        }

    return field_comparison


def run_comparison_test():
    """비교 테스트 실행"""
    print("=" * 80)
    print("🔍 RSS vs app-store-scraper 비교 테스트")
    print("=" * 80)
    print(f"테스트 시작: {datetime.now().isoformat()}")
    print(f"수집 목표: 앱당 {REVIEW_COUNT}개 리뷰")
    print()

    all_results = []

    for app_info in TEST_APPS:
        app_id = app_info['id']
        app_name = app_info['name']
        country = app_info['country']

        print("-" * 80)
        print(f"📱 테스트 앱: {app_name} (ID: {app_id}, Country: {country})")
        print("-" * 80)

        # RSS 테스트
        print("\n[1] RSS 방식 테스트 중...")
        rss_result = fetch_rss_reviews(app_id, country, REVIEW_COUNT)
        print(f"    수집된 리뷰: {rss_result['count']}개")
        print(f"    소요 시간: {rss_result['time_seconds']:.2f}초")
        print(f"    요청 수: {rss_result['requests_made']}")
        if rss_result['errors']:
            print(f"    오류: {rss_result['errors']}")

        # Scraper 테스트
        print("\n[2] app-store-scraper 방식 테스트 중...")
        scraper_result = fetch_scraper_reviews(app_id, country, app_name, REVIEW_COUNT)
        print(f"    수집된 리뷰: {scraper_result['count']}개")
        print(f"    소요 시간: {scraper_result['time_seconds']:.2f}초")
        if scraper_result['errors']:
            print(f"    오류: {scraper_result['errors']}")

        # 필드 비교
        print("\n[3] 필드별 데이터 존재 비교:")
        field_comparison = compare_fields(rss_result, scraper_result)
        print(f"    {'필드명':<25} | {'RSS':<5} | {'Scraper':<5}")
        print(f"    {'-' * 25}-+-{'-' * 5}-+-{'-' * 5}")
        for field, values in field_comparison.items():
            print(f"    {field:<25} | {values['rss']:<5} | {values['scraper']:<5}")

        # 샘플 리뷰 출력
        print("\n[4] 샘플 리뷰 비교:")
        if rss_result['reviews']:
            print("\n    === RSS 첫 번째 리뷰 ===")
            sample = rss_result['reviews'][0]
            for k, v in sample.items():
                v_str = str(v)[:80] + "..." if len(str(v)) > 80 else str(v)
                print(f"      {k}: {v_str}")

        if scraper_result['reviews']:
            print("\n    === Scraper 첫 번째 리뷰 ===")
            sample = scraper_result['reviews'][0]
            for k, v in sample.items():
                v_str = str(v)[:80] + "..." if len(str(v)) > 80 else str(v)
                print(f"      {k}: {v_str}")

        result = {
            'app': app_info,
            'rss': {
                'count': rss_result['count'],
                'time_seconds': rss_result['time_seconds'],
                'requests': rss_result['requests_made'],
                'errors': len(rss_result['errors'])
            },
            'scraper': {
                'count': scraper_result['count'],
                'time_seconds': scraper_result['time_seconds'],
                'errors': len(scraper_result['errors'])
            },
            'field_comparison': field_comparison
        }
        all_results.append(result)

        print()
        time.sleep(1)  # 앱 간 딜레이

    # 최종 요약
    print("=" * 80)
    print("📊 최종 비교 요약")
    print("=" * 80)

    print("\n### 1. 성능 비교")
    print(f"{'앱 이름':<15} | {'RSS 시간':<10} | {'Scraper 시간':<12} | {'RSS 수집':<10} | {'Scraper 수집':<12}")
    print(f"{'-' * 15}-+-{'-' * 10}-+-{'-' * 12}-+-{'-' * 10}-+-{'-' * 12}")

    total_rss_time = 0
    total_scraper_time = 0
    total_rss_count = 0
    total_scraper_count = 0

    for result in all_results:
        app_name = result['app']['name'][:14]
        rss_time = f"{result['rss']['time_seconds']:.2f}s"
        scraper_time = f"{result['scraper']['time_seconds']:.2f}s"
        rss_count = str(result['rss']['count'])
        scraper_count = str(result['scraper']['count'])

        print(f"{app_name:<15} | {rss_time:<10} | {scraper_time:<12} | {rss_count:<10} | {scraper_count:<12}")

        total_rss_time += result['rss']['time_seconds']
        total_scraper_time += result['scraper']['time_seconds']
        total_rss_count += result['rss']['count']
        total_scraper_count += result['scraper']['count']

    print(f"{'-' * 15}-+-{'-' * 10}-+-{'-' * 12}-+-{'-' * 10}-+-{'-' * 12}")
    print(f"{'합계':<15} | {total_rss_time:.2f}s{'':<5} | {total_scraper_time:.2f}s{'':<7} | {total_rss_count:<10} | {total_scraper_count:<12}")

    print("\n### 2. 필드 지원 비교")
    print("""
+---------------------------+--------+------------+
| 필드                      | RSS    | Scraper    |
+---------------------------+--------+------------+
| review_id                 | ✅     | ✅         |
| user_name                 | ✅     | ✅         |
| user_image                | ❌     | ❌         |
| score (평점)              | ✅     | ✅         |
| title                     | ✅     | ✅         |
| content                   | ✅     | ✅         |
| thumbs_up_count           | ✅     | ❌         |
| app_version               | ✅     | ✅         |
| reviewed_at               | ✅     | ✅         |
| developer_reply           | ❌     | ✅         |
| developer_reply_date      | ❌     | ✅         |
| language (언어)           | ❌     | ❌         |
+---------------------------+--------+------------+
""")

    print("\n### 3. 장단점 분석")
    print("""
┌────────────────────────────────────────────────────────────────────────────────┐
│ 📌 RSS 방식                                                                     │
├────────────────────────────────────────────────────────────────────────────────┤
│ 장점:                                                                          │
│   ✅ thumbs_up_count (추천 수) 제공                                             │
│   ✅ 안정적인 공식 API                                                          │
│   ✅ 외부 라이브러리 의존성 없음                                                 │
│   ✅ 빠른 요청 속도 (페이지당 50개 리뷰)                                          │
│                                                                                │
│ 단점:                                                                          │
│   ❌ 개발자 답변 미제공                                                         │
│   ❌ 언어 정보 미제공                                                           │
│   ❌ 최대 500개 리뷰 제한 (10페이지 × 50)                                        │
└────────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────────┐
│ 📌 app-store-scraper 방식                                                      │
├────────────────────────────────────────────────────────────────────────────────┤
│ 장점:                                                                          │
│   ✅ 개발자 답변 (developerResponse) 제공                                       │
│   ✅ 더 많은 리뷰 수집 가능 (제한 없음)                                          │
│   ✅ 사용하기 쉬운 API                                                          │
│                                                                                │
│ 단점:                                                                          │
│   ❌ thumbs_up_count 미제공                                                     │
│   ❌ 외부 라이브러리 의존성 (구버전 requests 요구)                                │
│   ❌ 마지막 업데이트: 2020년 (4년 이상 미유지보수)                                │
│   ❌ 요청 속도가 느림 (20개씩 증분)                                              │
│   ❌ 비공식 스크래핑 방식 (언제든 차단 가능)                                      │
└────────────────────────────────────────────────────────────────────────────────┘
""")

    print("\n### 4. 권장 사항")
    print("""
┌────────────────────────────────────────────────────────────────────────────────┐
│ 🎯 권장: 현재 RSS 방식 유지 + 선택적 Scraper 보완                               │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│ 이유:                                                                          │
│   1. RSS는 Apple의 공식 API로 안정성이 높음                                      │
│   2. app-store-scraper는 4년 이상 미유지보수로 장기 사용에 위험                   │
│   3. 의존성 충돌 문제 (requests 2.23.0 요구)                                     │
│   4. thumbs_up_count는 RSS에서만 제공됨                                         │
│                                                                                │
│ 보완 전략:                                                                     │
│   - 개발자 답변이 중요한 경우: app-store-web-scraper 검토 (더 최신)              │
│   - 500개 이상 리뷰 필요시: 여러 국가에서 분산 수집 (현재 방식 유지)              │
│   - 또는 iTunes Search API + RSS 조합 사용                                      │
│                                                                                │
└────────────────────────────────────────────────────────────────────────────────┘
""")

    # 결과 JSON 저장
    output_file = os.path.join(os.path.dirname(__file__), 'comparison_results.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'test_date': datetime.now().isoformat(),
            'review_count_target': REVIEW_COUNT,
            'results': all_results,
            'summary': {
                'total_rss_time': total_rss_time,
                'total_scraper_time': total_scraper_time,
                'total_rss_reviews': total_rss_count,
                'total_scraper_reviews': total_scraper_count
            }
        }, f, indent=2, ensure_ascii=False, default=str)

    print(f"\n결과가 저장됨: {output_file}")
    print(f"\n테스트 완료: {datetime.now().isoformat()}")


if __name__ == '__main__':
    run_comparison_test()
