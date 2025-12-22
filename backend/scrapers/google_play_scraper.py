# -*- coding: utf-8 -*-
"""
Google Play Store 신규 앱 스크래퍼
"""
import sys
import os
from datetime import datetime
from google_play_scraper import search, app

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import COUNTRIES, FETCH_LIMIT_PER_COUNTRY, LOG_FORMAT, get_proxies
from database.db import get_connection, log_step

# 프록시 설정이 있으면 google-play-scraper가 사용하는 requests 세션에 적용
proxies = get_proxies()
if proxies:
    import requests
    original_request = requests.Session.request

    def patched_request(self, method, url, **kwargs):
        """프록시를 자동으로 추가하는 패치된 request 메서드"""
        if 'proxies' not in kwargs:
            kwargs['proxies'] = proxies
        return original_request(self, method, url, **kwargs)

    requests.Session.request = patched_request


def scrape_new_apps_by_country(country_code, limit=FETCH_LIMIT_PER_COUNTRY):
    """
    특정 국가의 Google Play Store에서 신규 앱 수집

    Args:
        country_code: 국가 코드 (예: 'kr', 'us')
        limit: 수집할 최대 앱 개수

    Returns:
        수집된 앱 개수
    """
    start_time = datetime.now()
    log_step(f"Google Play 수집 [{country_code}]", "시작", start_time)

    apps_data = []

    try:
        # 신규 앱 검색 (최근 출시된 앱들)
        # google-play-scraper는 collection API가 제한적이므로
        # 카테고리별로 검색하여 최신 앱 수집
        search_queries = ["new apps", "최신 앱", "new release"]

        for query in search_queries:
            try:
                results = search(
                    query,
                    lang="en",
                    country=country_code,
                    n_hits=limit // len(search_queries)
                )

                for result in results[:limit // len(search_queries)]:
                    try:
                        # 상세 정보 가져오기
                        app_details = app(
                            result['appId'],
                            lang='en',
                            country=country_code
                        )

                        apps_data.append({
                            'app_id': app_details.get('appId'),
                            'platform': 'google_play',
                            'country_code': country_code,
                            'title': app_details.get('title'),
                            'developer': app_details.get('developer'),
                            'icon_url': app_details.get('icon'),
                            'rating': app_details.get('score'),
                            'rating_count': app_details.get('ratings'),
                            'installs': app_details.get('installs'),
                            'price': str(app_details.get('price', 0)) if app_details.get('price') else 'Free',
                            'category': app_details.get('genre'),
                            'description': app_details.get('description', '')[:500],  # 처음 500자만
                            'release_date': app_details.get('released'),
                            'updated_date': app_details.get('updated'),
                            'version': app_details.get('version'),
                            'url': app_details.get('url'),
                        })
                    except Exception as e:
                        print(f"앱 상세 정보 수집 실패: {result.get('appId', 'unknown')} - {str(e)}")
                        continue

            except Exception as e:
                print(f"검색 실패 [{query}]: {str(e)}")
                continue

        # 데이터베이스에 저장
        saved_count = save_apps_to_db(apps_data)

        log_step(f"Google Play 수집 [{country_code}]", f"완료 ({saved_count}개 앱 저장)", start_time)
        return saved_count

    except Exception as e:
        log_step(f"Google Play 수집 [{country_code}]", f"오류 발생: {str(e)}", start_time)
        return 0


def save_apps_to_db(apps_data):
    """앱 데이터를 데이터베이스에 저장"""
    if not apps_data:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    saved_count = 0

    for app_data in apps_data:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO apps (
                    app_id, platform, country_code, title, developer,
                    icon_url, rating, rating_count, installs, price,
                    category, description, release_date, updated_date,
                    version, url, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                app_data['app_id'],
                app_data['platform'],
                app_data['country_code'],
                app_data['title'],
                app_data['developer'],
                app_data['icon_url'],
                app_data['rating'],
                app_data['rating_count'],
                app_data['installs'],
                app_data['price'],
                app_data['category'],
                app_data['description'],
                app_data['release_date'],
                app_data['updated_date'],
                app_data['version'],
                app_data['url'],
            ))
            saved_count += 1
        except Exception as e:
            print(f"데이터 저장 실패: {app_data.get('app_id')} - {str(e)}")

    conn.commit()
    conn.close()

    return saved_count


def scrape_all_countries():
    """모든 국가의 Google Play Store에서 신규 앱 수집"""
    total_start = datetime.now()
    log_step("Google Play 전체 수집", "시작", total_start)

    total_apps = 0
    for country in COUNTRIES:
        count = scrape_new_apps_by_country(country['code'])
        total_apps += count

    log_step("Google Play 전체 수집", f"완료 (총 {total_apps}개 앱)", total_start)
    return total_apps


if __name__ == "__main__":
    scrape_all_countries()
