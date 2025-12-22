# -*- coding: utf-8 -*-
"""
Apple App Store 신규 앱 스크래퍼
"""
import sys
import os
from datetime import datetime
import requests
from app_store_scraper import AppStore

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import COUNTRIES, FETCH_LIMIT_PER_COUNTRY, LOG_FORMAT, get_proxies
from database.db import get_connection, log_step

# 현재 연도 자동 계산
CURRENT_YEAR = datetime.now().year
SEARCH_TERMS = ["new", str(CURRENT_YEAR), str(CURRENT_YEAR - 1)]


def scrape_new_apps_by_country(country_code, limit=FETCH_LIMIT_PER_COUNTRY):
    """
    특정 국가의 App Store에서 신규 앱 수집

    Args:
        country_code: 국가 코드 (예: 'kr', 'us')
        limit: 수집할 최대 앱 개수

    Returns:
        수집된 앱 개수
    """
    start_time = datetime.now()
    log_step(f"App Store 수집 [{country_code}]", "시작", start_time)

    apps_data = []

    try:
        # iTunes Search API를 사용하여 신규 앱 검색
        # term을 다양하게 하여 최신 앱 수집
        for term in SEARCH_TERMS:
            try:
                url = f"https://itunes.apple.com/search"
                params = {
                    'term': term,
                    'country': country_code.upper(),
                    'entity': 'software',
                    'limit': limit // len(SEARCH_TERMS),
                    'sort': 'recent'
                }

                # 프록시 설정 적용 (설정되지 않으면 None이므로 일반 통신)
                proxies = get_proxies()
                response = requests.get(url, params=params, timeout=10, proxies=proxies)
                if response.status_code != 200:
                    print(f"API 요청 실패 [{country_code}]: {response.status_code}")
                    continue

                data = response.json()
                results = data.get('results', [])

                for result in results:
                    # 중복 체크
                    if any(app['app_id'] == str(result.get('trackId')) for app in apps_data):
                        continue

                    apps_data.append({
                        'app_id': str(result.get('trackId')),
                        'platform': 'app_store',
                        'country_code': country_code,
                        'title': result.get('trackName'),
                        'developer': result.get('artistName'),
                        'icon_url': result.get('artworkUrl512') or result.get('artworkUrl100'),
                        'rating': result.get('averageUserRating'),
                        'rating_count': result.get('userRatingCount'),
                        'installs': None,  # App Store는 설치 수 제공 안 함
                        'price': str(result.get('price', 0)) if result.get('price') else 'Free',
                        'category': result.get('primaryGenreName'),
                        'description': result.get('description', '')[:500],  # 처음 500자만
                        'release_date': result.get('releaseDate'),
                        'updated_date': result.get('currentVersionReleaseDate'),
                        'version': result.get('version'),
                        'url': result.get('trackViewUrl'),
                    })

            except (requests.RequestException, ValueError, KeyError) as e:
                print(f"검색 실패 [{term}]: {str(e)}")
                continue

        # 데이터베이스에 저장
        saved_count = save_apps_to_db(apps_data)

        log_step(f"App Store 수집 [{country_code}]", f"완료 ({saved_count}개 앱 저장)", start_time)
        return saved_count

    except Exception as e:
        log_step(f"App Store 수집 [{country_code}]", f"오류 발생: {str(e)}", start_time)
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
    """모든 국가의 App Store에서 신규 앱 수집"""
    total_start = datetime.now()
    log_step("App Store 전체 수집", "시작", total_start)

    total_apps = 0
    for country in COUNTRIES:
        count = scrape_new_apps_by_country(country['code'])
        total_apps += count

    log_step("App Store 전체 수집", f"완료 (총 {total_apps}개 앱)", total_start)
    return total_apps


if __name__ == "__main__":
    scrape_all_countries()
