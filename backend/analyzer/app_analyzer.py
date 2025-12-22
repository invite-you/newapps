# -*- coding: utf-8 -*-
"""
주목할만한 앱 선별 로직
점수 기반 시스템으로 우수한 앱을 자동 선별
"""
import sys
import os
import math
from datetime import datetime

try:
    from dateutil import parser as date_parser
except ImportError:
    date_parser = None

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import SCORE_WEIGHTS, MINIMUM_RATING, MINIMUM_RATING_COUNT, MINIMUM_SCORE, LOG_FORMAT
from database.db import get_connection, log_step


def calculate_app_score(app):
    """
    앱 점수 계산

    점수 기준:
    - 평점 (30%): 평점이 높을수록 높은 점수
    - 리뷰 수 (20%): 리뷰가 많을수록 높은 점수
    - 설치 수 (20%): 설치가 많을수록 높은 점수
    - 최신성 (20%): 최근 출시/업데이트될수록 높은 점수
    - 성장률 (10%): 짧은 기간에 많은 리뷰를 받으면 높은 점수

    Returns:
        0-100 사이의 점수
    """
    score = 0.0

    # 1. 평점 점수 (0-30점)
    rating = app['rating'] or 0
    if rating > 0:
        score += (rating / 5.0) * 30 * SCORE_WEIGHTS['rating'] / 0.3

    # 2. 리뷰 수 점수 (0-20점)
    rating_count = app['rating_count'] or 0
    if rating_count > 0:
        # 로그 스케일 적용 (10,000개 리뷰 = 만점)
        review_score = min(math.log10(rating_count + 1) / 4.0, 1.0)
        score += review_score * 20 * SCORE_WEIGHTS['rating_count'] / 0.2

    # 3. 설치 수 점수 (0-20점) - Google Play만 해당
    installs = app['installs'] or "0"
    if app['platform'] == 'google_play' and installs:
        try:
            # "10,000+" 형식을 숫자로 변환
            installs_num = int(installs.replace('+', '').replace(',', ''))
            # 로그 스케일 적용 (1,000,000 설치 = 만점)
            install_score = min(math.log10(installs_num + 1) / 6.0, 1.0)
            score += install_score * 20 * SCORE_WEIGHTS['installs'] / 0.2
        except (ValueError, TypeError) as e:
            print(f"설치 수 파싱 실패: {installs} - {str(e)}")

    # 4. 최신성 점수 (0-20점)
    # 최근 30일 이내 = 만점, 그 이후로는 감소
    try:
        updated_date = app['updated_date'] or app['release_date']
        if updated_date and date_parser:
            # 날짜 파싱 (다양한 형식 처리)
            update_dt = date_parser.parse(updated_date)
            # 타임존 처리 (naive datetime으로 통일)
            if update_dt.tzinfo is not None:
                update_dt = update_dt.replace(tzinfo=None)
            days_ago = (datetime.now() - update_dt).days

            if days_ago <= 30:
                freshness_score = 1.0
            elif days_ago <= 90:
                freshness_score = 0.7
            elif days_ago <= 180:
                freshness_score = 0.4
            else:
                freshness_score = 0.1

            score += freshness_score * 20 * SCORE_WEIGHTS['freshness'] / 0.2
    except (ValueError, TypeError, AttributeError) as e:
        print(f"날짜 파싱 실패: {updated_date} - {str(e)}")

    # 5. 성장률 점수 (0-10점)
    # 리뷰 수 대비 앱의 나이로 계산
    try:
        if rating_count and rating_count > 0:
            release_date = app['release_date']
            if release_date and date_parser:
                release_dt = date_parser.parse(release_date)
                # 타임존 처리 (naive datetime으로 통일)
                if release_dt.tzinfo is not None:
                    release_dt = release_dt.replace(tzinfo=None)
                days_since_release = (datetime.now() - release_dt).days

                if days_since_release > 0:
                    # 하루 평균 리뷰 수
                    reviews_per_day = rating_count / days_since_release
                    # 하루 10개 이상 리뷰 = 만점
                    growth_score = min(math.log10(reviews_per_day * 10 + 1) / 2.0, 1.0)
                    score += growth_score * 10 * SCORE_WEIGHTS['growth_rate'] / 0.1
    except (ValueError, TypeError, AttributeError, ZeroDivisionError) as e:
        print(f"성장률 계산 실패: {str(e)}")

    return round(score, 2)


def analyze_and_update_scores():
    """모든 앱의 점수를 계산하고 업데이트"""
    start_time = datetime.now()
    log_step("앱 점수 계산", "시작", start_time)

    conn = get_connection()
    cursor = conn.cursor()

    # 모든 앱 조회
    cursor.execute("SELECT * FROM apps")
    apps = cursor.fetchall()

    updated_count = 0
    featured_count = 0

    for app in apps:
        # 점수 계산
        score = calculate_app_score(app)

        # 주목할만한 앱 판단
        is_featured = 0
        if (app['rating'] and app['rating'] >= MINIMUM_RATING and
            app['rating_count'] and app['rating_count'] >= MINIMUM_RATING_COUNT and
            score >= MINIMUM_SCORE):
            is_featured = 1
            featured_count += 1

        # 데이터베이스 업데이트
        cursor.execute("""
            UPDATE apps
            SET score = ?, is_featured = ?
            WHERE id = ?
        """, (score, is_featured, app['id']))

        updated_count += 1

    conn.commit()
    conn.close()

    log_step("앱 점수 계산", f"완료 ({updated_count}개 업데이트, {featured_count}개 주목 앱)", start_time)
    return updated_count, featured_count


if __name__ == "__main__":
    # python-dateutil 패키지 확인
    if date_parser is None:
        print("경고: python-dateutil 패키지가 설치되지 않았습니다.")
        print("날짜 기반 점수 계산이 제한됩니다.")
        print("설치 명령: pip install python-dateutil")

    analyze_and_update_scores()
