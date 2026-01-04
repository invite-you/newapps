# App Store 리뷰 수집 방식 비교 분석

## RSS vs app-store-scraper 라이브러리

**테스트 일자:** 2026-01-04
**테스트 앱:** Twitter/X, WhatsApp, Facebook, Instagram, YouTube
**수집 목표:** 앱당 100개 리뷰

---

## 1. 테스트 결과 요약

### 1.1 성능 비교

| 앱 이름 | RSS 시간 | RSS 수집 | Scraper 시간 | Scraper 수집 |
|---------|----------|----------|--------------|--------------|
| Twitter/X | 1.09s | 0개* | 9.23s | 0개 |
| WhatsApp | 1.19s | 100개 | 1.00s | 0개 |
| Facebook | 1.60s | 100개 | 0.46s | 0개 |
| Instagram | 1.82s | 100개 | 0.76s | 0개 |
| YouTube | 1.07s | 100개 | 0.83s | 0개 |
| **합계** | **6.77s** | **400개** | **12.28s** | **0개** |

> *Twitter/X는 SSL 핸드셰이크 오류 발생 (네트워크 환경 문제)

### 1.2 핵심 발견

- **RSS 방식**: 안정적으로 동작, 4개 앱에서 100개씩 총 400개 리뷰 수집 성공
- **app-store-scraper**: 완전히 작동하지 않음 (마지막 업데이트 2020년, API 변경됨)

---

## 2. 수집 필드 비교

### 2.1 필드별 지원 현황

| 필드 | RSS | app-store-scraper* | 설명 |
|------|-----|-------------------|------|
| `review_id` | ✅ | ✅ | 리뷰 고유 식별자 |
| `user_name` | ✅ | ✅ | 작성자 이름 |
| `user_image` | ❌ | ❌ | 프로필 이미지 URL |
| `score` | ✅ | ✅ | 평점 (1-5) |
| `title` | ✅ | ✅ | 리뷰 제목 |
| `content` | ✅ | ✅ | 리뷰 내용 |
| `thumbs_up_count` | ✅ | ❌ | 추천 수 (도움이 됨) |
| `app_version` | ✅ | ✅ | 리뷰 작성 시 앱 버전 |
| `reviewed_at` | ✅ | ✅ | 리뷰 작성 시간 |
| `developer_reply` | ❌ | ✅ | 개발자 답변 내용 |
| `developer_reply_date` | ❌ | ✅ | 개발자 답변 시간 |
| `language` | ❌ | ❌ | 리뷰 언어 |

> *app-store-scraper 필드는 라이브러리 문서 기준 (실제 테스트에서는 작동하지 않음)

### 2.2 RSS에서 수집되는 실제 데이터 예시

```json
{
  "review_id": "13583030183",
  "user_name": "Villarreal ponce",
  "score": 5,
  "title": "Me encanta",
  "content": "Lo mejor",
  "thumbs_up_count": 0,
  "app_version": "25.37.76",
  "reviewed_at": "2026-01-01T16:31:29-07:00"
}
```

---

## 3. 라이브러리 상태 분석

### 3.1 app-store-scraper (PyPI)

| 항목 | 값 |
|------|-----|
| 버전 | 0.3.5 |
| 마지막 업데이트 | 2020-11-12 (**4년+ 미유지보수**) |
| 의존성 | `requests==2.23.0` (구버전 강제) |
| GitHub 이슈 | 다수의 미해결 이슈 |
| 상태 | **작동하지 않음** (API 변경됨) |

### 3.2 app-store-web-scraper (대안)

| 항목 | 값 |
|------|-----|
| 버전 | 0.2.0 |
| 설명 | app-store-scraper의 포크 및 재작성 |
| Python 지원 | 3.8 - 3.12 |
| 상태 | 더 최신이지만 테스트 환경에서 타임아웃 |

---

## 4. 장단점 분석

### 4.1 RSS 방식

#### 장점
- Apple 공식 API (안정성 높음)
- 외부 라이브러리 의존성 없음 (requests만 필요)
- `thumbs_up_count` (추천 수) 제공
- 빠른 요청 속도 (페이지당 50개 리뷰)
- 국가별 리뷰 수집 지원

#### 단점
- 개발자 답변 미제공
- 언어 정보 미제공 (국가 정보만 있음)
- 국가당 최대 500개 리뷰 제한 (10페이지 x 50)

### 4.2 app-store-scraper 방식

#### 장점 (문서 기준)
- 개발자 답변 (developerResponse) 제공
- 리뷰 수집 제한 없음 (이론상)
- 간단한 API

#### 단점
- **현재 작동하지 않음** (치명적)
- 4년 이상 미유지보수
- `requests 2.23.0` 강제 의존성 (다른 패키지와 충돌)
- 비공식 스크래핑 (차단 위험)
- `thumbs_up_count` 미제공

---

## 5. 결론 및 권장사항

### 5.1 최종 권장: RSS 방식 유지

| 기준 | RSS | app-store-scraper |
|------|-----|-------------------|
| 안정성 | ⭐⭐⭐⭐⭐ | ⭐ (작동 안함) |
| 유지보수 | Apple 공식 | 4년+ 방치 |
| 의존성 | 최소 | 충돌 발생 |
| 수집 필드 | 8개 | - |
| 차단 위험 | 낮음 | 높음 |

### 5.2 현재 시스템의 강점

현재 프로젝트의 RSS 기반 시스템은 이미 최적화되어 있음:

1. **다국가 분산 수집**: sitemap에서 (language, country) 쌍을 추출하여 여러 국가에서 리뷰 수집
2. **균등 분배 전략**: 국가별 할당량 균등 분배 + 잔여 분배
3. **중복 방지**: review_id 기반 중복 필터링
4. **언어-국가 우선순위**: 70+ 언어, 150+ 국가 매핑

### 5.3 개선 방안 (필요시)

개발자 답변이 필요한 경우:
1. `app-store-web-scraper` 재검토 (더 최신 라이브러리)
2. 웹 스크래핑 직접 구현 (apps.apple.com)
3. App Store Connect API 사용 (개발자 계정 필요)

500개 이상 리뷰 필요 시:
- 현재 방식 유지 (여러 국가에서 분산 수집으로 이미 해결됨)

---

## 6. 파일 구조

```
backend/tests/
├── compare_rss_vs_scraper.py      # 비교 테스트 스크립트
├── comparison_results.json         # 테스트 결과 JSON
└── analysis_rss_vs_scraper.md     # 이 분석 문서
```

---

## 7. 참고 자료

- [app-store-scraper (PyPI)](https://pypi.org/project/app-store-scraper/)
- [app-store-web-scraper (PyPI)](https://pypi.org/project/app-store-web-scraper/)
- [iTunes RSS Feed Generator](https://rss.applemarketingtools.com/)
- 현재 구현: `backend/scrapers/app_store_reviews_collector.py`
