# NewApps - 앱 스토어 데이터 수집 시스템

App Store와 Google Play Store에서 앱 정보, 로컬라이제이션 데이터, 상세정보 및 리뷰를 자동으로 수집하는 시스템입니다.

## 프로젝트 개요

이 프로젝트는 앱 스토어의 sitemap을 활용하여 전 세계 앱의 다국어 정보를 효율적으로 수집합니다. 수집된 데이터는 앱 시장 분석, 트렌드 파악, 경쟁사 분석 등에 활용될 수 있습니다.

### 주요 기능

- **Sitemap 기반 앱 발견**: App Store/Play Store의 sitemap에서 앱 ID 및 다국어 로컬라이제이션 정보 수집
- **앱 상세정보 수집**: iTunes API 및 google-play-scraper를 통한 메타데이터, 수치 데이터 수집
- **다국어 데이터 수집**: 우선순위 기반 언어-국가 조합으로 효율적인 다국어 데이터 수집
- **리뷰 수집**: App Store RSS 피드 및 Play Store 스크레이퍼를 통한 사용자 리뷰 수집
- **시계열 데이터 관리**: 변경 시에만 새 레코드를 추가하여 시간에 따른 변화 추적
- **중복 방지**: MD5 해시 기반 sitemap 파일 변경 감지 및 중복 데이터 방지

## 프로젝트 구조

```
newapps/
├── AGENT.MD                 # 개발 지침
├── README.md                # 이 파일
└── backend/
    ├── collect_sitemaps.py      # Sitemap 수집 메인 스크립트
    ├── collect_app_details.py   # 상세정보/리뷰 수집 메인 스크립트
    ├── requirements.txt         # Python 의존성
    ├── test_comprehensive.py    # 종합 테스트 스크립트
    │
    ├── config/
    │   └── language_country_priority.py  # 언어-국가 우선순위 설정
    │
    ├── database/
    │   ├── sitemap_apps_db.py    # Sitemap 앱 DB (app_localizations)
    │   └── app_details_db.py     # 상세정보 DB (apps, reviews, metrics)
    │
    └── scrapers/
        ├── sitemap_utils.py              # Sitemap 파싱 유틸리티
        ├── app_store_sitemap_collector.py   # App Store sitemap 수집
        ├── play_store_sitemap_collector.py  # Play Store sitemap 수집
        ├── app_store_details_collector.py   # App Store 상세정보 수집
        ├── play_store_details_collector.py  # Play Store 상세정보 수집
        ├── app_store_reviews_collector.py   # App Store 리뷰 수집
        └── play_store_reviews_collector.py  # Play Store 리뷰 수집
```

## 설치

### 요구사항

- Python 3.9+
- SQLite3 (기본 포함)

### 의존성 설치

```bash
cd backend
pip install -r requirements.txt
```

#### 주요 의존성

| 패키지 | 버전 | 용도 |
|--------|------|------|
| requests | >=2.31.0 | HTTP 요청 |
| google-play-scraper | >=1.2.4 | Play Store 데이터 수집 |

## 사용법

### 1. Sitemap 수집

앱 스토어의 sitemap에서 앱 ID와 로컬라이제이션 정보를 수집합니다.

```bash
cd backend

# 모든 스토어 수집
python collect_sitemaps.py

# App Store만 수집
python collect_sitemaps.py --app-store

# Play Store만 수집
python collect_sitemaps.py --play-store

# 통계만 확인
python collect_sitemaps.py --stats
```

### 2. 앱 상세정보 수집

sitemap에서 발견된 앱들의 상세정보를 수집합니다.

```bash
cd backend

# 모든 스토어의 상세정보 + 리뷰 수집
python collect_app_details.py

# App Store만
python collect_app_details.py --app-store

# 상세정보만 (리뷰 제외)
python collect_app_details.py --details-only

# 리뷰만 수집
python collect_app_details.py --reviews-only

# 앱 개수 제한 (기본: 1000개)
python collect_app_details.py --limit 100

# 통계 확인
python collect_app_details.py --stats
```

### 3. 종합 테스트

시스템 전체를 테스트합니다.

```bash
cd backend
python test_comprehensive.py
```

## 데이터베이스 스키마

### sitemap_apps.db

앱 로컬라이제이션 정보를 저장합니다.

#### sitemap_files
| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | INTEGER | PK |
| platform | TEXT | 플랫폼 (app_store/play_store) |
| file_url | TEXT | sitemap 파일 URL |
| md5_hash | TEXT | 파일 MD5 해시 (변경 감지용) |
| last_collected_at | TEXT | 마지막 수집 시각 |
| app_count | INTEGER | 해당 파일의 앱 수 |

#### app_localizations
| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | INTEGER | PK |
| platform | TEXT | 플랫폼 |
| app_id | TEXT | 앱 ID |
| language | TEXT | 언어 코드 (ko, en 등) |
| country | TEXT | 국가 코드 (kr, us 등) |
| href | TEXT | 해당 로컬라이제이션 URL |
| source_file | TEXT | 수집된 sitemap 파일명 |
| first_seen_at | TEXT | 처음 발견 시각 |
| last_seen_at | TEXT | 마지막 발견 시각 |

### app_details.db

앱 상세정보, 수치 데이터, 리뷰를 저장합니다.

#### apps (시계열)
앱 메타데이터. 변경 시에만 새 레코드가 추가됩니다.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| app_id | TEXT | 앱 ID |
| platform | TEXT | 플랫폼 |
| bundle_id | TEXT | 번들 ID |
| version | TEXT | 앱 버전 |
| developer | TEXT | 개발자명 |
| price | REAL | 가격 |
| category_id | TEXT | 카테고리 ID |
| recorded_at | TEXT | 기록 시각 |

#### apps_localized (시계열)
다국어 텍스트 데이터.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| app_id | TEXT | 앱 ID |
| language | TEXT | 언어 코드 |
| title | TEXT | 앱 제목 |
| description | TEXT | 설명 |
| release_notes | TEXT | 릴리스 노트 |

#### apps_metrics (시계열)
수치 데이터.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| app_id | TEXT | 앱 ID |
| score | REAL | 평점 |
| ratings | INTEGER | 평가 수 |
| installs | TEXT | 설치 수 (Play Store) |
| histogram | TEXT | 점수 분포 JSON |

#### app_reviews
사용자 리뷰.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| app_id | TEXT | 앱 ID |
| review_id | TEXT | 외부 리뷰 ID (중복 방지) |
| user_name | TEXT | 작성자 |
| score | INTEGER | 평점 (1-5) |
| content | TEXT | 리뷰 내용 |
| reviewed_at | TEXT | 작성 시각 |

## 언어-국가 우선순위 시스템

효율적인 다국어 데이터 수집을 위해 언어별 최적의 국가를 정의합니다.

```python
# 예시: 프랑스어 데이터 수집 시
# FR(프랑스) > BE(벨기에) > CH(스위스) > CA(캐나다) 순으로 우선

LANGUAGE_COUNTRY_PRIORITY = {
    'en': ['US', 'GB', 'CA', 'AU', 'IN'],  # 영어: 미국 우선
    'fr': ['FR', 'BE', 'CH', 'CA'],         # 프랑스어: 프랑스 우선
    'es': ['MX', 'ES', 'AR', 'CO'],         # 스페인어: 멕시코 우선 (인구수)
    'pt': ['BR', 'PT'],                      # 포르투갈어: 브라질 우선
    'zh': ['CN', 'SG'],                      # 중국어: 중국 우선
    'ko': ['KR'],                            # 한국어: 한국
    'ja': ['JP'],                            # 일본어: 일본
    # ... 50+ 언어 지원
}
```

## 데이터 수집 흐름

```
┌─────────────────────────────────────────────────────────────────┐
│                     1. Sitemap 수집                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │ App Store    │    │ Play Store   │    │ sitemap_     │       │
│  │ sitemap.xml  │───▶│ sitemap.xml  │───▶│ apps.db      │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│         │                    │                  │                │
│         ▼                    ▼                  ▼                │
│  app_id + language + country 추출     app_localizations 저장     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     2. 상세정보 수집                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │ iTunes API   │    │ Play Store   │    │ app_         │       │
│  │ (lookup)     │───▶│ Scraper      │───▶│ details.db   │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│         │                    │                  │                │
│         ▼                    ▼                  ▼                │
│  - 메타데이터         - 다국어 텍스트      - 시계열 저장         │
│  - 수치 데이터        - 리뷰 데이터        - 변경 감지           │
└─────────────────────────────────────────────────────────────────┘
```

## 성능 최적화

### MD5 해시 기반 변경 감지
- sitemap 파일의 MD5 해시를 저장하여 변경된 파일만 재처리
- 불필요한 네트워크 요청 및 DB 업데이트 방지

### 시계열 데이터 중복 방지
- 새 데이터와 최신 레코드를 비교하여 변경 시에만 삽입
- 저장 공간 절약 및 의미 있는 변화만 기록

### 언어-국가 최적화
- 각 언어당 가장 큰 시장의 국가 데이터만 수집
- 중복 언어 데이터 수집 방지

### 요청 속도 제한
- 10ms 딜레이로 API 부하 최소화
- 지수 백오프 재시도 로직

## 테스트 결과 예시

```json
{
  "sitemap": {
    "app_store": {
      "index_urls": 786,
      "entries_in_first_file": 8510,
      "success": true
    },
    "play_store": {
      "index_urls": 50000,
      "entries_in_first_file": 398,
      "success": true
    }
  },
  "details": {
    "app_store": {
      "apps_processed": 5,
      "new_records": 5,
      "errors": 0
    }
  },
  "time_series": {
    "duplicate_prevention": true,
    "change_detection": true
  }
}
```

## 제한사항

- App Store: iTunes Lookup API 사용 (공식 API, 속도 제한 있음)
- Play Store: 비공식 스크레이퍼 사용 (정책 변경에 취약)
- 리뷰: 실행당 최대 20,000건으로 제한 (무한 루프 방지)

## 라이선스

이 프로젝트는 내부 사용 목적으로 제작되었습니다.

## 기여

1. 이 저장소를 포크합니다
2. 기능 브랜치를 생성합니다 (`git checkout -b feature/amazing-feature`)
3. 변경사항을 커밋합니다 (`git commit -m '새로운 기능 추가'`)
4. 브랜치에 푸시합니다 (`git push origin feature/amazing-feature`)
5. Pull Request를 생성합니다
