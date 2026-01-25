# 리뷰 수집 시스템 재설계

- **작성일**: 2026-01-25
- **상태**: 승인됨
- **목표**: 리뷰 수집 시스템의 안정성, 효율성, 모니터링 개선

---

## 1. 배경 및 문제점

### 현재 상태 (분석 결과)

| 항목 | 값 | 문제 |
|------|-----|------|
| 총 앱 수 | 1,452,061 | - |
| 상세정보 수집 완료 | 1,452,061 (100%) | 정상 |
| 리뷰 수집 시도 | 397,680 (27%) | 낮음 |
| 실제 리뷰 수집 | 1,602 (0.1%) | **심각** |
| App Store 리뷰 | 66,847건 | 대부분 0건 수집 |
| Play Store 리뷰 | **12건** | 거의 수집 안 됨 |

### 발견된 문제점

1. **App Store RSS API 차단**: IP 172.31.47.39에서 HTTP 403 반환
2. **Play Store 리뷰 수집 중단**: 2026-01-15 이후 9개 앱만 시도
3. **에러 핸들링 미흡**: 403/429/500 구분 없이 동일 처리
4. **변경 감지 없음**: 모든 앱을 매번 수집 시도 (비효율)
5. **모니터링 부재**: 실패 원인 분석 어려움

### IP별 API 접근성 (검증 완료)

| API | IP 1 (172.31.47.39) | IP 2 (172.31.40.115) |
|-----|---------------------|----------------------|
| App Store RSS | 403 차단 | **200 정상** |
| Play Store API | 정상 | 정상 |

---

## 2. 설계 목표

1. **변경 감지 기반 수집**: `reviews_count` 증가 시에만 수집
2. **최신 리뷰 우선**: 전체 수집 불가 시 최신 리뷰 중심
3. **IP 자동 감지**: 서버 IP 자동 탐지 → 스토어별 테스트 → 동작 IP 할당
4. **에러 분류 및 재시도**: 원인별 다른 재시도 전략
5. **수집 불가 표시**: API 한계 도달 시 명시적 표시

---

## 3. 아키텍처

### 전체 흐름

```
파이프라인 시작
    │
    ├─ IP 자동 감지 및 스토어별 테스트
    │
    ├─ Sitemap 수집
    │
    └─ 앱별 처리
           │
           ├─ 상세정보 수집 → reviews_count 획득
           │
           ├─ 변경 감지 (last_known vs new)
           │      │
           │      ├─ 첫 수집 → INITIAL 모드
           │      ├─ 증가함 → INCREMENTAL 모드
           │      └─ 변화 없음 → 스킵
           │
           └─ 리뷰 수집 실행
                  │
                  ├─ 성공 → 상태 업데이트
                  └─ 실패 → 에러 분류 → 재시도/기록
```

### 파일 구조

```
/home/ubuntu/newapps/backend/
├── core/                              # 신규
│   ├── __init__.py
│   ├── ip_manager.py                 # IP 자동 감지 및 스토어별 할당
│   ├── http_client.py                # IP 바인딩 + User-Agent + 재시도
│   ├── error_classifier.py           # HTTP 에러 분류
│   └── rate_limiter.py               # 요청 속도 제어
│
├── scrapers/
│   ├── base_review_collector.py      # 신규: 공통 수집기 베이스
│   ├── app_store_reviews_collector.py  # 수정
│   └── play_store_reviews_collector.py # 수정
│
└── database/
    ├── app_details_db.py             # 수정
    └── review_collection_db.py       # 신규: 리뷰 수집 상태 관리
```

---

## 4. IP 자동 감지 시스템

### IPManager 클래스

```python
class IPManager:
    TEST_ENDPOINTS = {
        'app_store': 'https://itunes.apple.com/us/rss/customerreviews/page=1/id=284882215/sortBy=mostRecent/json',
        'play_store': 'https://play.google.com/store/apps/details?id=com.whatsapp&hl=en&gl=us',
    }

    def discover_ips(self) -> List[str]:
        """서버의 모든 외부 IP 자동 감지"""

    def test_ip_for_store(self, ip: str, store: str) -> bool:
        """IP가 해당 스토어에 접근 가능한지 테스트"""

    def initialize(self) -> Dict[str, List[str]]:
        """모든 IP 감지 → 테스트 → 매핑 생성"""

    def get_ip_for_store(self, store: str, exclude: List[str] = None) -> Optional[str]:
        """스토어용 IP 반환 (실패한 IP 제외)"""
```

### 초기화 시점

- 파이프라인 시작 시 1회 실행
- 결과 로깅: 각 스토어별 동작 IP 목록
- 동작 IP 없는 스토어는 경고 로그

---

## 5. HTTP 클라이언트

### StoreHttpClient 클래스

```python
class StoreHttpClient:
    USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    RATE_LIMIT_DELAYS = [5, 10, 30]  # 초

    def request(self, url: str, platform: str) -> Result:
        """IP 바인딩 요청 + 에러 처리"""

    def _classify_error(self, response, exception) -> str:
        """에러 코드 분류"""

    def _retry_with_backoff(self, ...) -> Result:
        """RATE_LIMITED 시 백오프 재시도 (최대 3회)"""

    def _retry_with_alternate_ip(self, ...) -> Result:
        """IP_BLOCKED 시 다른 IP로 재시도"""
```

### 에러 분류 체계

| 에러 코드 | 조건 | 재시도 전략 |
|-----------|------|-------------|
| `IP_BLOCKED` | HTTP 403 | 다른 IP로 즉시 재시도 → 실패 시 다음 실행 |
| `RATE_LIMITED` | HTTP 429 | 5초→10초→30초 백오프 (3회) → 다음 실행 |
| `NETWORK_ERROR` | Timeout, Connection Error | 다음 실행 시 재시도 |
| `SERVER_ERROR` | HTTP 5xx | 다음 실행 시 재시도 |
| `APP_NOT_FOUND` | HTTP 404, NotFoundError | **재시도 안 함** (영구 실패) |
| `NO_REVIEWS` | 리뷰 0건 | reviews_count 변경 시만 재시도 |
| `API_LIMIT_REACHED` | 최대 페이지/개수 도달 | 정상 완료 (한계 표시) |
| `PARSE_ERROR` | JSON 파싱 실패 | 다음 실행 시 재시도 |

---

## 6. 리뷰 수집기

### 수집 모드

| 모드 | 조건 | 동작 |
|------|------|------|
| `INITIAL` | 첫 수집 | 최대한 많이 수집 (API 한계까지) |
| `INCREMENTAL` | reviews_count 증가 | 최신순 수집, 기존 review_id 만나면 중단 |

### BaseReviewCollector

```python
class BaseReviewCollector(ABC):
    def should_collect(self, app_id: str, new_reviews_count: int) -> Tuple[bool, CollectionMode]:
        """수집 여부 및 모드 결정"""

    def collect(self, app_id: str, mode: CollectionMode) -> CollectionResult:
        """리뷰 수집 실행"""

    @abstractmethod
    def _fetch_reviews_pages(self, app_id: str) -> Iterator[List[dict]]:
        """스토어별 구현"""
```

### 스토어별 한계

| 스토어 | 한계 | 설정값 |
|--------|------|--------|
| App Store | RSS 페이지 제한 | 10페이지 × 50개 × 5개국 = 최대 2,500개 |
| Play Store | 배치 크기 제한 | 200개/배치, 최대 10,000개 |

---

## 7. 데이터베이스 스키마

### 신규 테이블: review_collection_status

```sql
CREATE TABLE review_collection_status (
    app_id TEXT NOT NULL,
    platform TEXT NOT NULL,

    -- 시간 추적
    last_attempt_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,

    -- 변경 감지
    last_known_store_review_count INTEGER,  -- 스토어 전체 리뷰 수 (지난번)
    collected_review_count INTEGER DEFAULT 0, -- 실제 수집한 리뷰 수

    -- 실패 추적
    last_failure_reason TEXT,
    last_failure_detail TEXT,
    consecutive_failures INTEGER DEFAULT 0,

    -- 수집 한계
    collection_limited BOOLEAN DEFAULT FALSE,
    collection_limited_reason TEXT,

    -- 메타
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (app_id, platform)
);

-- 인덱스
CREATE INDEX idx_rcs_platform_failure
    ON review_collection_status (platform, last_failure_reason)
    WHERE last_failure_reason IS NOT NULL;

CREATE INDEX idx_rcs_consecutive_failures
    ON review_collection_status (consecutive_failures DESC)
    WHERE consecutive_failures > 0;

CREATE INDEX idx_rcs_last_attempt
    ON review_collection_status (platform, last_attempt_at DESC);
```

### 선택적 테이블: ip_store_mapping

```sql
CREATE TABLE ip_store_mapping (
    ip_address TEXT NOT NULL,
    platform TEXT NOT NULL,

    is_working BOOLEAN NOT NULL,
    last_tested_at TIMESTAMPTZ NOT NULL,
    last_error TEXT,

    PRIMARY KEY (ip_address, platform)
);
```

### 마이그레이션

```sql
-- 기존 collection_status에서 데이터 마이그레이션
INSERT INTO review_collection_status (
    app_id, platform,
    last_attempt_at, last_success_at,
    collected_review_count
)
SELECT
    app_id, platform,
    reviews_collected_at, reviews_collected_at,
    reviews_total_count
FROM collection_status
WHERE reviews_collected_at IS NOT NULL
ON CONFLICT (app_id, platform) DO NOTHING;
```

---

## 8. 상태 변화 예시

### 시나리오: Facebook 앱 (app_id: 284882215)

**Day 1: 첫 수집**
```
스토어 reviews_count: 50,000
모드: INITIAL
결과: 2,100개 수집 (RSS 한계)

상태:
  last_known_store_review_count: 50,000
  collected_review_count: 2,100
  collection_limited: TRUE
  collection_limited_reason: RSS_PAGE_LIMIT
```

**Day 2: 변화 없음**
```
스토어 reviews_count: 50,000 (동일)
결과: 스킵 (no_change)
```

**Day 3: 리뷰 증가**
```
스토어 reviews_count: 50,300 (+300)
모드: INCREMENTAL
결과: 290개 신규 수집 (기존 review_id에서 중단)

상태:
  last_known_store_review_count: 50,300
  collected_review_count: 2,390
```

**Day 4: Rate Limit**
```
스토어 reviews_count: 50,400 (+100)
결과: 실패 (RATE_LIMITED)

상태:
  last_failure_reason: RATE_LIMITED
  consecutive_failures: 1
```

**Day 5: 재시도 성공**
```
결과: 성공, 180개 수집

상태:
  last_failure_reason: NULL (초기화)
  consecutive_failures: 0 (초기화)
```

---

## 9. 모니터링 쿼리

```sql
-- 실패 원인별 통계
SELECT platform, last_failure_reason, COUNT(*) as count
FROM review_collection_status
WHERE last_failure_reason IS NOT NULL
GROUP BY platform, last_failure_reason
ORDER BY count DESC;

-- 연속 실패 앱 목록
SELECT app_id, platform, consecutive_failures, last_failure_reason
FROM review_collection_status
WHERE consecutive_failures >= 3
ORDER BY consecutive_failures DESC;

-- 수집 한계 도달 앱
SELECT platform, COUNT(*) as limited_apps
FROM review_collection_status
WHERE collection_limited = TRUE
GROUP BY platform;

-- 최근 24시간 수집 현황
SELECT
    platform,
    COUNT(*) as attempted,
    COUNT(CASE WHEN last_failure_reason IS NULL THEN 1 END) as succeeded,
    COUNT(CASE WHEN last_failure_reason IS NOT NULL THEN 1 END) as failed
FROM review_collection_status
WHERE last_attempt_at > NOW() - INTERVAL '24 hours'
GROUP BY platform;
```

---

## 10. 구현 계획

### Phase 1: 기반 구축
1. `core/` 디렉토리 및 모듈 생성
2. `review_collection_status` 테이블 생성
3. 기존 데이터 마이그레이션

### Phase 2: IP 관리
4. `IPManager` 구현
5. `StoreHttpClient` 구현
6. 에러 분류 로직 구현

### Phase 3: 수집기 개선
7. `BaseReviewCollector` 구현
8. App Store 수집기 수정
9. Play Store 수집기 수정

### Phase 4: 통합 및 테스트
10. 파이프라인 통합
11. 단위 테스트
12. 통합 테스트

### Phase 5: 배포
13. 스테이징 테스트
14. 프로덕션 배포
15. 모니터링 확인

---

## 11. 롤백 계획

```sql
-- 문제 발생 시
-- 1. 코드를 이전 버전으로 롤백
-- 2. 신규 테이블은 유지 (데이터 보존)
-- 3. collection_status의 기존 필드 계속 사용

-- 완전 롤백 필요 시
DROP TABLE IF EXISTS review_collection_status;
DROP TABLE IF EXISTS ip_store_mapping;
```

---

## 부록: 검증 결과

### App Store RSS 테스트

```bash
# IP 172.31.40.115에서 정상 동작 확인
curl -s --interface 172.31.40.115 \
  "https://itunes.apple.com/us/rss/customerreviews/page=1/id=284882215/sortBy=mostRecent/json"
# 결과: 38,573 bytes, 49개 리뷰
```

### Play Store API 테스트

```python
from google_play_scraper import reviews, Sort
result, token = reviews('com.whatsapp', lang='en', country='us', count=3)
# 결과: 정상 동작
```

### 리뷰 수집 한계

| 플랫폼 | 페이지당 | 최대 페이지 | 예상 수집량 |
|--------|----------|-------------|-------------|
| App Store RSS | ~50개 | 10페이지 | 국가당 ~500개 |
| Play Store API | 200개 | 무제한 | 설정값 (10,000개) |
