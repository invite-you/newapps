# 🌍 신규 앱 발견 (New Apps Discovery)

전 세계 안드로이드(Google Play)와 iOS(App Store) 앱스토어에서 신규 앱을 자동으로 수집하고, 주목할만한 앱을 소개하는 웹 애플리케이션입니다.

## ✨ 주요 기능

- **전 세계 앱스토어 커버리지**: 35개 이상 국가의 앱스토어 지원
- **자동 데이터 수집**: Google Play와 App Store에서 신규 앱 자동 수집
- **스마트 분석**: 평점, 리뷰 수, 최신성 등을 기반으로 주목할만한 앱 자동 선별
- **국가별 필터링**: 국가, 플랫폼별로 앱 검색 가능
- **점수 시스템**: 다양한 지표를 종합한 점수로 앱 품질 평가

## 🏗️ 기술 스택

### 백엔드
- **Python 3.x**: 데이터 수집 및 분석
- **SQLite**: 경량 데이터베이스
- **google-play-scraper**: Google Play 데이터 수집
- **app-store-scraper**: App Store 데이터 수집

### API 서버
- **Node.js + Express**: RESTful API 서버
- **sqlite3**: 데이터베이스 연동

### 프론트엔드
- **HTML/CSS/JavaScript**: 순수 웹 기술 (프레임워크 없음)
- **반응형 디자인**: 모바일/데스크톱 대응

## 📁 프로젝트 구조

```
newapps/
├── backend/                 # Python 백엔드
│   ├── config.py           # 전역 설정값
│   ├── main.py             # 메인 실행 스크립트
│   ├── requirements.txt    # Python 패키지 목록
│   ├── database/
│   │   └── db.py          # 데이터베이스 관리
│   ├── scrapers/
│   │   ├── google_play_scraper.py  # Google Play 수집
│   │   └── app_store_scraper.py    # App Store 수집
│   └── analyzer/
│       └── app_analyzer.py # 앱 분석 및 점수 계산
├── api/                    # Node.js API 서버
│   ├── server.js          # Express 서버
│   └── package.json       # Node.js 패키지 목록
├── frontend/              # 웹 프론트엔드
│   ├── index.html        # 메인 페이지
│   ├── style.css         # 스타일시트
│   └── app.js            # JavaScript
├── data/                 # 데이터 저장 (자동 생성)
│   └── apps.db           # SQLite 데이터베이스
└── README.md
```

## 🚀 설치 및 실행

### 1. Python 환경 설정

```bash
cd backend
pip install -r requirements.txt
```

### 2. 데이터 수집

```bash
# 데이터베이스 초기화 및 전체 데이터 수집
python main.py
```

> **참고**: 전체 국가의 데이터를 수집하는 데 시간이 소요될 수 있습니다.
> 각 단계별로 타임스탬프와 소요 시간이 로그로 출력됩니다.

### 3. API 서버 실행

```bash
cd ../api
npm install
npm start
```

서버가 http://localhost:3000 에서 실행됩니다.

### 4. 웹사이트 접속

브라우저에서 http://localhost:3000 접속

## 📊 점수 계산 시스템

앱의 품질을 평가하기 위해 다음 기준으로 점수(0-100점)를 계산합니다:

| 기준 | 비중 | 설명 |
|------|------|------|
| **평점** | 30% | 사용자 평점 (5점 만점) |
| **리뷰 수** | 20% | 리뷰 개수 (많을수록 신뢰도 높음) |
| **설치 수** | 20% | 다운로드 수 (Google Play만) |
| **최신성** | 20% | 최근 출시/업데이트 여부 |
| **성장률** | 10% | 단기간 리뷰 증가율 |

**주목할만한 앱 선별 기준:**
- 평점 4.0 이상
- 리뷰 10개 이상
- 종합 점수 60점 이상

## 🌏 지원 국가

35개 이상 국가 지원:

**아시아**: 대한민국, 일본, 중국, 대만, 홍콩, 싱가포르, 인도, 인도네시아, 태국, 베트남, 필리핀, 말레이시아

**북미**: 미국, 캐나다, 멕시코

**유럽**: 영국, 독일, 프랑스, 이탈리아, 스페인, 네덜란드, 스웨덴, 노르웨이, 덴마크, 핀란드, 폴란드, 러시아

**오세아니아**: 호주, 뉴질랜드

**남미**: 브라질, 아르헨티나, 칠레

**중동/아프리카**: 아랍에미리트, 사우디아라비아, 남아프리카공화국, 이집트

## 🔧 설정 변경

`backend/config.py` 파일에서 다양한 설정을 변경할 수 있습니다:

```python
# 국가별 수집 앱 개수
FETCH_LIMIT_PER_COUNTRY = 100

# 점수 가중치
SCORE_WEIGHTS = {
    "rating": 0.3,
    "rating_count": 0.2,
    "installs": 0.2,
    "freshness": 0.2,
    "growth_rate": 0.1,
}

# 주목 앱 기준
MINIMUM_RATING = 4.0
MINIMUM_RATING_COUNT = 10
```

## 📡 API 엔드포인트

### GET /api/stats
통계 정보 조회

**응답 예시:**
```json
{
  "total": 3500,
  "featured": 450,
  "byPlatform": [...],
  "byCountry": [...]
}
```

### GET /api/countries
국가 목록 조회

### GET /api/apps
앱 목록 조회

**쿼리 파라미터:**
- `country`: 국가 코드 (예: kr, us) 또는 'all'
- `platform`: 'google_play', 'app_store', 또는 'all'
- `featured`: 'true' 또는 'false' (주목 앱만 보기)
- `page`: 페이지 번호 (기본값: 1)

**응답 예시:**
```json
{
  "apps": [...],
  "total": 150,
  "page": 1,
  "totalPages": 8
}
```

### GET /api/apps/:id
앱 상세 정보 조회

## 🔄 정기 업데이트

데이터를 정기적으로 업데이트하려면 cron job 등을 활용하여 `backend/main.py`를 주기적으로 실행하세요.

**예시 (매일 오전 3시 실행):**
```bash
0 3 * * * cd /path/to/newapps/backend && python main.py
```

## 📝 개발 지침

이 프로젝트는 `AGENT.MD`의 지침을 따라 개발되었습니다:

1. **간결성 우선**: 최소한의 코드로 최대 효과
2. **성능 중시**: 불필요한 복잡도 제거
3. **타임스탬프 로그**: 모든 작업 단계 추적
4. **전역 설정**: config.py에 중앙 집중화
5. **한국어 문서화**: 코드 주석 및 커밋 메시지

## 🤝 기여

버그 리포트 및 기능 제안은 Issue를 통해 제출해주세요.

## 📄 라이선스

MIT License

## 🙏 감사

- [google-play-scraper](https://github.com/facundoolano/google-play-scraper)
- [app-store-scraper](https://github.com/facundoolano/app-store-scraper)