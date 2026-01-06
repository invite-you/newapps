# 데이터베이스 컬럼별 실제 데이터 예시

이 문서는 앱 수집 시스템에서 각 테이블에 저장되는 실제 데이터의 형태와 의미를 설명합니다.

---

## 1. Sitemap Apps Database

> **파일**: `sitemap_apps.db`
> **목적**: App Store와 Play Store의 Sitemap에서 발견된 앱들의 지역화(로컬라이제이션) 정보를 저장합니다.

### 1.1 `app_localizations` 테이블

특정 앱이 어떤 국가와 언어에서 제공되는지 추적하는 테이블입니다.
동일한 앱이 여러 국가/언어 조합으로 존재할 수 있어, 글로벌 앱의 지역 가용성을 파악하는 데 활용됩니다.

| 컬럼 | 설명 | 예시 1 | 예시 2 | 예시 3 |
|------|------|--------|--------|--------|
| `platform` | 앱이 배포된 플랫폼 | `app_store` | `play_store` | `app_store` |
| `app_id` | 앱 고유 식별자. App Store는 숫자 ID, Play Store는 패키지명 | `310633997` | `com.spotify.music` | `1094591345` |
| `language` | 앱이 지원하는 언어 코드 (ISO 639-1) | `ko` | `ja` | `en` |
| `country` | 앱이 배포된 국가 코드 (ISO 3166-1 alpha-2) | `kr` | `jp` | `gb` |
| `source_file` | 이 정보가 수집된 sitemap 파일명 | `sitemaps_apps_1.xml.gz` | `sitemaps_0_com.xml.gz` | `sitemaps_apps_5.xml.gz` |
| `first_seen_at` | 이 앱+국가+언어 조합이 처음 발견된 시각 | `2024-01-15T08:30:00` | `2024-02-20T14:22:31` | `2023-11-01T00:00:00` |
| `last_seen_at` | 가장 최근 sitemap에서 확인된 시각 | `2024-12-01T12:00:00` | `2024-12-01T12:00:00` | `2024-12-01T12:00:00` |

---

### 1.2 `sitemap_files` 테이블

수집한 Sitemap XML 파일들의 메타데이터를 관리합니다.
파일 변경 감지를 통해 불필요한 재수집을 방지하고 수집 효율을 높입니다.

| 컬럼 | 설명 | 예시 1 | 예시 2 | 예시 3 |
|------|------|--------|--------|--------|
| `platform` | Sitemap이 속한 플랫폼 | `app_store` | `play_store` | `play_store` |
| `file_url` | Sitemap XML.gz 파일의 전체 URL | `https://apps.apple.com/sitemaps_apps_index_1.xml.gz` | `https://play.google.com/sitemap/apps/sitemaps_0_com.xml.gz` | `https://play.google.com/sitemap/apps/sitemaps_1_org.xml.gz` |
| `md5_hash` | 파일 내용의 MD5 해시 (변경 감지용) | `a1b2c3d4e5f6g7h8i9j0` | `f9e8d7c6b5a4321098` | `1234abcd5678efgh90` |
| `last_collected_at` | 마지막으로 이 파일을 수집한 시각 | `2024-12-01T06:00:00` | `2024-12-01T07:30:00` | `2024-12-01T07:45:00` |
| `app_count` | 이 파일에서 추출된 앱 개수 | `48523` | `125000` | `87432` |

---

## 2. App Details Database

> **파일**: `app_details.db`
> **목적**: 각 앱의 상세 정보, 다국어 콘텐츠, 평점/리뷰 수치, 사용자 리뷰를 저장합니다.
> **특징**: 데이터가 변경될 때만 새 레코드를 생성하여 변경 이력을 누적합니다.

### 2.1 `apps` 테이블

앱의 기본 메타데이터를 저장하는 핵심 테이블입니다.
버전 업데이트, 가격 변경, 개발사 정보 변경 등이 발생하면 새 레코드가 추가됩니다.

| 컬럼 | 설명 | 예시 1 (App Store) | 예시 2 (Play Store) | 예시 3 (App Store) |
|------|------|--------|--------|--------|
| `app_id` | 앱 고유 식별자 | `310633997` | `com.spotify.music` | `333903271` |
| `platform` | 배포 플랫폼 | `app_store` | `play_store` | `app_store` |
| `bundle_id` | 앱 번들/패키지 ID | `net.whatsapp.WhatsApp` | `com.spotify.music` | `com.twitter.twitter` |
| `version` | 현재 앱 버전 | `24.25.84` | `8.9.84.527` | `10.25` |
| `developer` | 개발사/퍼블리셔 이름 | `WhatsApp Inc.` | `Spotify AB` | `X Corp.` |
| `developer_id` | 개발사 고유 ID | `1703852` | `4559640424668586238` | `2573643872` |
| `developer_email` | 개발사 연락 이메일 (Play Store만) | *(없음)* | `android-support@spotify.com` | *(없음)* |
| `developer_website` | 개발사 공식 웹사이트 | `https://www.whatsapp.com` | `https://www.spotify.com` | `https://x.com` |
| `icon_url` | 앱 아이콘 이미지 URL | `https://is1-ssl.mzstatic.com/image/thumb/...` | `https://play-lh.googleusercontent.com/...` | `https://is1-ssl.mzstatic.com/image/thumb/...` |
| `header_image` | 스토어 헤더 이미지 (Play Store만) | *(없음)* | `https://play-lh.googleusercontent.com/...` | *(없음)* |
| `screenshots` | 스크린샷 URL 목록 (JSON 배열) | `["https://...1.jpg", "https://...2.jpg"]` | `["https://...1.png", "https://...2.png"]` | `["https://...1.jpg"]` |
| `price` | 앱 가격 (0이면 무료) | `0.0` | `0.0` | `0.0` |
| `currency` | 가격 통화 코드 | `USD` | `USD` | `KRW` |
| `free` | 무료 앱 여부 (1=무료, 0=유료) | `1` | `1` | `1` |
| `has_iap` | 인앱결제 포함 여부 | `0` | `1` | `1` |
| `genre_id` | 장르/카테고리 ID | `6005` | `MUSIC_AND_AUDIO` | `6002` |
| `genre_name` | 장르명 (지역화) | `Social Networking` | `Music & Audio` | `Utilities` |
| `content_rating` | 연령 등급 | `12+` | `Teen` | `17+` |
| `content_rating_description` | 등급 상세 설명 | `Infrequent/Mild Mature/Suggestive Themes` | `Violence, Blood` | `Unrestricted Web Access` |
| `min_os_version` | 최소 지원 OS 버전 (App Store만) | `12.0` | *(없음)* | `14.0` |
| `file_size` | 앱 파일 크기 (바이트) | `232853504` | `67108864` | `156237824` |
| `supported_devices` | 지원 기기 목록 (JSON 배열, App Store만) | `["iPhone15,2", "iPad14,1"]` | *(없음)* | `["iPhone12,1", "iPod9,1"]` |
| `release_date` | 최초 출시일 | `2009-05-04T00:00:00` | `2013-04-03T00:00:00` | `2009-10-09T00:00:00` |
| `updated_date` | 최근 업데이트일 | `2024-11-28T00:00:00` | `2024-12-01T00:00:00` | `2024-11-25T00:00:00` |
| `privacy_policy_url` | 개인정보처리방침 URL | `https://www.whatsapp.com/legal/privacy-policy` | `https://www.spotify.com/legal/privacy-policy` | `https://twitter.com/privacy` |

---

### 2.2 `apps_localized` 테이블

앱의 다국어 텍스트 콘텐츠를 저장합니다.
동일 앱이라도 언어별로 제목, 설명, 업데이트 노트가 다를 수 있어 별도 테이블로 관리합니다.
기준 언어(영어)와 내용이 동일한 경우 저장하지 않아 저장공간을 최적화합니다.

| 컬럼 | 설명 | 예시 1 (한국어) | 예시 2 (일본어) | 예시 3 (프랑스어) |
|------|------|--------|--------|--------|
| `app_id` | 앱 고유 식별자 | `310633997` | `com.spotify.music` | `333903271` |
| `platform` | 배포 플랫폼 | `app_store` | `play_store` | `app_store` |
| `language` | 언어 코드 | `ko` | `ja` | `fr` |
| `title` | 앱 제목 (현지화) | `WhatsApp Messenger` | `Spotify: 音楽とポッドキャスト` | `X` |
| `summary` | 앱 한줄 요약 (Play Store만) | *(없음)* | `何百万もの曲とポッドキャストを無料で楽しめます` | *(없음)* |
| `description` | 앱 전체 설명 (현지화) | `WhatsApp은 전 세계 수십억 명이 사용하는 무료 메시징 및 통화 앱입니다...` | `Spotifyで、何百万もの楽曲やポッドキャストを発見。あなたの好みに合わせた...` | `Rejoignez la conversation ! X est la source incontournable pour...` |
| `release_notes` | 최신 버전 업데이트 내용 | `버그 수정 및 성능 개선` | `バグ修正と安定性の向上` | `Correction de bugs et amélioration des performances` |

---

### 2.3 `apps_metrics` 테이블

앱의 수치 데이터(평점, 리뷰 수, 다운로드 수)를 저장합니다.
이 수치들은 시간에 따라 변하므로, 변경이 감지될 때마다 새 레코드가 추가되어 시계열 분석이 가능합니다.

| 컬럼 | 설명 | 예시 1 | 예시 2 | 예시 3 |
|------|------|--------|--------|--------|
| `app_id` | 앱 고유 식별자 | `310633997` | `com.spotify.music` | `com.supercell.clashofclans` |
| `platform` | 배포 플랫폼 | `app_store` | `play_store` | `play_store` |
| `score` | 평균 평점 (1.0~5.0) | `4.7` | `4.4` | `4.1` |
| `ratings` | 평점을 매긴 사용자 수 | `25847392` | `32156789` | `58234567` |
| `reviews_count` | 텍스트 리뷰 개수 | `1523847` | `8945623` | `12567890` |
| `installs` | 설치 수 (표시용 텍스트) | *(없음 - App Store)* | `1,000,000,000+` | `500,000,000+` |
| `installs_exact` | 정확한 설치 수 (Play Store만) | *(없음)* | `1284567890` | `523456789` |
| `histogram` | 별점별 분포 (JSON 배열: [1점, 2점, 3점, 4점, 5점]) | `[125000, 180000, 350000, 2500000, 22692392]` | `[1500000, 2000000, 3500000, 8000000, 17156789]` | `[5000000, 4000000, 7000000, 15000000, 27234567]` |

---

### 2.4 `app_reviews` 테이블

사용자들이 작성한 개별 리뷰를 저장합니다.
리뷰 ID를 기준으로 중복을 방지하며, 개발사 답글이 있는 경우 함께 저장됩니다.
앱당 최대 20,000건의 리뷰를 수집합니다.

| 컬럼 | 설명 | 예시 1 | 예시 2 | 예시 3 |
|------|------|--------|--------|--------|
| `app_id` | 앱 고유 식별자 | `310633997` | `com.spotify.music` | `333903271` |
| `platform` | 배포 플랫폼 | `app_store` | `play_store` | `app_store` |
| `review_id` | 리뷰 고유 ID (플랫폼 제공) | `10847234567` | `gp:AOqpTOH...abc123` | `10923456789` |
| `country` | 리뷰 작성 국가 | `kr` | `us` | `jp` |
| `language` | 리뷰 작성 언어 | `ko` | `en` | `ja` |
| `user_name` | 리뷰 작성자 닉네임 | `행복한사용자` | `MusicLover2024` | `太郎` |
| `user_image` | 작성자 프로필 이미지 URL | *(없음 - App Store)* | `https://lh3.googleusercontent.com/a/...` | *(없음)* |
| `score` | 사용자가 준 별점 (1~5) | `5` | `4` | `2` |
| `title` | 리뷰 제목 (App Store만) | `정말 편리해요!` | *(없음)* | `使いにくくなった` |
| `content` | 리뷰 본문 | `가족들과 연락하기 정말 좋아요. 영상통화도 잘 되고 무료라서 최고입니다.` | `Great app for discovering new music. The algorithm really understands my taste. Sometimes ads are a bit much though.` | `アップデート後、UIが変わって使いづらくなりました。元に戻してください。` |
| `thumbs_up_count` | 리뷰에 대한 '도움됨' 수 | `127` | `3542` | `89` |
| `app_version` | 리뷰 작성 시점의 앱 버전 | `24.20.75` | `8.9.50.432` | `10.20` |
| `reviewed_at` | 리뷰 작성 시각 | `2024-11-15T14:32:00` | `2024-11-28T09:15:23` | `2024-10-05T22:45:00` |
| `reply_content` | 개발사 답글 (있는 경우) | *(없음)* | `Thank you for your feedback! We're always working to improve the ad experience for our free users.` | *(없음)* |
| `replied_at` | 개발사 답글 시각 | *(없음)* | `2024-11-29T11:20:00` | *(없음)* |

---

### 2.5 `failed_apps` 테이블

수집 실패한 앱을 추적하여 불필요한 재시도를 방지합니다.
삭제된 앱, 지역 제한 앱, 접근 불가 앱 등이 기록됩니다.

| 컬럼 | 설명 | 예시 1 | 예시 2 | 예시 3 |
|------|------|--------|--------|--------|
| `app_id` | 앱 고유 식별자 | `123456789` | `com.removed.app` | `987654321` |
| `platform` | 배포 플랫폼 | `app_store` | `play_store` | `app_store` |
| `reason` | 실패 사유 | `not_found` | `removed` | `region_locked` |
| `failed_at` | 실패 확인 시각 | `2024-11-01T10:00:00` | `2024-10-15T14:30:00` | `2024-11-20T08:45:00` |

---

### 2.6 `collection_status` 테이블

각 앱의 수집 진행 상황을 관리합니다.
마지막 수집 시각을 기반으로 업데이트 주기를 결정하고, 오래된 앱(2년 이상 업데이트 없음)은 수집 빈도를 낮춥니다.

| 컬럼 | 설명 | 예시 1 | 예시 2 | 예시 3 |
|------|------|--------|--------|--------|
| `app_id` | 앱 고유 식별자 | `310633997` | `com.spotify.music` | `old.abandoned.app` |
| `platform` | 배포 플랫폼 | `app_store` | `play_store` | `play_store` |
| `details_collected_at` | 상세정보 마지막 수집 시각 | `2024-12-01T06:30:00` | `2024-12-01T07:15:00` | `2024-06-15T10:00:00` |
| `reviews_collected_at` | 리뷰 마지막 수집 시각 | `2024-12-01T06:35:00` | `2024-12-01T07:20:00` | `2024-06-15T10:05:00` |
| `reviews_total_count` | 현재까지 수집된 총 리뷰 수 | `15238` | `20000` | `342` |
| `initial_review_done` | 초기 대량 리뷰 수집 완료 여부 | `1` | `1` | `1` |

---

## 데이터 흐름 요약

```
┌──────────────────┐     ┌───────────────────────────────────────────┐
│   Sitemap 수집    │────▶│  sitemap_apps.db                          │
│  (XML.gz 파싱)    │     │  ├─ sitemap_files (파일 메타데이터)         │
└──────────────────┘     │  └─ app_localizations (국가/언어 정보)      │
                         └───────────────────────────────────────────┘
                                            │
                                            ▼
                                   수집 대상 앱 결정
                                            │
                                            ▼
┌──────────────────┐     ┌───────────────────────────────────────────┐
│   API 호출        │────▶│  app_details.db                           │
│ (iTunes/Play)    │     │  ├─ apps (기본 메타데이터)                   │
└──────────────────┘     │  ├─ apps_localized (다국어 콘텐츠)          │
                         │  ├─ apps_metrics (평점/리뷰수/설치수)        │
                         │  ├─ app_reviews (개별 리뷰)                 │
                         │  ├─ failed_apps (실패 추적)                 │
                         │  └─ collection_status (수집 현황)           │
                         └───────────────────────────────────────────┘
```

---

## 주요 설계 원칙

1. **변경 감지 저장**: 동일 데이터는 중복 저장하지 않고, 변경 시에만 새 레코드 생성
2. **시계열 분석**: `recorded_at` 필드로 모든 변경 이력 추적 가능
3. **최적화**: 기준 언어와 동일한 현지화 콘텐츠 저장 생략, WAL 모드 사용
4. **버려진 앱 감지**: 2년 이상 업데이트 없는 앱은 수집 빈도 자동 감소
5. **중복 방지**: 리뷰 ID 기반 UNIQUE 제약으로 동일 리뷰 중복 저장 방지
