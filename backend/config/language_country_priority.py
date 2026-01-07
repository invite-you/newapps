"""
언어-국가 우선순위 설정

앱 스토어 시장 규모 및 사용자 수를 기반으로 각 언어에 대한 최적의 국가를 정의합니다.

참고 데이터 (2024년 기준):
1. 미국 (US): ~$58B (세계 최대 시장)
2. 중국 (CN): ~$43B
3. 일본 (JP): ~$25B
4. 영국 (GB): ~$9B
5. 한국 (KR): ~$8B
6. 독일 (DE): ~$7B
7. 프랑스 (FR): ~$4B
8. 캐나다 (CA): ~$4B
9. 호주 (AU): ~$3B
10. 브라질 (BR): ~$3B
"""
from datetime import datetime

# 언어별 우선 국가
# key: 언어 코드 (ISO 639-1)
# value: 우선순위 순서의 국가 코드 목록 (ISO 3166-1 alpha-2)
#
# 원칙:
# 1. 해당 언어를 공식어로 사용하는 가장 큰 시장 우선
# 2. 사용자 수가 많은 국가 우선
# 3. 경제력 (구매력) 고려

LANGUAGE_COUNTRY_PRIORITY = {
    # === 주요 언어 (Tier 1) - 앱스토어 매출 상위 ===

    # 영어: 미국 > 영국 > 캐나다 > 호주 > 인도
    'en': ['US', 'GB', 'CA', 'AU', 'IN', 'NZ', 'IE', 'SG', 'ZA', 'PH'],

    # 중국어 간체: 중국 본토
    'zh': ['CN', 'SG'],
    'zh-hans': ['CN', 'SG'],
    'zh-cn': ['CN'],

    # 중국어 번체: 대만 > 홍콩 > 마카오
    'zh-hant': ['TW', 'HK', 'MO'],
    'zh-tw': ['TW'],
    'zh-hk': ['HK'],

    # 일본어: 일본 (단일 시장)
    'ja': ['JP'],

    # 한국어: 한국 (단일 시장)
    'ko': ['KR'],

    # 독일어: 독일 > 오스트리아 > 스위스
    # (독일이 오스트리아, 스위스보다 시장 5배 이상 큼)
    'de': ['DE', 'AT', 'CH'],

    # 프랑스어: 프랑스 > 벨기에 > 스위스 > 캐나다
    # (프랑스가 캐나다 프랑스어권보다 시장 3배 큼)
    'fr': ['FR', 'BE', 'CH', 'CA'],

    # === 유럽 언어 (Tier 2) ===

    # 스페인어: 멕시코 > 스페인 > 아르헨티나
    # (멕시코가 스페인보다 인구 2배, 앱 다운로드 수 1위)
    'es': ['MX', 'ES', 'AR', 'CO', 'CL', 'PE'],

    # 포르투갈어: 브라질 > 포르투갈
    # (브라질 시장이 포르투갈보다 20배 이상 큼)
    'pt': ['BR', 'PT'],
    'pt-br': ['BR'],
    'pt-pt': ['PT'],

    # 이탈리아어: 이탈리아 > 스위스
    'it': ['IT', 'CH'],

    # 러시아어: 러시아
    'ru': ['RU', 'BY', 'KZ', 'UA'],

    # 네덜란드어: 네덜란드 > 벨기에
    'nl': ['NL', 'BE'],

    # 폴란드어: 폴란드
    'pl': ['PL'],

    # 터키어: 터키
    'tr': ['TR'],

    # 체코어: 체코
    'cs': ['CZ'],

    # 그리스어: 그리스 > 키프로스
    'el': ['GR', 'CY'],

    # 헝가리어: 헝가리
    'hu': ['HU'],

    # 루마니아어: 루마니아
    'ro': ['RO'],

    # 우크라이나어: 우크라이나
    'uk': ['UA'],

    # === 북유럽 언어 ===

    # 스웨덴어: 스웨덴 > 핀란드
    'sv': ['SE', 'FI'],

    # 노르웨이어: 노르웨이
    'no': ['NO'],
    'nb': ['NO'],  # 노르웨이어 부크몰
    'nn': ['NO'],  # 노르웨이어 뉘노르스크

    # 덴마크어: 덴마크
    'da': ['DK'],

    # 핀란드어: 핀란드
    'fi': ['FI'],

    # === 아시아 언어 ===

    # 태국어: 태국
    'th': ['TH'],

    # 베트남어: 베트남
    'vi': ['VN'],

    # 인도네시아어: 인도네시아
    'id': ['ID'],

    # 말레이어: 말레이시아 > 싱가포르
    'ms': ['MY', 'SG', 'BN'],

    # 타갈로그어/필리핀어: 필리핀
    'tl': ['PH'],
    'fil': ['PH'],

    # 힌디어: 인도
    'hi': ['IN'],

    # 벵골어: 방글라데시 > 인도
    'bn': ['BD', 'IN'],

    # 타밀어: 인도 > 스리랑카 > 싱가포르
    'ta': ['IN', 'LK', 'SG'],

    # 텔루구어: 인도
    'te': ['IN'],

    # 마라티어: 인도
    'mr': ['IN'],

    # 구자라트어: 인도
    'gu': ['IN'],

    # 칸나다어: 인도
    'kn': ['IN'],

    # 말라얄람어: 인도
    'ml': ['IN'],

    # 펀자브어: 인도 > 파키스탄
    'pa': ['IN', 'PK'],

    # 우르두어: 파키스탄 > 인도
    'ur': ['PK', 'IN'],

    # === 중동 언어 ===

    # 아랍어: 사우디아라비아 > UAE > 이집트
    # (사우디가 앱스토어 매출 1위, UAE 2위)
    'ar': ['SA', 'AE', 'EG', 'KW', 'QA', 'BH', 'OM', 'JO', 'LB', 'MA'],

    # 히브리어: 이스라엘
    'he': ['IL'],
    'iw': ['IL'],  # 구 코드

    # 페르시아어: 이란
    'fa': ['IR'],

    # === 기타 언어 ===

    # 슬로바키아어: 슬로바키아
    'sk': ['SK'],

    # 슬로베니아어: 슬로베니아
    'sl': ['SI'],

    # 크로아티아어: 크로아티아
    'hr': ['HR'],

    # 세르비아어: 세르비아
    'sr': ['RS'],

    # 불가리아어: 불가리아
    'bg': ['BG'],

    # 리투아니아어: 리투아니아
    'lt': ['LT'],

    # 라트비아어: 라트비아
    'lv': ['LV'],

    # 에스토니아어: 에스토니아
    'et': ['EE'],

    # 카탈루냐어: 스페인
    'ca': ['ES'],

    # 바스크어: 스페인
    'eu': ['ES'],

    # 갈리시아어: 스페인
    'gl': ['ES'],

    # 아프리칸스어: 남아프리카
    'af': ['ZA'],

    # 스와힐리어: 탄자니아 > 케냐
    'sw': ['TZ', 'KE'],
}

SESSION_ID = None

# 주요 언어 목록 (우선 수집 대상)
# 글로벌 앱스토어 매출 상위 언어권
PRIORITY_LANGUAGES = [
    'en',   # 영어 - 세계 최대
    'zh',   # 중국어 - 2위
    'ja',   # 일본어 - 3위
    'ko',   # 한국어 - 5위 (의외로 높음)
    'de',   # 독일어 - 6위
    'fr',   # 프랑스어 - 7위
    'es',   # 스페인어 - 인구 대비 시장
    'pt',   # 포르투갈어 - 브라질 시장
    'ru',   # 러시아어
    'it',   # 이탈리아어
]


def get_primary_country(language: str) -> str:
    """
    언어에 대한 최우선 국가를 반환합니다.

    Args:
        language: 언어 코드 (예: 'fr', 'zh-hans')

    Returns:
        국가 코드 (예: 'FR', 'CN')
    """
    lang = language.lower().strip()

    # 정확히 일치하는 경우
    if lang in LANGUAGE_COUNTRY_PRIORITY:
        return LANGUAGE_COUNTRY_PRIORITY[lang][0]

    # 하이픈으로 분리된 경우 기본 언어 코드로 시도
    if '-' in lang:
        base_lang = lang.split('-')[0]
        if base_lang in LANGUAGE_COUNTRY_PRIORITY:
            return LANGUAGE_COUNTRY_PRIORITY[base_lang][0]

    # 언더스코어로 분리된 경우
    if '_' in lang:
        base_lang = lang.split('_')[0]
        if base_lang in LANGUAGE_COUNTRY_PRIORITY:
            return LANGUAGE_COUNTRY_PRIORITY[base_lang][0]

    # 찾지 못한 경우 US 반환 (기본값)
    return 'US'


def get_country_priority_list(language: str) -> list:
    """
    언어에 대한 국가 우선순위 목록을 반환합니다.

    Args:
        language: 언어 코드

    Returns:
        국가 코드 목록 (우선순위 순)
    """
    lang = language.lower().strip()

    if lang in LANGUAGE_COUNTRY_PRIORITY:
        return LANGUAGE_COUNTRY_PRIORITY[lang].copy()

    if '-' in lang:
        base_lang = lang.split('-')[0]
        if base_lang in LANGUAGE_COUNTRY_PRIORITY:
            return LANGUAGE_COUNTRY_PRIORITY[base_lang].copy()

    if '_' in lang:
        base_lang = lang.split('_')[0]
        if base_lang in LANGUAGE_COUNTRY_PRIORITY:
            return LANGUAGE_COUNTRY_PRIORITY[base_lang].copy()

    return ['US']


def get_best_country_for_language(language: str, available_countries: list) -> str:
    """
    주어진 언어와 사용 가능한 국가 목록에서 최적의 국가를 선택합니다.

    Args:
        language: 언어 코드
        available_countries: 사용 가능한 국가 코드 목록

    Returns:
        선택된 국가 코드
    """
    if not available_countries:
        return get_primary_country(language)

    # 대문자로 정규화
    available_upper = [c.upper() for c in available_countries]

    # 우선순위 목록에서 사용 가능한 첫 번째 국가 선택
    priority_list = get_country_priority_list(language)

    for country in priority_list:
        if country.upper() in available_upper:
            return country.upper()

    # 우선순위에 없으면 첫 번째 사용 가능한 국가 반환
    return available_countries[0].upper()


def sort_language_country_pairs(pairs: list) -> list:
    """
    (language, country) 쌍 목록을 우선순위에 따라 정렬합니다.

    Args:
        pairs: [(language, country), ...] 형태의 목록

    Returns:
        정렬된 [(language, country), ...] 목록
    """
    def priority_key(pair):
        lang, country = pair
        lang = lang.lower()
        country = country.upper()

        # 언어 우선순위 (PRIORITY_LANGUAGES 기준)
        try:
            lang_priority = PRIORITY_LANGUAGES.index(lang.split('-')[0])
        except ValueError:
            lang_priority = 999

        # 해당 언어 내에서 국가 우선순위
        country_priority_list = get_country_priority_list(lang)
        try:
            country_priority = country_priority_list.index(country)
        except ValueError:
            country_priority = 999

        return (lang_priority, country_priority, lang, country)

    return sorted(pairs, key=priority_key)


def select_best_pairs_for_collection(pairs: list, max_languages: int = 10) -> list:
    """
    수집할 최적의 (language, country) 쌍을 선택합니다.

    각 언어당 가장 적합한 국가를 하나씩만 선택합니다.

    Args:
        pairs: [(language, country), ...] 형태의 목록
        max_languages: 최대 수집할 언어 수

    Returns:
        선택된 [(language, country), ...] 목록
    """
    if not pairs:
        return [('en', 'US')]

    # 언어별로 국가 그룹화
    lang_countries = {}
    for lang, country in pairs:
        base_lang = lang.lower().split('-')[0]
        if base_lang not in lang_countries:
            lang_countries[base_lang] = []
        lang_countries[base_lang].append((lang, country))

    # 각 언어에 대해 최적의 국가 선택
    selected = []

    # 우선순위 언어부터 처리
    for priority_lang in PRIORITY_LANGUAGES:
        if priority_lang in lang_countries and len(selected) < max_languages:
            lang_pairs = lang_countries[priority_lang]
            countries = [c for _, c in lang_pairs]
            best_country = get_best_country_for_language(priority_lang, countries)

            # 원래 언어 코드 찾기 (zh-hans vs zh 등)
            for orig_lang, orig_country in lang_pairs:
                if orig_country.upper() == best_country.upper():
                    selected.append((orig_lang, orig_country))
                    break
            else:
                # 정확히 일치하는 게 없으면 첫 번째 것 사용
                selected.append((priority_lang, best_country))

            del lang_countries[priority_lang]

    # 나머지 언어 처리
    for lang, lang_pairs in sorted(lang_countries.items()):
        if len(selected) >= max_languages:
            break

        countries = [c for _, c in lang_pairs]
        best_country = get_best_country_for_language(lang, countries)

        for orig_lang, orig_country in lang_pairs:
            if orig_country.upper() == best_country.upper():
                selected.append((orig_lang, orig_country))
                break
        else:
            selected.append((lang, best_country))

    return selected


# 테스트
if __name__ == '__main__':
    from utils.logger import get_timestamped_logger

    SESSION_ID = datetime.now().strftime('%Y%m%d_%H%M%S')
    logger = get_timestamped_logger(
        "language_country_priority",
        file_prefix="language_country_priority",
        session_id=SESSION_ID,
    )

    # 테스트 케이스
    logger.info("=== 언어별 최우선 국가 ===")
    test_languages = ['en', 'fr', 'de', 'zh', 'zh-hans', 'zh-hant', 'ko', 'ja', 'es', 'pt', 'ar']
    for lang in test_languages:
        logger.info(f"  {lang}: {get_primary_country(lang)}")

    logger.info("\n=== 최적 국가 선택 테스트 ===")
    # 프랑스어: CA, FR 중 FR 선택해야 함
    fr_countries = ['CA', 'FR', 'BE']
    logger.info(f"  French with {fr_countries}: {get_best_country_for_language('fr', fr_countries)}")

    # 포르투갈어: BR, PT 중 BR 선택해야 함
    pt_countries = ['PT', 'BR']
    logger.info(f"  Portuguese with {pt_countries}: {get_best_country_for_language('pt', pt_countries)}")

    # 스페인어: ES, MX 중 MX 선택해야 함
    es_countries = ['ES', 'AR', 'MX']
    logger.info(f"  Spanish with {es_countries}: {get_best_country_for_language('es', es_countries)}")

    logger.info("\n=== 수집할 쌍 선택 테스트 ===")
    test_pairs = [
        ('fr', 'CA'), ('fr', 'FR'), ('fr', 'BE'),
        ('en', 'US'), ('en', 'GB'), ('en', 'AU'),
        ('de', 'AT'), ('de', 'DE'), ('de', 'CH'),
        ('ko', 'KR'),
        ('ja', 'JP'),
        ('zh', 'CN'), ('zh', 'TW'),
    ]
    selected = select_best_pairs_for_collection(test_pairs, max_languages=5)
    logger.info(f"  Input: {len(test_pairs)} pairs")
    logger.info("  Selected (max 5):")
    for lang, country in selected:
        logger.info(f"    - {lang}: {country}")
