"""
Core 모듈

리뷰 수집 시스템의 핵심 기능을 제공합니다:

- ip_manager: 서버 IP 자동 감지 및 스토어별 할당 (IP 로테이션 지원)
- http_client: IP 바인딩 HTTP 클라이언트 (에러 분류, 재시도, IP 로테이션)
- review_collection_integration: 기존 수집기와의 통합 레이어

간단한 사용 (통합 컨텍스트):
    from core.review_collection_integration import get_review_collection_context

    # 전역 컨텍스트 얻기 (자동 초기화)
    ctx = get_review_collection_context()

    # 수집 여부 판단
    should, mode = ctx.should_collect('284882215', 'app_store', 50000)

    # HTTP 요청 (IP 로테이션 자동 적용)
    result = ctx.request(url, 'app_store')

    # 결과 기록
    if result.success:
        ctx.record_success('284882215', 'app_store', 50000, 100)

상세 사용 (개별 컴포넌트):
    from core.ip_manager import IPManager
    from core.http_client import StoreHttpClient

    # IP Manager 초기화
    ip_manager = IPManager()
    store_ips = ip_manager.initialize()
    # {'app_store': ['172.31.40.115'], 'play_store': ['172.31.47.39']}

    # HTTP 클라이언트 사용 (IP 로테이션 활성화)
    client = StoreHttpClient(ip_manager, use_rotation=True)
    result = client.request(url, 'app_store')
"""
