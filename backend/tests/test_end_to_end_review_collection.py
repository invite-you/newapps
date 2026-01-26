"""
리뷰 수집 시스템 End-to-End 통합 테스트

실제 DB와 네트워크를 사용하여 전체 시스템이
올바르게 동작하는지 검증합니다.

테스트 실행:
    # 전체 통합 테스트
    python -m pytest tests/test_end_to_end_review_collection.py -v

    # integration 마크가 있는 테스트만 (실제 네트워크 사용)
    python -m pytest tests/test_end_to_end_review_collection.py -v -m integration

Note:
    - 일부 테스트는 실제 DB 연결이 필요합니다
    - 일부 테스트는 실제 네트워크 요청을 수행합니다
"""
import os
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock

# 테스트 대상 모듈
from core.ip_manager import IPManager
from core.http_client import StoreHttpClient, HttpResult, HttpErrorCode
from core.review_collection_integration import (
    ReviewCollectionContext,
    get_review_collection_context,
    reset_review_collection_context,
)
from database.review_collection_db import (
    init_review_collection_tables,
    get_review_collection_status,
    upsert_review_collection_status,
    record_collection_success,
    record_collection_failure,
    should_collect_reviews,
    save_ip_store_mapping,
    get_working_ips_for_store,
    CollectionMode,
    ErrorCode,
    db_cursor,
)


# =============================================================================
# 픽스처
# =============================================================================

@pytest.fixture(scope="module")
def db_setup():
    """
    테스트용 DB 테이블 설정

    모듈 단위로 한 번만 실행됩니다.
    """
    try:
        init_review_collection_tables()
        yield True
    except Exception as e:
        pytest.skip(f"DB 연결 실패: {e}")


@pytest.fixture
def clean_test_data(db_setup):
    """
    테스트 데이터 정리

    각 테스트 전후로 test_ 접두사가 붙은 데이터를 정리합니다.
    """
    # 테스트 전 정리
    _cleanup_test_data()
    yield
    # 테스트 후 정리
    _cleanup_test_data()


def _cleanup_test_data():
    """테스트 데이터 삭제"""
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                DELETE FROM review_collection_status
                WHERE app_id LIKE 'test_%'
            """)
            cursor.execute("""
                DELETE FROM ip_store_mapping
                WHERE ip_address LIKE 'test_%'
            """)
    except Exception:
        pass  # DB 연결 실패 시 무시


@pytest.fixture
def ip_manager():
    """테스트용 IP Manager"""
    manager = IPManager()
    # 테스트용 IP 수동 설정
    manager.store_ip_map = {
        'app_store': ['10.0.0.1', '10.0.0.2'],
        'play_store': ['10.0.0.1', '10.0.0.2', '10.0.0.3']
    }
    manager.available_ips = ['10.0.0.1', '10.0.0.2', '10.0.0.3']
    return manager


@pytest.fixture
def http_client(ip_manager):
    """테스트용 HTTP Client"""
    return StoreHttpClient(ip_manager, use_rotation=True)


@pytest.fixture
def collection_context():
    """테스트용 수집 컨텍스트"""
    reset_review_collection_context()
    ctx = ReviewCollectionContext(use_ip_rotation=True)
    # 초기화는 수동으로 필요할 때만
    yield ctx
    reset_review_collection_context()


# =============================================================================
# 1. IP Manager 통합 테스트
# =============================================================================

class TestIPManagerIntegration:
    """IP Manager 실제 동작 테스트"""

    def test_discover_real_ips(self):
        """실제 서버 IP 감지 테스트"""
        manager = IPManager()
        ips = manager.discover_ips()

        # 최소 하나의 IP가 감지되어야 함
        assert len(ips) > 0
        assert all('.' in ip for ip in ips)  # IPv4 형식

    @pytest.mark.integration
    def test_initialize_with_real_endpoints(self):
        """실제 엔드포인트로 초기화 테스트"""
        manager = IPManager()
        store_ips = manager.initialize()

        # 결과가 딕셔너리여야 함
        assert isinstance(store_ips, dict)
        assert 'app_store' in store_ips
        assert 'play_store' in store_ips

        # 초기화 시간이 설정되어야 함
        assert manager.last_initialized_at is not None

    def test_rotation_distributes_requests(self, ip_manager):
        """IP 로테이션이 요청을 분산시키는지 테스트"""
        request_counts = {ip: 0 for ip in ip_manager.store_ip_map['app_store']}

        # 100번 요청 시뮬레이션
        for _ in range(100):
            ip = ip_manager.get_next_ip_for_store('app_store')
            request_counts[ip] += 1

        # 각 IP가 고르게 사용되어야 함 (2개 IP면 각각 50번씩)
        values = list(request_counts.values())
        assert max(values) - min(values) <= 1  # 최대 1 차이


# =============================================================================
# 2. HTTP Client 통합 테스트
# =============================================================================

class TestHttpClientIntegration:
    """HTTP Client 실제 동작 테스트"""

    def test_rotation_uses_different_ips(self, http_client, ip_manager):
        """로테이션 시 다른 IP가 사용되는지 테스트"""
        used_ips = []

        # _do_request를 모의하여 IP만 기록
        original_do_request = http_client._do_request

        def mock_do_request(url, ip, *args, **kwargs):
            used_ips.append(ip)
            return HttpResult(success=True, data={}, used_ip=ip)

        http_client._do_request = mock_do_request

        # 4번 요청
        for _ in range(4):
            http_client.request('http://test.com', 'app_store')

        # 2개 IP가 번갈아 사용되어야 함
        assert len(set(used_ips)) == 2
        assert used_ips[0] != used_ips[1]
        assert used_ips[2] == used_ips[0]  # 순환

    def test_failed_ip_excluded_from_rotation(self, http_client, ip_manager):
        """실패한 IP가 로테이션에서 제외되는지 테스트"""
        call_count = 0

        def mock_do_request(url, ip, *args, **kwargs):
            nonlocal call_count
            call_count += 1

            if ip == '10.0.0.1':
                # 첫 번째 IP는 차단됨
                return HttpResult(
                    success=False,
                    error_code=HttpErrorCode.IP_BLOCKED,
                    used_ip=ip
                )
            else:
                # 두 번째 IP는 성공
                return HttpResult(success=True, data={}, used_ip=ip)

        http_client._do_request = mock_do_request

        # 요청 실행 (첫 번째 IP 실패 → 두 번째 IP 성공)
        result = http_client.request('http://test.com', 'app_store')

        assert result.success is True
        assert '10.0.0.1' in http_client.failed_ips.get('app_store', [])
        assert call_count == 2  # 2번 호출 (실패 + 재시도)

    def test_reset_clears_failed_ips(self, http_client):
        """reset이 실패 IP를 초기화하는지 테스트"""
        http_client.failed_ips['app_store'] = ['10.0.0.1']
        http_client.reset_all('app_store')

        assert http_client.failed_ips.get('app_store', []) == []


# =============================================================================
# 3. 상태 추적 통합 테스트 (DB 필요)
# =============================================================================

class TestStatusTrackingIntegration:
    """상태 추적 DB 통합 테스트"""

    def test_full_collection_cycle(self, clean_test_data):
        """전체 수집 사이클 테스트: 판단 → 수집 → 기록"""
        app_id = 'test_full_cycle_app'
        platform = 'app_store'

        # 1. 첫 수집 여부 판단
        should, mode, reason = should_collect_reviews(app_id, platform, 1000)

        assert should is True
        assert mode == CollectionMode.INITIAL
        assert reason is None

        # 2. 수집 성공 기록
        record_collection_success(
            app_id=app_id,
            platform=platform,
            store_review_count=1000,
            collected_count=500,
            collection_limited=True,
            limited_reason='RSS_PAGE_LIMIT'
        )

        # 3. 상태 확인
        status = get_review_collection_status(app_id, platform)

        assert status is not None
        assert status['last_known_store_review_count'] == 1000
        assert status['consecutive_failures'] == 0
        assert status['collection_limited'] is True

        # 4. 변화 없을 때 판단
        should2, mode2, reason2 = should_collect_reviews(app_id, platform, 1000)

        assert should2 is False
        assert reason2 == 'no_change'

        # 5. 리뷰 수 증가 시 판단
        should3, mode3, reason3 = should_collect_reviews(app_id, platform, 1500)

        assert should3 is True
        assert mode3 == CollectionMode.INCREMENTAL

    def test_failure_tracking(self, clean_test_data):
        """실패 추적 테스트"""
        app_id = 'test_failure_app'
        platform = 'app_store'

        # 연속 실패 기록
        for i in range(3):
            failures = record_collection_failure(
                app_id=app_id,
                platform=platform,
                store_review_count=1000,
                failure_reason=ErrorCode.RATE_LIMITED,
                failure_detail=f'Attempt {i+1}'
            )
            assert failures == i + 1

        # 상태 확인
        status = get_review_collection_status(app_id, platform)
        assert status['consecutive_failures'] == 3
        assert status['last_failure_reason'] == ErrorCode.RATE_LIMITED

        # 성공 후 실패 카운터 초기화
        record_collection_success(
            app_id=app_id,
            platform=platform,
            store_review_count=1000,
            collected_count=100
        )

        status2 = get_review_collection_status(app_id, platform)
        assert status2['consecutive_failures'] == 0
        assert status2['last_failure_reason'] is None

    def test_ip_store_mapping(self, clean_test_data):
        """IP-스토어 매핑 저장/조회 테스트"""
        # 테스트 IP 저장
        save_ip_store_mapping(
            ip_address='test_192.168.1.1',
            platform='app_store',
            is_working=True
        )
        save_ip_store_mapping(
            ip_address='test_192.168.1.2',
            platform='app_store',
            is_working=False,
            error='HTTP 403'
        )

        # 동작하는 IP 조회
        working_ips = get_working_ips_for_store('app_store')

        # 테스트 IP 필터링 (다른 테스트의 데이터 제외)
        test_working_ips = [ip for ip in working_ips if ip.startswith('test_')]

        assert 'test_192.168.1.1' in test_working_ips
        assert 'test_192.168.1.2' not in test_working_ips


# =============================================================================
# 4. 통합 컨텍스트 테스트
# =============================================================================

class TestCollectionContextIntegration:
    """ReviewCollectionContext 통합 테스트"""

    def test_context_workflow(self, clean_test_data):
        """컨텍스트 전체 워크플로우 테스트"""
        ctx = ReviewCollectionContext(use_ip_rotation=True)

        # IP Manager 수동 설정 (실제 네트워크 테스트 회피)
        ctx.ip_manager.store_ip_map = {
            'app_store': ['10.0.0.1', '10.0.0.2'],
            'play_store': ['10.0.0.1']
        }
        ctx.ip_manager.available_ips = ['10.0.0.1', '10.0.0.2']

        # HTTP Client 수동 생성
        ctx.http_client = StoreHttpClient(ctx.ip_manager, use_rotation=True)
        ctx.initialized = True

        app_id = 'test_context_workflow_app'
        platform = 'app_store'

        # 1. 수집 여부 판단
        should, mode = ctx.should_collect(app_id, platform, 2000)

        assert should is True
        assert mode == CollectionMode.INITIAL
        assert ctx.stats['apps_checked'] == 1

        # 2. 수집 성공 기록
        ctx.record_success(app_id, platform, 2000, 150)

        assert ctx.stats['apps_collected'] == 1
        assert ctx.stats['reviews_collected'] == 150

        # 3. 상태 확인
        status = ctx.get_status(app_id, platform)

        assert status is not None
        assert status['last_known_store_review_count'] == 2000

        # 4. 통계 확인
        stats = ctx.get_stats()

        assert stats['apps_checked'] == 1
        assert stats['apps_collected'] == 1
        assert stats['reviews_collected'] == 150
        assert 'rotation' in stats

    def test_context_error_handling(self, clean_test_data):
        """컨텍스트 에러 처리 테스트"""
        ctx = ReviewCollectionContext()
        ctx.initialized = True
        ctx.http_client = MagicMock()

        app_id = 'test_context_error_app'
        platform = 'play_store'

        # 실패 기록
        failures = ctx.record_failure(
            app_id, platform, 1000,
            ErrorCode.IP_BLOCKED,
            'All IPs blocked'
        )

        assert failures == 1
        assert ctx.stats['errors'] == 1

        # 상태 확인
        status = ctx.get_status(app_id, platform)

        assert status['last_failure_reason'] == ErrorCode.IP_BLOCKED
        assert status['consecutive_failures'] == 1


# =============================================================================
# 5. 실제 네트워크 통합 테스트 (선택적)
# =============================================================================

@pytest.mark.integration
class TestRealNetworkIntegration:
    """
    실제 네트워크를 사용하는 통합 테스트

    이 테스트는 실제 외부 서버에 요청을 보내므로
    네트워크 상태에 따라 실패할 수 있습니다.
    """

    def test_app_store_rss_request(self):
        """실제 App Store RSS 요청 테스트"""
        manager = IPManager()
        manager.initialize()

        if not manager.store_ip_map.get('app_store'):
            pytest.skip("App Store에 사용 가능한 IP 없음")

        client = StoreHttpClient(manager, use_rotation=True)

        # Facebook 앱의 RSS 피드 요청
        url = 'https://itunes.apple.com/us/rss/customerreviews/page=1/id=284882215/sortBy=mostRecent/json'
        result = client.request(url, 'app_store', parse_json=True)

        if result.success:
            assert 'feed' in result.data
            assert result.used_ip is not None
        else:
            # IP 차단 등의 이유로 실패할 수 있음
            assert result.error_code in [
                HttpErrorCode.IP_BLOCKED,
                HttpErrorCode.RATE_LIMITED,
                HttpErrorCode.NETWORK_ERROR,
            ]

    def test_play_store_request(self):
        """실제 Play Store 요청 테스트"""
        manager = IPManager()
        manager.initialize()

        if not manager.store_ip_map.get('play_store'):
            pytest.skip("Play Store에 사용 가능한 IP 없음")

        client = StoreHttpClient(manager, use_rotation=True)

        # WhatsApp 앱 페이지 요청
        url = 'https://play.google.com/store/apps/details?id=com.whatsapp&hl=en&gl=us'
        result = client.request(url, 'play_store', parse_json=False)

        if result.success:
            assert len(result.data) > 1000  # HTML 응답
            assert result.used_ip is not None
        else:
            assert result.error_code in [
                HttpErrorCode.IP_BLOCKED,
                HttpErrorCode.RATE_LIMITED,
                HttpErrorCode.NETWORK_ERROR,
            ]


# =============================================================================
# 6. 수집기 통합 테스트
# =============================================================================

class TestCollectorIntegration:
    """수집기 클래스 통합 테스트"""

    def test_app_store_collector_imports(self):
        """App Store 수집기 import 테스트"""
        from scrapers.app_store_reviews_collector import (
            AppStoreReviewsCollector,
            NEW_INTEGRATION_AVAILABLE
        )

        assert NEW_INTEGRATION_AVAILABLE is True

    def test_play_store_collector_imports(self):
        """Play Store 수집기 import 테스트"""
        from scrapers.play_store_reviews_collector import (
            PlayStoreReviewsCollector,
            NEW_STATUS_TRACKING_AVAILABLE
        )

        assert NEW_STATUS_TRACKING_AVAILABLE is True

    def test_app_store_collector_modes(self):
        """App Store 수집기 모드 테스트"""
        from scrapers.app_store_reviews_collector import AppStoreReviewsCollector

        # 새 통합 시스템 비활성화
        collector_old = AppStoreReviewsCollector(
            verbose=False,
            use_new_integration=False
        )
        assert collector_old.use_new_integration is False

        # 새 통합 시스템 활성화 (기본값)
        collector_new = AppStoreReviewsCollector(
            verbose=False,
            use_new_integration=True
        )
        assert collector_new.use_new_integration is True
        assert collector_new.collection_context is not None

    def test_play_store_collector_modes(self):
        """Play Store 수집기 모드 테스트"""
        from scrapers.play_store_reviews_collector import PlayStoreReviewsCollector

        # 새 상태 추적 비활성화
        collector_old = PlayStoreReviewsCollector(
            verbose=False,
            use_new_status_tracking=False
        )
        assert collector_old.use_new_status_tracking is False

        # 새 상태 추적 활성화 (기본값)
        collector_new = PlayStoreReviewsCollector(
            verbose=False,
            use_new_status_tracking=True
        )
        assert collector_new.use_new_status_tracking is True


# =============================================================================
# 7. 성능/스트레스 테스트
# =============================================================================

class TestPerformance:
    """성능 및 스트레스 테스트"""

    def test_rotation_performance(self, ip_manager):
        """IP 로테이션 성능 테스트"""
        import time

        iterations = 10000
        start = time.perf_counter()

        for _ in range(iterations):
            ip_manager.get_next_ip_for_store('app_store')

        elapsed = time.perf_counter() - start

        # 10,000번 로테이션이 1초 이내에 완료되어야 함
        assert elapsed < 1.0
        print(f"\n로테이션 성능: {iterations}회 / {elapsed:.3f}초")

    def test_status_tracking_performance(self, clean_test_data):
        """상태 추적 성능 테스트"""
        import time

        iterations = 100
        start = time.perf_counter()

        for i in range(iterations):
            app_id = f'test_perf_app_{i}'

            # 수집 여부 판단
            should_collect_reviews(app_id, 'app_store', 1000)

            # 성공 기록
            record_collection_success(app_id, 'app_store', 1000, 50)

        elapsed = time.perf_counter() - start

        # 100회 사이클이 10초 이내에 완료되어야 함
        assert elapsed < 10.0
        print(f"\n상태 추적 성능: {iterations}회 / {elapsed:.3f}초")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
