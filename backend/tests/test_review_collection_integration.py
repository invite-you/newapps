"""
리뷰 수집 통합 테스트

core.review_collection_integration 모듈과
수집기 통합 기능을 테스트합니다.
"""
import pytest
from unittest.mock import patch, MagicMock

from core.review_collection_integration import (
    ReviewCollectionContext,
    get_review_collection_context,
    reset_review_collection_context,
    map_http_error_to_db_error,
)
from core.http_client import HttpErrorCode
from database.review_collection_db import ErrorCode, CollectionMode


class TestReviewCollectionContext:
    """ReviewCollectionContext 클래스 테스트"""

    @pytest.fixture(autouse=True)
    def setup(self):
        """각 테스트 전 전역 컨텍스트 초기화"""
        reset_review_collection_context()

    def test_context_creation(self):
        """컨텍스트 생성 테스트"""
        ctx = ReviewCollectionContext(use_ip_rotation=True)

        assert ctx.ip_manager is not None
        assert ctx.http_client is None  # 초기화 전
        assert ctx.initialized is False
        assert ctx.use_ip_rotation is True

    def test_context_stats_initial(self):
        """초기 통계 테스트"""
        ctx = ReviewCollectionContext()

        assert ctx.stats['apps_checked'] == 0
        assert ctx.stats['apps_collected'] == 0
        assert ctx.stats['apps_skipped'] == 0
        assert ctx.stats['reviews_collected'] == 0
        assert ctx.stats['errors'] == 0

    @patch('core.review_collection_integration.init_review_collection_tables')
    @patch.object(ReviewCollectionContext, '_save_ip_test_results')
    def test_initialize(self, mock_save, mock_init_tables):
        """초기화 테스트"""
        ctx = ReviewCollectionContext()

        # IP Manager의 initialize를 모의
        ctx.ip_manager.store_ip_map = {
            'app_store': ['1.1.1.1'],
            'play_store': ['2.2.2.2']
        }
        ctx.ip_manager.available_ips = ['1.1.1.1', '2.2.2.2']

        with patch.object(ctx.ip_manager, 'initialize', return_value=ctx.ip_manager.store_ip_map):
            result = ctx.initialize(save_to_db=True)

        assert ctx.initialized is True
        assert ctx.http_client is not None
        assert result == {'app_store': ['1.1.1.1'], 'play_store': ['2.2.2.2']}
        mock_init_tables.assert_called_once()
        mock_save.assert_called_once()

    def test_should_collect_tracks_stats(self):
        """should_collect이 통계를 추적하는지 테스트"""
        ctx = ReviewCollectionContext()
        ctx.initialized = True

        with patch('core.review_collection_integration.should_collect_reviews') as mock:
            # 수집해야 하는 경우
            mock.return_value = (True, CollectionMode.INITIAL, None)
            should, mode = ctx.should_collect('app1', 'app_store', 1000)

            assert should is True
            assert mode == CollectionMode.INITIAL
            assert ctx.stats['apps_checked'] == 1
            assert ctx.stats['apps_skipped'] == 0

            # 스킵하는 경우
            mock.return_value = (False, None, "no_change")
            should, mode = ctx.should_collect('app2', 'app_store', 1000)

            assert should is False
            assert ctx.stats['apps_checked'] == 2
            assert ctx.stats['apps_skipped'] == 1

    def test_record_success_tracks_stats(self):
        """record_success가 통계를 추적하는지 테스트"""
        ctx = ReviewCollectionContext()
        ctx.initialized = True

        with patch('core.review_collection_integration.record_collection_success') as mock:
            ctx.record_success('app1', 'app_store', 1000, 50)

        assert ctx.stats['apps_collected'] == 1
        assert ctx.stats['reviews_collected'] == 50
        mock.assert_called_once()

    def test_record_failure_tracks_stats(self):
        """record_failure가 통계를 추적하는지 테스트"""
        ctx = ReviewCollectionContext()
        ctx.initialized = True

        with patch('core.review_collection_integration.record_collection_failure', return_value=1) as mock:
            failures = ctx.record_failure('app1', 'app_store', 1000, ErrorCode.RATE_LIMITED)

        assert failures == 1
        assert ctx.stats['errors'] == 1
        mock.assert_called_once()


class TestGlobalContext:
    """전역 컨텍스트 테스트"""

    @pytest.fixture(autouse=True)
    def setup(self):
        """각 테스트 전 전역 컨텍스트 초기화"""
        reset_review_collection_context()

    def test_get_context_creates_singleton(self):
        """전역 컨텍스트가 싱글톤인지 테스트"""
        with patch.object(ReviewCollectionContext, 'initialize'):
            ctx1 = get_review_collection_context(auto_initialize=False)
            ctx2 = get_review_collection_context(auto_initialize=False)

        assert ctx1 is ctx2

    def test_reset_context_clears_singleton(self):
        """reset이 싱글톤을 초기화하는지 테스트"""
        with patch.object(ReviewCollectionContext, 'initialize'):
            ctx1 = get_review_collection_context(auto_initialize=False)
            reset_review_collection_context()
            ctx2 = get_review_collection_context(auto_initialize=False)

        assert ctx1 is not ctx2


class TestErrorMapping:
    """에러 코드 매핑 테스트"""

    def test_map_ip_blocked(self):
        """IP_BLOCKED 매핑 테스트"""
        result = map_http_error_to_db_error(HttpErrorCode.IP_BLOCKED)
        assert result == ErrorCode.IP_BLOCKED

    def test_map_rate_limited(self):
        """RATE_LIMITED 매핑 테스트"""
        result = map_http_error_to_db_error(HttpErrorCode.RATE_LIMITED)
        assert result == ErrorCode.RATE_LIMITED

    def test_map_network_error(self):
        """NETWORK_ERROR 매핑 테스트"""
        result = map_http_error_to_db_error(HttpErrorCode.NETWORK_ERROR)
        assert result == ErrorCode.NETWORK_ERROR

    def test_map_unknown_defaults_to_network_error(self):
        """알 수 없는 코드는 NETWORK_ERROR로 매핑"""
        result = map_http_error_to_db_error("UNKNOWN_CODE")
        assert result == ErrorCode.NETWORK_ERROR


class TestRequestMethod:
    """request 메서드 테스트"""

    def test_request_without_init_raises(self):
        """초기화 없이 request 호출 시 에러"""
        ctx = ReviewCollectionContext()

        with pytest.raises(RuntimeError, match="초기화되지 않음"):
            ctx.request("http://example.com", "app_store")

    def test_request_uses_http_client(self):
        """request가 http_client를 사용하는지 테스트"""
        ctx = ReviewCollectionContext()
        ctx.initialized = True

        # http_client 모의
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_client.request.return_value = mock_result
        ctx.http_client = mock_client

        result = ctx.request("http://example.com", "app_store", parse_json=True)

        assert result is mock_result
        mock_client.request.assert_called_once_with(
            "http://example.com", "app_store", parse_json=True
        )


class TestResetSession:
    """reset_session 메서드 테스트"""

    def test_reset_session_calls_http_client(self):
        """reset_session이 http_client.reset_all을 호출하는지 테스트"""
        ctx = ReviewCollectionContext()
        ctx.initialized = True

        mock_client = MagicMock()
        ctx.http_client = mock_client

        ctx.reset_session('app_store')

        mock_client.reset_all.assert_called_once_with('app_store')

    def test_reset_session_handles_no_client(self):
        """http_client 없이도 에러 안 나는지 테스트"""
        ctx = ReviewCollectionContext()

        # 에러 없이 실행되어야 함
        ctx.reset_session()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
