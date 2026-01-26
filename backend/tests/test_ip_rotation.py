"""
IP 로테이션 기능 테스트

IPManager의 get_next_ip_for_store() 메서드와
StoreHttpClient의 로테이션 기능을 테스트합니다.
"""
import pytest
from unittest.mock import patch, MagicMock

from core.ip_manager import IPManager, IPTestResult
from core.http_client import StoreHttpClient, HttpResult, HttpErrorCode


class TestIPManagerRotation:
    """IPManager의 IP 로테이션 기능 테스트"""

    def test_rotation_basic(self):
        """기본 라운드 로빈 로테이션 테스트 (4개 IP)"""
        manager = IPManager()
        # 수동으로 IP 목록 설정
        manager.store_ip_map = {
            'app_store': ['1.1.1.1', '2.2.2.2', '3.3.3.3', '4.4.4.4']
        }

        # 첫 번째 순환
        assert manager.get_next_ip_for_store('app_store') == '1.1.1.1'
        assert manager.get_next_ip_for_store('app_store') == '2.2.2.2'
        assert manager.get_next_ip_for_store('app_store') == '3.3.3.3'
        assert manager.get_next_ip_for_store('app_store') == '4.4.4.4'

        # 두 번째 순환 (처음으로 돌아감)
        assert manager.get_next_ip_for_store('app_store') == '1.1.1.1'
        assert manager.get_next_ip_for_store('app_store') == '2.2.2.2'

    def test_rotation_with_exclude(self):
        """제외 목록이 있을 때 로테이션 테스트"""
        manager = IPManager()
        manager.store_ip_map = {
            'app_store': ['1.1.1.1', '2.2.2.2', '3.3.3.3', '4.4.4.4']
        }

        # 2.2.2.2를 제외하면 나머지 3개만 순환
        exclude = ['2.2.2.2']

        # 인덱스 0부터 시작, exclude 제외 후 [1.1.1.1, 3.3.3.3, 4.4.4.4]
        ip1 = manager.get_next_ip_for_store('app_store', exclude=exclude)
        ip2 = manager.get_next_ip_for_store('app_store', exclude=exclude)
        ip3 = manager.get_next_ip_for_store('app_store', exclude=exclude)
        ip4 = manager.get_next_ip_for_store('app_store', exclude=exclude)  # 순환

        # 2.2.2.2가 포함되지 않아야 함
        used_ips = [ip1, ip2, ip3, ip4]
        assert '2.2.2.2' not in used_ips

    def test_rotation_single_ip(self):
        """IP가 1개일 때 로테이션 테스트"""
        manager = IPManager()
        manager.store_ip_map = {
            'app_store': ['1.1.1.1']
        }

        # 항상 같은 IP 반환
        assert manager.get_next_ip_for_store('app_store') == '1.1.1.1'
        assert manager.get_next_ip_for_store('app_store') == '1.1.1.1'
        assert manager.get_next_ip_for_store('app_store') == '1.1.1.1'

    def test_rotation_no_ip(self):
        """IP가 없을 때 None 반환"""
        manager = IPManager()
        manager.store_ip_map = {'app_store': []}

        assert manager.get_next_ip_for_store('app_store') is None

    def test_rotation_all_excluded(self):
        """모든 IP가 제외되면 None 반환"""
        manager = IPManager()
        manager.store_ip_map = {
            'app_store': ['1.1.1.1', '2.2.2.2']
        }

        exclude = ['1.1.1.1', '2.2.2.2']
        assert manager.get_next_ip_for_store('app_store', exclude=exclude) is None

    def test_rotation_reset(self):
        """로테이션 인덱스 초기화 테스트"""
        manager = IPManager()
        manager.store_ip_map = {
            'app_store': ['1.1.1.1', '2.2.2.2', '3.3.3.3']
        }

        # 몇 번 호출하여 인덱스 이동
        manager.get_next_ip_for_store('app_store')  # -> 1, index=1
        manager.get_next_ip_for_store('app_store')  # -> 2, index=2

        # 인덱스 초기화
        manager.reset_rotation('app_store')

        # 다시 첫 번째 IP부터 시작
        assert manager.get_next_ip_for_store('app_store') == '1.1.1.1'

    def test_rotation_stats(self):
        """로테이션 통계 테스트"""
        manager = IPManager()
        manager.store_ip_map = {
            'app_store': ['1.1.1.1', '2.2.2.2'],
            'play_store': ['3.3.3.3']
        }

        # 몇 번 요청
        manager.get_next_ip_for_store('app_store')
        manager.get_next_ip_for_store('app_store')
        manager.get_next_ip_for_store('play_store')

        stats = manager.get_rotation_stats()

        assert stats['app_store']['requests'] == 2
        assert stats['app_store']['total_ips'] == 2
        assert stats['play_store']['requests'] == 1
        assert stats['play_store']['total_ips'] == 1

    def test_multiple_stores_independent(self):
        """스토어별로 독립적인 로테이션 테스트"""
        manager = IPManager()
        manager.store_ip_map = {
            'app_store': ['A1', 'A2'],
            'play_store': ['P1', 'P2', 'P3']
        }

        # app_store 로테이션
        assert manager.get_next_ip_for_store('app_store') == 'A1'

        # play_store는 자체 인덱스 사용
        assert manager.get_next_ip_for_store('play_store') == 'P1'
        assert manager.get_next_ip_for_store('play_store') == 'P2'

        # app_store 다시 - 이전 인덱스 유지
        assert manager.get_next_ip_for_store('app_store') == 'A2'


class TestStoreHttpClientRotation:
    """StoreHttpClient의 로테이션 통합 테스트"""

    def test_rotation_enabled_by_default(self):
        """로테이션이 기본적으로 활성화되어 있는지 테스트"""
        ip_manager = IPManager()
        client = StoreHttpClient(ip_manager)

        assert client.use_rotation is True

    def test_rotation_can_be_disabled(self):
        """로테이션 비활성화 테스트"""
        ip_manager = IPManager()
        client = StoreHttpClient(ip_manager, use_rotation=False)

        assert client.use_rotation is False

    def test_set_rotation(self):
        """set_rotation 메서드 테스트"""
        ip_manager = IPManager()
        client = StoreHttpClient(ip_manager)

        client.set_rotation(False)
        assert client.use_rotation is False

        client.set_rotation(True)
        assert client.use_rotation is True

    def test_rotation_stats(self):
        """get_rotation_stats 메서드 테스트"""
        ip_manager = IPManager()
        ip_manager.store_ip_map = {
            'app_store': ['1.1.1.1', '2.2.2.2']
        }
        client = StoreHttpClient(ip_manager)

        # 실패한 IP 추가
        client.failed_ips['app_store'] = ['1.1.1.1']

        stats = client.get_rotation_stats()

        assert stats['rotation_enabled'] is True
        assert stats['stores']['app_store']['failed_ips'] == ['1.1.1.1']

    def test_reset_all(self):
        """reset_all 메서드 테스트"""
        ip_manager = IPManager()
        ip_manager.store_ip_map = {
            'app_store': ['1.1.1.1', '2.2.2.2']
        }
        client = StoreHttpClient(ip_manager)

        # 상태 변경
        client.failed_ips['app_store'] = ['1.1.1.1']
        ip_manager._rotation_index['app_store'] = 1

        # 전체 초기화
        client.reset_all()

        assert client.failed_ips.get('app_store', []) == []
        assert ip_manager._rotation_index.get('app_store', 0) == 0

    @patch.object(StoreHttpClient, '_do_request')
    def test_rotation_uses_different_ips(self, mock_request):
        """로테이션 시 다른 IP가 사용되는지 테스트"""
        ip_manager = IPManager()
        ip_manager.store_ip_map = {
            'app_store': ['1.1.1.1', '2.2.2.2', '3.3.3.3']
        }

        # 성공 응답 모의
        mock_request.return_value = HttpResult(
            success=True,
            data={'test': 'data'},
            used_ip=None  # 실제 호출에서 설정됨
        )

        client = StoreHttpClient(ip_manager, use_rotation=True)

        # 3번 요청
        client.request('http://example.com', 'app_store')
        client.request('http://example.com', 'app_store')
        client.request('http://example.com', 'app_store')

        # _do_request가 호출된 IP 확인
        called_ips = [call[0][1] for call in mock_request.call_args_list]

        # 각각 다른 IP로 호출되었어야 함
        assert called_ips == ['1.1.1.1', '2.2.2.2', '3.3.3.3']

    @patch.object(StoreHttpClient, '_do_request')
    def test_no_rotation_uses_same_ip(self, mock_request):
        """로테이션 비활성화 시 같은 IP 사용 테스트"""
        ip_manager = IPManager()
        ip_manager.store_ip_map = {
            'app_store': ['1.1.1.1', '2.2.2.2', '3.3.3.3']
        }

        mock_request.return_value = HttpResult(
            success=True,
            data={'test': 'data'},
            used_ip=None
        )

        client = StoreHttpClient(ip_manager, use_rotation=False)

        # 3번 요청
        client.request('http://example.com', 'app_store')
        client.request('http://example.com', 'app_store')
        client.request('http://example.com', 'app_store')

        # 모두 같은 IP로 호출
        called_ips = [call[0][1] for call in mock_request.call_args_list]

        # 모두 첫 번째 IP 사용
        assert called_ips == ['1.1.1.1', '1.1.1.1', '1.1.1.1']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
