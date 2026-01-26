"""
IPManager 테스트

IP 자동 감지 및 스토어별 할당 기능을 테스트합니다.
"""
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from core.ip_manager import (
    IPManager,
    IPTestResult,
    SourceAddressAdapter,
    TEST_ENDPOINTS,
)


class TestIPTestResult:
    """IPTestResult 데이터 클래스 테스트"""

    def test_creates_result_with_defaults(self):
        """기본값으로 결과 생성 테스트"""
        result = IPTestResult(
            ip='172.31.47.39',
            platform='app_store',
            is_working=True
        )

        assert result.ip == '172.31.47.39'
        assert result.platform == 'app_store'
        assert result.is_working is True
        assert result.status_code is None
        assert result.error is None
        assert isinstance(result.tested_at, datetime)

    def test_creates_result_with_all_fields(self):
        """모든 필드로 결과 생성 테스트"""
        result = IPTestResult(
            ip='172.31.47.39',
            platform='app_store',
            is_working=False,
            status_code=403,
            error='HTTP 403 Forbidden'
        )

        assert result.status_code == 403
        assert result.error == 'HTTP 403 Forbidden'


class TestIPManagerDiscoverIPs:
    """IPManager.discover_ips() 테스트"""

    def test_parses_hostname_output(self):
        """hostname -I 출력 파싱 테스트"""
        manager = IPManager()

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='172.31.47.39 172.31.40.115 \n'
            )

            ips = manager.discover_ips()

        assert ips == ['172.31.47.39', '172.31.40.115']
        assert manager.available_ips == ['172.31.47.39', '172.31.40.115']

    def test_excludes_localhost(self):
        """로컬호스트 IP 제외 테스트"""
        manager = IPManager()

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='127.0.0.1 172.31.47.39 127.0.1.1\n'
            )

            ips = manager.discover_ips()

        assert '127.0.0.1' not in ips
        assert '127.0.1.1' not in ips
        assert '172.31.47.39' in ips

    def test_excludes_ipv6(self):
        """IPv6 주소 제외 테스트"""
        manager = IPManager()

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='172.31.47.39 fe80::1 ::1 2001:db8::1\n'
            )

            ips = manager.discover_ips()

        # IPv4만 포함
        assert ips == ['172.31.47.39']

    def test_handles_command_failure(self):
        """명령 실패 처리 테스트"""
        manager = IPManager()

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stderr='Command failed'
            )

            ips = manager.discover_ips()

        assert ips == []

    def test_handles_timeout(self):
        """타임아웃 처리 테스트"""
        manager = IPManager()

        with patch('subprocess.run') as mock_run:
            import subprocess
            mock_run.side_effect = subprocess.TimeoutExpired('hostname', 5)

            ips = manager.discover_ips()

        assert ips == []

    def test_handles_empty_output(self):
        """빈 출력 처리 테스트"""
        manager = IPManager()

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='\n'
            )

            ips = manager.discover_ips()

        assert ips == []


class TestIPManagerValidation:
    """IPManager._is_valid_external_ipv4() 테스트"""

    def test_valid_ipv4(self):
        """유효한 IPv4 주소 테스트"""
        manager = IPManager()

        assert manager._is_valid_external_ipv4('172.31.47.39') is True
        assert manager._is_valid_external_ipv4('192.168.1.1') is True
        assert manager._is_valid_external_ipv4('10.0.0.1') is True
        assert manager._is_valid_external_ipv4('8.8.8.8') is True

    def test_invalid_localhost(self):
        """로컬호스트 제외 테스트"""
        manager = IPManager()

        assert manager._is_valid_external_ipv4('127.0.0.1') is False
        assert manager._is_valid_external_ipv4('127.0.1.1') is False
        assert manager._is_valid_external_ipv4('127.255.255.255') is False

    def test_invalid_ipv6(self):
        """IPv6 제외 테스트"""
        manager = IPManager()

        assert manager._is_valid_external_ipv4('::1') is False
        assert manager._is_valid_external_ipv4('fe80::1') is False
        assert manager._is_valid_external_ipv4('2001:db8::1') is False

    def test_invalid_format(self):
        """잘못된 형식 테스트"""
        manager = IPManager()

        assert manager._is_valid_external_ipv4('invalid') is False
        assert manager._is_valid_external_ipv4('') is False
        assert manager._is_valid_external_ipv4('192.168.1') is False
        assert manager._is_valid_external_ipv4('192.168.1.1.1') is False
        assert manager._is_valid_external_ipv4('256.1.1.1') is False


class TestIPManagerTestStore:
    """IPManager.test_ip_for_store() 테스트"""

    def test_unknown_store_returns_failure(self):
        """알 수 없는 스토어 테스트"""
        manager = IPManager()

        result = manager.test_ip_for_store('172.31.47.39', 'unknown_store')

        assert result.is_working is False
        assert '알 수 없는 스토어' in result.error

    @patch('core.ip_manager.requests.Session')
    def test_successful_request(self, mock_session_class):
        """성공적인 요청 테스트"""
        # Mock 설정
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'x' * 200  # 200바이트

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        manager = IPManager()
        result = manager.test_ip_for_store('172.31.47.39', 'app_store')

        assert result.is_working is True
        assert result.status_code == 200
        assert result.error is None

    @patch('core.ip_manager.requests.Session')
    def test_403_returns_failure(self, mock_session_class):
        """403 응답 테스트"""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.content = b'Forbidden'

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        manager = IPManager()
        result = manager.test_ip_for_store('172.31.47.39', 'app_store')

        assert result.is_working is False
        assert result.status_code == 403
        assert 'HTTP 403' in result.error

    @patch('core.ip_manager.requests.Session')
    def test_timeout_returns_failure(self, mock_session_class):
        """타임아웃 테스트"""
        import requests

        mock_session = MagicMock()
        mock_session.get.side_effect = requests.exceptions.Timeout()
        mock_session_class.return_value = mock_session

        manager = IPManager()
        result = manager.test_ip_for_store('172.31.47.39', 'app_store')

        assert result.is_working is False
        assert '타임아웃' in result.error

    @patch('core.ip_manager.requests.Session')
    def test_small_response_returns_failure(self, mock_session_class):
        """작은 응답 (100바이트 미만) 테스트"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'small'  # 5바이트

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        manager = IPManager()
        result = manager.test_ip_for_store('172.31.47.39', 'app_store')

        assert result.is_working is False


class TestIPManagerGetIP:
    """IPManager.get_ip_for_store() 테스트"""

    def test_returns_first_available_ip(self):
        """첫 번째 사용 가능한 IP 반환 테스트"""
        manager = IPManager()
        manager.store_ip_map = {
            'app_store': ['172.31.47.39', '172.31.40.115']
        }

        ip = manager.get_ip_for_store('app_store')

        assert ip == '172.31.47.39'

    def test_excludes_failed_ip(self):
        """실패한 IP 제외 테스트"""
        manager = IPManager()
        manager.store_ip_map = {
            'app_store': ['172.31.47.39', '172.31.40.115']
        }

        ip = manager.get_ip_for_store('app_store', exclude=['172.31.47.39'])

        assert ip == '172.31.40.115'

    def test_returns_none_when_all_excluded(self):
        """모든 IP 제외 시 None 반환 테스트"""
        manager = IPManager()
        manager.store_ip_map = {
            'app_store': ['172.31.47.39']
        }

        ip = manager.get_ip_for_store('app_store', exclude=['172.31.47.39'])

        assert ip is None

    def test_returns_none_for_unknown_store(self):
        """알 수 없는 스토어에 대해 None 반환 테스트"""
        manager = IPManager()
        manager.store_ip_map = {'app_store': ['172.31.47.39']}

        ip = manager.get_ip_for_store('unknown_store')

        assert ip is None


class TestIPManagerInitialize:
    """IPManager.initialize() 테스트"""

    @patch.object(IPManager, 'test_ip_for_store')
    @patch.object(IPManager, 'discover_ips')
    def test_initializes_store_ip_map(self, mock_discover, mock_test):
        """스토어 IP 맵 초기화 테스트"""
        mock_discover.return_value = ['172.31.47.39', '172.31.40.115']

        # app_store: 두 번째 IP만 동작
        # play_store: 두 IP 모두 동작
        def test_side_effect(ip, store):
            if store == 'app_store':
                return IPTestResult(
                    ip=ip, platform=store,
                    is_working=(ip == '172.31.40.115')
                )
            else:  # play_store
                return IPTestResult(
                    ip=ip, platform=store,
                    is_working=True
                )

        mock_test.side_effect = test_side_effect

        manager = IPManager()
        result = manager.initialize()

        assert result['app_store'] == ['172.31.40.115']
        assert result['play_store'] == ['172.31.47.39', '172.31.40.115']
        assert manager.last_initialized_at is not None

    @patch.object(IPManager, 'discover_ips')
    def test_returns_empty_when_no_ips(self, mock_discover):
        """IP 없을 때 빈 딕셔너리 반환 테스트"""
        mock_discover.return_value = []

        manager = IPManager()
        result = manager.initialize()

        assert result == {}


class TestIPManagerHelpers:
    """IPManager 헬퍼 메서드 테스트"""

    def test_get_all_ips_for_store_returns_copy(self):
        """목록 복사본 반환 테스트"""
        manager = IPManager()
        manager.store_ip_map = {'app_store': ['172.31.47.39']}

        ips = manager.get_all_ips_for_store('app_store')
        ips.append('172.31.40.115')  # 복사본 수정

        # 원본은 변경되지 않음
        assert manager.store_ip_map['app_store'] == ['172.31.47.39']

    def test_has_working_ip(self):
        """동작 IP 확인 테스트"""
        manager = IPManager()
        manager.store_ip_map = {
            'app_store': ['172.31.47.39'],
            'play_store': []
        }

        assert manager.has_working_ip('app_store') is True
        assert manager.has_working_ip('play_store') is False
        assert manager.has_working_ip('unknown') is False

    def test_get_test_summary(self):
        """테스트 요약 테스트"""
        manager = IPManager()
        manager.test_results = [
            IPTestResult('172.31.47.39', 'app_store', True),
            IPTestResult('172.31.40.115', 'app_store', False),
            IPTestResult('172.31.47.39', 'play_store', True),
            IPTestResult('172.31.40.115', 'play_store', True),
        ]

        summary = manager.get_test_summary()

        assert summary['app_store'] == {'success': 1, 'fail': 1}
        assert summary['play_store'] == {'success': 2, 'fail': 0}


class TestSourceAddressAdapter:
    """SourceAddressAdapter 테스트"""

    def test_stores_source_address(self):
        """소스 주소 저장 테스트"""
        adapter = SourceAddressAdapter('172.31.47.39')

        assert adapter.source_address == '172.31.47.39'

    def test_init_poolmanager_sets_source_address(self):
        """풀 매니저 소스 주소 설정 테스트"""
        adapter = SourceAddressAdapter('172.31.47.39')

        # init_poolmanager 호출 시 source_address가 kwargs에 추가되는지 확인
        with patch.object(
            adapter.__class__.__bases__[0],  # HTTPAdapter
            'init_poolmanager'
        ) as mock_init:
            adapter.init_poolmanager(connections=10, maxsize=10)

            mock_init.assert_called_once()
            call_kwargs = mock_init.call_args[1]
            assert call_kwargs['source_address'] == ('172.31.47.39', 0)


# =============================================================================
# 통합 테스트 (실제 네트워크 사용)
# =============================================================================

@pytest.mark.integration
class TestIPManagerIntegration:
    """IPManager 통합 테스트 (실제 네트워크 사용)"""

    def test_discover_real_ips(self):
        """실제 IP 감지 테스트"""
        manager = IPManager()
        ips = manager.discover_ips()

        # 최소 하나의 IP가 감지되어야 함
        assert len(ips) > 0

        # 모든 IP가 유효한 IPv4 형식이어야 함
        for ip in ips:
            assert manager._is_valid_external_ipv4(ip)

    def test_initialize_with_real_network(self):
        """실제 네트워크로 초기화 테스트"""
        manager = IPManager()
        store_ips = manager.initialize()

        # 결과가 딕셔너리여야 함
        assert isinstance(store_ips, dict)

        # 최소 하나의 IP가 감지되어야 함
        assert len(manager.available_ips) > 0

        # 테스트 결과가 기록되어야 함
        assert len(manager.test_results) > 0
