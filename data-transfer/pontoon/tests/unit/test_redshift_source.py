import pytest
from unittest.mock import Mock, patch
from pontoon.source.redshift_source import RedshiftSource
from pontoon.base import Namespace


class TestRedshiftSource:
    """Test RedshiftSource implementation"""

    def test_inheritance_from_postgresql_source(self):
        """Test that RedshiftSource properly inherits from PostgreSQLSource"""
        from pontoon.source.postgresql_source import PostgreSQLSource
        
        # Verify inheritance
        assert issubclass(RedshiftSource, PostgreSQLSource)

    def test_validate_auth_type_basic_success(self):
        """Test that 'basic' authentication type is accepted"""
        config = {
            'connect': {
                'host': 'test-cluster.redshift.amazonaws.com',
                'port': '5439',
                'user': 'testuser',
                'password': 'testpass',
                'database': 'testdb',
                'auth_type': 'basic'
            },
            'mode': Mock(),
            'streams': []
        }
        
        with patch('pontoon.source.postgresql_source.create_engine'):
            source = RedshiftSource(config, Mock(), {})
            # If we get here without exception, auth validation passed
            assert source is not None

    def test_validate_auth_type_invalid_fails(self):
        """Test that invalid authentication types are rejected"""
        config = {
            'connect': {
                'host': 'test-cluster.redshift.amazonaws.com',
                'port': '5439',
                'user': 'testuser',
                'password': 'testpass',
                'database': 'testdb',
                'auth_type': 'service_account'
            },
            'mode': Mock(),
            'streams': []
        }
        
        with patch('pontoon.source.postgresql_source.create_engine'):
            with pytest.raises(ValueError, match="Redshift source only supports 'basic' authentication"):
                RedshiftSource(config, Mock(), {})

    @patch('pontoon.source.postgresql_source.create_engine')
    def test_create_engine_inherits_postgresql_behavior(self, mock_create_engine):
        """Test that engine creation inherits PostgreSQL behavior"""
        config = {
            'connect': {
                'host': 'test-cluster.redshift.amazonaws.com',
                'port': '5439',
                'user': 'testuser',
                'password': 'testpass',
                'database': 'testdb',
                'auth_type': 'basic'
            },
            'mode': Mock(),
            'streams': []
        }
        
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        source = RedshiftSource(config, Mock(), {})
        
        # Verify create_engine was called with PostgreSQL connection string
        mock_create_engine.assert_called_once_with(
            "postgresql+psycopg2://testuser:testpass@test-cluster.redshift.amazonaws.com:5439/testdb"
        )

    @patch('pontoon.source.postgresql_source.create_engine')
    def test_get_namespace_inherits_postgresql_behavior(self, mock_create_engine):
        """Test that namespace extraction inherits PostgreSQL behavior"""
        config = {
            'connect': {
                'host': 'test-cluster.redshift.amazonaws.com',
                'port': '5439',
                'user': 'testuser',
                'password': 'testpass',
                'database': 'testdb',
                'auth_type': 'basic'
            },
            'mode': Mock(),
            'streams': []
        }
        
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        source = RedshiftSource(config, Mock(), {})
        
        # Verify namespace is extracted from database field
        assert str(source._namespace) == str(Namespace('testdb'))

    def test_redshift_specific_error_message(self):
        """Test that error messages are Redshift-specific"""
        config = {
            'connect': {
                'host': 'test-cluster.redshift.amazonaws.com',
                'port': '5439',
                'user': 'testuser',
                'password': 'testpass',
                'database': 'testdb',
                'auth_type': 'oauth'
            },
            'mode': Mock(),
            'streams': []
        }
        
        with patch('pontoon.source.postgresql_source.create_engine'):
            with pytest.raises(ValueError) as exc_info:
                RedshiftSource(config, Mock(), {})
            
            # Verify the error message mentions Redshift specifically
            assert "Redshift source only supports 'basic' authentication" in str(exc_info.value)
            assert "got 'oauth'" in str(exc_info.value)