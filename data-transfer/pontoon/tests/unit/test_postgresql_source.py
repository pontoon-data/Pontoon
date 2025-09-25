import pytest
from unittest.mock import Mock, patch, MagicMock
from pontoon.source.postgresql_source import PostgreSQLSource
from pontoon.base import Namespace, Mode
from datetime import datetime, timezone


class TestPostgreSQLSource:
    """Unit tests for the refactored PostgreSQLSource implementation"""

    def test_create_engine(self):
        """Test PostgreSQL engine creation with connection string generation"""
        connect_config = {
            'auth_type': 'basic',
            'host': 'localhost',
            'port': '5432',
            'user': 'testuser',
            'password': 'testpass',
            'database': 'testdb'
        }
        
        config = {
            'connect': connect_config,
            'mode': Mock(),
            'streams': []
        }
        
        with patch('pontoon.source.postgresql_source.create_engine') as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine
            
            source = PostgreSQLSource(config, Mock(), {})
            
            # Verify create_engine was called with correct connection string
            mock_create_engine.assert_called_with(
                "postgresql+psycopg2://testuser:testpass@localhost:5432/testdb"
            )

    def test_create_engine_custom_port(self):
        """Test PostgreSQL engine creation with custom port"""
        connect_config = {
            'auth_type': 'basic',
            'host': 'localhost',
            'port': '5433',
            'user': 'testuser',
            'password': 'testpass',
            'database': 'testdb'
        }
        
        config = {
            'connect': connect_config,
            'mode': Mock(),
            'streams': []
        }
        
        with patch('pontoon.source.postgresql_source.create_engine') as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine
            
            source = PostgreSQLSource(config, Mock(), {})
            
            # Verify create_engine was called with custom port
            mock_create_engine.assert_called_with(
                "postgresql+psycopg2://testuser:testpass@localhost:5433/testdb"
            )

    def test_validate_auth_type_basic(self):
        """Test that 'basic' authentication type is accepted"""
        connect_config = {
            'auth_type': 'basic',
            'host': 'localhost',
            'user': 'testuser',
            'password': 'testpass',
            'database': 'testdb'
        }
        
        config = {
            'connect': connect_config,
            'mode': Mock(),
            'streams': []
        }
        
        with patch('pontoon.source.postgresql_source.create_engine'):
            # Should not raise an exception
            source = PostgreSQLSource(config, Mock(), {})

    def test_validate_auth_type_invalid(self):
        """Test that non-'basic' authentication types are rejected"""
        connect_config = {
            'auth_type': 'service_account',
            'host': 'localhost',
            'user': 'testuser',
            'password': 'testpass',
            'database': 'testdb'
        }
        
        config = {
            'connect': connect_config,
            'mode': Mock(),
            'streams': []
        }
        
        with patch('pontoon.source.postgresql_source.create_engine'):
            with pytest.raises(ValueError, match="PostgreSQL source only supports 'basic' authentication, got 'service_account'"):
                PostgreSQLSource(config, Mock(), {})

    def test_get_namespace(self):
        """Test namespace extraction from 'database' field"""
        connect_config = {
            'auth_type': 'basic',
            'host': 'localhost',
            'user': 'testuser',
            'password': 'testpass',
            'database': 'mydb'
        }
        
        config = {
            'connect': connect_config,
            'mode': Mock(),
            'streams': []
        }
        
        with patch('pontoon.source.postgresql_source.create_engine'):
            source = PostgreSQLSource(config, Mock(), {})
            
            # Verify namespace is extracted from database field
            assert source._namespace.name == 'mydb'

    def test_get_namespace_missing_database(self):
        """Test that missing 'database' field raises error"""
        connect_config = {
            'auth_type': 'basic',
            'host': 'localhost',
            'user': 'testuser',
            'password': 'testpass'
            # Missing 'database' field
        }
        
        config = {
            'connect': connect_config,
            'mode': Mock(),
            'streams': []
        }
        
        with patch('pontoon.source.postgresql_source.create_engine'):
            with pytest.raises(ValueError, match="PostgreSQL connection config must include 'database' field"):
                PostgreSQLSource(config, Mock(), {})

    def test_inspect_streams_impl_uses_default(self):
        """Test that PostgreSQL uses default stream inspection implementation"""
        connect_config = {
            'auth_type': 'basic',
            'host': 'localhost',
            'user': 'testuser',
            'password': 'testpass',
            'database': 'testdb'
        }
        
        config = {
            'connect': connect_config,
            'mode': Mock(),
            'streams': []
        }
        
        with patch('pontoon.source.postgresql_source.create_engine'):
            source = PostgreSQLSource(config, Mock(), {})
            
            # Mock the inspect_standard_streams method
            with patch.object(source, 'inspect_standard_streams') as mock_inspect:
                mock_inspect.return_value = [{'test': 'stream'}]
                
                result = source._inspect_streams_impl()
                
                # Verify it calls the default implementation
                mock_inspect.assert_called_once()
                assert result == [{'test': 'stream'}]


class TestPostgreSQLSourceIntegration:
    """Integration tests for PostgreSQLSource to verify it works with the full system"""

    def test_postgresql_source_instantiation(self):
        """Test that PostgreSQLSource can be instantiated with proper config"""
        connect_config = {
            'auth_type': 'basic',
            'host': 'localhost',
            'port': '5432',
            'user': 'testuser',
            'password': 'testpass',
            'database': 'testdb'
        }
        
        mode_config = {
            'type': Mode.FULL_REFRESH
        }
        
        config = {
            'connect': connect_config,
            'mode': Mode(mode_config),
            'streams': [{
                'schema': 'public',
                'table': 'test_table',
                'primary_field': 'id'
            }]
        }
        
        # Mock the SQLAlchemy engine creation
        with patch('pontoon.source.postgresql_source.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            # Mock cache implementation
            mock_cache = Mock()
            
            # Create the PostgreSQL source
            source = PostgreSQLSource(config, mock_cache, {})
            
            # Verify the source was created correctly
            assert source._namespace.name == 'testdb'
            assert source._engine == mock_engine
            
            # Verify the connection string was built correctly
            mock_create_engine.assert_called_with(
                "postgresql+psycopg2://testuser:testpass@localhost:5432/testdb"
            )

    def test_postgresql_source_test_connect(self):
        """Test that test_connect method works"""
        connect_config = {
            'auth_type': 'basic',
            'host': 'localhost',
            'port': '5432',
            'user': 'testuser',
            'password': 'testpass',
            'database': 'testdb'
        }
        
        config = {
            'connect': connect_config,
            'mode': Mock(),
            'streams': []
        }
        
        with patch('pontoon.source.postgresql_source.create_engine') as mock_create_engine:
            # Mock engine and connection
            mock_connection = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_connection
            mock_create_engine.return_value = mock_engine
            
            source = PostgreSQLSource(config, Mock(), {})
            
            # Test the connection
            result = source.test_connect()
            
            # Verify connection was attempted
            assert result == True
            mock_engine.connect.assert_called_once()

    def test_postgresql_source_inspect_streams(self):
        """Test that inspect_streams method works and uses default implementation"""
        connect_config = {
            'auth_type': 'basic',
            'host': 'localhost',
            'port': '5432',
            'user': 'testuser',
            'password': 'testpass',
            'database': 'testdb'
        }
        
        config = {
            'connect': connect_config,
            'mode': Mock(),
            'streams': []
        }
        
        with patch('pontoon.source.postgresql_source.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            source = PostgreSQLSource(config, Mock(), {})
            
            # Mock the inspect_standard_streams method
            expected_streams = [
                {
                    'schema_name': 'public',
                    'stream_name': 'users',
                    'fields': [
                        {'name': 'id', 'type': 'INTEGER'},
                        {'name': 'name', 'type': 'VARCHAR'}
                    ]
                }
            ]
            
            with patch.object(source, 'inspect_standard_streams', return_value=expected_streams):
                result = source.inspect_streams()
                
                # Verify it returns the expected streams
                assert result == expected_streams