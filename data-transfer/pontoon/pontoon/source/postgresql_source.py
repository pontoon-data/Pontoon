from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pontoon.base import Namespace
from pontoon.source.sql_source import SQLSource


class PostgreSQLSource(SQLSource):
    """PostgreSQL-specific implementation of SQLSource"""

    def _create_engine(self, connect_config: dict) -> Engine:
        """Create PostgreSQL-specific SQLAlchemy engine"""
        host = connect_config.get('host')
        port = connect_config.get('port', '5432')
        user = connect_config.get('user')
        password = connect_config.get('password')
        database = connect_config.get('database')
        
        # Build PostgreSQL connection string using psycopg2 driver
        connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        
        return create_engine(connection_string)

    def _validate_auth_type(self, auth_type: str) -> None:
        """Validate authentication type for PostgreSQL - only 'basic' is supported"""
        if auth_type != 'basic':
            raise ValueError(f"PostgreSQL source only supports 'basic' authentication, got '{auth_type}'")

    def _get_namespace(self, connect_config: dict) -> Namespace:
        """Extract namespace from PostgreSQL connection config """
        
        # Check for separate database field
        database = connect_config.get('database')
        if not database:
            raise ValueError("PostgreSQL connection config must include 'database' field")
        
        return Namespace(database)