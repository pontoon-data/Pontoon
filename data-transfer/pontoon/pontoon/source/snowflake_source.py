from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pontoon.base import Namespace
from pontoon.source.sql_source import SQLSource


class SnowflakeSource(SQLSource):
    """Snowflake-specific implementation of SQLSource"""

    def _create_engine(self, connect_config: dict) -> Engine:
        """Create Snowflake-specific SQLAlchemy engine"""
        user = connect_config.get('user')
        access_token = connect_config.get('access_token')
        account = connect_config.get('account')
        database = connect_config.get('database')
        warehouse = connect_config.get('warehouse')
        
        # Validate required fields
        if not user:
            raise ValueError("Snowflake connection config must include 'user' field")
        if not access_token:
            raise ValueError("Snowflake connection config must include 'access_token' field")
        if not account:
            raise ValueError("Snowflake connection config must include 'account' field")
        if not database:
            raise ValueError("Snowflake connection config must include 'database' field")
        if not warehouse:
            raise ValueError("Snowflake connection config must include 'warehouse' field")
        
        # Build Snowflake connection string
        # Format: snowflake://user:access_token@account/database/schema?warehouse=warehouse
        # Note: We'll use 'public' as default schema since it's commonly available
        schema = connect_config.get('schema', 'public')
        connection_string = f"snowflake://{user}:{access_token}@{account}/{database}/{schema}?warehouse={warehouse}"
        
        return create_engine(connection_string)

    def _validate_auth_type(self, auth_type: str) -> None:
        """Validate authentication type for Snowflake - only 'access_token' is supported"""
        if auth_type != 'access_token':
            raise ValueError(f"Snowflake source only supports 'access_token' authentication, got '{auth_type}'")

    def _get_namespace(self, connect_config: dict) -> Namespace:
        """Extract namespace from Snowflake connection config using database field"""
        database = connect_config.get('database')
        if not database:
            raise ValueError("Snowflake connection config must include 'database' field")
        
        return Namespace(database)