import json
from typing import List
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from pontoon.base import Namespace
from pontoon.source.sql_source import SQLSource


class BigQuerySource(SQLSource):
    """BigQuery-specific implementation of SQLSource"""

    def _create_engine(self, connect_config: dict) -> Engine:
        """Create BigQuery-specific SQLAlchemy engine with service account authentication"""
        project_id = connect_config.get('project_id')
        service_account = connect_config.get('service_account')
        
        if not project_id:
            raise ValueError("BigQuery connection config must include 'project_id' field")
        
        if not service_account:
            raise ValueError("BigQuery connection config must include 'service_account' field")
        
        # Parse service account JSON
        try:
            credentials_info = json.loads(service_account)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid service account JSON: {e}")
        
        # Build BigQuery connection string and create engine
        connection_string = f"bigquery://{project_id}"
        
        # Get chunk size from connect config, defaulting to 1024 if not specified
        chunk_size = connect_config.get('chunk_size', 1024)
        
        return create_engine(
            connection_string,
            credentials_info=credentials_info,
            arraysize=chunk_size
        )

    def _validate_auth_type(self, auth_type: str) -> None:
        """Validate authentication type for BigQuery - only 'service_account' is supported"""
        if auth_type != 'service_account':
            raise ValueError(f"BigQuery source only supports 'service_account' authentication, got '{auth_type}'")

    def _get_namespace(self, connect_config: dict) -> Namespace:
        """Extract namespace from BigQuery connection config using project_id"""
        project_id = connect_config.get('project_id')
        if not project_id:
            raise ValueError("BigQuery connection config must include 'project_id' field")
        
        return Namespace(project_id)

    def _inspect_streams_impl(self) -> List[dict]:
        """BigQuery-specific stream inspection logic"""
        streams = []

        with self._connect() as conn:
            # Use the inspector to get schema and table information
            inspector = inspect(conn)

            # Get all available schemas (datasets in BigQuery terminology)
            schemas = inspector.get_schema_names()

            for schema in schemas:
                # For each table in the schema
                for table in inspector.get_table_names(schema=schema):
                    # BigQuery table names come in format "project.dataset.table"
                    # We need to extract just the table name
                    if '.' in table:
                        _, table_name = table.split('.', 1)
                        if '.' in table_name:
                            # Handle case where table is "dataset.table"
                            _, table_name = table_name.split('.', 1)
                    else:
                        table_name = table
                    
                    # Get column information using the full table reference
                    project_id = self._config['connect']['project_id']
                    full_table_name = f"{project_id}.{schema}.{table_name}"
                    
                    try:
                        columns = inspector.get_columns(full_table_name)
                        streams.append({
                            'schema_name': schema,
                            'stream_name': table_name,
                            'fields': [{'name': col['name'], 'type': str(col['type'])} for col in columns]
                        })
                    except Exception:
                        # Skip tables that can't be inspected (e.g., views, external tables)
                        continue

        return streams