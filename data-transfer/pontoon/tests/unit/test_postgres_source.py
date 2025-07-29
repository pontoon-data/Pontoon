import pytest
from datetime import datetime, timedelta, timezone, date
import pyarrow as pa
from pontoon import Stream, Mode, Namespace
from pontoon.source.sql_source import SQLSource, SQLUtil
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Boolean, Float, Text, Date
from sqlalchemy.dialects.postgresql import UUID, JSONB, TIMESTAMP


class TestPostgresSource:
    """ 
    Tests specific to PostgreSQL schema handling and type conversion
    """

    def test_jsonb_type_handling(self):
        """
        Test how JSONB type is handled in the schema conversion process
        """
        # Create a simple table with JSONB to see how it's handled
        metadata = MetaData()
        table = Table(
            'test_jsonb',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('data', JSONB),
        )
        
        # Simulate the column inspection process
        columns = []
        for col in table.columns:
            try:
                # JSONB actually has a python_type attribute - it's dict
                columns.append((col.name, col.type.python_type))
            except NotImplementedError:
                # Fallback to string representation for complex types
                columns.append((col.name, str(col.type)))
        
        # JSONB has python_type of dict, which gets converted to string via PY_CONVERSION_MAP
        assert columns[1][1] == dict  # event_data column should be dict type
        
        # Test that we can build a schema with this
        pyarrow_schema = Stream.build_schema(columns)
        assert pyarrow_schema.names == ['id', 'data']
        assert pyarrow_schema.types[1] == pa.string()  # dict gets converted to string

    def test_string_type_fallback(self):
        """
        Test what happens when SQLAlchemy types don't have python_type and fall back to string
        """
        # Create a table with a type that might not have python_type
        metadata = MetaData()
        table = Table(
            'test_string_fallback',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(100)),  # This should have python_type
        )
        
        # Simulate the column inspection process
        columns = []
        for col in table.columns:
            try:
                # This should work for String type
                columns.append((col.name, col.type.python_type))
            except NotImplementedError:
                # This fallback should not be reached for String
                columns.append((col.name, str(col.type)))
        
        # Both columns should have python_type
        assert columns[0][1] == int  # id column
        assert columns[1][1] == str  # data column
        
        # Test that we can build a schema
        pyarrow_schema = Stream.build_schema(columns)
        assert pyarrow_schema.names == ['id', 'data']
        assert pyarrow_schema.types[0] == pa.int64()
        assert pyarrow_schema.types[1] == pa.string()

    def test_string_fallback_with_date(self):
        """
        Test the string fallback case with a DATE type that would return 'DATE' as string
        """
        # Manually create columns that simulate the string fallback case
        columns = [
            ('id', int),
            ('event_date', 'DATE'),  # This simulates str(col.type) for a DATE column
        ]
        
        # This should work because 'DATE' is in PY_CONVERSION_MAP
        pyarrow_schema = Stream.build_schema(columns)
        assert pyarrow_schema.names == ['id', 'event_date']
        assert pyarrow_schema.types[0] == pa.int64()
        assert pyarrow_schema.types[1] == pa.date32()  # 'DATE' maps to date32

    def test_postgres_schema_to_pyarrow(self):
        """
        Test converting a PostgreSQL schema to PyArrow schema using SQLAlchemy
        This simulates how the SQLSource would inspect a PostgreSQL table and convert its schema
        """
        
        # Create a PostgreSQL-style schema using SQLAlchemy
        metadata = MetaData()
        table = Table(
            'user_events',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer, nullable=False),
            Column('event_type', String(50), nullable=False),
            Column('event_data', JSONB),
            Column('created_at', TIMESTAMP(timezone=True), nullable=False),
            Column('updated_at', TIMESTAMP(timezone=True)),
            Column('event_date', Date, nullable=False),  # DATE column
            Column('is_active', Boolean, default=True),
            Column('score', Float),
            Column('description', Text),
            schema='analytics'
        )
        
        # Simulate the SQLSource column inspection process
        columns = []
        for col in table.columns:
            try:
                # Try to use the alchemy mapped python type if available
                columns.append((col.name, col.type.python_type))
            except NotImplementedError:
                # Fallback to string representation for complex types
                columns.append((col.name, str(col.type)))
        
        # Convert to PyArrow schema using Stream.build_schema
        pyarrow_schema = Stream.build_schema(columns)
        
        # Verify the schema structure
        expected_fields = [
            ('id', pa.int64()),
            ('user_id', pa.int64()),
            ('event_type', pa.string()),
            ('event_data', pa.string()),  # JSONB (dict) gets converted to string via PY_CONVERSION_MAP
            ('created_at', pa.timestamp('us', tz='UTC')),
            ('updated_at', pa.timestamp('us', tz='UTC')),
            ('event_date', pa.date32()),  # DATE gets converted to date32
            ('is_active', pa.bool_()),
            ('score', pa.float64()),
            ('description', pa.string())
        ]
        
        expected_schema = pa.schema(expected_fields)
        
        # Assert the schemas match
        assert pyarrow_schema.equals(expected_schema)
        
        # Test building a Stream with the converted schema
        stream = Stream(
            'user_events',
            'analytics',
            pyarrow_schema,
            primary_field='id',
            cursor_field='created_at'
        )
        
        # Verify the stream was created correctly
        assert stream.name == 'user_events'
        assert stream.schema_name == 'analytics'
        assert stream.primary_field == 'id'
        assert stream.cursor_field == 'created_at'
        assert len(stream.schema.names) == 10  # Updated to include the new DATE column
        
        # Test building a select query with the PostgreSQL schema
        now = datetime(2025, 1, 14, 18, 49, 32, 0, tzinfo=timezone.utc)
        mode = Mode({
            'type': Mode.INCREMENTAL,
            'start': now - timedelta(hours=24),
            'end': now
        })
        
        select_query = SQLUtil.build_select_query(stream, mode)
        expected_query = "SELECT id,user_id,event_type,event_data,created_at,updated_at,event_date,is_active,score,description FROM analytics.user_events WHERE created_at >= '2025-01-13T18:49:32+00:00' AND created_at < '2025-01-14T18:49:32+00:00'"
        
        assert select_query == expected_query

    def test_sqlite_date_handling_fixed(self):
        """
        Test that the SQLite cache fix works correctly with date objects
        """
        # Create a PostgreSQL-style schema with a date column
        metadata = MetaData()
        table = Table(
            'test_dates',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('event_date', Date, nullable=False),
            Column('created_at', TIMESTAMP(timezone=True), nullable=False),
        )
        
        # Simulate the column inspection process
        columns = []
        for col in table.columns:
            try:
                columns.append((col.name, col.type.python_type))
            except NotImplementedError:
                columns.append((col.name, str(col.type)))
        
        # Convert to PyArrow schema
        pyarrow_schema = Stream.build_schema(columns)
        
        # Create a stream
        stream = Stream(
            'test_dates',
            'public',
            pyarrow_schema,
            primary_field='id',
            cursor_field='created_at'
        )
        
        # Test that the SQLite cache can handle date objects correctly
        from pontoon.cache.sqlite_cache import SqliteCache
        import tempfile
        import os
        
        # Create a temporary SQLite cache
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        
        try:
            cache = SqliteCache(
                namespace=Namespace("test"),
                config={'db': temp_db.name, 'chunk_size': 1000}
            )
            
            # Create sample data with date objects
            sample_row = [1, date(2025, 1, 15), datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)]
            record = stream.to_record(sample_row)
            
            # This should work without errors now
            cache.write(stream, [record])
            
            # Verify we can read it back
            records = list(cache.read(stream))
            assert len(records) == 1
            assert records[0].data[0] == 1  # id
            assert records[0].data[1] == date(2025, 1, 15)  # event_date
            assert records[0].data[2] == datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)  # created_at
            
            # Close the cache properly
            cache.close()
            
        finally:
            # Clean up
            if os.path.exists(temp_db.name):
                os.unlink(temp_db.name)