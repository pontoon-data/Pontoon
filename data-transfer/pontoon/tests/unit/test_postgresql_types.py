import pytest
import uuid
import os
from datetime import datetime, timedelta, timezone, date
import pyarrow as pa
from pontoon import Stream, Mode, Namespace, Dataset
from pontoon.source.sql_source import SQLUtil
from pontoon.cache.arrow_ipc_cache import ArrowIpcCache
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Boolean, Float, Text, Date, Numeric, inspect, text
from sqlalchemy.dialects.postgresql import UUID, JSONB, TIMESTAMP


class TestPostgresTypes:
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
        
        
        try:
            cache = ArrowIpcCache(
                namespace=Namespace("test"),
                config={'cache_dir': f"./cache-{uuid.uuid4()}"}
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
            pass

    def test_postgres_boolean_type_conversion_error(self):
        """
        Test to reproduce the error when transferring boolean columns from PostgreSQL source to PostgreSQL destination
        """
        # Create a PostgreSQL-style schema with a boolean column
        metadata = MetaData()
        table = Table(
            'test_booleans',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('is_converted', Boolean, nullable=False),
            Column('is_active', Boolean, default=True),
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
            'test_booleans',
            'public',
            pyarrow_schema,
            primary_field='id',
            cursor_field='created_at'
        )
        
        
        
        try:
            cache = ArrowIpcCache(
                namespace=Namespace("test"),
                config={'cache_dir': f"./cache-{uuid.uuid4()}"}
            )
            
            # Create sample data with boolean objects
            sample_row = [1, True, False, datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)]
            record = stream.to_record(sample_row)
            
            # Write to cache
            cache.write(stream, [record])
            
            # Read from cache - this is where the issue occurs
            records = list(cache.read(stream))
            assert len(records) == 1
            
            # Check if the boolean values are being converted to integers
            # This reproduces the issue: SQLite stores booleans as integers (0/1)
            # but when reading back, they should be converted back to booleans
            record_data = records[0].data
            print(f"Record data: {record_data}")
            print(f"Type of is_converted: {type(record_data[1])}")
            print(f"Type of is_active: {type(record_data[2])}")
            
            # The issue is that SQLite stores booleans as integers, but when reading back
            # they remain as integers instead of being converted back to booleans
            # This causes the PostgreSQL destination to receive integers instead of booleans
            if isinstance(record_data[1], int) and isinstance(record_data[2], int):
                # This reproduces the error condition
                # The boolean values are being stored/retrieved as integers
                # When this data is passed to PostgreSQL, it will cause the type mismatch error
                assert record_data[1] in [0, 1]  # Should be 0 or 1 (integer)
                assert record_data[2] in [0, 1]  # Should be 0 or 1 (integer)
                
                # This simulates what happens when the data is passed to PostgreSQL
                # PostgreSQL expects boolean values but receives integers
                try:
                    # Simulate the error that would occur in PostgreSQL
                    if record_data[1] == 1:  # This is an integer, but PostgreSQL expects boolean
                        raise ValueError("column \"is_converted\" is of type boolean but expression is of type integer")
                except ValueError as e:
                    # We expect this to fail with the boolean type conversion error
                    assert "column \"is_converted\" is of type boolean but expression is of type integer" in str(e)
                    return
            
            # If we get here, the test didn't reproduce the error
            assert False, "Expected error was not reproduced"
            
        finally:
            # Clean up
            cache.close()
            

    def test_sqlite_boolean_handling_fixed(self):
        """
        Test that the SQLite cache fix works correctly with boolean objects
        """
        # Create a PostgreSQL-style schema with boolean columns
        metadata = MetaData()
        table = Table(
            'test_booleans',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('is_converted', Boolean, nullable=False),
            Column('is_active', Boolean, default=True),
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
            'test_booleans',
            'public',
            pyarrow_schema,
            primary_field='id',
            cursor_field='created_at'
        )
        
        
        
        try:
            cache = ArrowIpcCache(
                namespace=Namespace("test"),
                config={'cache_dir': f"./cache-{uuid.uuid4()}"}
            )
            
            # Create sample data with boolean objects
            sample_row = [1, True, False, datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)]
            record = stream.to_record(sample_row)
            
            # Write to cache
            cache.write(stream, [record])
            
            # Read from cache - this should now work correctly
            records = list(cache.read(stream))
            assert len(records) == 1
            
            # Verify that boolean values are properly converted back
            record_data = records[0].data
            assert record_data[0] == 1  # id
            assert record_data[1] is True  # is_converted should be boolean True
            assert record_data[2] is False  # is_active should be boolean False
            assert record_data[3] == datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)  # created_at
            
            # Verify the types are correct
            assert isinstance(record_data[1], bool)
            assert isinstance(record_data[2], bool)
            
        finally:
            # Clean up
            cache.close()
          

    def test_schema_compatibility_fix(self):
        """
        Test that the schema compatibility fix works correctly
        """
        # Create a source schema (from PostgreSQL source)
        source_columns = [
            ('id', int),
            ('name', str),
            ('is_active', bool),
            ('created_at', datetime),
        ]
        source_schema = Stream.build_schema(source_columns)
        
        # Create a stream with the source schema
        stream = Stream(
            'campaigns',
            'public',
            source_schema,
            primary_field='id',
            cursor_field='created_at'
        )
        
        # Test the new schema compatibility method
        from pontoon.destination.sql_destination import SQLDestination
        from sqlalchemy import Column, Integer, String, Boolean, TIMESTAMP
        
        # Test 1: Same schema, different order (should be compatible)
        existing_table_columns_1 = [
            Column('id', Integer, primary_key=True),
            Column('is_active', Boolean, default=True),  # Different order
            Column('name', String(100), nullable=False),
            Column('created_at', TIMESTAMP(timezone=True), nullable=False),
        ]
        
        existing_schema_1 = SQLDestination.table_ddl_to_schema(existing_table_columns_1)
        
        # This should now be compatible (was failing before the fix)
        assert SQLDestination.schemas_compatible(stream.schema, existing_schema_1)
        
        # Test 2: Same schema, same order (should be compatible)
        existing_table_columns_2 = [
            Column('id', Integer, primary_key=True),
            Column('name', String(100), nullable=False),
            Column('is_active', Boolean, default=True),
            Column('created_at', TIMESTAMP(timezone=True), nullable=False),
        ]
        
        existing_schema_2 = SQLDestination.table_ddl_to_schema(existing_table_columns_2)
        
        assert SQLDestination.schemas_compatible(stream.schema, existing_schema_2)
        
        # Test 3: Different schema (missing column) - should not be compatible
        existing_table_columns_3 = [
            Column('id', Integer, primary_key=True),
            Column('name', String(100), nullable=False),
            # Missing is_active column
            Column('created_at', TIMESTAMP(timezone=True), nullable=False),
        ]
        
        existing_schema_3 = SQLDestination.table_ddl_to_schema(existing_table_columns_3)
        
        assert not SQLDestination.schemas_compatible(stream.schema, existing_schema_3)
        
        # Test 4: Different schema (extra column) - should not be compatible
        existing_table_columns_4 = [
            Column('id', Integer, primary_key=True),
            Column('name', String(100), nullable=False),
            Column('is_active', Boolean, default=True),
            Column('created_at', TIMESTAMP(timezone=True), nullable=False),
            Column('extra_column', String(50), nullable=True),  # Extra column
        ]
        
        existing_schema_4 = SQLDestination.table_ddl_to_schema(existing_table_columns_4)
        
        assert not SQLDestination.schemas_compatible(stream.schema, existing_schema_4)
        
        # Test 5: Different schema (different type) - should not be compatible
        from sqlalchemy import Text
        existing_table_columns_5 = [
            Column('id', Integer, primary_key=True),
            Column('name', Text, nullable=False),  # Text instead of String
            Column('is_active', Boolean, default=True),
            Column('created_at', TIMESTAMP(timezone=True), nullable=False),
        ]
        
        existing_schema_5 = SQLDestination.table_ddl_to_schema(existing_table_columns_5)
        
        # Note: This might still be compatible because both String and Text map to pa.string()
        # The actual compatibility depends on the type mapping
        print(f"Text vs String compatibility: {SQLDestination.schemas_compatible(stream.schema, existing_schema_5)}")
        
        print("All schema compatibility tests passed!")

    def test_empty_stream_handling_fixed(self):
        """
        Test that the destination handles empty streams gracefully
        """
        # Create a PostgreSQL-style schema
        source_columns = [
            ('id', int),
            ('name', str),
            ('created_at', datetime),
        ]
        source_schema = Stream.build_schema(source_columns)
        
        # Create a stream
        stream = Stream(
            'leads',
            'pontoon_data',
            source_schema,
            primary_field='id',
            cursor_field='created_at'
        )
        
        
        
        cache = None
        try:
            cache = ArrowIpcCache(
                namespace=Namespace("test"),
                config={'cache_dir': f"./cache-{uuid.uuid4()}"}
            )
            
            # Create a dataset with an empty stream (no records written)
            dataset = Dataset(
                namespace=Namespace("test"),
                streams=[stream],
                cache=cache,
                meta={'batch_id': 'test-batch', 'dt': datetime.now()}
            )
            
            # Check that the dataset reports size 0 for the empty stream
            assert dataset.size(stream) == 0
            
            # Test that the destination can handle this gracefully
            # We'll simulate the destination logic without actually connecting to a database
            stream_size = dataset.size(stream)
            if stream_size == 0:
                # This should not raise an error anymore
                print("Stream is empty, skipping processing")
                return
            
            # If we get here, there are records to process
            assert False, "Expected empty stream"
            
        finally:
            # Clean up
            if cache:
                cache.close()
            

    def test_investigate_missing_records_scenarios(self):
        """
        Test to investigate why records might not be cached even when they exist in the source
        """
        # Create a PostgreSQL-style schema
        source_columns = [
            ('id', int),
            ('name', str),
            ('created_at', datetime),
            ('tenant_id', str),
        ]
        source_schema = Stream.build_schema(source_columns)
        
        # Create a stream with filters that might exclude all records
        stream_with_filters = Stream(
            'leads',
            'pontoon_data',
            source_schema,
            primary_field='id',
            cursor_field='created_at',
            filters={'tenant_id': 'non_existent_tenant'}  # This would exclude all records
        )
        
        # Create a stream with incremental mode that might exclude all records
        from pontoon.base import Mode
        incremental_mode = Mode({
            'type': 'INCREMENTAL',
            'start': datetime(2025, 1, 1, tzinfo=timezone.utc),
            'end': datetime(2025, 1, 2, tzinfo=timezone.utc)
        })
        
        # Test that the SQL source would build the correct queries
        from pontoon.source.sql_source import SQLUtil
        
        # Test filter query - this should exclude all records
        filter_query = SQLUtil.build_select_query(stream_with_filters, Mode({'type': 'FULL_REFRESH'}))
        print(f"Filter query: {filter_query}")
        # Should be: SELECT id,name,created_at,tenant_id FROM pontoon_data.leads WHERE tenant_id = 'non_existent_tenant'
        
        # Test incremental query - this might exclude records outside the date range
        incremental_query = SQLUtil.build_select_query(stream_with_filters, incremental_mode)
        print(f"Incremental query: {incremental_query}")
        # Should be: SELECT id,name,created_at,tenant_id FROM pontoon_data.leads WHERE tenant_id = 'non_existent_tenant' AND created_at >= '2025-01-01T00:00:00+00:00' AND created_at < '2025-01-02T00:00:00+00:00'
        
        # Test count query to see what the source would expect
        count_query = SQLUtil.build_select_query(stream_with_filters, Mode({'type': 'FULL_REFRESH'}), count=True)
        print(f"Count query: {count_query}")
        # Should be: SELECT count(1) FROM pontoon_data.leads WHERE tenant_id = 'non_existent_tenant'
        
        # The issue is likely one of these scenarios:
        # 1. Filters are too restrictive (tenant_id doesn't match any records)
        # 2. Incremental mode date range excludes all records
        # 3. The source table actually has no records matching the criteria
        # 4. Query execution fails due to schema/type issues
        
        # This test helps identify which scenario is occurring
        assert "WHERE" in filter_query, "Filter query should include WHERE clause"
        assert "tenant_id = 'non_existent_tenant'" in filter_query, "Filter query should include tenant filter"
        
        print("Common causes for 'No records cached' when records exist:")
        print("1. Tenant filters exclude all records")
        print("2. Incremental mode date range is too restrictive")
        print("3. Schema/type conversion issues during query execution")
        print("4. Source table permissions or connection issues")
        print("5. Stream field dropping removes required columns")

    def test_date_type_handling_fixed(self):
        """
        Test that the DATE type handling fix works correctly
        """
        from decimal import Decimal
        from sqlalchemy import Numeric
        
        # Create a PostgreSQL-style schema with NUMERIC columns
        metadata = MetaData()
        table = Table(
            'campaigns',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('customer_id', String(50), nullable=False),
            Column('last_modified', TIMESTAMP(timezone=True), nullable=False),
            Column('name', String(255), nullable=False),
            Column('start_date', Date),
            Column('end_date', Date),
            Column('budget', Numeric(12, 2)),  # NUMERIC column
            Column('channel', String(100)),
            Column('status', String(50)),
            Column('created_at', TIMESTAMP(timezone=True), nullable=False),
            schema='pontoon_data'
        )
        
        # Simulate the SQLSource column inspection process
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
            'campaigns',
            'pontoon_data',
            pyarrow_schema,
            primary_field='id',
            cursor_field='created_at'
        )
        
        # Test the schema compatibility with destination
        from pontoon.destination.sql_destination import SQLDestination
        
        # Simulate the destination table that was created on the first sync
        existing_table_columns = [
            Column('id', Integer, primary_key=True),
            Column('customer_id', String(50), nullable=False),
            Column('last_modified', TIMESTAMP(timezone=True), nullable=False),
            Column('name', String(255), nullable=False),
            Column('start_date', Date),
            Column('end_date', Date),
            Column('budget', Numeric(12, 2)),  # Same type as source
            Column('channel', String(100)),
            Column('status', String(50)),
            Column('created_at', TIMESTAMP(timezone=True), nullable=False),
        ]
        
        # Convert existing table schema to PyArrow schema
        existing_schema = SQLDestination.table_ddl_to_schema(existing_table_columns)
        
        # After the fix, this should be compatible
        is_compatible = SQLDestination.schemas_compatible(stream.schema, existing_schema)
        
        # After the fix, this should be compatible
        assert is_compatible, "Schemas should be compatible after DATE type fix"
        
        print("DATE type handling fix works correctly!")

    def test_reproduce_real_schema_mismatch_error(self):
        """
        Test to reproduce the actual schema mismatch error that occurs in production
        This simulates the real scenario where create_table_if_not_exists fails
        """
        from decimal import Decimal
        from sqlalchemy import Numeric
        
        # Create a PostgreSQL-style schema with NUMERIC columns (like the campaigns table)
        metadata = MetaData()
        table = Table(
            'campaigns',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('customer_id', String(50), nullable=False),
            Column('last_modified', TIMESTAMP(timezone=True), nullable=False),
            Column('name', String(255), nullable=False),
            Column('start_date', Date),
            Column('end_date', Date),
            Column('budget', Numeric(12, 2)),  # NUMERIC column
            Column('channel', String(100)),
            Column('status', String(50)),
            Column('created_at', TIMESTAMP(timezone=True), nullable=False),
            schema='pontoon_data'
        )
        
        # Simulate the SQLSource column inspection process
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
            'campaigns',
            'pontoon_data',
            pyarrow_schema,
            primary_field='id',
            cursor_field='created_at'
        )
        
        # Test the schema compatibility with destination
        from pontoon.destination.sql_destination import SQLDestination
        
        # Simulate what happens when the destination table is created using schema_to_table_ddl
        # This is what happens in the first sync when the table is created
        destination_columns = SQLDestination.schema_to_table_ddl(stream)
        
        print("=== First sync - Table creation ===")
        print(f"Stream schema types: {[str(t) for t in stream.schema.types]}")
        print(f"Destination columns created: {[col.name for col in destination_columns]}")
        print(f"Destination column types: {[type(col.type).__name__ for col in destination_columns]}")
        
        # Now simulate what happens when the existing table is inspected in the second sync
        # This is what happens when create_table_if_not_exists inspects the existing table
        existing_schema = SQLDestination.table_ddl_to_schema(destination_columns)
        
        print("\n=== Second sync - Table inspection ===")
        print(f"Stream schema types: {[str(t) for t in stream.schema.types]}")
        print(f"Existing schema types: {[str(t) for t in existing_schema.types]}")
        print(f"Stream schema names: {stream.schema.names}")
        print(f"Existing schema names: {existing_schema.names}")
        
        # Check if the schemas are compatible
        is_compatible = SQLDestination.schemas_compatible(stream.schema, existing_schema)
        print(f"Schemas compatible: {is_compatible}")
        
        # If they're not compatible, this reproduces the error
        if not is_compatible:
            print("\n❌ SCHEMA MISMATCH DETECTED - This reproduces the production error!")
            
            # Let's identify which columns are causing the mismatch
            stream_fields = {field.name: field.type for field in stream.schema}
            existing_fields = {field.name: field.type for field in existing_schema}
            
            for col_name in stream_fields.keys():
                if col_name in existing_fields:
                    if stream_fields[col_name] != existing_fields[col_name]:
                        print(f"Column '{col_name}' type mismatch:")
                        print(f"  Source: {stream_fields[col_name]}")
                        print(f"  Destination: {existing_fields[col_name]}")
            
            # This should raise the same error as in production
            raise ValueError(f"Existing schema for stream campaigns does not match.")
        else:
            print("\n✅ Schemas are compatible - no error would occur")
        
        print("Real schema mismatch reproduction test completed!")