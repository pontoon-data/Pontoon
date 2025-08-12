import os
import json
import glob
import uuid
import pytest
from datetime import datetime, timezone

from sqlalchemy import create_engine, inspect, MetaData, Table, text

from pontoon import configure_logging
from pontoon import get_source, get_destination, get_source_by_vendor, get_destination_by_vendor
from pontoon import SqliteCache
from pontoon import Progress, Mode
from pontoon import SourceConnectionFailed, SourceStreamDoesNotExist, SourceStreamInvalidSchema
from pontoon import DestinationConnectionFailed, DestinationStreamInvalidSchema

from tests.integration.common import clear_cache_files, get_memory_source, read_progress_handler, write_progress_handler

from dotenv import load_dotenv
load_dotenv()


clear_cache_files()

class TestSnowflakeConnectors:
        
    def test_snowflake_source(self):
        
        def get_test_source(mode_config={}, with_config={}, connect_config={}, streams_config={}):
            test_mode_config = {
                'type': Mode.INCREMENTAL,
                'period': Mode.DAILY,
                'start': datetime(2025, 7, 1, tzinfo=timezone.utc),
                'end': datetime(2025, 7, 2, tzinfo=timezone.utc)
            } | mode_config

            test_with_config = {
                'batch_id': True,
                'last_sync': True
            } | with_config

            test_connect_config = {
                'auth_type': 'access_token',
                'vendor_type': 'snowflake',
                'user': os.environ['SNOWFLAKE_USER'],
                'access_token': os.environ['SNOWFLAKE_ACCESS_TOKEN'],
                'account': os.environ['SNOWFLAKE_ACCOUNT'],
                'database': os.environ['SNOWFLAKE_DATABASE'],
                'warehouse': os.environ['SNOWFLAKE_WAREHOUSE']
            } | connect_config

            test_streams_config = [{
                'schema': 'pontoon',
                'table': 'leads_xs',
                'primary_field': 'id',
                'cursor_field': 'updated_at',
                'filters': {'customer_id': 'Customer1'},
                'drop_fields': ['customer_id']
            } | streams_config]

            return get_source(
                get_source_by_vendor('snowflake'),
                config = {
                    'mode': Mode(test_mode_config),
                    'with': test_with_config,
                    'connect': test_connect_config,
                    'streams': test_streams_config
                },
                cache_implementation = SqliteCache,
                cache_config = {
                    'db': f"_snowflake_{uuid.uuid4()}_cache.db",
                    'chunk_size': 1024
                }
            )
    
        # Cannot connect to source
        src = get_test_source(connect_config={'account': 'doesnotexist'})
        with pytest.raises(SourceConnectionFailed):
            src.test_connect()
        with pytest.raises(SourceConnectionFailed):
            src.read(progress_callback=read_progress_handler)
        

        # Source stream does not exist
        src = get_test_source(streams_config={'table': 'doesnotexist'})
        assert src.test_connect() == True
        with pytest.raises(SourceStreamDoesNotExist):
            src.read(progress_callback=read_progress_handler)

        # Source stream field does not exist
        src = get_test_source(streams_config={'primary_field': 'doesnotexist'})
        assert src.test_connect() == True
        with pytest.raises(SourceStreamInvalidSchema):
            src.read(progress_callback=read_progress_handler)

        # Empty record set
        src = get_test_source(mode_config={'start': datetime(2010, 1, 1, tzinfo=timezone.utc), 'end': datetime(2010, 1, 2, tzinfo=timezone.utc)})
        ds = src.read(progress_callback=read_progress_handler)
        assert ds.size(ds.streams[0]) == 0

        # Full refresh
        src = get_test_source(mode_config={'type': Mode.FULL_REFRESH})
        ds = src.read(progress_callback=read_progress_handler)
        assert ds.size(ds.streams[0]) == 329

        # Incremental
        src = get_test_source()
        ds = src.read(progress_callback=read_progress_handler)
        assert ds.size(ds.streams[0]) == 87

     
    def test_snowflake_destination(self):

        def get_test_destination(mode_config={}, connect_config={}):

            test_mode_config = {
                'type': Mode.INCREMENTAL,
                'period': Mode.DAILY,
                'start': datetime(2025, 1, 1, tzinfo=timezone.utc),
                'end': datetime(2025, 1, 2, tzinfo=timezone.utc)
            } | mode_config

            test_connect_config = {
                'auth_type': 'access_token',
                'vendor_type': 'snowflake',
                'stage_name': 'pontoon',
                'create_stage': True,
                'delete_stage': True, 
                'user': os.environ['SNOWFLAKE_USER'],
                'access_token': os.environ['SNOWFLAKE_ACCESS_TOKEN'],
                'account': os.environ['SNOWFLAKE_ACCOUNT'],
                'database': os.environ['SNOWFLAKE_DATABASE'],
                'warehouse': os.environ['SNOWFLAKE_WAREHOUSE'],
                'target_schema': 'target'
            } | connect_config

            return get_destination(
                get_destination_by_vendor('snowflake'),
                config = {
                    'mode': Mode(test_mode_config),
                    'connect': test_connect_config
                }
            )

        def connect():
            return create_engine(
                    f"snowflake://{os.environ['SNOWFLAKE_USER']}:{os.environ['SNOWFLAKE_ACCESS_TOKEN']}@"\
                    f"{os.environ['SNOWFLAKE_ACCOUNT']}/{os.environ['SNOWFLAKE_DATABASE']}?warehouse={os.environ['SNOWFLAKE_WAREHOUSE']}"
            ).connect()

        def drop():
            with connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS target.pontoon_transfer_test"))


        drop()

        # Cannot connect to destination
        dest = get_test_destination(connect_config={'account': 'doesnotexist'})
        ds = get_memory_source().read(progress_callback=read_progress_handler)
        with pytest.raises(DestinationConnectionFailed):
            dest.write(ds, progress_callback=write_progress_handler)


        # Existing destination schema is not compatible (destination changed)
        dest = get_test_destination()
        ds = get_memory_source().read(progress_callback=read_progress_handler)
        dest.write(ds, progress_callback=write_progress_handler)
        with connect() as conn:
            conn.execute(text("ALTER TABLE target.pontoon_transfer_test DROP COLUMN customer_id"))

        dest = get_test_destination()
        ds = get_memory_source().read(progress_callback=read_progress_handler)
        with pytest.raises(DestinationStreamInvalidSchema):
            dest.write(ds, progress_callback=write_progress_handler)

        drop()

        # Existing destination schema is not compatible (source changed)
        dest = get_test_destination()
        ds = get_memory_source().read(progress_callback=read_progress_handler)
        dest.write(ds, progress_callback=write_progress_handler)

        dest = get_test_destination()
        ds = get_memory_source(streams_config={'drop_fields': ['customer_id']}).read(progress_callback=read_progress_handler)
        with pytest.raises(DestinationStreamInvalidSchema):
            dest.write(ds, progress_callback=write_progress_handler)

        # Full refresh should overwrite destination with different schema
        dest = get_test_destination(mode_config={'type': Mode.FULL_REFRESH})
        ds = get_memory_source(streams_config={'drop_fields': ['customer_id']}).read(progress_callback=read_progress_handler)
        dest.write(ds, progress_callback=write_progress_handler)
        dest.integrity().check_batch_volume(ds)
        
        drop()

        # Full refresh 
        dest = get_test_destination(mode_config={'type': Mode.FULL_REFRESH})
        ds = get_memory_source(mode_config={'type': Mode.FULL_REFRESH}).read(progress_callback=read_progress_handler)
        dest.write(ds, progress_callback=write_progress_handler)
        dest.integrity().check_batch_volume(ds)

        with connect() as conn:
            count = conn.execute(text("SELECT COUNT(1) FROM target.pontoon_transfer_test")).scalar()
            assert count == 29

        # Full refresh followed by full refresh
        dest = get_test_destination(mode_config={'type': Mode.FULL_REFRESH})
        ds = get_memory_source(mode_config={'type': Mode.FULL_REFRESH}).read(progress_callback=read_progress_handler)
        dest.write(ds, progress_callback=write_progress_handler)
        dest.integrity().check_batch_volume(ds)

        with connect() as conn:
            count = conn.execute(text("SELECT COUNT(1) FROM target.pontoon_transfer_test")).scalar()
            assert count == 29


        # Full refresh, then incremental load
        dest = get_test_destination(mode_config={'start': datetime(2025, 1, 2, tzinfo=timezone.utc), 'end': datetime(2025, 1, 3, tzinfo=timezone.utc)})
        ds = get_memory_source(mode_config={'start': datetime(2025, 1, 2, tzinfo=timezone.utc), 'end': datetime(2025, 1, 3, tzinfo=timezone.utc)}).read(progress_callback=read_progress_handler)
        dest.write(ds, progress_callback=write_progress_handler)
        dest.integrity().check_batch_volume(ds)

        with connect() as conn:
            count = conn.execute(text("SELECT COUNT(1) FROM target.pontoon_transfer_test WHERE customer_id='Customer1' AND updated_at >= '2025-01-02' AND updated_at < '2025-01-03'")).scalar()
            assert count == 7

        # Incremental reload same day
        dest = get_test_destination(mode_config={'start': datetime(2025, 1, 2, tzinfo=timezone.utc), 'end': datetime(2025, 1, 3, tzinfo=timezone.utc)})
        ds = get_memory_source(mode_config={'start': datetime(2025, 1, 2, tzinfo=timezone.utc), 'end': datetime(2025, 1, 3, tzinfo=timezone.utc)}).read(progress_callback=read_progress_handler)
        dest.write(ds, progress_callback=write_progress_handler)
        dest.integrity().check_batch_volume(ds)

        with connect() as conn:
            count = conn.execute(text("SELECT COUNT(1) FROM target.pontoon_transfer_test WHERE customer_id='Customer1' AND updated_at >= '2025-01-02' AND updated_at < '2025-01-03'")).scalar()
            assert count == 7

        # Incremental load second day
        dest = get_test_destination(mode_config={'start': datetime(2025, 1, 3, tzinfo=timezone.utc), 'end': datetime(2025, 1, 4, tzinfo=timezone.utc)})
        ds = get_memory_source(mode_config={'start': datetime(2025, 1, 3, tzinfo=timezone.utc), 'end': datetime(2025, 1, 4, tzinfo=timezone.utc)}).read(progress_callback=read_progress_handler)
        dest.write(ds, progress_callback=write_progress_handler)
        dest.integrity().check_batch_volume(ds)

        with connect() as conn:
            count = conn.execute(text("SELECT COUNT(1) FROM target.pontoon_transfer_test WHERE customer_id='Customer1' AND updated_at >= '2025-01-03' AND updated_at < '2025-01-04'")).scalar()
            assert count == 7

        # Incremental load third day
        dest = get_test_destination(mode_config={'start': datetime(2025, 1, 4, tzinfo=timezone.utc), 'end': datetime(2025, 1, 5, tzinfo=timezone.utc)})
        ds = get_memory_source(mode_config={'start': datetime(2025, 1, 4, tzinfo=timezone.utc), 'end': datetime(2025, 1, 5, tzinfo=timezone.utc)}).read(progress_callback=read_progress_handler)
        dest.write(ds, progress_callback=write_progress_handler)
        dest.integrity().check_batch_volume(ds)

        with connect() as conn:
            count = conn.execute(text("SELECT COUNT(1) FROM target.pontoon_transfer_test WHERE customer_id='Customer1' AND updated_at >= '2025-01-04' AND updated_at < '2025-01-05'")).scalar()
            assert count == 6

            count = conn.execute(text("SELECT COUNT(1) FROM target.pontoon_transfer_test")).scalar()
            assert count == 29

        drop()

