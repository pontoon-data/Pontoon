import os
import json
import glob
import uuid
import pytest
from datetime import datetime, timezone

from sqlalchemy import create_engine, inspect, MetaData, Table, text

from pontoon import configure_logging
from pontoon import get_source, get_destination, get_source_by_vendor, get_destination_by_vendor
from pontoon import ArrowIpcCache
from pontoon import Progress, Mode
from pontoon import SourceConnectionFailed, SourceStreamDoesNotExist, SourceStreamInvalidSchema
from pontoon import DestinationConnectionFailed, DestinationStreamInvalidSchema

from tests.integration.common import clear_cache_files, get_memory_source, read_progress_handler, write_progress_handler

from dotenv import load_dotenv
load_dotenv()


clear_cache_files()

class TestABSConnectors:
        
    def test_abs_source(self):

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
            
            } | connect_config

            test_streams_config = [{
                'schema': 'source',
                'table': 'leads_xs',
                'primary_field': 'id',
                'cursor_field': 'updated_at',
                'filters': {'customer_id': 'Customer1'},
                'drop_fields': ['customer_id']
            } | streams_config]

            return get_source(
                get_source_by_vendor('abs'),
                config = {
                    'mode': Mode(test_mode_config),
                    'with': test_with_config,
                    'connect': test_connect_config,
                    'streams': test_streams_config
                },
                cache_implementation = ArrowIpcCache,
                cache_config = {
                    'cache_dir': f"cache-abs-{uuid.uuid4()}",
                }
            )
    
        # Azure Blob is not available as a source connector yet
        return
     
    def test_abs_destination(self):

        def get_test_destination(mode_config={}, connect_config={}):

            test_mode_config = {
                'type': Mode.INCREMENTAL,
                'period': Mode.DAILY,
                'start': datetime(2025, 1, 1, tzinfo=timezone.utc),
                'end': datetime(2025, 1, 2, tzinfo=timezone.utc)
            } | mode_config

            test_connect_config = {
                'auth_type': 'connection_string',
                'vendor_type': 'abs',
                'format': 'HIVE',
                'blob_container': os.environ['AZURE_BLOB_CONTAINER'],
                'blob_prefix': 'pontoon-hive',
                'blob_connection_string': os.environ['AZURE_CONNECTION_STRING']
            } | connect_config

            return get_destination(
                get_destination_by_vendor('abs'),
                config = {
                    'mode': Mode(test_mode_config),
                    'connect': test_connect_config
                }
            )

        def connect():
            pass

        def drop():
            pass

        
        drop()
        
        # Full refresh 
        dest = get_test_destination(mode_config={'type': Mode.FULL_REFRESH})
        ds = get_memory_source(mode_config={'type': Mode.FULL_REFRESH}).read(progress_callback=read_progress_handler)
        dest.write(ds, progress_callback=write_progress_handler)
        
        # Azure Blob integrity checker is not implemented yet
        # dest.integrity().check_batch_volume(ds)

        drop()

