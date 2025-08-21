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

class TestS3Connectors:
        
    def test_s3_source(self):

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
                get_source_by_vendor('s3'),
                config = {
                    'mode': Mode(test_mode_config),
                    'with': test_with_config,
                    'connect': test_connect_config,
                    'streams': test_streams_config
                },
                cache_implementation = SqliteCache,
                cache_config = {
                    'db': f"_s3_{uuid.uuid4()}_cache.db",
                    'chunk_size': 1024
                }
            )
    
        # S3 is not available as a source connector yet
        return
     
    def test_s3_destination(self):

        def get_test_destination(mode_config={}, connect_config={}):

            test_mode_config = {
                'type': Mode.INCREMENTAL,
                'period': Mode.DAILY,
                'start': datetime(2025, 1, 1, tzinfo=timezone.utc),
                'end': datetime(2025, 1, 2, tzinfo=timezone.utc)
            } | mode_config

            test_connect_config = {
                'auth_type': 'basic',
                'vendor_type': 's3',
                'format': 'HIVE',
                's3_bucket': os.environ['S3_BUCKET'],
                's3_prefix': 'pontoon-hive',
                's3_region': os.environ['AWS_DEFAULT_REGION'],
                'aws_access_key_id': os.environ['AWS_ACCESS_KEY_ID'],
                'aws_secret_access_key': os.environ['AWS_SECRET_ACCESS_KEY']
            } | connect_config

            return get_destination(
                get_destination_by_vendor('s3'),
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
        
        # S3 integrity checker is not implemented yet
        # dest.integrity().check_batch_volume(ds)

        drop()

