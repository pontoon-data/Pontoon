import os
import json
import glob
import psutil
from datetime import datetime, timezone
from pontoon import configure_logging
from pontoon import get_source, get_destination, get_source_by_vendor, get_destination_by_vendor
from pontoon import SqliteCache
from pontoon import Progress, Mode

from dotenv import load_dotenv


load_dotenv()

for f in glob.glob("*_cache.db"):
    os.remove(f)


def read_progress_handler(progress:Progress):
    if progress.total_records > 0:
        print(f"Source read complete: {progress.total_records}")
    else:
        if progress.processed % 100000 == 0:
            print(f"Reading source: read={progress.processed})")


def write_progress_handler(progress:Progress):
    if progress.total_records > 0:
        print(f"Destination write complete: {progress.total_records}")
    else:
        if progress.processed % 100000 == 0:
            print(f"Writing destination: wrote={progress.processed})")


def get_memory_source():
    return get_source(
        get_source_by_vendor('memory'),
        config={
            'mode': Mode({
                'type': Mode.INCREMENTAL,
                'period': Mode.DAILY,
                'start': datetime(2025, 1, 1, tzinfo=timezone.utc),
                'end': datetime(2025, 1, 2, tzinfo=timezone.utc)
            })
        }
    )


mode_config = {
    'type': Mode.INCREMENTAL,
    'period': Mode.DAILY,
    'start': datetime(2025, 7, 1, tzinfo=timezone.utc),
    'end': datetime(2025, 7, 2, tzinfo=timezone.utc)
}

with_config = {
    'checksum': True,
    'batch_id': True,
    'version': '1.0.0',
    'last_sync': True
}


console_dest = get_destination(
    get_destination_by_vendor('console'),
    config = {
        'connect': {
            'limit': 3
        }
    }
)


postgresql_src = get_source(
    get_source_by_vendor('postgresql'),
    config = {
        'mode': Mode(mode_config),
        'connect': {
            'auth_type': 'basic',
            'vendor_type': 'postgresql',
            'host': os.environ['POSTGRES_HOST'],
            'port': '5432',
            'user': os.environ['POSTGRES_USER'],
            'password': os.environ['POSTGRES_PASSWORD'],
            'database': os.environ['POSTGRES_DATABASE']
        },
        'streams': [{
            'schema': 'source',
            'table': 'leads_xs',
            'primary_field': 'id',
            'cursor_field': 'updated_at',
            'filters': {'customer_id': 'Customer1'},
            'drop_fields': ['customer_id']
        }]
    },
    cache_implementation = SqliteCache,
    cache_config = {
        'db': '_postgresql_cache.db',
        'chunk_size': 1024
    }
)

redshift_src = get_source(
    get_source_by_vendor('redshift'),
    config = {
        'mode': Mode(mode_config),
        'connect': {
            'auth_type': 'basic',
            'vendor_type': 'redshift',
            'host': os.environ['REDSHIFT_HOST'],
            'port': '5439',
            'user': os.environ['REDSHIFT_USER'],
            'password': os.environ['REDSHIFT_PASSWORD'],
            'database': os.environ['REDSHIFT_DATABASE']
        },
        'streams': [{
            'schema': 'source',
            'table': 'leads_xs',
            'primary_field': 'id',
            'cursor_field': 'updated_at',
            'filters': {'customer_id': 'Customer1'},
            'drop_fields': ['customer_id']
        }]
    },
    cache_implementation = SqliteCache,
    cache_config = {
        'db': '_redshift_cache.db',
        'chunk_size': 1024
    }
)

bigquery_src = get_source(
    get_source_by_vendor('bigquery'),
    config = {
        'mode': Mode(mode_config),
        'connect': {
            'auth_type': 'service_account',
            'vendor_type': 'bigquery',
            'project_id': os.environ['BQ_PROJECT_ID'],
            'service_account': open(os.environ['GCP_SERVICE_ACCOUNT_FILE']).read()
        },
        'streams': [{
            'schema': 'pontoon',
            'table': 'leads_xs',
            'primary_field': 'id',
            'cursor_field': 'updated_at',
            'filters': {'customer_id': 'Customer1'},
            'drop_fields': ['customer_id']
        }]
    },
    cache_implementation = SqliteCache,
    cache_config = {
        'db': '_bigquery_cache.db',
        'chunk_size': 1024
    }
)

snowflake_src = get_source(
    get_source_by_vendor('snowflake'),
    config = {
        'mode': Mode(mode_config),
        'connect': {
            'auth_type': 'access_token',
            'vendor_type': 'snowflake',
            'user': os.environ['SNOWFLAKE_USER'],
            'access_token': os.environ['SNOWFLAKE_ACCESS_TOKEN'],
            'account': os.environ['SNOWFLAKE_ACCOUNT'],
            'database': os.environ['SNOWFLAKE_DATABASE'],
            'warehouse': os.environ['SNOWFLAKE_WAREHOUSE']
        },
        'streams': [{
            'schema': 'pontoon',
            'table': 'leads_xs',
            'primary_field': 'id',
            'cursor_field': 'updated_at',
            'filters': {'customer_id': 'Customer1'},
            'drop_fields': ['customer_id']
        }]
    },
    cache_implementation = SqliteCache,
    cache_config = {
        'db': '_snowflake_cache.db',
        'chunk_size': 1024
    }
)


###
### ======================================================================
###


postgresql_dest = get_destination(
    get_destination_by_vendor('postgresql'),
    config = {
        'mode': Mode(mode_config),
        'connect': {
            'auth_type': 'basic',
            'vendor_type': 'postgresql',
            'host': os.environ['POSTGRES_HOST'],
            'port': '5432',
            'user': os.environ['POSTGRES_USER'],
            'password': os.environ['POSTGRES_PASSWORD'],
            'database': os.environ['POSTGRES_DATABASE'],
            'target_schema': 'target'
        }
    }
)


redshift_dest = get_destination(
    get_destination_by_vendor('redshift'),
    config = {
        'mode': Mode(mode_config),
        'connect': {
            'auth_type': 'basic',
            'vendor_type': 'redshift',
            's3_bucket': os.environ['S3_BUCKET'],
            's3_prefix': 'pontoon',
            's3_region': os.environ['AWS_DEFAULT_REGION'],
            'iam_role': os.environ['REDSHIFT_IAM_ROLE'],
            'aws_access_key_id': os.environ['AWS_ACCESS_KEY_ID'],
            'aws_secret_access_key': os.environ['AWS_SECRET_ACCESS_KEY'],
            'host': os.environ['REDSHIFT_HOST'],
            'user': os.environ['REDSHIFT_USER'],
            'password': os.environ['REDSHIFT_PASSWORD'],
            'database': os.environ['REDSHIFT_DATABASE'],
            'port': '5439',
            'target_schema': 'target'
        }
    }
)

bigquery_dest = get_destination(
    get_destination_by_vendor('bigquery'),
    config = {
        'mode': Mode(mode_config),
        'connect': {
            'auth_type': 'service_account',
            'vendor_type': 'bigquery',
            'gcs_bucket_name': os.environ['GCS_BUCKET'],
            'gcs_bucket_path': 'pontoon',
            'service_account': json.loads(open(os.environ['GCP_SERVICE_ACCOUNT_FILE']).read()),
            'target_schema': 'target'
        }
    }
)


snowflake_dest = get_destination(
    get_destination_by_vendor('snowflake'),
    config = {
        'mode': Mode(mode_config),
        'connect': {
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
        }
    }
)


###
### ======================================================================
###


class TestCoreConnectors:

    def test_memory_source(self):
        ds = get_memory_source().read(progress_callback=read_progress_handler)
        console_dest.write(ds, progress_callback=write_progress_handler)
        
    def test_postgres_source(self):
        ds = postgresql_src.read(progress_callback=read_progress_handler)
        console_dest.write(ds, progress_callback=write_progress_handler)
    
    def test_redshift_source(self):
        ds = redshift_src.read(progress_callback=read_progress_handler)
        console_dest.write(ds, progress_callback=write_progress_handler)
    
    def test_bigquery_source(self):
        ds = bigquery_src.read(progress_callback=read_progress_handler)
        console_dest.write(ds, progress_callback=write_progress_handler)
    
    def test_snowflake_source(self):
        ds = snowflake_src.read(progress_callback=read_progress_handler)
        console_dest.write(ds, progress_callback=write_progress_handler)
    
    def test_postgres_destination(self):
        ds = get_memory_source().read(progress_callback=read_progress_handler)
        postgresql_dest.write(ds, progress_callback=write_progress_handler)
    
    def test_redshift_destination(self):
        ds = get_memory_source().read(progress_callback=read_progress_handler)
        redshift_dest.write(ds, progress_callback=write_progress_handler)
    
    def test_bigquery_destination(self):
        ds = get_memory_source().read(progress_callback=read_progress_handler)
        bigquery_dest.write(ds, progress_callback=write_progress_handler)
    
    def test_snowflake_destination(self):
        ds = get_memory_source().read(progress_callback=read_progress_handler)
        snowflake_dest.write(ds, progress_callback=write_progress_handler)


