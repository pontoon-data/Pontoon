from pontoon.logging_config import configure_logging, logger
from pontoon.source.sql_source import SQLSource
from pontoon.source.memory_source import MemorySource
from pontoon.destination.glue_destination import GlueDestination
from pontoon.destination.stdout_destination import StdoutDestination
from pontoon.destination.sql_destination import SQLDestination
from pontoon.destination.redshift_destination import RedshiftDestination
from pontoon.destination.bigquery_destination import BigQueryDestination
from pontoon.destination.s3_destination import S3Destination
from pontoon.destination.gcs_destination import GCSDestination
from pontoon.destination.snowflake_storage_destination import SnowflakeStorageDestination
from pontoon.destination.snowflake_destination import SnowflakeDestination
from pontoon.destination.postgres_destination import PostgresDestination
from pontoon.destination.dynamic import create_multi_destination
from pontoon.destination.integrity import Integrity


# Make these available as top level imports
from pontoon.base import Progress
from pontoon.cache.memory_cache import MemoryCache
from pontoon.cache.sqlite_cache import SqliteCache
from pontoon.base import Namespace, Stream, Record, Dataset, Cache, Mode, Source, Destination
from pontoon.base import SourceConnectionFailed, SourceStreamDoesNotExist, SourceStreamInvalidSchema
from pontoon.base import DestinationConnectionFailed, DestinationStreamInvalidSchema
from pontoon.base import StreamMissingField


__sources = {}
__destinations = {}


def get_source(name, config={}, cache_implementation=MemoryCache, cache_config={}): 
    return __sources[name](
        config, 
        cache_implementation,
        cache_config
    )


def get_destination(name, config={}):
    return __destinations[name](config)


def get_source_by_vendor(vendor_type:str) -> str:
    return __vendor_source_map[vendor_type]


def get_destination_by_vendor(vendor_type:str) -> str:
    return __vendor_destination_map[vendor_type]


# register source and destination connectors
__sources['source-sql'] = SQLSource
__sources['source-memory'] = MemorySource
__destinations['destination-sql'] = SQLDestination
__destinations['destination-glue'] = GlueDestination
__destinations['destination-stdout'] = StdoutDestination
__destinations['destination-redshift'] = RedshiftDestination
__destinations['destination-bigquery'] = BigQueryDestination
__destinations['destination-s3'] = S3Destination
__destinations['destination-gcs'] = GCSDestination
__destinations['destination-snowflake-storage'] = SnowflakeStorageDestination
__destinations['destination-snowflake'] = SnowflakeDestination
__destinations['destination-postgres'] = PostgresDestination

# multi-step / compound destinations
__destinations['destination-redshift-s3'] = create_multi_destination([S3Destination, RedshiftDestination])
__destinations['destination-glue-s3'] = create_multi_destination([S3Destination, GlueDestination])
__destinations['destination-bigquery-gcs'] = create_multi_destination([GCSDestination, BigQueryDestination])
__destinations['destination-snowflake-sms'] = create_multi_destination([SnowflakeStorageDestination, SnowflakeDestination])

# map from vendor types to source and destination connectors
__vendor_source_map = {
    'memory': 'source-memory',
    'redshift': 'source-sql',
    'snowflake': 'source-sql',
    'bigquery': 'source-sql',
    'postgresql': 'source-sql',
    'mysql': 'source-sql',
    'athena': 'source-sql'
}

__vendor_destination_map = {
    'console': 'destination-stdout',
    'redshift': 'destination-redshift-s3',
    'snowflake': 'destination-snowflake-sms',
    'bigquery': 'destination-bigquery-gcs',
    'glue': 'destination-glue-s3',
    's3': 'destination-s3',
    'gcs': 'destination-gcs',
    'postgresql': 'destination-postgres'
}


# configure package logging defaults
configure_logging()