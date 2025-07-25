import json
import re
from typing import List, Dict, Tuple, Generator, Any
from datetime import datetime, timezone, date
from decimal import Decimal
from sqlalchemy import create_engine, inspect, MetaData, Table, text, select, func

from pontoon import logger
from pontoon.base import Source, Namespace, Stream, Dataset, Progress, Mode


class SQLUtil:
    """ SQL manipulation and generation helpers for the SQLSource class """ 

    @staticmethod
    def to_sql_value(value:Any) -> str:
        # covert a python value to string for inclusion in SQL
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            return f"'{value.replace("'", "''")}'"  
        elif isinstance(value, (int, float, Decimal)):
            return str(value)
        elif isinstance(value, (datetime, date)):
            return f"'{value.isoformat()}'"  # ISO 8601 format
        elif isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        else:
            raise ValueError(f"to_sql_value() unsupported data type: {type(value)}")


    @staticmethod
    def safe_identifier(name: str, default=None):
        # Sanitize a name to be a safe SQL identifier (e.g., table name, schema name)
        # Returns default if name is invalid, or throws ValueError
        
        parts = name.split(".")
        safe_parts = []
        for part in parts:
            # keep letters, numbers, underscores
            cleaned = re.sub(r'\W+', '_', part)
            
            # remove leading digits (so identifier doesn't start with number)
            cleaned = re.sub(r'^[0-9]+', '', cleaned)

            # truncate to typical max identifier length
            cleaned = cleaned[:64]

            # fallback if name becomes empty or invalid
            if not cleaned or not re.match(r'^[A-Za-z_]', cleaned):
                if default is not None:
                    cleaned = default
                else:
                    raise ValueError(f"Invalid SQL identifier: {name}")
            
            safe_parts.append(cleaned)

        return ".".join(safe_parts)


    @staticmethod
    def build_select_query(stream:Stream, mode:Mode, count:bool=False) -> str:
        
        # shorthand pointers
        e = SQLUtil.to_sql_value
        s = SQLUtil.safe_identifier

        cols = ','.join([s(col) for col in stream.schema.names])
        filters = []
        where_clause = ''

        if mode.type == Mode.INCREMENTAL:
            filters.append({'col': stream.cursor_field, 'op': '>=', 'value': e(mode.start)})
            filters.append({'col': stream.cursor_field, 'op': '<', 'value': e(mode.end)})
        
        if stream.filters:
            for col, v in stream.filters.items():
                filters.append({'col': col, 'op': '=', 'value': e(v)})

        if filters:
            serial = [f"{s(f['col'])} {f['op']} {f['value']}" for f in filters]
            where_clause = f" WHERE {' AND '.join(serial)}"

        if count == True:
            func = "count(1)"
        else:
            func = f"{cols}"

        select_query = f"SELECT {func} FROM {s(stream.schema_name)}.{s(stream.name)}{where_clause}"

        return select_query


class SQLSource(Source):
    """ A Source implementation that can read from any database supported by SQLAlchemy """


    def __init__(self, config, cache_implementation, cache_config={}):
        self._config = config
        self._streams = []

        connect = config.get('connect')

        if connect.get('database'):
            self._namespace = Namespace(connect.get('database'))
        elif connect.get('project_id'):
            self._namespace = Namespace(connect.get('project_id'))
        else:
            self._namespace = Namespace('unknown')

        # our sync mode config
        self._mode = config.get('mode')
        
        # create an intermediate cache to hold records
        self._cache = cache_implementation(self._namespace, cache_config)
        
        # batch size for reading records from source
        self._chunk_size = connect.get('chunk_size', 1024)

        # time of ingestion
        self._sync_time = config.get('dt', datetime.now(timezone.utc))
        self._batch_id = str(int(self._sync_time.timestamp()*1000))

        # additional fields to augment streams with
        self._with = config.get('with', {})
 
        # configure the SQLAlchemy engine
        auth_type = connect.get('auth_type')

        if connect.get('dsn'):
            self._engine = create_engine(connect.get('dsn'))
        else:
            vendor_type = connect['vendor_type']
            if vendor_type in ['redshift', 'postgresql']:
                if auth_type != 'basic':
                    raise Exception(f'SQLSource (source-sql) does not support auth_type {auth_type} for {vendor_type}')
                self._engine = create_engine(
                    f"postgresql+psycopg2://{connect['user']}:{connect['password']}@"\
                    f"{connect['host']}:{connect['port']}/{connect['database']}"
                )
            elif vendor_type == 'bigquery':
                if auth_type != 'service_account':
                    raise Exception(f'SQLSource (source-sql) does not support auth_type {auth_type} for {vendor_type}')
                self._engine = create_engine(
                    f"bigquery://{connect['project_id']}", 
                    credentials_info=json.loads(connect['service_account']), 
                    arraysize=self._chunk_size
                )
            elif vendor_type == 'snowflake':
                if auth_type != 'access_token':
                    raise Exception(f'SQLSource (source-sql) does not support auth_type {auth_type} for {vendor_type}')
                self._engine = create_engine(
                    f"snowflake://{connect['user']}:{connect['access_token']}@"\
                    f"{connect['account']}/{connect['database']}?warehouse={connect['warehouse']}"
                )
            else:
                raise Exception(f'SQLSource (source-sql) does not support vendor_type: {vendor_type}')

    
    def _connect(self):
        return self._engine.connect()

    
    def test_connect(self):
        # connect test
        with self._connect() as conn:
            return True



    def inspect_standard_streams(self):
        ignore_schemas = ['information_schema','pg_catalog','sys','sqlite_master']

        streams = []

        with self._connect() as conn:
            # Use the inspector to get schema and table information
            inspector = inspect(conn)

            # Get all available schemas
            schemas = inspector.get_schema_names()

            # For each schema
            for schema in schemas:
                if schema in ignore_schemas:
                    continue
                
                # For each table in the schema
                for table in inspector.get_table_names(schema=schema):
                    columns = inspector.get_columns(table, schema=schema)
                    streams.append({
                        'schema_name': schema, 
                        'stream_name': table, 
                        'fields': [{'name': col['name'], 'type': str(col['type'])} for col in columns]
                    })
        
        return streams


    def inspect_bigquery_streams(self):
        
        streams = []

        with self._connect() as conn:
            # Use the inspector to get schema and table information
            inspector = inspect(conn)

            # Get all available schemas
            schemas = inspector.get_schema_names()

            for schema in schemas:
            
                # For each table in the schema
                for table in inspector.get_table_names(schema=schema):
                    _, table_name = table.split('.') 
                        
                    columns = inspector.get_columns(f"{self._config['connect']['project_id']}.{schema}.{table_name}")
                    streams.append({
                        'schema_name': schema, 
                        'stream_name': table_name, 
                        'fields': [{'name': col['name'], 'type': str(col['type'])} for col in columns]
                    })

        return streams



    def inspect_streams(self):
        # schema info reflection 
        vendor_type = self._config['connect']['vendor_type']

        if vendor_type in ['postgresql', 'redshift', 'snowflake']:
            return self.inspect_standard_streams()
        
        if vendor_type == 'bigquery':
            return self.inspect_bigquery_streams()        

        return []
        


    
    def read(self, progress_callback=None) -> Dataset:
        # Read from source and write to a cached Dataset

        with self._connect() as conn:

            # for each configured stream (i.e. table)...
            for stream_config in self._config['streams']:

                # determine the schema
                table_name = stream_config['table']
                metadata = MetaData()
                table = Table(table_name, metadata, schema=stream_config['schema'], autoload_with=conn)
                
                # table doesn't exist?
                if not table.exists(conn):
                    raise Exception(f"SQLSource (source-sql) table does not exist: {stream_config['schema']}.{table_name}")

                columns = []
                for col in table.columns:
                    try:
                        # try to use the alchemy mapped python type if available
                        columns.append((col.name, col.type.python_type))
                    except NotImplementedError:
                        columns.append((col.name, str(col.type)))

                # create a schema from the column field + types
                stream = Stream(
                    name=stream_config['table'],
                    schema_name=stream_config['schema'],
                    primary_field=stream_config.get('primary_field', None),
                    cursor_field=stream_config.get('cursor_field', None),
                    filters=stream_config.get('filters', None),
                    schema=Stream.build_schema(columns)
                )

                count_query = SQLUtil.build_select_query(stream, self._mode, count=True) 
                select_query = SQLUtil.build_select_query(stream, self._mode)

                # ignore any stream fields?
                if stream_config.get('drop_fields'):
                    for field in stream_config.get('drop_fields'):
                        stream.drop_field(field)

                # add bookkeeping columns to stream if configured
                if self._with.get('batch_id'):
                    stream.with_batch_id(self._batch_id)
                if self._with.get('checksum'):
                    stream.with_checksum()
                if self._with.get('version'):
                    stream.with_version(self._with.get('version'))
                if self._with.get('last_sync'):
                    stream.with_last_synced_at(self._sync_time)

                self._streams.append(stream)

                # configure progress tracking
                total_count = conn.execute(text(count_query)).scalar_one()
                progress = Progress(
                    f"{self._namespace}/{stream.schema_name}/{stream.name}",
                    total=total_count,
                    processed=0
                )
                if callable(progress_callback):
                    progress.subscribe(progress_callback)

                # execute our main query 
                result = conn.execution_options(
                    stream_results=True, 
                    max_row_buffer=self._chunk_size
                ).execute(
                    text(select_query)
                )

                # read the stream into cache
                batch = []
                for row in result:
                    batch.append(stream.to_record(row))
                    if(len(batch)) == self._chunk_size:
                        self._cache.write(stream, batch)
                        progress.update(self._chunk_size, increment=True)
                        batch = []

                if len(batch) > 0:
                    self._cache.write(stream, batch)
                    progress.update(len(batch), increment=True)       

        # return our dataset
        return Dataset(
            self._namespace, 
            self._streams, 
            self._cache,
            meta = {
                'batch_id': self._batch_id, 
                'dt': self._sync_time
            }
        )


    def close(self):
        self._cache.close()