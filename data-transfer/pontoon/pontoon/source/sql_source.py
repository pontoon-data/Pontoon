import json
from typing import List, Dict, Tuple, Generator, Any
from datetime import datetime, timezone, date
from decimal import Decimal
from sqlalchemy import create_engine, inspect, MetaData, Table, text

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
    def build_select_query(stream:Stream, mode:Mode) -> str:
        # note on sql injection / validation:
        #     - all of the schema, table and column names used here are validated against the db already
        #     - column values are sanitized 
        
        cols = ','.join(stream.schema.names)
        filters = []
        where_clause = ''

        # shorthand pointer
        e = SQLUtil.to_sql_value

        if mode.type == Mode.INCREMENTAL:
            filters.append({'col': stream.cursor_field, 'op': '>=', 'value': e(mode.start)})
            filters.append({'col': stream.cursor_field, 'op': '<', 'value': e(mode.end)})
        
        if stream.filters:
            for col, v in stream.filters.items():
                filters.append({'col': col, 'op': '=', 'value': e(v)})

        if filters:
            serial = [f"{f['col']} {f['op']} {f['value']}" for f in filters]
            where_clause = f" WHERE {' AND '.join(serial)}"

        select_query = f"SELECT {cols} FROM {stream.schema_name}.{stream.name}{where_clause}"

        return select_query



class SQLSource(Source):
    """ A Source implementation that can read from any database supported by SQLAlchemy """


    def __init__(self, config, cache_implementation, cache_config={}):
        self._config = config
        self._streams = []
        self._progress_callback = None

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
                if auth_type != 'basic':
                    raise Exception(f'SQLSource (source-sql) does not support auth_type {auth_type} for {vendor_type}')
                self._engine = create_engine(
                    f"snowflake://{connect['user']}:{connect['password']}@"\
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



    def inspect_postgresql_streams(self):
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

        if vendor_type in ['postgresql', 'redshift']:
            return self.inspect_postgresql_streams()
        
        if vendor_type == 'bigquery':
            return self.inspect_bigquery_streams()

        return []
        


    
    def read(self, progress_callback=None) -> Dataset:
        # Read from source and write to a cached Dataset
        
        if callable(progress_callback):
            self._progress_callback = progress_callback
        else:
            self._progress_callback = lambda *args, **kwargs: None

        total_records = 0

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

                columns = [(col.name, col.type.python_type) for col in table.columns]
                
                # create a schema from the column field + types
                stream = Stream(
                    name=stream_config['table'],
                    schema_name=stream_config['schema'],
                    primary_field=stream_config.get('primary_field', None),
                    cursor_field=stream_config.get('cursor_field', None),
                    filters=stream_config.get('filters', None),
                    schema=Stream.build_schema(columns)
                )

                # likely a separate config block with boolean include options
                self._streams.append(stream)

                # read the stream into cache
                query = SQLUtil.build_select_query(stream, self._mode)
                result = conn.execution_options(
                    stream_results=True, 
                    max_row_buffer=self._chunk_size
                ).execute(
                    text(query)
                )

                # ignore any stream fields?
                if stream_config.get('drop_fields'):
                    for field in stream_config.get('drop_fields'):
                        print(f'dropping field {field}')
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

                batch = []
                for row in result:
                    batch.append(stream.to_record(row))
                    total_records += 1
                    if(len(batch)) == self._chunk_size:
                        self._cache.write(stream, batch)
                        self._progress_callback(Progress(-1, total_records))
                        batch = []

                if len(batch) > 0:
                    self._cache.write(stream, batch)
                    self._progress_callback(Progress(-1, total_records))        

        # final progress update
        self._progress_callback(Progress(total_records, 0))

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