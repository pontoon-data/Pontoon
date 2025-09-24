import json
import re
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Generator, Any
from datetime import datetime, timezone, date
from decimal import Decimal
from sqlalchemy import create_engine, inspect, MetaData, Table, text, select, func
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError, InterfaceError, DatabaseError, NoSuchTableError

from pontoon import logger
from pontoon.base import Source, Namespace, Stream, Dataset, Progress, Mode
from pontoon.base import StreamMissingField
from pontoon.base import SourceConnectionFailed, \
                        SourceStreamDoesNotExist, \
                        SourceStreamInvalidSchema


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


class SQLSource(Source, ABC):
    """ Abstract base class for SQL source implementations """

    def __init__(self, config, cache_implementation, cache_config={}):
        self._config = config
        self._streams = []

        connect = config.get('connect')

        # Extract namespace using database-specific logic
        self._namespace = self._get_namespace(connect)

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
 
        # Validate authentication type for this database
        auth_type = connect.get('auth_type')
        self._validate_auth_type(auth_type)

        # Create database-specific engine
        self._engine = self._create_engine(connect)

    @abstractmethod
    def _create_engine(self, connect_config: dict) -> Engine:
        """Create database-specific SQLAlchemy engine"""
        pass

    @abstractmethod
    def _validate_auth_type(self, auth_type: str) -> None:
        """Validate authentication type for this database"""
        pass

    @abstractmethod
    def _get_namespace(self, connect_config: dict) -> Namespace:
        """Extract namespace from connection config"""
        pass

    def _inspect_streams_impl(self) -> List[dict]:
        """Database-specific stream inspection - default implementation"""
        return self.inspect_standard_streams()

    def _connect(self):
        try:
            return self._engine.connect()
        except (OperationalError, InterfaceError, DatabaseError) as e:
            raise SourceConnectionFailed("Failed to connect to source database") from e

    def test_connect(self):
        """Test database connection using template method pattern"""
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


    def inspect_streams(self):
        """Inspect available streams using database-specific implementation"""
        return self._inspect_streams_impl()
        


    def read(self, progress_callback=None) -> Dataset:
        """Read from source and write to a cached Dataset using template method pattern"""

        with self._connect() as conn:

            # for each configured stream (i.e. table)...
            for stream_config in self._config['streams']:

                # determine the schema
                table_name = stream_config['table']
                metadata = MetaData()

                try:
                    # table doesn't exist?
                    table = Table(table_name, metadata, schema=stream_config['schema'], autoload_with=conn)
                except NoSuchTableError as e:
                    raise SourceStreamDoesNotExist(f"SQLSource (source-sql) table does not exist: {stream_config['schema']}.{table_name}") from e
                
                columns = []
                for col in table.columns:
                    try:
                        # try to use the alchemy mapped python type if available
                        columns.append((col.name, col.type.python_type))
                    except NotImplementedError:
                        columns.append((col.name, str(col.type)))

                # create a schema from the column field + types
                try:
                    stream = Stream(
                        name=stream_config['table'],
                        schema_name=stream_config['schema'],
                        primary_field=stream_config.get('primary_field', None),
                        cursor_field=stream_config.get('cursor_field', None),
                        filters=stream_config.get('filters', None),
                        schema=Stream.build_schema(columns)
                    )
                except StreamMissingField as e:
                    raise SourceStreamInvalidSchema(e) from e

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
                    f"source+sql://{self._namespace}/{stream.schema_name}/{stream.name}",
                    total=total_count,
                    processed=0
                )
                if callable(progress_callback):
                    progress.subscribe(progress_callback)

                if total_count == 0:
                    progress.message("No records to process")
                    continue

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

                # close the server side cursor
                result.close()

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
        """Close the cache - common cleanup logic"""
        self._cache.close()