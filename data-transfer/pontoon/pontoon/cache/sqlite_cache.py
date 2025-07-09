import sqlite3
import pyarrow as pa
from datetime import datetime
from typing import List, Dict, Tuple, Generator, Any
from pontoon.base import Cache, Namespace, Stream, Record


def adapt_datetime_iso(val):
    return val.isoformat()

def convert_datetime(val):
    return datetime.fromisoformat(val.decode())


sqlite3.register_adapter(datetime, adapt_datetime_iso)
sqlite3.register_converter("datetime", convert_datetime)


class SqliteCache(Cache):
    """ A Cache implementation backed by Sqlite DB for memory efficiency """
    

    def __init__(self, namespace:Namespace, config:Dict[str,Any]={}):
        self._namespace = namespace
        self._config = config
        self._conn = sqlite3.connect(config['db'], detect_types=sqlite3.PARSE_DECLTYPES)
        self._cursor = self._conn.cursor()
        self._chunk_size = config.get('chunk_size', 1000)
        
        self._cursor.execute("PRAGMA synchronous = OFF")
        self._cursor.execute("PRAGMA journal_mode = MEMORY")

        # track which streams we've created tables for
        # faster than asking sqlite on every read/write
        self._stream_tables = {}
    

    def _arrow_to_sqlite_type(arrow_type):
        # maps an arrow data type to a SQLite data type
        if pa.types.is_integer(arrow_type):
            return "INTEGER"
        elif pa.types.is_floating(arrow_type):
            return "REAL"
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "TEXT"
        elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return "BLOB"
        elif pa.types.is_boolean(arrow_type):
            return "INTEGER"
        elif pa.types.is_date(arrow_type) or pa.types.is_timestamp(arrow_type):
            return "datetime"
        elif pa.types.is_decimal(arrow_type):
            return "TEXT"
        elif pa.types.is_list(arrow_type) or pa.types.is_struct(arrow_type):
            return "TEXT"
        elif pa.types.is_null(arrow_type):
            return "NULL"
        else:
            raise ValueError(f"Unsupported Arrow type: {arrow_type}")

    
    def _stream_table_name(self, stream):
        return f"{stream.schema_name}__{stream.name}"
    

    def _create_stream_table(self, stream:Stream):
        # create table dynamically based on stream schema

        table_name = self._stream_table_name(stream)

        columns = ", ".join(f"{col} {SqliteCache._arrow_to_sqlite_type(dtype)}" 
                            for col, dtype in zip(stream.schema.names, stream.schema.types))
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
        self._cursor.execute(create_table_query)
        self._conn.commit()
        
        # save so we have an efficient way to check if it exists
        self._stream_tables[table_name] = True

    
    def _insert_rows_to_stream(self, stream:Stream, records:List[Record]):
        # batch insert rows to a stream table

        table_name = self._stream_table_name(stream)
        
        # insert rows in batch mode with executemany
        placeholders = ", ".join("?" for _ in stream.schema.names)
        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        self._cursor.executemany(insert_query, [tuple(record.data) for record in records])

    
    def _rows_to_records(self, stream:Stream, rows):
        # covert a sqlite row back into a record      
        return [Record(list(row)) for row in rows]

    
    def write(self, stream:Stream, records:List[Record]):
        if self._stream_table_name(stream) not in self._stream_tables:
            self._create_stream_table(stream)
        self._insert_rows_to_stream(stream, records)
    
    
    def read(self, stream:Stream) -> Generator[Record, None, None]:
        if self._stream_table_name(stream) not in self._stream_tables:
            raise ValueError(f'No records cached for stream {stream.schema_name}.{stream.name}')

        cursor = self._conn.cursor()
        cursor.execute(f"SELECT * FROM {self._stream_table_name(stream)}")
        while True:
            # streaming read of records 
            rows = cursor.fetchmany(self._chunk_size)
            if not rows:
                break
            records = self._rows_to_records(stream, rows)
            for record in records:
                yield record

    def close(self):
        self._conn.commit()
        self._conn.close()