from typing import List, Dict, Tuple, Generator, Any
import pyarrow as pa
from sqlalchemy import create_engine, inspect, MetaData, Table, Column, text, insert
from sqlalchemy import Integer, BigInteger, SmallInteger, String, Text, Float, Numeric, Boolean, Date, Time, DateTime
from snowflake.sqlalchemy import TIMESTAMP_LTZ, TIMESTAMP_NTZ, TIMESTAMP_TZ 
from sqlalchemy.orm import sessionmaker

from pontoon.base import Destination, Dataset, Stream, Record, Mode, Progress


class SQLDestination(Destination):
    """ A Destination that writes to SQL data stores supported by SQLAlchemy """


    # map arrow types to sqlalchemy types
    PYARROW_TO_ALCHEMY = {
        pa.int32(): Integer,
        pa.int64(): BigInteger,
        pa.float16(): Float,
        pa.float32(): Float,
        pa.float64(): Float,
        pa.string(): String,
        pa.binary(): String,  
        pa.bool_(): Boolean,
        pa.timestamp('us', tz='UTC'): DateTime(True),  
        pa.date32(): DateTime,
        pa.date64(): DateTime
    }


    # map alchemy types to arrow 
    ALCHEMY_TO_ARROW = {
        String: pa.string(),
        Text: pa.string(),
        Float: pa.float64(),
        Integer: pa.int64(),
        BigInteger: pa.int64(),
        SmallInteger: pa.int64(),
        Numeric: pa.float64(),
        Boolean: pa.bool_(),
        Date: pa.timestamp('us', tz='UTC'),
        Time: pa.timestamp('us', tz='UTC'),
        DateTime: pa.timestamp('us', tz='UTC'),
    }

    # map of special dialect types to generic alchemy
    DIALECT_TO_ALCHEMY = {
        TIMESTAMP_LTZ: DateTime,
        TIMESTAMP_NTZ: DateTime,
        TIMESTAMP_TZ: DateTime
    }


    @staticmethod
    def alchemy_generic_type(col_type):
        # map alchemy dialect types up to generic alchemy types
        
        if type(col_type) in SQLDestination.DIALECT_TO_ALCHEMY:
            return SQLDestination.DIALECT_TO_ALCHEMY[type(col_type)]

        for gtype in SQLDestination.ALCHEMY_TO_ARROW.keys():
            if isinstance(col_type, gtype):

                # special case for numeric types that need to be mapped as integers
                if isinstance(col_type, Numeric) and hasattr(col_type, 'scale'):
                    if col_type.scale == 0:
                        return Integer
                    else:
                        return gtype

                return gtype

        raise ValueError(f"{col_type} ({type(col_type)}), {use_type} does not have a SQLAlchemy generic type")


    @staticmethod
    def schema_to_table_ddl(stream:Stream):
        # Turn an arrow schema into alchemy column definitions
        columns = []
        for i in range(len(stream.schema.names)):
            name = stream.schema.names[i]
            arrow_type = stream.schema.types[i]
            columns.append(Column(
                name, 
                SQLDestination.PYARROW_TO_ALCHEMY[arrow_type],
                primary_key=True if name == stream.primary_field else False
            ))

        return columns


    @staticmethod
    def table_ddl_to_schema(cols) -> pa.Schema:
        # Turn alchemy columns into an arrow schema
        fields = []
        for col in list(cols):
            fields.append(
                (col.name, 
                SQLDestination.ALCHEMY_TO_ARROW[SQLDestination.alchemy_generic_type(col.type)])
            )
        return pa.schema(fields)
    

    @staticmethod
    def create_table_if_not_exists(conn, stream:Stream, override_name:str = None):
        # create a table for this stream if it doesn't already exist
        
        insp = inspect(conn)
        metadata_obj = MetaData()
        table = None

        name = override_name if override_name is not None else stream.name

        if insp.has_table(name, stream.schema_name):
                    
            # does the existing table's schema match the stream being written?
            table = Table(name, metadata_obj, schema=stream.schema_name, autoload_with=insp)
            existing_schema = SQLDestination.table_ddl_to_schema(table.columns)
            
            # if not, we can't write to it
            if not existing_schema.equals(stream.schema):
                raise ValueError(f"Existing schema for stream {name} does not match.")

        else:

            # table doesn't exist, create it
            columns = SQLDestination.schema_to_table_ddl(stream)

            table = Table(
                name,
                metadata_obj,
                *columns,
                schema=stream.schema_name
            )
    
            with conn.begin():
                table.create(bind=conn)
        
        return table


    @staticmethod
    def drop_table(conn, table_name:str):
        # drop a table
        with conn.begin():
            conn.execute(text(f"DROP TABLE {table_name}"))    



    def __init__(self, config):
        self._ds = None
        self._config = config
        self._mode = config.get('mode')
        self._progress_callback = None
        self._drop_after_complete = config.get('drop_after_complete', False)
        self._connection_info = config.get('connect')

        connect = config.get('connect')

        # batch size for reading records from cache and writing to destination
        self._chunk_size = connect.get('chunk_size', 1024)

        # configure the SQLAlchemy engine
        auth_type = connect.get('auth_type')

        # implement basic auth type here
        if auth_type == 'basic':
            if connect.get('dsn'):
                self._engine = create_engine(connect.get('dsn'))
            else:
                self._engine = create_engine(
                    f"{connect['driver']}://{connect['user']}:{connect['password']}@"\
                    f"{connect['host']}:{connect['port']}/{connect['database']}"
                ) 


    def _connect(self):
        return self._engine.connect()


    def _batch_to_rows(self, stream:Stream, batch:List[Record]):
        # Turn a batch of records into a list of python dicts
        cols = stream.schema.names
        cols_len = len(cols)
        return [
            {cols[i]: record.data[i] for i in range(cols_len)} for record in batch
        ]


    def _write_batch(self, conn, table, stream:Stream, batch:List[Record]):
        # write a batch of records to the database
        conn.execute(insert(table), self._batch_to_rows(stream, batch))

    
    def write(self, ds:Dataset, progress_callback = None):
        # Write a dataset to the destination database 

        self._ds = ds

        # setup callbacks for progress updates
        if callable(progress_callback):
            self._progress_callback = progress_callback
        else:
            self._progress_callback = lambda *args, **kwargs: None

        # base connector only does full refresh right now
        if self._mode.type != Mode.FULL_REFRESH:
            raise Exception("SQLDestination (destination-sql) only supports FULL_REFRESH replication mode.")

        total_records = 0

        with self._connect() as conn:

            for stream in ds.streams:

                # create a table for the stream if it doesn't exist
                table = SQLDestination.create_table_if_not_exists(conn, stream)
                
                # truncate the table
                conn.execute(table.delete())

                # now we have a destination table with matching schema
                # write records to the destination table
                batch = []
                for record in ds.read(stream):
                    batch.append(record)
                    if len(batch) == self._chunk_size:
                        total_records += self._chunk_size
                        self._write_batch(conn, table, stream, batch)
                        self._progress_callback(Progress(-1, total_records))
                        batch = []
                
                if batch:
                    total_records += len(batch)
                    self._write_batch(conn, table, stream, batch)
                    self._progress_callback(Progress(-1, total_records))
                
                # drop tables after load?
                if self._drop_after_complete == True:
                    table.drop(conn)
        
        # final progress update
        self._progress_callback(Progress(total_records, 0))

    def close(self):
        pass
