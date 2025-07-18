# ------------------------------------------------------------------------------
# Copyright (c) 2025 Pontoon, Inc.
# All Rights Reserved.
# ------------------------------------------------------------------------------


import hashlib
import json
from uuid import UUID
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Generator, Any

import pyarrow as pa



class Namespace:
    """ A class to represent a namespace """
    def __init__(self, name:str):
        self.name = name


class Record:
    """ A class to represent a data record """
    def __init__(self, data:List[Any]):
        self.data = data


class Stream:
    """ 
        A class to represent a typed stream of records

        * This is where the data type mapping between Python types and Arrow data types happens
        * Arrow provides a very portable, agnostic set of types for analytical datasets
        * Some translation logic is required because not all Python types map 1:1 to Arrow
            -> Particularly complex types like UUID or Decimal
    
    """

    # type mapping from python types to Arrow types
    PY_TO_PYARROW_MAP = {
        int: pa.int64(),
        float: pa.float64(),
        str: pa.string(),
        bool: pa.bool_(),
        bytes: pa.binary(),
        datetime: pa.timestamp('us', tz='UTC'),
        type(None): pa.null()  # NoneType corresponds to NULL
    }

    # python types that have no Arrow equivalent have to be "simplified"
    PY_CONVERSION_MAP = {
        UUID: str,
        Decimal: float,
        'TIMESTAMP_NTZ': datetime
    }

    def __init__(self, name:str, schema_name:str, schema:pa.Schema, primary_field:str=None, cursor_field:str=None, filters:Dict[str,Any]=None):
        # A Stream is essentially just a name and a schema for the records
        self.name = name
        self.schema_name = schema_name
        self.schema = schema
    
        self.primary_field = primary_field
        self.cursor_field = cursor_field
        self.filters = filters

        self._drop_fields = []
        self._extra_fields = {}

        if self.primary_field is not None and self.primary_field not in self.schema.names:
            self._missing_field(self.primary_field)

        if self.cursor_field is not None and self.cursor_field not in self.schema.names:
            self._missing_field(self.cursor_field)

        if self.filters:
            for field_name, val in self.filters.items():
                if field_name not in self.schema.names:
                     self._missing_field(field_name)

    
    def _compute_checksum(self, row:List[Any]) -> str:
        # Convert all values to string and concatenate them
        str_vals = ''.join(map(str, row))
        
        # encode the concatenated string to bytes
        enc_str = str_vals.encode('utf-8')

        # generate an MD5 hash
        md5_hash = hashlib.md5(enc_str).hexdigest()
        
        return md5_hash


    def _missing_field(self, field_name:str):
        raise Exception(f"Stream {self.schema_name}.{self.name} does not have field: {field_name}")

    
    def with_field(self, field_name:str, field_type:Any, value:Any) -> 'Stream':
        self.schema = self.schema.append(pa.field(field_name, field_type))
        self._extra_fields[field_name] = value
        return self
    
    
    def with_checksum(self, field_name:str='pontoon__checksum') -> 'Stream':
        return self.with_field(field_name, pa.string(), self._compute_checksum)
    
    
    def with_batch_id(self, batch_id:str, field_name:str='pontoon__batch_id') -> 'Stream':
        return self.with_field(field_name, pa.string(), batch_id)
        
    
    def with_last_synced_at(self, sync_dt:datetime, field_name:str='pontoon__last_synced_at') -> 'Stream':
        return self.with_field(field_name, pa.timestamp('us', tz='UTC'), sync_dt.isoformat())
    
    
    def with_version(self, version:str, field_name='pontoon__version') -> 'Stream':
        return self.with_field(field_name, pa.string(), version)

    
    def drop_field(self, field_name:str) -> 'Stream':
        if field_name not in self.schema.names:
            raise Exception(f"Stream {self.schema_name}.{self.name} does not have field: {field_name}")
        field_idx = self.schema.get_field_index(field_name)
        self.schema = self.schema.remove(field_idx)
        self._drop_fields.append(field_idx)


    def to_record(self, row:List[Any]) -> Record:
        # take a row of raw data and return a schema Record """

        type_map = Stream.PY_CONVERSION_MAP

        def _convert(val):
            py_type = type(val)
            if py_type is datetime:
                return val.astimezone(timezone.utc)
            return type_map.get(py_type, lambda x: x)(val)

        new_row = [val for i, val in enumerate(row) if i not in self._drop_fields]
        new_row = list(map(_convert, new_row))
        new_row += [None] * len(self._extra_fields)
        
        field_names = self.schema.names
        field_lookup = dict(zip(field_names, range(len(field_names))))

        for field, val in self._extra_fields.items():
            idx = field_lookup[field]
            if callable(val):
                new_row[idx] = val(row)
            else:
                new_row[idx] = val

        return Record(new_row)


    @staticmethod
    def infer_schema(cols:List[str], sample:List[Any]) -> pa.Schema:
        
        # Infer an Arrow schema by looking at a row of sample data
        arrow_types = [pa.infer_type([element]) for element in sample] 
        field_tuples = list(zip(cols, arrow_types))
        return pa.schema(field_tuples)


    @staticmethod
    def build_schema(cols:List[Tuple[str, Any]]) -> pa.Schema:

        # Build an Arrow schema from explicit python types and column names
        arrow_types = []
        col_names = [name for (name, _) in cols]

        for (_, py_type) in cols:
            use_type = Stream.PY_CONVERSION_MAP.get(py_type, py_type)
            if use_type not in Stream.PY_TO_PYARROW_MAP:
                raise ValueError(f"No Arrow type mapping for {use_type}")
            arrow_types.append(Stream.PY_TO_PYARROW_MAP[use_type])

        field_tuples = list(zip(col_names, arrow_types))
        return pa.schema(field_tuples)



class Cache(ABC):
    """ Abstract base class to represent a stream cache """

    @abstractmethod
    def __init__(self, namespace:Namespace, config:Dict[str, Any]):
        pass

    @abstractmethod
    def write(self, stream:Stream, records:List[Record]) -> int:
        pass

    @abstractmethod
    def read(self, stream:Stream) -> Generator[Record, None, None]:
        pass

    @abstractmethod
    def close(self):
        pass



class Dataset:
    """ A class to represent a set of data streams """
    def __init__(self, namespace:Namespace, streams:List[Stream], cache:Cache, meta:dict):
        self.namespace = namespace
        self.streams = streams
        self.meta = meta
        self._rename_map = {}
        
        # A DataSet is backed by a cache implementation 
        self._cache = cache

    def read(self, stream:Stream) -> Generator[Record, None, None]:
        
        # has the stream been renamed?
        if(stream.name, stream.schema_name) in self._rename_map:
            old_name, old_schema_name = self._rename_map[(stream.name, stream.schema_name)]
            old_stream = Stream(old_name, old_schema_name, stream.schema)
            return self._cache.read(old_stream)
        
        return self._cache.read(stream)


    def rename_stream(self, stream_name:str, schema_name:str, new_name:str, new_schema:str):
        for stream in self.streams:
            if stream.name == stream_name and stream.schema_name == schema_name:
                self._rename_map[(new_name, new_schema)] = (stream.name, stream.schema_name)
                stream.name = new_name
                stream.schema_name = new_schema
                return
        raise ValueError(f"{stream_name} not in dataset.")


class Progress:
    """ A class to represent progress of stream processing """

    def __init__(self, total_records:int, processed:int):
        self.total_records = total_records
        self.processed = processed


class Mode:
    """ A class to represent a replication modality between a Source and Destination """
    
    # replication types
    FULL_REFRESH = 'FULL_REFRESH'
    INCREMENTAL = 'INCREMENTAL'

    # replication periods
    WEEKLY = 'WEEKLY'
    DAILY = 'DAILY'
    SIXHOURLY = 'SIXHOURLY'
    HOURLY = 'HOURLY'

    # timedeltas for periods
    _DELTA = {
        'WEEKLY': timedelta(days=7, hours=12),
        'DAILY': timedelta(days=1, hours=3),
        'SIXHOURLY': timedelta(hours=6, minutes=30),
        'HOURLY': timedelta(hours=1, minutes=15)
    }

    def __init__(self, config):
        self.type = config.get('type', Mode.FULL_REFRESH)
        self.period = config.get('period', Mode.DAILY)
        self.start = None
        self.end = None

        if config.get('start'):
            start = config.get('start')
            if isinstance(start, datetime):
                self.start = start
            else:
                self.start = datetime.fromisoformat(start)
        
        if config.get('end'):
            end = config.get('end')
            if isinstance(end, datetime):
                self.end = end
            else:
                self.end = datetime.fromisoformat(end)
        
    
    def __str__(self):
        return json.dumps({
            'type': self.type,
            'period': self.period,
            'start': self.start.isoformat() if self.start else None,
            'end': self.end.isoformat() if self.end else None
        })
    
    @staticmethod
    def delta(period:str):
        return Mode._DELTA[period]


class Source(ABC):
    """ Abstract base class to represent a data source connector """

    @abstractmethod
    def __init__(self, config, cache, cache_config):
        pass

    @abstractmethod
    def read(self, progress_callback=None) -> Dataset:
        pass

    @abstractmethod
    def test_connect(self):
        pass
    
    @abstractmethod
    def inspect_streams(self):
        pass

    @abstractmethod
    def close(self):
        pass


class Destination(ABC):
    """ Abstract base class to represent a data destination connector """

    @abstractmethod
    def __init__(self, config):
        pass

    @abstractmethod
    def write(self, ds: Dataset, progress_callback=None):
        pass
    
    @abstractmethod
    def close(self):
        pass