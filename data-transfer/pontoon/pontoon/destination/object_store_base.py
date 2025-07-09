import time
from typing import Protocol, List, Dict, Any
from abc import abstractmethod
import tempfile
from datetime import datetime
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from pontoon.base import Namespace, Destination, Stream, Dataset, Record, Progress



class ObjectStoreConfig(Protocol):
    """ A protocol (interface / duck type) to represent object storage configuration """
    
    @property
    def bucket_name(self) -> str:
        pass
    
    @property
    def bucket_path(self) -> str:
        pass

    @property
    def scheme(self) -> str:
        pass



class ObjectStoreBase(Destination):
    """ An abstract base class for Destinations that write Parquet to object stores """

    
    @staticmethod
    def _batch_to_table(stream:Stream, batch:List[Record]):
        # Turn a batch of records into an Arrow table with schema enforced
        table_rows = []
        cols = stream.schema.names
        for record in batch:
            table_rows.append({cols[i]: record.data[i] for i in range(len(cols))})
        return pa.Table.from_pylist(table_rows, schema=stream.schema)


    @staticmethod
    def _write_parquet(stream:Stream, batch:List[Record], output_path:str = None, parquet_config={}):
        # write an arrow table to a file as parquet
        if output_path is None:
            _, file_path = tempfile.mkstemp()
        else:
            file_path = output_path

        table = ObjectStoreBase._batch_to_table(stream, batch)

        pq.write_table(
            table, 
            file_path, 
            compression = parquet_config.get('compression', 'NONE')
        )
        return file_path
    
    
    @staticmethod
    def get_object_name(stream:Stream, dt:datetime, batch_id:str, batch_index:int):
        # e.g. pontoon__events_2025_01_10_1740773449235_0.parquet
        date = dt.strftime('%Y_%m_%d')
        return f"{stream.schema_name}__{stream.name}_{date}_{batch_id}_{batch_index}.parquet"


    @staticmethod
    def get_object_path(config:ObjectStoreConfig, namespace:Namespace, stream:Stream, dt:datetime, batch_id:str):
        # e.g. events/postgres/pontoon__events/2025-01-10/1740773449235/
        date = dt.strftime('%Y-%m-%d')
        return f"{config.bucket_path}/{namespace.name}/{stream.schema_name}__{stream.name}/{date}/{batch_id}/"
    
    
    @staticmethod
    def get_object_filename(config:ObjectStoreConfig, namespace:Namespace, stream:Stream, dt:datetime, batch_id:str, batch_index:int):
        # e.g. events/postgres/pontoon__events/2025-01-10/1740773449235/pontoon__events_2025_01_10_1740773449235_0.parquet
        return f"{ObjectStoreBase.get_object_path(config, namespace, stream, dt, batch_id)}{ObjectStoreBase.get_object_name(stream, dt, batch_id, batch_index)}"


    @staticmethod
    def get_object_path_uri(config:ObjectStoreConfig, namespace:Namespace, stream:Stream, dt:datetime, batch_id:str):
        # e.g. s3://bucket/events/postgres/pontoon__events/2025-01-10/1740773449235/
        return f"{config.scheme}://{config.bucket_name}/{ObjectStoreBase.get_object_path(config, namespace, stream, dt, batch_id)}"

    
    
    def __init__(self, config):
        self._config = config
        self._batch_size = config.get('batch_size', 10000)
        
        self._dt = None
        self._batch_id = None
        self._ds = None
        self._progress_callback = None
        
    
    @abstractmethod
    def _write_batch(self, stream:Stream, batch:List[Record], batch_index:int): pass

    
    def write(self, ds:Dataset, progress_callback=None):
        
        self._ds = ds
        self._batch_id = ds.meta.get('batch_id')
        self._dt = ds.meta.get('dt')

        total_records = 0

        if callable(progress_callback):
            self._progress_callback = progress_callback
        else:
            self._progress_callback = lambda *args, **kwargs: None

        # write streams in batches
        for stream in ds.streams:
            batch = []
            batch_index = 0
            for record in ds.read(stream):
                batch.append(record)
                if len(batch) == self._batch_size:
                    total_records += self._batch_size
                    self._write_batch(stream, batch, batch_index)
                    self._progress_callback(Progress(-1, total_records))
                    batch = []
                    batch_index += 1
            
            if batch:
                total_records += len(batch)
                self._write_batch(stream, batch, batch_index)
                self._progress_callback(Progress(-1, total_records))
        
        # we're done
        self._progress_callback(Progress(total_records, 0))
    

    def close(self):
        pass
    
