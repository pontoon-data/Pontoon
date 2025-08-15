import os
from typing import List, Dict, Any
from azure.storage.blob import BlobServiceClient
from pontoon.base import Namespace, Destination, Stream, Dataset, Record, Progress
from pontoon.base import DestinationError
from pontoon.destination import ObjectStoreBase
from pontoon.destination.integrity import ABSIntegrity


class ABSConfig:
    """ A class to represent an Azure Blob configuration block """

    def __init__(self, config):
        self.scheme = 'blob'
        self.bucket_name = config.get('blob_container')
        self.bucket_path = config.get('blob_prefix', '')
        self.region = ''

        self.bucket_name = self.bucket_name.lstrip('abfss://').rstrip('/')
        self.bucket_path = self.bucket_path.lstrip('/').rstrip('/')



class ABSDestination(ObjectStoreBase):
    """ A Destination that writes to Azure Blob Store in Parquet format """


    def __init__(self, config):
        
        super().__init__(config) 

        # our azure blob config config
        self._abs_config = ABSConfig(config['connect'])

        if self._format not in ['staging', 'hive']:
            raise DestinationError(f'Format {self._format} is not supported by Azure Blob Store')


    def _get_abs_client(self):
        # get Azure Blob container client using configured auth type

        connect = self._config.get('connect')
        auth_type = connect.get('auth_type')
        
        if auth_type not in ['connection_string']:
            raise Exception(f"ABSDestination (destination-abs) does not support auth type '{auth_type}'")
        
        blob_service_client = BlobServiceClient.from_connection_string(
            connect.get('blob_connection_string')
        )
        return blob_service_client.get_container_client(
            connect.get('blob_container')
        )

    def _write_stream(self, stream:Stream):
        pass
    
    def _write_batch(self, stream:Stream, batch:List[Record], batch_index:int):
       
        # Write a batch of records to azure blob formatted as Parquet
        abs_client = self._get_abs_client()

        # write the parquet file
        parquet_file_path = ObjectStoreBase._write_parquet(
            stream,
            batch,
            parquet_config=self._config.get('parquet', {})
        )
        
        # upload to azure
        if self._format == 'staging':
            parquet_abs_path = ObjectStoreBase.get_object_filename(
                self._abs_config, 
                self._ds.namespace, 
                stream, 
                self._dt,
                self._batch_id, 
                batch_index
            )
        elif self._format == 'hive':
            parquet_abs_path = ObjectStoreBase.get_hive_filename(
                self._abs_config, 
                self._ds.namespace, 
                stream, 
                self._dt,
                self._batch_id, 
                batch_index
            )

        blob = abs_client.get_blob_client(parquet_abs_path)
        with open(parquet_file_path, 'rb') as data:
            blob.upload_blob(data, overwrite=True)

        # clean up
        os.remove(parquet_file_path)
   

    def integrity(self):
        return ABSIntegrity(self._get_abs_client())    
