import os
import json
import tempfile
from typing import List, Dict, Any
from google.cloud import storage
from pontoon.base import Namespace, Destination, Stream, Dataset, Record, Progress
from pontoon.base import DestinationError
from pontoon.destination import ObjectStoreBase
from pontoon.destination.integrity import GCSIntegrity


class GCSConfig:
    """ A class to represent an GCS configuration block """

    def __init__(self, config):
        self.scheme = 'gs'
        self.bucket_name = config.get('gcs_bucket_name')
        self.bucket_path = config.get('gcs_bucket_path', '')

        self.bucket_name = self.bucket_name.lstrip('gs://').rstrip('/')
        self.bucket_path = self.bucket_path.lstrip('/').rstrip('/')



class GCSDestination(ObjectStoreBase):
    """ A Destination that writes to GCS in Parquet format """


    def __init__(self, config):

        super().__init__(config) 

        connect = config.get('connect')

        # our GCS config
        self._gcs_config = GCSConfig(connect)

        # parquet config
        self._parquet_config = connect.get('parquet', {})

        # our GCP service account
        with tempfile.NamedTemporaryFile(mode='w', suffix=".json", delete=False) as temp_file:
            temp_file.write(connect.get('service_account'))
            self._service_account_file = temp_file.name

        if self._format not in ['staging', 'hive']:
            raise DestinationError(f'Format {self._format} is not supported by GCS')
    
    
    def _write_stream(self, stream:Stream):
        pass


    def _write_batch(self, stream:Stream, batch:List[Record], batch_index:int):
        # Write a batch of records to GCS formatted as Parquet
        
        gcs = storage.Client.from_service_account_json(self._service_account_file)
        bucket = gcs.bucket(self._gcs_config.bucket_name)

        # write the parquet file
        parquet_file_path = ObjectStoreBase._write_parquet(
            stream,
            batch,
            parquet_config=self._parquet_config
        )
        
        # upload to GCS
        if self._format == 'staging':
            parquet_gcs_path = ObjectStoreBase.get_object_filename(
                self._gcs_config, 
                self._ds.namespace, 
                stream, 
                self._dt, 
                self._batch_id,
                batch_index
            )
        elif self._format == 'hive':
            parquet_gcs_path = ObjectStoreBase.get_hive_filename(
                self._gcs_config, 
                self._ds.namespace, 
                stream, 
                self._dt, 
                self._batch_id,
                batch_index
            )


        blob = bucket.blob(parquet_gcs_path)
        blob.upload_from_filename(parquet_file_path)
        
        # clean up
        os.remove(parquet_file_path)
   

    def integrity(self):
        return GCSIntegrity(
            storage.Client.from_service_account_json(self._service_account_file)
        )