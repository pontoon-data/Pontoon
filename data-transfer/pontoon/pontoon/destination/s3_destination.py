import os
from typing import List, Dict, Any
import boto3
from pontoon.base import Namespace, Destination, Stream, Dataset, Record, Progress
from pontoon.base import DestinationError
from pontoon.destination import ObjectStoreBase
from pontoon.destination.integrity import S3Integrity


class S3Config:
    """ A class to represent an S3 configuration block """

    def __init__(self, config):
        self.scheme = 's3'
        self.bucket_name = config.get('s3_bucket')
        self.bucket_path = config.get('s3_prefix', '')
        self.region = config.get('s3_region', '')

        self.bucket_name = self.bucket_name.lstrip('s3://').rstrip('/')
        self.bucket_path = self.bucket_path.lstrip('/').rstrip('/')



class S3Destination(ObjectStoreBase):
    """ A Destination that writes to S3 in Parquet format """


    def __init__(self, config):
        
        super().__init__(config) 

        # our s3 config
        self._s3_config = S3Config(config['connect'])

        if self._format not in ['staging', 'hive']:
            raise DestinationError(f'Format {self._format} is not supported by S3')

    def _get_s3_client(self):
        # get S3 client using configured auth type

        connect = self._config.get('connect')
        auth_type = connect.get('auth_type')
        
        if auth_type not in ['basic']:
            raise Exception(f"S3Destination (destination-s3) does not support auth type '{auth_type}'")
        
        return boto3.client(
            's3',
            aws_access_key_id=connect.get('aws_access_key_id'),
            aws_secret_access_key=connect.get('aws_secret_access_key'),
            region_name=self._s3_config.region
        )

    def _write_stream(self, stream:Stream):
        pass
    
    def _write_batch(self, stream:Stream, batch:List[Record], batch_index:int):
       
        # Write a batch of records to S3 formatted as Parquet
        s3 = self._get_s3_client()

        # write the parquet file
        parquet_file_path = ObjectStoreBase._write_parquet(
            stream,
            batch,
            parquet_config=self._config.get('parquet', {})
        )
        
        # upload to s3
        if self._format == 'staging':
            parquet_s3_path = ObjectStoreBase.get_object_filename(
                self._s3_config, 
                self._ds.namespace, 
                stream, 
                self._dt,
                self._batch_id, 
                batch_index
            )
        elif self._format == 'hive':
            parquet_s3_path = ObjectStoreBase.get_hive_filename(
                self._s3_config, 
                self._ds.namespace, 
                stream, 
                self._dt,
                self._batch_id, 
                batch_index
            )

        s3.upload_file(parquet_file_path, self._s3_config.bucket_name, parquet_s3_path)
        
        # clean up
        os.remove(parquet_file_path)
   

    def integrity(self):
        return S3Integrity(self._get_s3_client())    
