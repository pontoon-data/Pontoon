import os
from typing import List, Dict, Any
from datetime import datetime
import snowflake.connector
from pontoon.base import Namespace, Destination, Stream, Dataset, Record, Progress
from pontoon.base import DestinationError, DestinationConnectionFailed
from pontoon.source.sql_source import SQLUtil
from pontoon.destination import ObjectStoreBase
from pontoon.destination.integrity import SMSIntegrity


class SnowflakeStorageDestination(ObjectStoreBase):
    """ A Destination that writes to Snowflake managed storage as Parquet """


    def __init__(self, config):
        
        super().__init__(config) 

        epoch = str(int(datetime.utcnow().timestamp() * 1000))

        # name for our temporary loading stage (like a bucket)
        connect = config.get('connect')
        self._stage_name = connect.get('stage_name')
        self._create_stage = connect.get('create_stage', False)
        self._parquet_config = connect.get('parquet', {})

        if self._format != 'staging':
            raise DestinationError(f'Format {self._format} is not supported by Snowflake Storage')

    
    def _get_snowflake_client(self):
        c = self._config.get('connect')
        try:
            return snowflake.connector.connect(
                user=c['user'],
                password=c['access_token'],
                account=c['account'],
                warehouse=c['warehouse'],
                database=c['database'],
                schema=c['target_schema']
            )
        except Exception as e:
            raise DestinationConnectionFailed("Failed to connect to Snowflake") from e


    def _write_stream(self, stream:Stream):
        pass


    def _write_batch(self, stream:Stream, batch:List[Record], batch_index:int):
        # Write a batch of records to snowflake storage formatted as Parquet
        
        snow = self._get_snowflake_client()
        cur = snow.cursor()

        # get the filename for our parquet file
        parquet_file_path = ObjectStoreBase.get_object_name(
            stream,
            self._dt,
            self._batch_id,
            batch_index
        )

        # write the parquet file locally
        ObjectStoreBase._write_parquet(
            stream,
            batch,
            output_path=parquet_file_path,
            parquet_config=self._parquet_config
        )

        # upload to snowflake
        upload_query = f"PUT file://{parquet_file_path} @{self._stage_name}"
        cur.execute(upload_query)
        cur.close()
        snow.close()
        
        # clean up
        os.remove(parquet_file_path)
   
    
    def integrity(self):
        return SMSIntegrity(self._get_snowflake_client())
        
    
    def write(self, ds:Dataset, progress_callback=None):
        # Write a dataset to Snowflake storage stage

        if self._create_stage:
            if self._stage_name == None:
                self._stage_name = f"pontoon_{ds.meta.get('batch_id')}"
            
            ds.meta['stage_name'] = self._stage_name

            snow = self._get_snowflake_client()
            cur = snow.cursor()
            cur.execute(f"CREATE OR REPLACE STAGE {SQLUtil.safe_identifier(self._stage_name)} FILE_FORMAT = (TYPE = PARQUET)")
            cur.close()
            snow.close()

        # defer to ObjectStoreBase for the rest
        super().write(ds, progress_callback)
