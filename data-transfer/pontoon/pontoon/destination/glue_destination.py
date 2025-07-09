import time
from typing import List, Dict, Any
from datetime import datetime

import boto3

from pontoon.base import Destination, Stream, Dataset, Record, Progress
from pontoon.destination.s3_destination import S3Destination, S3Config


class GlueDestination(Destination):
    """ A Destination that crawls Glue Catalog tables from S3 """

    def __init__(self, config):
        self._config = config
        self._ds = None
        self._dt = None
        self._batch_id = None

        connect = config.get('connect')
        self._glue_iam_role = connect['glue_iam_role']
        self._glue_database = connect['glue_database']
        self._progress_callback = None
        
        # our s3 config
        self._s3_config = S3Config(connect)

    
    def _get_glue_client(self):
        # Get the Glue client based on auth type

        connect = self._config.get('connect')
        auth_type = connect.get('auth_type')
        
        if auth_type not in ['credentials']:
            raise Exception(f"GlueDestination (destination-glue) does not support auth type '{auth_type}'")
        
        return boto3.client(
            'glue',
            aws_access_key_id=connect.get('aws_access_key_id'),
            aws_secret_access_key=connect.get('aws_secret_access_key'),
            region_name=self._s3_config.region
        )


    def _get_crawler_name(self):
        return f"PontoonGlueDestination_{self._ds.namespace.name}_{str(int(datetime.now().timestamp()*1000))}"

       
    def _crawl(self):
        # Run an ephemeral Glue crawler to update the Glue catalog
        glue = self._get_glue_client()
        crawler_name = self._get_crawler_name() 
        
        # crawl each stream prefix to create separate tables
        stream_paths = [
            {"Path": S3Destination.get_object_path_uri(
                self._s3_config, 
                self._ds.namespace, 
                stream,
                self._ds.meta('dt'),
                self._ds.meta('batch_id')
            )} for stream in self._ds.streams 
        ]

        # create the crawler and start it
        glue.create_crawler(
            Name=crawler_name,
            Role=self._glue_iam_role,
            DatabaseName=self._glue_database,
            Targets={"S3Targets": stream_paths}
        )
        glue.start_crawler(Name=crawler_name)

        # wait for the crawler to complete
        while True:
            response = glue.get_crawler(Name=crawler_name)
            state = response['Crawler']['State']
            if state == 'READY':
                break
            time.sleep(10)
        
        # clean up
        glue.delete_crawler(Name=crawler_name)


    def write(self, ds:Dataset, progress_callback=None):
        # Write a dataset to Glue
        self._ds = ds

        if callable(progress_callback):
            self._progress_callback = progress_callback
        else:
            self._progress_callback = lambda *args, **kwargs: None

        # run glue crawler to update the catalog
        self._crawl()


    def close(self):
        pass