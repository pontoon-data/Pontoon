from typing import List, Dict, Tuple, Generator, Any
from sqlalchemy import text

from pontoon.base import Destination, Dataset, Stream, Record, Progress, Mode
from pontoon.source.sql_source import SQLUtil
from pontoon.destination.sql_destination import SQLDestination
from pontoon.destination.s3_destination import S3Destination, S3Config


class RedshiftSQLUtil:
    """ A class to help generate Redshift specific SQL statements """

    @staticmethod
    def create_temp_table(table_name:str, like_table_name:str) -> str:
        return f"CREATE TEMP TABLE {SQLUtil.safe_identifier(table_name)} "\
               f"(LIKE {SQLUtil.safe_identifier(like_table_name)})"
    

    @staticmethod
    def copy_from_s3(table_name:str, s3_uri:str, iam_role:str, s3_region:str) -> str:
        return f"COPY {SQLUtil.safe_identifier(table_name)} "\
               f"FROM '{s3_uri}' "\
               f"IAM_ROLE '{iam_role}' "\
               f"FORMAT AS PARQUET "\
               f"REGION '{s3_region}'"
    

    @staticmethod
    def upsert(target_table_name:str, stage_table_name:str, cols:List[str], primary_key:str) -> (str, str):
        s = SQLUtil.safe_identifier
        cols_str = ','.join([s(col) for col in cols])
        delete_sql = f"DELETE FROM {s(target_table_name)} "\
                     f"USING {s(stage_table_name)} "\
                     f"WHERE {s(target_table_name)}.{s(primary_key)} = {s(stage_table_name)}.{s(primary_key)}"

        insert_sql = f"INSERT INTO {s(target_table_name)} ({cols_str}) "\
                     f"(SELECT {cols_str} FROM {s(stage_table_name)})"

        return delete_sql, insert_sql



class RedshiftDestination(SQLDestination):
    """ A Destination that writes to Redshift:
            - uses generic SQL layer for most DDL operations (from SQLDestination)
            - loads data from an S3 location
            - uses an UPSERT approach 
    """

    def __init__(self, config):

        config['connect']['driver'] = 'postgresql+psycopg2'

        super().__init__(config)  # Call SQLDestination constructor
        
        # specific to Redshift COPY
        connect = config.get('connect')
        self._s3_config = S3Config(connect)
        self._iam_role = connect.get('iam_role')

    
    def write(self, ds:Dataset, progress_callback = None):
        # Write a dataset to the destination database 
        self._ds = ds

        with self._connect() as conn:

            for stream in ds.streams:

                # configure progress tracking
                progress = Progress(
                    f"destination+redshift://{ds.namespace}/{stream.schema_name}/{stream.name}",
                    total=ds.size(stream),
                    processed=0
                )
                if callable(progress_callback):
                    progress.subscribe(progress_callback)

                # create a table for the stream if it doesn't exist
                table = SQLDestination.create_table_if_not_exists(conn, stream)
                
                target_table_name = f"{stream.schema_name}.{stream.name}"
                stage_table_name = f"temp_{stream.schema_name}_{stream.name}"

                if self._mode.type == Mode.FULL_REFRESH:
                    with conn.begin():
                        # delete all records from the table
                        conn.execute(table.delete())

                # temporary staging table
                create_stage_sql = RedshiftSQLUtil.create_temp_table(stage_table_name, target_table_name)
                
                # S3 copy into the stage table
                copy_sql = RedshiftSQLUtil.copy_from_s3(
                    stage_table_name,
                    S3Destination.get_object_path_uri(
                        self._s3_config, 
                        ds.namespace, 
                        stream,
                        self._ds.meta.get('dt'),
                        self._ds.meta.get('batch_id')
                    ),
                    self._iam_role,
                    self._s3_config.region
                )

                # upsert statements
                upsert_delete_sql, upsert_insert_sql = RedshiftSQLUtil.upsert(
                    target_table_name,
                    stage_table_name,
                    stream.schema.names,
                    stream.primary_field
                )

                # create the staging table and load data into it
                with conn.begin():
                    progress.message("Copying records from S3")
                    conn.execute(text(create_stage_sql))
                    conn.execute(text(copy_sql))
                
                # upsert staging into target table
                with conn.begin():
                    progress.message("Upserting records into target table")
                    conn.execute(text(upsert_delete_sql))
                    conn.execute(text(upsert_insert_sql))

                # drop the staging table
                SQLDestination.drop_table(conn, stage_table_name)

                # drop target table after loading?
                if self._drop_after_complete == True:
                    SQLDestination.drop_table(conn, target_table_name)

                progress.update(ds.size(stream))

    
    def close(self):
        pass