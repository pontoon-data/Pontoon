import json
from typing import List, Dict, Tuple, Generator, Any
from sqlalchemy import create_engine, inspect, MetaData, Table, text

from pontoon.base import Destination, Dataset, Stream, Record, Progress, Mode
from pontoon.base import DestinationConnectionFailed, DestinationStreamInvalidSchema

from pontoon.source.sql_source import SQLUtil
from pontoon.destination.sql_destination import SQLDestination
from pontoon.destination.gcs_destination import GCSDestination, GCSConfig


class BigQuerySQLUtil:
    """ A class to help generate Big Query specific SQL statements """
 

    @staticmethod
    def load_from_gcs(table_name:str, gcs_uri:str) -> str:
        return f"LOAD DATA OVERWRITE {SQLUtil.safe_identifier(table_name)} "\
               f"FROM FILES ("\
               f"format = 'PARQUET',"\
               f"uris = ['{gcs_uri}*.parquet']);"

    @staticmethod
    def create_table_if_not_exists(source_table_name:str, new_table_name:str) -> str:
        return f"CREATE TABLE IF NOT EXISTS {SQLUtil.safe_identifier(new_table_name)} AS SELECT * FROM {SQLUtil.safe_identifier(source_table_name)} WHERE 1=0"
    

    @staticmethod
    def merge(target_table_name:str, stage_table_name:str, cols:List[str], primary_key:str) -> str:
        s = SQLUtil.safe_identifier
        cols_str = ','.join([s(col) for col in cols])
        cols_stage_str = ','.join([f"stage.{s(col)}" for col in cols])
        update_set_str = ','.join([f"target.{s(col)}=stage.{s(col)}" for col in cols if col != primary_key])

        merge_sql = f"MERGE INTO {s(target_table_name)} AS target "\
                    f"USING {s(stage_table_name)} AS stage "\
                    f"ON target.{s(primary_key)} = stage.{s(primary_key)} "\
                    f"WHEN MATCHED THEN "\
                    f"UPDATE SET {update_set_str} "\
                    f"WHEN NOT MATCHED THEN "\
                    f"INSERT ({cols_str}) "\
                    f"VALUES ({cols_stage_str})"

        return merge_sql


class BigQueryDestination(SQLDestination):
    """ A Destination that writes to Big Query:
            - uses generic SQL layer for DDL operations (from SQLDestination)
            - loads data from a GCS location 
    """

    def __init__(self, config):
        super().__init__(config)

        connect = config.get('connect')
        self._gcs_config = GCSConfig(connect)

        # big query connection
        auth_type = connect.get('auth_type')
        if auth_type == 'service_account':       
            self._engine = create_engine(
                f"bigquery://{connect['project_id']}", 
                credentials_info=json.loads(connect['service_account'])
            )
        else:
            raise Exception(f"BigQuery (destination-bigquery) does not support auth type '{auth_type}'")

    
    def write(self, ds:Dataset, progress_callback = None):
        # Write a dataset to the destination database 
        self._ds = ds

        with self._connect() as conn:

            insp = inspect(conn)
            metadata_obj = MetaData()

            for stream in ds.streams:

                # configure progress tracking
                progress = Progress(
                    f"destination+bigquery://{ds.namespace}/{stream.schema_name}/{stream.name}",
                    total=ds.size(stream),
                    processed=0
                )
                if callable(progress_callback):
                    progress.subscribe(progress_callback)

                # Check if there are any records to process
                stream_size = ds.size(stream)
                if stream_size == 0:
                    progress.message("No records to process for this stream")
                    continue

                # staging and target table names
                target_table_name = f"{stream.schema_name}.{stream.name}"
                stage_table_name = f"{stream.schema_name}.__temp_{stream.name}"

                # sql to load to staging from gcs
                load_sql = BigQuerySQLUtil.load_from_gcs(
                    stage_table_name,
                    GCSDestination.get_object_path_uri(
                        self._gcs_config, 
                        ds.namespace, 
                        stream,
                        self._ds.meta.get('dt'),
                        self._ds.meta.get('batch_id')
                    )
                )

                # delete records depending on sync mode
                if self._mode.type == Mode.FULL_REFRESH:
                    progress.message("Dropping target table")
                    SQLDestination.drop_table(conn, target_table_name)

                # Drop staging table if it happens to exist from previous failed load
                # BQ does not support TEMP tables, so we use a real table - can't assume it will be cleaned up
                SQLDestination.drop_table(conn, stage_table_name)

                progress.message("Running LOAD from GCS")
                with conn.begin():
                    conn.execute(text(load_sql))

                create_target_sql = BigQuerySQLUtil.create_table_if_not_exists(stage_table_name, target_table_name)
                progress.message("Ensuring target table exists")
                with conn.begin():
                    conn.execute(text(create_target_sql))


                # Check that target and stage schemas are compatible 
                target_table = Table(stream.name, metadata_obj, schema=stream.schema_name, autoload_with=insp)
                target_table_schema = SQLDestination.table_ddl_to_schema(target_table.columns)

                stage_table = Table(f"__temp_{stream.name}", metadata_obj, schema=stream.schema_name, autoload_with=insp)
                stage_table_schema = SQLDestination.table_ddl_to_schema(stage_table.columns)
            
                # Use flexible schema comparison that ignores column order
                if not SQLDestination.schemas_compatible(target_table_schema, stage_table_schema):
                    raise DestinationStreamInvalidSchema(f"Existing schema for stream {stream.name} does not match.")

                
                # sql to MERGE the staging table into the target table
                merge_sql = BigQuerySQLUtil.merge(
                    target_table_name,
                    stage_table_name,
                    stream.schema.names,
                    stream.primary_field
                )

                progress.message("Merging data into target table")
                with conn.begin(): 
                    conn.execute(text(merge_sql))
                    
                SQLDestination.drop_table(conn, stage_table_name)

                 # drop target table after loading?
                if self._drop_after_complete == True:
                    SQLDestination.drop_table(conn, target_table_name)
                    
                progress.update(ds.size(stream))

    
    def close(self):
        pass