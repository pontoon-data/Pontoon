from typing import List, Dict, Tuple, Generator, Any
from sqlalchemy import create_engine, text

from pontoon.base import Destination, Dataset, Stream, Record, Progress, Mode
from pontoon.source.sql_source import SQLUtil
from pontoon.destination.sql_destination import SQLDestination


class SnowflakeSQLUtil:

    @staticmethod
    def create_temp_table(temp_table_name:str, source_table_name:str) -> str:
        return f"CREATE TEMPORARY TABLE {SQLUtil.safe_identifier(temp_table_name)} LIKE {SQLUtil.safe_identifier(source_table_name)}"

    @staticmethod
    def copy_into_table(target_table_name:str, stage_name:str, pattern:str) -> str:
        return f"COPY INTO {SQLUtil.safe_identifier(target_table_name)} "\
               f"FROM @{SQLUtil.safe_identifier(stage_name)} "\
               f"FILE_FORMAT = (TYPE = PARQUET) "\
               f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "\
               f"PATTERN = '{pattern}'"

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


class SnowflakeDestination(SQLDestination):
    """ A Destination that writes to Snowflake:
            - uses generic SQL layer for DDL operations (from SQLDestination)
            - loads data from a stage into Snowflake tables using COPY
    """

    def __init__(self, config):
        super().__init__(config)  # Call SQLDestination constructor

        connect = config.get('connect')
        self._stage_name = connect.get('stage_name')
        self._delete_stage = connect.get('delete_stage', False)

        auth_type = connect.get('auth_type')
        if auth_type == 'access_token':
            self._engine = create_engine(
                f"snowflake://{connect['user']}:{connect['access_token']}@"\
                f"{connect['account']}/{connect['database']}/{connect['target_schema']}?warehouse={connect['warehouse']}"
            )
        else:
            raise Exception(f"Snowflake (destination-snowflake) does not support auth type '{auth_type}'")

    
    def write(self, ds:Dataset, progress_callback = None):
        # Write a dataset to the destination database 
        self._ds = ds

        if self._stage_name == None:
            self._stage_name = self._ds.meta('stage_name')

        with self._connect() as conn:

            for stream in ds.streams:

                # configure progress tracking
                progress = Progress(
                    f"destination+snowflake://{ds.namespace}/{stream.schema_name}/{stream.name}",
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

                target_table_name = f"{stream.schema_name}.{stream.name}"
                stage_table_name = f"{stream.schema_name}.__temp_{stream.name}"

                # drop target depending on sync mode
                if self._mode.type == Mode.FULL_REFRESH:
                    SQLDestination.drop_table(conn, target_table_name)

                # create a table for the stream if it doesn't exist
                SQLDestination.create_table_if_not_exists(conn, stream)

                # sql to create staging table
                stage_table_sql = SnowflakeSQLUtil.create_temp_table(
                    stage_table_name, 
                    target_table_name
                )

                # sql to copy from storage stage into the staging table
                copy_sql = SnowflakeSQLUtil.copy_into_table(
                    stage_table_name, 
                    self._stage_name, 
                    f".*{stream.schema_name}__{stream.name}.*\\.parquet"
                )

                # sql to MERGE the staging table into the target table
                merge_sql = SnowflakeSQLUtil.merge(
                    target_table_name,
                    stage_table_name,
                    stream.schema.names,
                    stream.primary_field
                )

                # run the copy
                with conn.begin():
                    progress.message("Loading records from stage")
                    conn.execute(text(stage_table_sql))
                    conn.execute(text(copy_sql))
                
                # run the merge
                with conn.begin():
                    progress.message("Merging records into target table")
                    conn.execute(text(merge_sql))
                
                # clean up
                SQLDestination.drop_table(conn, stage_table_name)

                 # drop target table after loading?
                if self._drop_after_complete == True:
                    SQLDestination.drop_table(conn, target_table_name)
            
                progress.update(ds.size(stream))

            # delete the loading stage if configured to
            if self._delete_stage:
                with conn.begin():
                    conn.execute(text(f"DROP STAGE {self._stage_name}"))


    
    def close(self):
        pass