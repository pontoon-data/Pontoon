from typing import List, Dict, Tuple, Generator, Any
from sqlalchemy import text

from pontoon.base import Destination, Dataset, Stream, Record, Progress, Mode
from pontoon.destination.sql_destination import SQLDestination


class SnowflakeSQLUtil:

    @staticmethod
    def create_temp_table(temp_table_name:str, source_table_name:str) -> str:
        return f"CREATE TEMPORARY TABLE {temp_table_name} LIKE {source_table_name}"

    @staticmethod
    def copy_into_table(target_table_name:str, stage_name:str, pattern:str) -> str:
        return f"COPY INTO {target_table_name} "\
               f"FROM @{stage_name} "\
               f"FILE_FORMAT = (TYPE = PARQUET) "\
               f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "\
               f"PATTERN = '{pattern}'"

    @staticmethod
    def merge(target_table_name:str, stage_table_name:str, cols:List[str], primary_key:str) -> str:
        cols_str = ','.join(cols)
        cols_stage_str = ','.join([f"stage.{col}" for col in cols])
        update_set_str = ','.join([f"target.{col}=stage.{col}" for col in cols if col != primary_key])

        merge_sql = f"MERGE INTO {target_table_name} AS target "\
                    f"USING {stage_table_name} AS stage "\
                    f"ON target.{primary_key} = stage.{primary_key} "\
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
        self._delete_stage = connect('delete_stage', False)

    
    def write(self, ds:Dataset, progress_callback = None):
        # Write a dataset to the destination database 
        self._ds = ds

        if self._stage_name == None:
            self._stage_name = self._ds.meta('stage_name')

        # setup callbacks for progress updates
        if callable(progress_callback):
            self._progress_callback = progress_callback
        else:
            self._progress_callback = lambda *args, **kwargs: None

        with self._connect() as conn:

            for stream in ds.streams:

                # initial progress
                self._progress_callback(Progress(-1, 0))

                # create a table for the stream if it doesn't exist
                table = SQLDestination.create_table_if_not_exists(conn, stream)

                target_table_name = f"{stream.schema_name}.{stream.name}"
                stage_table_name = f"{stream.schema_name}.__temp_{stream.name}"
                
                # delete records depending on sync mode
                if self._mode.type == Mode.FULL_REFRESH:
                    with conn.begin():
                        conn.execute(table.delete())

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
                    conn.execute(text(stage_table_sql))
                    conn.execute(text(copy_sql))
                
                # run the merge
                with conn.begin():
                    conn.execute(text(merge_sql))
                
                # clean up
                SQLDestination.drop_table(conn, stage_table_name)

                 # drop target table after loading?
                if self._drop_after_complete == True:
                    SQLDestination.drop_table(conn, target_table_name)
            

            # delete the loading stage if configured to
            if self._delete_stage:
                with conn.begin():
                    conn.execute(text(f"DROP STAGE {self._stage_name}"))


        # final progress update
        self._progress_callback(Progress(1, 0))

    
    def close(self):
        pass