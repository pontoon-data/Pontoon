from psycopg2 import sql
from psycopg2.extras import execute_values
from typing import List, Dict, Tuple, Generator, Any

from pontoon.base import Destination, Dataset, Stream, Record, Progress, Mode
from pontoon.destination.sql_destination import SQLDestination



class PostgresSQLUtil:
    """ A class to help generate Postgres specific SQL statements """

    @staticmethod
    def parse_table(full_name:str) -> (str, str):
        parts = full_name.split('.')
        if len(parts) == 2:
            return sql.Identifier(parts[0]), sql.Identifier(parts[1])
        else:
            return None, sql.Identifier(parts[0])


    @staticmethod
    def create_temp_table(table_name:str, like_table_name:str) -> str:
        temp_schema, temp_table = PostgresSQLUtil.parse_table(table_name)
        like_schema, like_table = PostgresSQLUtil.parse_table(like_table_name)

        if temp_schema:
            temp_table_sql = sql.SQL('{}.{}').format(temp_schema, temp_table)
        else:
            temp_table_sql = temp_table

        if like_schema:
            like_table_sql = sql.SQL('{}.{}').format(like_schema, like_table)
        else:
            like_table_sql = like_table

        return sql.SQL('CREATE TEMP TABLE {} (LIKE {})').format(
            temp_table_sql,
            like_table_sql
        )


    @staticmethod
    def drop_table(table_name:str):
        schema, table = PostgresSQLUtil.parse_table(table_name)
        if schema:
            return sql.SQL("DROP TABLE {}.{}").format(schema, table)
        else:
            return sql.SQL("DROP TABLE {}").format(table)


    @staticmethod
    def upsert(target_table_name:str, stage_table_name:str, cols:List[str], primary_key:str) -> str:
        # Split schema and table parts safely if needed

        target_schema, target_table = PostgresSQLUtil.parse_table(target_table_name)
        stage_schema, stage_table = PostgresSQLUtil.parse_table(stage_table_name)

        column_identifiers = [sql.Identifier(col) for col in cols]
        excluded_assignments = [
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
            for col in cols if col != primary_key
        ]

        # Build base SQL parts
        if target_schema:
            target_table_sql = sql.SQL("{}.{}").format(target_schema, target_table)
        else:
            target_table_sql = target_table

        if stage_schema:
            stage_table_sql = sql.SQL("{}.{}").format(stage_schema, stage_table)
        else:
            stage_table_sql = stage_table

        # Build the full SQL statement
        upsert_sql = sql.SQL("""
            INSERT INTO {target_table} ({columns})
            SELECT {columns}
            FROM {stage_table}
            ON CONFLICT ({pk})
            DO UPDATE SET {updates}
        """).format(
            target_table=target_table_sql,
            stage_table=stage_table_sql,
            columns=sql.SQL(', ').join(column_identifiers),
            pk=sql.Identifier(primary_key),
            updates=sql.SQL(', ').join(excluded_assignments)
        )

        return upsert_sql



class PostgresDestination(SQLDestination):
    """ A Destination that writes to Postgres:
            - creates the target table (but not schema) if not exists
            - batch inserts records to a temporary staging table
            - merges the staging table into the target table
    """


    def __init__(self, config):
        config['connect']['driver'] = 'postgresql+psycopg2'
        super().__init__(config)  # Call SQLDestination constructor
    

    def _write_batch(self, conn, stage_table_name:str, cols:List[str], batch:List[Record]):
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"INSERT INTO {stage_table_name} ({', '.join(cols)}) VALUES %s",
                [tuple(record.data) for record in batch]
            )


    def write(self, ds:Dataset, progress_callback = None):
        # Write a dataset to the destination database 
        self._ds = ds

        # setup callbacks for progress updates
        if callable(progress_callback):
            self._progress_callback = progress_callback
        else:
            self._progress_callback = lambda *args, **kwargs: None

        
        # initial progress
        self._progress_callback(Progress(-1, 0))

        # optionally rename destination schema
        target_schema = self._config['connect'].get('target_schema')
        if target_schema: 
            for stream in ds.streams:
                ds.rename_stream(
                    stream.name, 
                    stream.schema_name,
                    stream.name,           # keep the existing table name
                    target_schema,         # new schema name
                )

        # sync each stream
        for stream in ds.streams:

            with self._connect() as conn:
                # create target table for the stream if it doesn't exist
                table = SQLDestination.create_table_if_not_exists(conn, stream)


            # using the raw psycopg2 connection for efficiency
            conn = self._engine.raw_connection()
                
            target_table_name = f"{stream.schema_name}.{stream.name}"
            stage_table_name = f"temp_{stream.schema_name}_{stream.name}"

            # temporary staging table
            create_stage_sql = PostgresSQLUtil.create_temp_table(stage_table_name, target_table_name)
            
            # upsert statement (INSERT ON CONFLICT UPDATE)
            upsert_sql = PostgresSQLUtil.upsert(
                target_table_name,
                stage_table_name,
                stream.schema.names,
                stream.primary_field
            )

            # create the staging table
            with conn.cursor() as cur:
                cur.execute(create_stage_sql)

            conn.commit()
            
            # load into the stage table
            # note: standard postgresql doesn't support copy from cloud storage
            batch = []
            total_records = 0
            for record in ds.read(stream):
                batch.append(record)
                if len(batch) == self._chunk_size:
                    total_records += self._chunk_size
                    self._write_batch(conn, stage_table_name, stream.schema.names, batch)
                    self._progress_callback(Progress(-1, total_records))
                    batch = []
            
            if batch:
                total_records += len(batch)
                self._write_batch(conn, stage_table_name, stream.schema.names, batch)
                self._progress_callback(Progress(-1, total_records))
            
            conn.commit()
            
            with conn.cursor() as cur:
                # delete all records from the table
                if self._mode.type == Mode.FULL_REFRESH:
                    cur.execute(
                        sql.SQL("DELETE FROM {}.{}").format(
                            sql.Identifier(stream.schema_name),
                            sql.Identifier(stream.name)
                        )
                    )
            
                # upsert staging into target table
                cur.execute(upsert_sql)

                # drop the staging table
                with conn.cursor() as cur:
                    cur.execute(
                        PostgresSQLUtil.drop_table(stage_table_name)
                    )

                # drop target table after loading?
                if self._drop_after_complete == True:
                    with conn.cursor() as cur:
                        cur.execute(
                            PostgresSQLUtil.drop_table(target_table_name)
                        )

            conn.commit()

            conn.close()

        # final progress update
        self._progress_callback(Progress(1, 0))

    
    def close(self):
        pass