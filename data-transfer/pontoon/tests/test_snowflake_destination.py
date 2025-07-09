import pytest
from pontoon.destination.snowflake_destination import SnowflakeSQLUtil

class TestSnowflakeSQLUtil:

    def test_create_temp_table(self):
        temp_table_name = "temp_table"
        source_table_name = "source_table"
        expected_sql = f"CREATE TEMPORARY TABLE {temp_table_name} LIKE {source_table_name}"
        assert SnowflakeSQLUtil.create_temp_table(temp_table_name, source_table_name) == expected_sql

    def test_copy_into_table(self):
        target_table_name = "target_table"
        stage_name = "stage"
        pattern = "file_pattern.*"
        expected_sql = (
            f"COPY INTO {target_table_name} "
            f"FROM @{stage_name} "
            f"FILE_FORMAT = (TYPE = PARQUET) "
            f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
            f"PATTERN = '{pattern}'"
        )
        assert SnowflakeSQLUtil.copy_into_table(target_table_name, stage_name, pattern) == expected_sql

    def test_merge(self):
        target_table_name = "target_table"
        stage_table_name = "stage_table"
        cols = ["id", "name", "value"]
        primary_key = "id"
        cols_str = ",".join(cols)
        cols_stage_str = ",".join([f"stage.{col}" for col in cols])
        update_set_str = ",".join([f"target.{col}=stage.{col}" for col in cols if col != primary_key])
        expected_sql = (
            f"MERGE INTO {target_table_name} AS target "
            f"USING {stage_table_name} AS stage "
            f"ON target.{primary_key} = stage.{primary_key} "
            f"WHEN MATCHED THEN "
            f"UPDATE SET {update_set_str} "
            f"WHEN NOT MATCHED THEN "
            f"INSERT ({cols_str}) "
            f"VALUES ({cols_stage_str})"
        )
        assert SnowflakeSQLUtil.merge(target_table_name, stage_table_name, cols, primary_key) == expected_sql
