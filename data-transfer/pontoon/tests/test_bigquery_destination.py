import pytest
from pontoon.destination.bigquery_destination import BigQuerySQLUtil  

class TestBigQuerySQLUtil:
    

    def test_load_from_gcs(self):
        table_name = "my_table"
        gcs_uri = "gs://my_bucket/my_data"
        expected_sql = (
            "LOAD DATA OVERWRITE my_table "
            "FROM FILES (format = 'PARQUET',uris = ['gs://my_bucket/my_data*.parquet']);"
        )
        assert BigQuerySQLUtil.load_from_gcs(table_name, gcs_uri) == expected_sql

    def test_create_table_if_not_exists(self):
        source_table_name = "my_table"
        new_table_name= "my_new_table"
        expected_sql = (
            "CREATE TABLE IF NOT EXISTS my_new_table AS SELECT * FROM my_table WHERE 1=0"
        )
        assert BigQuerySQLUtil.create_table_if_not_exists(source_table_name, new_table_name) == expected_sql

    def test_merge(self):
        target_table_name = "target_table"
        stage_table_name = "stage_table"
        cols = ["id", "name", "value"]
        primary_key = "id"
        expected_sql = (
            "MERGE INTO target_table AS target "
            "USING stage_table AS stage "
            "ON target.id = stage.id "
            "WHEN MATCHED THEN "
            "UPDATE SET target.name=stage.name,target.value=stage.value "
            "WHEN NOT MATCHED THEN "
            "INSERT (id,name,value) "
            "VALUES (stage.id,stage.name,stage.value)"
        )
        assert BigQuerySQLUtil.merge(target_table_name, stage_table_name, cols, primary_key) == expected_sql

    def test_merge_with_different_columns(self):
        target_table_name = "target_table"
        stage_table_name = "stage_table"
        cols = ["id", "description", "amount"]
        primary_key = "id"
        expected_sql = (
            "MERGE INTO target_table AS target "
            "USING stage_table AS stage "
            "ON target.id = stage.id "
            "WHEN MATCHED THEN "
            "UPDATE SET target.description=stage.description,target.amount=stage.amount "
            "WHEN NOT MATCHED THEN "
            "INSERT (id,description,amount) "
            "VALUES (stage.id,stage.description,stage.amount)"
        )
        assert BigQuerySQLUtil.merge(target_table_name, stage_table_name, cols, primary_key) == expected_sql
