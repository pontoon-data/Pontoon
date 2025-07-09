import pytest
from typing import List
from pontoon.destination.redshift_destination import RedshiftSQLUtil  


class TestRedshiftSQLUtil:

    def test_create_temp_table(self):
        table_name = "temp_table"
        like_table_name = "source_table"
        expected_sql = "CREATE TEMP TABLE temp_table (LIKE source_table)"
        result = RedshiftSQLUtil.create_temp_table(table_name, like_table_name)
        assert result == expected_sql

    def test_copy_from_s3(self):
        table_name = "target_table"
        s3_uri = "s3://my-bucket/my-path/"
        iam_role = "arn:aws:iam::123456789012:role/MyRedshiftRole"
        s3_region = "us-east-1"
        expected_sql = (
            "COPY target_table "
            "FROM 's3://my-bucket/my-path/' "
            "IAM_ROLE 'arn:aws:iam::123456789012:role/MyRedshiftRole' "
            "FORMAT AS PARQUET "
            "REGION 'us-east-1'"
        )
        result = RedshiftSQLUtil.copy_from_s3(table_name, s3_uri, iam_role, s3_region)
        assert result == expected_sql

    def test_upsert(self):
        target_table_name = "target_table"
        stage_table_name = "stage_table"
        cols = ["id", "name", "email"]
        primary_key = "id"
        
        expected_delete_sql = (
            "DELETE FROM target_table "
            "USING stage_table "
            "WHERE target_table.id = stage_table.id"
        )
        expected_insert_sql = (
            "INSERT INTO target_table (id,name,email) "
            "(SELECT id,name,email FROM stage_table)"
        )
        
        delete_sql, insert_sql = RedshiftSQLUtil.upsert(target_table_name, stage_table_name, cols, primary_key)
        
        assert delete_sql == expected_delete_sql
        assert insert_sql == expected_insert_sql
