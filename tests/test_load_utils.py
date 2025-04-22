from moto import mock_aws
import boto3
import pytest
from src.load_lambda.load_utils import (
    list_new_from_s3,
    write_to_database,
    get_parquet_files,
    get_parameter,
    put_parameter,
    close_db_connection)
import os
from datetime import datetime
from time import sleep
from io import BytesIO
from src.db.connection import connect_to_test_db
from src.db.seed import seed_db
import pandas as pd
import re
from pg8000 import DatabaseError


def read_test_database(table_name: str) -> list[dict]:
    """Reads all data from a specified table in the test database

    Args:
        table_name (str): Table to read data from

    Returns:
        list[dict]: List of dictionaries in the format 
        [{column_1_name:column_1_value, column_2_name:column_2_value}, etc.]
    """
    conn = None
    try:
        conn = connect_to_test_db()
        query_str = f"SELECT row_to_json({table_name}) FROM {table_name}"  # nosec
        results = conn.run(query_str)
        formatted_data = [result[0] for result in results]
        return formatted_data
    finally:
        if conn:
            close_db_connection(conn)


def load_test_data(table_name: str) -> list[dict]:
    """Loads data from example data parquet files. Used within assert statements
    for data verification 

    Args:
        table_name (str): Name of table the data file is for

    Returns:
        list[dict]: List of dictionaries in the format 
        [{column_1_name:column_1_value, column_2_name:column_2_value}, etc.]
    """
    df = pd.read_parquet(f"data_examples/test_load_data/{table_name}.parquet")
    if table_name == "dim_date":
        df['date_id'] = df['date_id'].dt.strftime("%Y-%m-%d")
    formatted_data = df.to_dict("records")
    return formatted_data


@pytest.fixture(scope="function")
def create_db_tables():
    seed_db()


@pytest.fixture(scope="function")
def db_credentials():
    """Mocked Main DB Credentials"""
    os.environ["W_USER"] = os.getenv("TEST_USER")
    os.environ["W_PASSWORD"] = os.getenv("TEST_PASSWORD")
    os.environ["W_DATABASE"] = os.getenv("TEST_DATABASE")
    os.environ["W_HOST"] = os.getenv("TEST_HOST")
    os.environ["W_PORT"] = os.getenv("TEST_PORT")


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")
        s3_client.create_bucket(
            Bucket="processing-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3_client


class TestLoadUtils:
    def test_list_new_from_s3(self, s3_client):
        for i in range(3):
            s3_client.put_object(
                Bucket="processing-bucket", Key=f"testing/test_object_{i}.txt"
            )
            if i == 1:
                last_run = datetime.now()
            sleep(1)
        new_objects = list_new_from_s3(
            s3_client, last_run, "processing-bucket", "testing"
        )
        assert new_objects[0] == "testing/test_object_2.txt"

    def test_list_new_from_s3_no_new_files(self, s3_client):
        new_objects = list_new_from_s3(
            s3_client, None, "processing-bucket", "testing"
        )
        assert new_objects == []

    def test_list_new_from_s3_first_run(self, s3_client):
        for i in range(3):
            s3_client.put_object(
                Bucket="processing-bucket", Key=f"testing/test_object_{i}.txt"
            )
        new_objects = list_new_from_s3(
            s3_client, None, "processing-bucket", "testing"
        )
        assert new_objects == ["testing/test_object_0.txt", "testing/test_object_1.txt",
                               "testing/test_object_2.txt"]

    def test_get_parquet_file_returns_list_of_correct_length(self, s3_client):
        s3_client.upload_file(Bucket="processing-bucket", Key="test_staff.parquet",
                              Filename="data_examples/test_load_data/dim_staff.parquet")
        result = get_parquet_files(s3_client, ["test_staff.parquet"], "processing-bucket")
        assert len(result) == 1

    def test_get_parquet_file_returns_list_containing_correct_types(self, s3_client):
        s3_client.upload_file(Bucket="processing-bucket", Key="test_staff.parquet",
                              Filename="data_examples/test_load_data/dim_staff.parquet")
        result = get_parquet_files(s3_client, ["test_staff.parquet"], "processing-bucket")
        assert isinstance(result[0], BytesIO)

    def test_get_parquet_file_returns_all_files_requested(self, s3_client):
        for i in range(3):
            s3_client.upload_file(
                Bucket="processing-bucket",
                Key=f"test_staff_{i}.parquet",
                Filename="data_examples/test_load_data/dim_staff.parquet",
            )
        result = get_parquet_files(
            s3_client,
            ["test_staff_0.parquet", "test_staff_1.parquet", "test_staff_2.parquet"],
            "processing-bucket",
        )
        assert len(result) == 3


class TestDatabaseWrite:
    def test_write_to_database_dimension_tables(self, create_db_tables, db_credentials):
        tables = ["dim_date", "dim_location", "dim_design", "dim_currency", "dim_counterparty"]
        for table in tables:
            write_to_database(table, [f"data_examples/test_load_data/{table}.parquet"])
            result = read_test_database(table)
            assert result == load_test_data(table)

    def test_write_to_database_dimensions_duplicates_are_handled(
        self, create_db_tables, db_credentials
    ):
        tables = ["dim_date", "dim_location", "dim_design", "dim_currency", "dim_counterparty"]
        for table in tables:
            for _ in range(2):
                write_to_database(table, [f"data_examples/test_load_data/{table}.parquet"])
            result = read_test_database(table)
            assert result == load_test_data(table)

    def test_write_to_database_fact_sales_order(self, create_db_tables, db_credentials):
        tables = ["dim_location", "dim_design", "dim_currency", "dim_counterparty", "dim_date"]
        for table in tables:
            write_to_database(table, [f"data_examples/test_load_data/{table}.parquet"])
        write_to_database("fact_sales_order", ["data_examples/test_load_data/fact_sales_order.parquet"])
        result = read_test_database("fact_sales_order")
        date_regex = r"^\d{4}-\d{2}-\d{2}$"
        time_regex = r"^\d{2}:\d{2}:\d{2}.?\d{0,6}$"
        for item in result:
            assert re.match(date_regex, item["agreed_delivery_date"])
            assert isinstance(item["agreed_delivery_location_id"], int)
            assert re.match(date_regex, item["agreed_payment_date"])
            assert isinstance(item["counterparty_id"], int)
            assert re.match(date_regex, item["created_date"])
            assert re.match(time_regex, item["created_time"])
            assert isinstance(item["currency_id"], int)
            assert isinstance(item["design_id"], int)
            assert re.match(date_regex, item["last_updated_date"])
            assert re.match(time_regex, item["last_updated_time"])
            assert isinstance(item["sales_order_id"], int)
            assert isinstance(item["sales_record_id"], int)
            assert isinstance(item["sales_staff_id"], int)
            assert isinstance(item["unit_price"], float)
            assert isinstance(item["units_sold"], int)

    def test_write_to_database_fact_sales_order_duplicates_are_handled(self, create_db_tables, db_credentials):
        tables = ["dim_location", "dim_design", "dim_currency", "dim_counterparty", "dim_date"]
        for table in tables:
            write_to_database(table, [f"data_examples/test_load_data/{table}.parquet"])
        for _ in range(2):
            write_to_database("fact_sales_order", ["data_examples/test_load_data/fact_sales_order.parquet"])
        result = read_test_database("fact_sales_order")
        date_regex = r"^\d{4}-\d{2}-\d{2}$"
        time_regex = r"^\d{2}:\d{2}:\d{2}.?\d{0,6}$"
        for item in result:
            assert re.match(date_regex, item["agreed_delivery_date"])
            assert isinstance(item["agreed_delivery_location_id"], int)
            assert re.match(date_regex, item["agreed_payment_date"])
            assert isinstance(item["counterparty_id"], int)
            assert re.match(date_regex, item["created_date"])
            assert re.match(time_regex, item["created_time"])
            assert isinstance(item["currency_id"], int)
            assert isinstance(item["design_id"], int)
            assert re.match(date_regex, item["last_updated_date"])
            assert re.match(time_regex, item["last_updated_time"])
            assert isinstance(item["sales_order_id"], int)
            assert isinstance(item["sales_record_id"], int)
            assert isinstance(item["sales_staff_id"], int)
            assert isinstance(item["unit_price"], float)
            assert isinstance(item["units_sold"], int)
        assert len(result) == 50

    def test_write_to_database_re_raises_database_errors_not_related_to_duplicate_data(self,
                                                                                       create_db_tables,
                                                                                       db_credentials):
        with pytest.raises(DatabaseError):
            write_to_database("dim_test", ["data_examples/test_load_data/dim_design.parquet"])


@mock_aws
class TestGetBucketName:
    def test_get_parameter_returns_correct_value(self):
        ssm_client = boto3.client("ssm")
        ssm_client.put_parameter(Name="test_parameter", Value="test_value", Type="String")
        result = get_parameter(ssm_client, "test_parameter")
        assert result == "test_value"


@mock_aws
class TestPutParameter:
    def test_put_parameter_stores_correct_value(self):
        ssm_client = boto3.client("ssm")
        ssm_client.put_parameter(Name="load_last_run", Value="test_value", Type="String")
        put_parameter(ssm_client, datetime(2023, 10, 1))
        result = ssm_client.get_parameter(Name="load_last_run")
        assert result["Parameter"]["Value"] == "2023_10_01-00_00_00"
