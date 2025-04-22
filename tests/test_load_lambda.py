from src.load_lambda.lambda_handler import load_data
import boto3
from botocore.exceptions import ClientError
from moto import mock_aws
from pg8000 import DatabaseError
import pytest
import os
from src.db.seed import seed_db
from datetime import datetime
from tests.test_load_utils import read_test_database, load_test_data
import re
from time import sleep

"""
Tests:
handles no new data
handles first run DONE
handles only new data DONE


handles databases error
handles unexpected error
handles ClientError
"""


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
def ssm_client(aws_credentials):
    with mock_aws():
        ssm_client = boto3.client("ssm", region_name="eu-west-2")
        ssm_client.put_parameter(Name="processed_bucket_name",
                                 Value="processing-bucket", Type="String")
        yield ssm_client


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")
        s3_client.create_bucket(
            Bucket="processing-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        folder_list = ["dim_date", "dim_counterparty", "dim_currency", "dim_design",
                       "dim_location", "dim_staff", "fact_sales_order"]
        current_date = datetime.now().strftime("%Y/%m/%d/%H_%M")
        for folder in folder_list:
            s3_client.upload_file(
                Bucket="processing-bucket",
                Filename=f"data_examples/test_load_data/{folder}.parquet",
                Key=f"{folder}/transformed/{current_date}-{folder}.parquet",
            )
        yield s3_client


class TestLoadLambda:
    def test_load_lambda_last_run_is_none(self, create_db_tables, db_credentials,
                                          s3_client, ssm_client):
        ssm_client.put_parameter(Name="load_last_run",
                                 Value="None",
                                 Type="String")
        load_data({}, {})
        folder_list = ["dim_date", "dim_counterparty", "dim_currency", "dim_design",
                       "dim_location", "dim_staff"]
        for folder in folder_list:
            result = read_test_database(folder)
            assert result == load_test_data(folder)

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

    def test_load_lambda_only_gets_new_data(self, create_db_tables, db_credentials,
                                            s3_client, ssm_client):
        last_run_value = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
        sleep(3)
        ssm_client.put_parameter(Name="load_last_run",
                                 Value=last_run_value,
                                 Type="String")
        folder_list = ["dim_date", "dim_counterparty", "dim_currency", "dim_design",
                       "dim_location", "dim_staff"]
        current_date = datetime.now().strftime("%Y/%m/%d/%H_%M")
        for folder in folder_list:
            s3_client.upload_file(
                Bucket="processing-bucket",
                Filename=f"data_examples/test_load_data/{folder}.parquet",
                Key=f"{folder}/transformed/{current_date}-{folder}-1.parquet",
            )

        load_data({}, {})

        for folder in folder_list:
            result = read_test_database(folder)
            assert result == load_test_data(folder)
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


@mock_aws
class TestLoadLambdaErrors:
    def test_load_lambda_handles_database_error(self, create_db_tables, db_credentials,
                                                ssm_client, caplog):
        last_run_value = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
        ssm_client.put_parameter(Name="load_last_run",
                                 Value=last_run_value,
                                 Type="String")
        sleep(1)
        s3_client = boto3.client("s3", region_name="eu-west-2")
        s3_client.create_bucket(
            Bucket="processing-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.upload_file(Bucket="processing-bucket",
                              Filename="data_examples/test_load_data/fail_dim_date.parquet",
                              Key="dim_date/transformed/dim_date.parquet")
        load_data({}, {})
        assert "Database Error" in caplog.text

    def test_load_lambda_handles_client_error(self, ssm_client):
        assert "ClientError" in load_data({}, {})

    def test_load_lambda_handles_all_errors(self, ssm_client):
        ssm_client.put_parameter(Name="load_last_run",
                                 Value="test",
                                 Type="String")
        assert "Unexpected error" in load_data({}, {})
