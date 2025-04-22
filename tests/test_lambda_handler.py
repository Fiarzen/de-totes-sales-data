from src.extract_lambda.lambda_handler import lambda_handler
import boto3
import datetime
from moto import mock_aws
import pytest
import os
import json
from unittest.mock import patch


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
            Bucket="test-bucket", CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )
        yield s3_client


@pytest.fixture(scope="function")
def ssm_client(aws_credentials):
    with mock_aws():
        ssm_client = boto3.client("ssm", region_name="eu-west-2")
        ssm_client.put_parameter(Name="lambda_last_run", Value="2020_11_11-10_10", Type="String")
        ssm_client.put_parameter(Name="ingestion_bucket_name", Value="test-bucket", Type="String")
        yield ssm_client


class TestLambdaHandler:
    @patch("src.extract_lambda.lambda_handler.dt")
    def test_objects_put_in_test_bucket(self, dt_mock, s3_client, ssm_client):

        dt_mock.now.return_value = datetime.datetime(2020, 12, 12, 12, 12, 12)
        dt_mock.strptime.return_value = datetime.datetime(2020, 12, 12, 12, 12, 12)
        lambda_handler({}, {})

        objects = s3_client.list_objects_v2(Bucket="test-bucket")
        tables = [
            "address",
            "design",
            "counterparty",
            "sales_order",
            "transaction",
            "payment",
            "purchase_order",
            "payment_type",
            "currency",
            "department",
            "staff",
        ]
        file_keys = [item["Key"] for item in objects["Contents"]]
        for table in tables:
            assert f"{table}/2020/12/12/12-12-{table}.json" in file_keys

    def test_number_of_objects_in_bucket(self, s3_client, ssm_client):
        lambda_handler({}, {})
        objects = s3_client.list_objects_v2(Bucket="test-bucket")
        assert len(objects["Contents"]) == 11

    @patch("src.extract_lambda.lambda_handler.dt")
    def test_object_value_types_in_test_bucket(self, dt_mock, s3_client, ssm_client):

        dt_mock.now.return_value = datetime.datetime(2020, 12, 12, 12, 12, 12)
        dt_mock.strptime.return_value = datetime.datetime(2020, 12, 12, 12, 12, 12)
        lambda_handler({}, {})

        object = s3_client.get_object(Bucket="test-bucket", Key="currency/2020/12/12/12-12-currency.json")
        currencies = json.loads(object["Body"].read().decode("utf-8"))
        for currency in currencies:
            assert isinstance(currency["currency_id"], int)
            assert isinstance(currency["currency_code"], str)
            assert isinstance(currency["created_at"], str)
            assert isinstance(currency["last_updated"], str)

    @patch("src.extract_lambda.lambda_handler.dt")
    def test_when_previous_time_is_none(self, dt_mock, s3_client, ssm_client):

        dt_mock.now.return_value = datetime.datetime(2020, 12, 12, 12, 12, 12)
        dt_mock.strptime.return_value = datetime.datetime(2020, 12, 12, 12, 12, 12)
        ssm_client.put_parameter(Name="lambda_last_run", Value="None", Overwrite=True, Type="String")
        lambda_handler({}, {})
        object_1 = s3_client.get_object(
            Bucket="test-bucket", Key="transaction/2020/12/12/12-12-transaction.json"
        )

        dt_mock.now.return_value = datetime.datetime(2023, 10, 12, 12, 12, 12)
        dt_mock.strptime.return_value = datetime.datetime(2023, 10, 12, 12, 12, 12)
        ssm_client.put_parameter(
            Name="lambda_last_run", Value="2023_10_12-12_12", Overwrite=True, Type="String"
        )
        lambda_handler({}, {})
        object_2 = s3_client.get_object(
            Bucket="test-bucket", Key="transaction/2023/10/12/12-12-transaction.json"
        )
        currencies_1 = json.loads(object_1["Body"].read().decode("utf-8"))
        currencies_2 = json.loads(object_2["Body"].read().decode("utf-8"))
        assert len(currencies_1) > len(currencies_2)

    @mock_aws
    def test_returns_exception_errros(self):
        result = lambda_handler({}, {})
        assert "Unexpected Error" in result
