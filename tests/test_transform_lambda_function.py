import pytest
import json
from moto import mock_aws
import boto3
from src.transform_lambda.lambda_handler import lambda_handler
import os
from botocore.exceptions import ClientError
from unittest.mock import patch


bucket_name = "ingestion_bucket_name"
key = "staff/raw_data.json"


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture
def ssm_mock(aws_credentials):
    with mock_aws():
        client = boto3.client("ssm")
        client.put_parameter(
            Name="ingestion_bucket_name",
            Description="A test parameter",
            Value="ingestion_bucket_name",
            Type="String",
            Overwrite=True
        )
        client.put_parameter(
            Name="processed_bucket_name",
            Description="A test parameter",
            Value="processed_bucket_name",
            Type="String",
            Overwrite=True
        )
        yield


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    """Create a mocked S3 client using moto."""
    with mock_aws():
        client = boto3.client("s3", region_name="eu-west-2")
        client.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )
        client.create_bucket(
            Bucket="processed_bucket_name", CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )
        yield client


@pytest.fixture
def s3_setup(s3_client):
    """Fixture to set up mocked S3 bucket and upload sample data."""
    sample_data = [{"staff_id": 1, "first_name": "John", "last_name": "Doe",
                    "department_id": 1, "email_address": "john.doe@example.com",
                    "created_at": "2022-11-03T14:20:49.962", "last_updated": "2022-11-03T14:20:49.962"}]
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(sample_data))

    extra_staff_data = [{"staff_id": 2, "first_name": "John", "last_name": "Doe",
                         "department_id": 1, "email_address": "john.doe@example.com",
                         "created_at": "2022-11-03T14:20:49.962", "last_updated": "2022-11-03T14:20:49.962"}]
    s3_client.put_object(Bucket=bucket_name, Key="staff/extra_staff_data.json",
                         Body=json.dumps(extra_staff_data))

    sample_data_1 = [{"department_id": 1, "department_name": "Sales", "location": "HQ",
                      "manager": "Abbey", "created_at": "2022-11-03T14:20:49.962",
                      "last_updated": "2022-11-03T14:20:49.962"},
                     {"department_id": 1, "department_name": "Sales", "location": "HQ",
                      "manager": "Abbey", "created_at": "2022-11-03T14:20:49.962",
                      "last_updated": "2022-11-03T14:20:49.962"}]
    s3_client.put_object(Bucket=bucket_name, Key="department/24/11/20/12-10-department.json",
                         Body=json.dumps(sample_data_1))
    sales_sample_data = [{"sales_order_id": 2, "created_at": "2022-11-03T14:20:52.186",
                          "last_updated": "2022-11-03T14:20:52.186", "design_id": 3, "staff_id": 19,
                          "counterparty_id": 8, "units_sold": 42972, "unit_price": 3.94, "currency_id": 2,
                          "agreed_delivery_date": "2022-11-07",
                          "agreed_payment_date": "2022-11-08", "agreed_delivery_location_id": 8}]
    s3_client.put_object(Bucket=bucket_name, Key="sales_order/24/11/20/12-10-sales_order.json",
                         Body=json.dumps(sales_sample_data))
    sample_address_data = [{"address_id": 1, "address_line_1": "6826 Herzog Via", "address_line_2": None,
                            "district": "Avon", "city": "New Patienceburgh", "postal_code": "28441",
                            "country": "Turkey", "phone": "1803 637401",
                            "created_at": "2022-11-03T14:20:49.962",
                            "last_updated": "2022-11-03T14:20:49.962"}]
    s3_client.put_object(Bucket=bucket_name, Key="address/24/11/20/12-10-address.json",
                         Body=json.dumps(sample_address_data))
    sample_payment_data = [{"payment_id": 1, "created_at": "2022-11-03T14:20:52.186",
                            "last_updated": "2022-11-03T15:20:52.186", "transaction_id": 1,
                            "counterparty_id": 1, "payment_amount": 42.50, "currency_id": 2,
                            "payment_type_id": 1, "paid": True, "payment_date": "2022-11-07",
                            "company_ac_number": 1, "counterparty_ac_number": 8}]
    s3_client.put_object(Bucket=bucket_name, Key="payment/24/11/20/12-10-payment.json",
                         Body=json.dumps(sample_payment_data))
    yield


@pytest.fixture
def mock_s3_client_error():
    """Fixture to mock S3 client raising a ClientError for get_object."""
    with patch("src.transform_lambda.lambda_handler.s3_client.get_object") as mock_get_object:
        mock_get_object.side_effect = ClientError(
            error_response={"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}},
            operation_name="GetObject",
        )
        yield mock_get_object


@mock_aws
class TestLambdaHandler:
    def test_lambda_handler_success(self, ssm_mock, s3_setup, caplog):
        """Test lambda_handler with valid event and data."""
        event = {"Records": [{"s3": {"bucket": {"name": bucket_name}, "object": {"key": key}}}]}
        context = {}
        response = lambda_handler(event, context)
        assert response == "Successfully ran"
        assert "Successfully processed staff data to Parquet." in caplog.text

    def test_records_not_in_event(self):
        """Test lamda_handler with records not in event"""
        event = {}
        context = {}
        with pytest.raises(ValueError, match="Invalid event structure:"):
            lambda_handler(event, context)

    def test_lambda_handler_empty_file(self, s3_client, mock_s3_client_error):
        """Test lambda_handler with an empty S3 file."""

        s3_client.put_object(Bucket=bucket_name, Key=key, Body="")

        event = {"Records": [{"s3": {"bucket": {"name": bucket_name}, "object": {"key": key}}}]}

        context = {}
        with pytest.raises(ClientError, match="The specified key does not exist."):
            lambda_handler(event, context)

    def test_lambda_handler_nonexistent_key(self, mock_s3_client_error):
        """Test lambda_handler with nonexistent S3 object."""
        non_existent_key = "nonexistent/key.json"

        event = {"Records": [{"s3": {"bucket": {"name": bucket_name}, "object": {"key": non_existent_key}}}]}

        context = {}
        with pytest.raises(ClientError, match="The specified key does not exist."):
            lambda_handler(event, context)

    def test_lambda_handler_multiple_records(self, s3_setup, ssm_mock, caplog):
        """Test lambda_handler with multiple records in the event."""
        # Upload another object to the bucket
        event = {
            "Records": [
                {"s3": {"bucket": {"name": bucket_name}, "object": {"key": "staff/extra_staff_data.json"}}},
                {"s3": {"bucket": {"name": bucket_name}, "object": {"key": key}}}
            ]
        }
        context = {}
        response = lambda_handler(event, context)
        assert response == "Successfully ran"
        assert "Successfully processed staff data to Parquet." in caplog.text

    def test_lambda_handler_processes_dim_date_table_from_sales_data(self, s3_client, ssm_mock, s3_setup,
                                                                     caplog):
        """Test lambda_handler with sales order data creates dim_date as well as sales."""
        event = {"Records": [{"s3": {"bucket": {"name": bucket_name}, "object": {
            "key": "sales_order/24/11/20/12-10-sales_order.json"}}}]}
        context = {}
        response = lambda_handler(event, context)
        assert response == "Successfully ran"
        assert "Successfully processed fact_sales_order data to Parquet." in caplog.text
        assert "Successfully processed dim_date data to Parquet." in caplog.text

    def test_lambda_handler_address_data_to_dim_location(self, s3_setup, ssm_mock, caplog):
        """Test lambda handler for address data"""
        event = {"Records": [{"s3": {"bucket": {"name": bucket_name}, "object": {
                 "key": "address/24/11/20/12-10-address.json"}}}]}
        context = {}
        response = lambda_handler(event, context)
        assert response == "Successfully ran"
        assert "Successfully processed address data to Parquet." in caplog.text

    def test_lambda_handler_payment_data_to_fact_payment(self, s3_setup, ssm_mock, caplog):
        """Test lambda handler for payment data"""
        event = {"Records": [{"s3": {"bucket": {"name": bucket_name}, "object": {
                 "key": "payment/24/11/20/12-10-payment.json"}}}]}
        context = {}
        response = lambda_handler(event, context)
        assert response == "Successfully ran"
        assert "Successfully processed payment data to Parquet." in caplog.text
