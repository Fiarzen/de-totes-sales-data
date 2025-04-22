import pytest
from src.extract_lambda.utils import get_data, put_object, get_parameter, put_parameter
from src.extract_lambda.connection import create_conn, close_db_connection
import datetime
from moto import mock_aws
import boto3
import os


@pytest.fixture()
def db():
    db = create_conn()
    yield db
    close_db_connection(db)


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


class TestGetData:
    def test_get_data_correct_type(self, db):
        result = get_data("currency", datetime.datetime(2022, 10, 1))

        assert isinstance(result, list)
        for currency in result:
            assert isinstance(currency, dict)

    def test_all_dicts_contain_all_keys(self, db):
        keys = ["currency_id", "currency_code", "created_at", "last_updated"]
        result = get_data("currency", datetime.datetime(2022, 10, 1))
        for currency in result:
            assert all([key in currency for key in keys])

    def test_objects_have_correct_value_type(self, db):
        result = get_data("currency", datetime.datetime(2022, 10, 1))
        expected = {"currency_id": int, "currency_code": str, "created_at": str, "last_updated": str}
        for currency in result:
            for key, value in expected.items():
                assert isinstance(currency[key], value)

    def test_results_correctly_filtered_by_date(self, db):
        result = get_data("staff", datetime.datetime(2022, 10, 1))
        for staff in result:
            staff_last_updated = datetime.datetime.fromisoformat(staff["last_updated"])
            assert staff_last_updated > datetime.datetime(2022, 10, 1)

    def test_when_date_is_none(self, db):
        result_1 = get_data("staff", None)
        result_2 = get_data("staff", datetime.datetime(2023, 10, 1))
        assert len(result_1) > len(result_2)


class TestPutObject:
    def test_put_object_successfully(self, s3_client):
        data = [{"test": 1}]
        put_object(s3_client, data, "my_table", "test-bucket", datetime.datetime(2024, 11, 12, 11, 52))
        objects = s3_client.list_objects_v2(Bucket="test-bucket")
        assert objects["Contents"][0]["Key"] == "my_table/2024/11/12/11-52-my_table.json"

    def test_correct_data_in_bucket(self, s3_client):
        data = [{"test": 1}]
        put_object(s3_client, data, "my_table", "test-bucket", datetime.datetime(2024, 11, 12, 11, 52))
        objects = s3_client.get_object(Bucket="test-bucket", Key="my_table/2024/11/12/11-52-my_table.json")
        assert objects["Body"].read().decode("utf-8") == '[{"test": 1}]'


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
        ssm_client.put_parameter(Name="lambda_last_run", Value="test_value", Type="String")
        put_parameter(ssm_client, datetime.datetime(2023, 10, 1))
        result = ssm_client.get_parameter(Name="lambda_last_run")
        assert result["Parameter"]["Value"] == "2023_10_01-00_00"
