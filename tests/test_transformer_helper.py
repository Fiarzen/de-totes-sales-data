import pytest
import pandas as pd
from src.transform_lambda.transform_helpers import (
    transform_dim_staff,
    transform_dim_location,
    transform_dim_design,
    transform_fact_sales_order,
    transform_dim_currency,
    transform_dim_counterparty,
    transform_dim_date,
    save_to_parquet,
    transform_data,
    transform_dim_transaction,
    transform_dim_payment_type,
    transform_fact_payment,
    transform_fact_purchase_order)
from moto import mock_aws
import boto3
import pyarrow.parquet as pq
import io
import os
import datetime
import json

bucket_name = "test-bucket"


@pytest.fixture(scope="class")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def ssm_mock(aws_credentials):
    with mock_aws():
        client = boto3.client("ssm")
        client.put_parameter(
            Name="ingestion_bucket_name",
            Description="A test parameter",
            Value="test-bucket",
            Type="String",
            Overwrite=True,
        )
        client.put_parameter(
            Name="processed_bucket_name",
            Description="A test parameter",
            Value="test-processed-bucket",
            Type="String",
            Overwrite=True,
        )
        yield


@pytest.fixture(scope="class")
def s3_client(aws_credentials):
    """Create a mocked S3 client using moto."""
    with mock_aws():
        client = boto3.client("s3", region_name="eu-west-2")
        client.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )
        client.create_bucket(
            Bucket="test-processed-bucket", CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )
        yield client


@pytest.fixture
def s3_setup(s3_client):
    """Fixture to set up mocked S3 bucket and upload sample data."""
    sample_data = [{"department_id": 1, "department_name": "Sales", "location": "HQ",
                    "manager": "Abbey", "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962"},
                   {"department_id": 2, "department_name": "Sales", "location": "HQ",
                    "manager": "Abbey", "created_at": "2022-11-03T14:20:49.962",
                    "last_updated": "2022-11-03T14:20:49.962"}]
    s3_client.put_object(Bucket=bucket_name, Key="department/24/11/20/12-00-department.json",
                         Body=json.dumps(sample_data))
    sample_data_1 = [{"department_id": 3, "department_name": "Sales", "location": "HQ",
                      "manager": "Abbey", "created_at": "2022-11-03T14:20:49.962",
                      "last_updated": "2022-11-03T14:20:49.962"}]
    s3_client.put_object(Bucket=bucket_name, Key="department/24/11/20/12-10-department.json",
                         Body=json.dumps(sample_data_1))
    sample_data_2 = [{"address_id": 1, "address_line_1": "6826 Herzog Via", "address_line_2": None,
                      "district": "Avon", "city": "New Patienceburgh", "postal_code": "28441",
                      "country": "Turkey", "phone": "1803 637401", "created_at": "2022-11-03T14:20:49.962",
                      "last_updated": "2022-11-03T14:20:49.962"}]
    s3_client.put_object(Bucket=bucket_name, Key="address/24/11/20/12-10-department.json",
                         Body=json.dumps(sample_data_2))
    yield


@pytest.fixture
def staff_data():
    """Sample data for testing transform_dim_staff."""
    return [
        {
            "staff_id": 1,
            "first_name": "Abbey",
            "last_name": "Ola",
            "email_address": "abbey.ola@me.com",
            "department_id": 1,
            "created_at": "2022-11-03T14:20:49.962",
            "last_updated": "2022-11-03T14:20:49.962",
        },
    ]


@pytest.fixture
def address_data():
    """Sample data for testing transform_dim_location"""
    return [{"address_id": 1, "address_line_1": "6826 Herzog Via", "address_line_2": None,
             "district": "Avon", "city": "New Patienceburgh", "postal_code": "28441", "country": "Turkey",
             "phone": "1803 637401", "created_at": "2022-11-03T14:20:49.962",
             "last_updated": "2022-11-03T14:20:49.962"}]


@pytest.fixture
def design_data():
    """Sample data for testing transform_dim_design"""
    return [{"design_id": 8, "created_at": "2022-11-03T14:20:49.962", "design_name": "Wooden",
             "file_location": "/usr", "file_name": "wooden-20220717-npgz.json",
             "last_updated": "2022-11-03T14:20:49.962"}]


@pytest.fixture
def sales_order_data():
    """Sample data for testing transform_fact_sales_order"""
    return [{"sales_order_id": 2, "created_at": "2022-11-03T14:20:52.186",
             "last_updated": "2022-11-03T15:20:52.186", "design_id": 3, "staff_id": 19, "counterparty_id": 8,
             "units_sold": 42972, "unit_price": 3.94, "currency_id": 2, "agreed_delivery_date": "2022-11-07",
             "agreed_payment_date": "2022-11-08", "agreed_delivery_location_id": 8},
            {"sales_order_id": 3, "created_at": "2022-11-03T14:20:52.186",
             "last_updated": "2022-11-03T14:20:52.186", "design_id": 3, "staff_id": 19, "counterparty_id": 8,
             "units_sold": 42972, "unit_price": 3.94, "currency_id": 2, "agreed_delivery_date": "2022-11-07",
             "agreed_payment_date": "2023-11-08", "agreed_delivery_location_id": 8}]


@pytest.fixture
def currency_data():
    """Sample data for testing transform_dim_currency"""
    return [{"currency_id": 1, "currency_code": "GBP", "created_at": "2022-11-03T14:20:49.962",
             "last_updated": "2022-11-03T14:20:49.962"},
            {"currency_id": 2, "currency_code": "USD", "created_at": "2022-11-03T14:20:49.962",
             "last_updated": "2022-11-03T14:20:49.962"},
            {"currency_id": 3, "currency_code": "EUR", "created_at": "2022-11-03T14:20:49.962",
             "last_updated": "2022-11-03T14:20:49.962"}]


@pytest.fixture
def counterparty_data():
    """Sample data for testing transform_dim_counterparty"""
    return [{"counterparty_id": 1, "counterparty_legal_name": "Fahey and Sons",
             "legal_address_id": 1, "commercial_contact": "Micheal Toy",
             "delivery_contact": "Mrs. Lucy Runolfsdottir", "created_at": "2022-11-03T14:20:51.563",
             "last_updated": "2022-11-03T14:20:51.563"}]


@pytest.fixture
def transaction_data():
    """Sample data for testing transform_dim_transaction"""
    return [{"transaction_id": 1, "transaction_type": "cash", "sales_order_id": 2,
             "purchase_order_id": 1, "created_at": "2022-11-03T14:20:49.962",
             "last_updated": "2022-11-03T14:20:49.962"}]


@pytest.fixture
def payment_type_data():
    """Sample data for testing transform_dim_payment_type"""
    return [{"payment_type_id": 1, "payment_type_name": "name",
             "created_at": "2022-11-03T14:20:49.962",
             "last_updated": "2022-11-03T14:20:49.962"}]


@pytest.fixture
def payment_data():
    """Sample data for testing transform_fact_payment"""
    return [{"payment_id": 1, "created_at": "2022-11-03T14:20:52.186",
             "last_updated": "2022-11-03T15:20:52.186", "transaction_id": 1, "counterparty_id": 1,
             "payment_amount": 42.50, "currency_id": 2, "payment_type_id": 1,
             "paid": True, "payment_date": "2022-11-07",
             "company_ac_number": 1, "counterparty_ac_number": 8}]


@pytest.fixture
def purchase_order_data():
    """Sample data for testing transform_fac_purchase_order"""
    return [{"purchase_order_id": 1, "created_at": "2022-11-03T14:20:52.186",
             "last_updated": "2022-11-03T15:20:52.186", "staff_id": 1, "counterparty_id": 1,
             "item_code": "code", "item_quantity": 1, "item_unit_price": 10.45,
             "currency_id": 1, "agreed_delivery_date": "2022-11-07",
             "agreed_payment_date": "2022-11-07", "agreed_delivery_location_id": 1}]


@mock_aws
class TestTransformHelpers:
    def test_transform_dim_staff(self, staff_data, ssm_mock, s3_setup):
        """Test transforming staff data to dim_staff format."""
        df = transform_dim_staff(staff_data)
        assert isinstance(df, pd.DataFrame)
        assert "staff_id" in df.columns
        assert "first_name" in df.columns
        assert "department_name" in df.columns
        assert df.iloc[0]["first_name"] == "Abbey"

    def test_transform_dim_staff_no_staff_raise_key_error(self):
        """Test transforming when no staff data is provided."""
        empty_staff_data = []
        with pytest.raises(KeyError):
            transform_dim_staff(empty_staff_data)

    def test_transform_dim_location(self, address_data):
        """Test transforming address data to dim_location format"""
        df = transform_dim_location(address_data)
        assert isinstance(df, pd.DataFrame)
        assert "location_id" in df.columns
        assert df.iloc[0]["district"] == "Avon"

    def test_transform_dim_design(self, design_data):
        """Test transform design data to dim_design format"""
        df = transform_dim_design(design_data)
        assert isinstance(df, pd.DataFrame)
        assert "design_id" in df.columns
        assert df.iloc[0]["design_name"] == "Wooden"

    def test_transform_fact_sales_order(self, sales_order_data):
        """Test transform sales order data to fact_sales_order"""
        df = transform_fact_sales_order(sales_order_data)
        assert isinstance(df, pd.DataFrame)
        assert "sales_order_id" in df.columns
        assert df.iloc[0]["created_date"] == datetime.date(2022, 11, 3)

    def test_transform_dim_currency(self, currency_data):
        """Test transform currency data to dim_currency format"""
        df = transform_dim_currency(currency_data)
        assert isinstance(df, pd.DataFrame)
        assert "currency_id" in df.columns
        assert df.iloc[0]["currency_code"] == "GBP"

    def test_transform_dim_counterparty(self, counterparty_data, s3_setup, ssm_mock):
        """Test transform counterparty data to dim_counterparty"""
        df = transform_dim_counterparty(counterparty_data)
        assert isinstance(df, pd.DataFrame)
        assert "counterparty_legal_address_line_1" in df.columns
        assert df.iloc[0]["counterparty_legal_country"] == "Turkey"

    def test_transform_dim_date(self, sales_order_data):
        """Test transform sales order data to dim_date"""
        df = transform_dim_date(sales_order_data)
        assert isinstance(df, pd.DataFrame)
        assert "date_id" in df.columns
        assert df.iloc[0]["month_name"] == "November"

    def test_transform_dim_transaction(self, transaction_data):
        """Test transform transaction data to dim_transaction"""
        df = transform_dim_transaction(transaction_data)
        assert isinstance(df, pd.DataFrame)
        assert "transaction_id" in df.columns
        assert df.iloc[0]["transaction_type"] == "cash"

    def test_transform_dim_payment_type(self, payment_type_data):
        """Test transform payment_type data to dim_payment_type"""
        df = transform_dim_payment_type(payment_type_data)
        assert isinstance(df, pd.DataFrame)
        assert "payment_type_id" in df.columns
        assert df.iloc[0]["payment_type_name"] == "name"

    def test_transform_fact_payment(self, payment_data):
        """Test transform payment data to fact_payment"""
        df = transform_fact_payment(payment_data)
        assert isinstance(df, pd.DataFrame)
        assert "payment_id" in df.columns
        assert df.iloc[0]["paid"]

    def test_transform_fact_purchase_order(self, purchase_order_data):
        """Test transform purchase order data to fact_purchase_order"""
        df = transform_fact_purchase_order(purchase_order_data)
        assert isinstance(df, pd.DataFrame)
        assert "purchase_order_id" in df.columns
        assert df.iloc[0]["staff_id"] == 1

    def test_save_to_parquet(self, s3_client, staff_data):
        """Test saving DataFrame to Parquet format in S3."""
        df = transform_dim_staff(staff_data)
        s3_path = "staff/transformed/test.parquet"
        save_to_parquet(df=df, s3_path=s3_path, client=s3_client, bucket=bucket_name)

        # Check if file was uploaded
        obj = s3_client.get_object(Bucket=bucket_name, Key=s3_path)
        with io.BytesIO(obj["Body"].read()) as f:
            table = pq.read_table(f)
            result_df = table.to_pandas()

        assert isinstance(result_df, pd.DataFrame)
        assert "first_name" in result_df.columns
        assert result_df.iloc[0]["first_name"] == "Abbey"

    def test_save_to_parquet_no_bucket_name(self, s3_client, staff_data):
        """Test saving DataFrame to Parquet format in S3."""
        df = transform_dim_staff(staff_data)
        s3_path = "staff/transformed/test.parquet"
        save_to_parquet(df=df, s3_path=s3_path, client=s3_client, bucket=None)

        # Check if file was uploaded
        obj = s3_client.get_object(Bucket='test-processed-bucket', Key=s3_path)
        with io.BytesIO(obj["Body"].read()) as f:
            table = pq.read_table(f)
            result_df = table.to_pandas()

        assert isinstance(result_df, pd.DataFrame)
        assert "first_name" in result_df.columns
        assert result_df.iloc[0]["first_name"] == "Abbey"

    def test_transform_data_staff_data(self, s3_client, staff_data):
        """Test passing staff data into transform data"""
        df = transform_data(staff_data, "staff")
        assert isinstance(df, pd.DataFrame)
        assert "staff_id" in df.columns
        assert "first_name" in df.columns
        assert "department_name" in df.columns
        assert df.iloc[0]["first_name"] == "Abbey"

    def test_transform_data_sales_order_data(self, s3_client, sales_order_data):
        """Test passing sales data into transform data"""
        df = transform_data(sales_order_data, "sales_order")
        assert isinstance(df, list)
        assert isinstance(df[0], pd.DataFrame)
        assert isinstance(df[1], pd.DataFrame)

    def test_transform_data_address(self, s3_client, address_data):
        """Test passing address data into transform data"""
        df = transform_data(address_data, "address")
        assert isinstance(df, pd.DataFrame)
        assert "location_id" in df.columns
        assert df.iloc[0]["district"] == "Avon"

    def test_transform_data_design(self, s3_client, design_data):
        """Test passing design data into transform data"""
        df = transform_data(design_data, "design")
        assert isinstance(df, pd.DataFrame)
        assert "design_id" in df.columns
        assert df.iloc[0]["design_name"] == "Wooden"

    def test_transform_data_currency(self, s3_client, currency_data):
        """Test passing currency data into transform data"""
        df = transform_data(currency_data, "currency")
        assert isinstance(df, pd.DataFrame)
        assert "currency_id" in df.columns
        assert df.iloc[0]["currency_code"] == "GBP"

    def test_transform_data_counterparty(self, s3_client, counterparty_data):
        """Test passing counterparty data into transform data"""
        df = transform_data(counterparty_data, "counterparty")
        assert isinstance(df, pd.DataFrame)
        assert "counterparty_legal_address_line_1" in df.columns
        assert df.iloc[0]["counterparty_legal_country"] == "Turkey"

    def test_transform_data_no_transform_defined_for_table(self, s3_client, counterparty_data, caplog):
        """Test passing table name with no defined transformation"""
        transform_data(counterparty_data, "beans")
        assert "No transformation defined for table: beans" in caplog.text

    def test_transform_data_transaction(self, s3_client, transaction_data):
        """Test passing transaction data into transform data"""
        df = transform_data(transaction_data, "transaction")
        assert isinstance(df, pd.DataFrame)
        assert "transaction_id" in df.columns
        assert df.iloc[0]["transaction_type"] == "cash"

    def test_transform_data_payment_type(self, s3_client, payment_type_data):
        """Test passing payment type data into transform data"""
        df = transform_data(payment_type_data, "payment_type")
        assert isinstance(df, pd.DataFrame)
        assert "payment_type_id" in df.columns
        assert df.iloc[0]["payment_type_name"] == "name"

    def test_transform_data_payment(self, s3_client, payment_data):
        """Test passing payment data into transform data"""
        df = transform_data(payment_data, "payment")
        assert isinstance(df, pd.DataFrame)
        assert "payment_id" in df.columns
        assert df.iloc[0]["payment_amount"] == 42.50

    def test_transform_data_purchase_order(self, s3_client, purchase_order_data):
        """Test passing purchase order data into transform data"""
        df = transform_data(purchase_order_data, "purchase_order")
        assert isinstance(df, pd.DataFrame)
        assert "purchase_order_id" in df.columns
        assert df.iloc[0]["staff_id"] == 1
