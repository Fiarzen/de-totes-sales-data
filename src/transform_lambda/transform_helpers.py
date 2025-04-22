import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import json
from decimal import Decimal

logger = logging.getLogger(__name__)


def get_parameter(client: boto3.client, parameter_name: str):
    """gets parameter from parameter store

    Args:
        client (boto3.client): ssm_client
        parameter_name (str): parameter name

    Raises:
        Exception: Catches all exceptions

    Returns:
        str: parameter value
    """

    result = client.get_parameter(Name=parameter_name)
    return result["Parameter"]["Value"]


def transform_data(raw_data, table_name):
    """Transforms raw data to the data warehouse schema using match-case."""
    match table_name:
        case "staff":
            return transform_dim_staff(raw_data)
        case "sales_order":
            return [transform_fact_sales_order(raw_data), transform_dim_date(raw_data)]
        case "address":
            return transform_dim_location(raw_data)
        case "design":
            return transform_dim_design(raw_data)
        case "currency":
            return transform_dim_currency(raw_data)
        case "counterparty":
            return transform_dim_counterparty(raw_data)
        case "transaction":
            return transform_dim_transaction(raw_data)
        case "payment_type":
            return transform_dim_payment_type(raw_data)
        case "payment":
            return transform_fact_payment(raw_data)
        case "purchase_order":
            return transform_fact_purchase_order(raw_data)
        case _:
            logger.warning(f"No transformation defined for table: {table_name}")
            return None


def transform_dim_staff(staff_data):
    """Transforms staff data to dim_staff format."""

    s3_client = boto3.client("s3")
    ssm_client = boto3.client("ssm")
    bucket = get_parameter(ssm_client, "ingestion_bucket_name")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix="department/")
    key = response["Contents"][0]["Key"]

    all_department_data = []

    for object in response["Contents"]:
        key = object["Key"]
        item = s3_client.get_object(Bucket=bucket, Key=key)
        data = item["Body"].read()
        dep = json.loads(data, parse_float=Decimal)
        for i in dep:
            all_department_data.append(i)

    df_department = pd.DataFrame(all_department_data)
    df_staff = pd.DataFrame(staff_data)

    # Merge with department data to get department_name and location
    df_merged = pd.merge(
        df_staff,
        df_department[["department_id", "department_name", "location"]],
        how="left",
        left_on="department_id",
        right_on="department_id",
    )

    # Drop unnecessary columns
    df_dim_staff = df_merged.drop(columns=["department_id", "created_at", "last_updated"])
    return df_dim_staff


def transform_dim_location(data):
    """Transforms address data to dim_location format."""
    df = pd.DataFrame(data)
    df = df.rename(
        columns={
            "address_id": "location_id",
            "address_line_1": "address_line_1",
            "address_line_2": "address_line_2",
            "district": "district",
            "city": "city",
            "postal_code": "postal_code",
            "country": "country",
            "phone": "phone",
        }
    )

    df["address_line_2"] = df["address_line_2"].fillna("None")
    df["district"] = df["district"].fillna("None")

    df = df.drop(columns=["created_at", "last_updated"])

    return df


def transform_dim_design(data):
    """Transforms design data to dim_design format."""
    df = pd.DataFrame(data)
    df = df.rename(
        columns={
            "design_id": "design_id",
            "design_name": "design_name",
            "file_location": "file_location",
            "file_name": "file_name",
        }
    )

    df = df.drop(columns=["created_at", "last_updated"])

    return df


def transform_fact_sales_order(sales_order_data):
    """Transforms sales_order data to fact_sales_order format."""
    df = pd.DataFrame(sales_order_data)
    df_fact_sales_order = pd.DataFrame(
        {
            "sales_record_id": df.index + 1,
            "sales_order_id": df["sales_order_id"],
            "created_date": pd.to_datetime(df["created_at"], format="ISO8601").dt.date,
            "created_time": pd.to_datetime(df["created_at"], format="ISO8601").dt.time,
            "last_updated_date": pd.to_datetime(df["last_updated"], format="ISO8601").dt.date,
            "last_updated_time": pd.to_datetime(df["last_updated"], format="ISO8601").dt.time,
            "sales_staff_id": df["staff_id"],
            "counterparty_id": df["counterparty_id"],
            "units_sold": df["units_sold"],
            "unit_price": df["unit_price"].astype(float),
            "currency_id": 1,
            "design_id": df["design_id"],
            "agreed_payment_date": pd.to_datetime(df["agreed_payment_date"], format="ISO8601").dt.date,
            "agreed_delivery_date": pd.to_datetime(df["agreed_delivery_date"], format="ISO8601").dt.date,
            "agreed_delivery_location_id": df["agreed_delivery_location_id"],
        }
    )

    logger.info("Transformed sales_order data to fact_sales_order schema.")
    return df_fact_sales_order


def transform_dim_date(data):
    """Transforms raw date data to dim_date format."""
    df_sales_data = pd.DataFrame(data)
    date_columns = [
        pd.to_datetime(df_sales_data["created_at"], format="ISO8601"),
        pd.to_datetime(df_sales_data["last_updated"], format="ISO8601"),
        pd.to_datetime(df_sales_data["agreed_payment_date"], format="ISO8601"),
        pd.to_datetime(df_sales_data["agreed_delivery_date"], format="ISO8601")
    ]

    unique_dates = pd.concat(date_columns).drop_duplicates()

    df = pd.DataFrame({"date_id": unique_dates})

    df["year"] = df["date_id"].dt.year
    df["month"] = df["date_id"].dt.month
    df["day"] = df["date_id"].dt.day
    df["day_of_week"] = df["date_id"].dt.weekday + 1
    df["day_name"] = df["date_id"].dt.strftime("%A")
    df["month_name"] = df["date_id"].dt.strftime("%B")
    df["quarter"] = df["month"].apply(lambda x: (x - 1) // 3 + 1)

    df.drop_duplicates(subset=["year", "month", "day"], inplace=True)

    df = df[["date_id", "year", "month", "day", "day_of_week", "day_name", "month_name", "quarter"]]
    return df


def transform_dim_currency(data):
    """Transforms raw currency data to dim_currency format."""
    df = pd.DataFrame(data)
    df = df.rename(columns={"currency_code": "currency_code", "currency_name": "currency_name"})

    if "currency_name" not in df.columns:
        currency_names = {
            "USD": "United States Dollar",
            "GBP": "British Pound",
            "EUR": "Euro",
        }
        df["currency_name"] = df["currency_code"].map(currency_names)

    return df[["currency_id", "currency_code", "currency_name"]]


def transform_dim_counterparty(counterparty_data):
    """Transforms raw counterparty data to dim_counterparty format."""
    df = pd.DataFrame(counterparty_data)
    s3_client = boto3.client("s3")
    ssm_client = boto3.client("ssm")
    bucket = get_parameter(ssm_client, "ingestion_bucket_name")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix="address/")
    key = response["Contents"][0]["Key"]

    all_address_data = []

    for object in response["Contents"]:
        key = object["Key"]
        item = s3_client.get_object(Bucket=bucket, Key=key)
        data = item["Body"].read()
        dep = json.loads(data, parse_float=Decimal)
        for i in dep:
            all_address_data.append(i)

    df_address = pd.DataFrame(all_address_data)

    merged_df = pd.merge(df, df_address, left_on="legal_address_id", right_on="address_id", how="left")

    merged_df = merged_df.rename(
        columns={
            "counterparty_id": "counterparty_id",
            "counterparty_legal_name": "counterparty_legal_name",
            "address_line_1": "counterparty_legal_address_line_1",
            "address_line_2": "counterparty_legal_address_line_2",
            "district": "counterparty_legal_district",
            "city": "counterparty_legal_city",
            "postal_code": "counterparty_legal_postal_code",
            "country": "counterparty_legal_country",
            "phone": "counterparty_legal_phone_number",
        }
    )

    dim_counterparty_df = merged_df[
        [
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_address_line_2",
            "counterparty_legal_district",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone_number",
        ]
    ]

    return dim_counterparty_df


def transform_dim_transaction(transaction_data):
    """Transforms raw currency data to dim_transaction format."""
    df = pd.DataFrame(transaction_data)
    df = df.rename(columns={"transaction_id": "transaction_id",
                            "transaction_type": "transaction_type",
                            "sales_order_id": "sales_order_id",
                            "purchase_order_id": "purchase_order_id"})

    df = df.drop(columns=["created_at", "last_updated"])

    return df[["transaction_id", "transaction_type", "sales_order_id", "purchase_order_id"]]


def transform_dim_payment_type(payment_type_data):
    """Transform raw payment data to dim_payment_type"""
    df = pd.DataFrame(payment_type_data)
    df = df.rename(columns={"payment_type_id": "payment_type_id",
                            "payment_type_name": "payment_type_name"})

    df = df.drop(columns=["created_at", "last_updated"])
    return df[["payment_type_id", "payment_type_name"]]


def transform_fact_payment(payment_data):
    """Transforms payment data to fact_payment format."""
    df = pd.DataFrame(payment_data)
    df_fact_payment = pd.DataFrame(
        {
            "payment_record_id": df.index + 1,
            "payment_id": df["payment_id"],
            "created_date": pd.to_datetime(df["created_at"], format="ISO8601").dt.date,
            "created_time": pd.to_datetime(df["created_at"], format="ISO8601").dt.time,
            "last_updated_date": pd.to_datetime(df["last_updated"], format="ISO8601").dt.date,
            "last_updated_time": pd.to_datetime(df["last_updated"], format="ISO8601").dt.time,
            "transaction_id": df["transaction_id"],
            "counterparty_id": df["counterparty_id"],
            "payment_amount": df["payment_amount"].round(2),
            "currency_id": df["currency_id"],
            "payment_type_id": df["payment_type_id"],
            "paid": df["paid"],
            "payment_date": pd.to_datetime(df["payment_date"], format="ISO8601").dt.date
            }
    )

    df = df.drop(columns=["company_ac_number", "counterparty_ac_number"])

    return df_fact_payment


def transform_fact_purchase_order(puchase_order_data):
    """Transforms purchase order data to fact_purchase_order format."""
    df = pd.DataFrame(puchase_order_data)
    df_fact_puchase_order = pd.DataFrame(
        {
            "purchase_record_id": df.index + 1,
            "purchase_order_id": df["purchase_order_id"],
            "created_date": pd.to_datetime(df["created_at"], format="ISO8601").dt.date,
            "created_time": pd.to_datetime(df["created_at"], format="ISO8601").dt.time,
            "last_updated_date": pd.to_datetime(df["last_updated"], format="ISO8601").dt.date,
            "last_updated_time": pd.to_datetime(df["last_updated"], format="ISO8601").dt.time,
            "staff_id": df["staff_id"],
            "counterparty_id": df["counterparty_id"],
            "item_code": df["item_code"],
            "item_quantity": df["item_quantity"],
            "item_unit_price": df["item_unit_price"].round(2),
            "currency_id": df["currency_id"],
            "agreed_delivery_date": pd.to_datetime(df["agreed_delivery_date"], format="ISO8601").dt.date,
            "agreed_payment_date": pd.to_datetime(df["agreed_payment_date"], format="ISO8601").dt.date,
            "agreed_delivery_location_id": df["agreed_delivery_location_id"]
            }
    )

    return df_fact_puchase_order


def save_to_parquet(df, s3_path, client, bucket=None):
    """get bucket name and save the DataFrame to Parquet format in S3."""
    if not bucket:
        ssm_client = boto3.client("ssm")
        bucket = get_parameter(ssm_client, "processed_bucket_name")

    table = pa.Table.from_pandas(df)
    pq_buffer = pa.BufferOutputStream()
    pq.write_table(table, pq_buffer)

    client.put_object(Bucket=bucket, Key=s3_path, Body=pq_buffer.getvalue().to_pybytes())
    logger.info(f"Saved {s3_path} to processed S3 bucket")
