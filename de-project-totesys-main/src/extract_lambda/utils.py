import boto3
import json
import datetime

try:
    from src.extract_lambda.connection import create_conn, close_db_connection
except ImportError:
    from connection import create_conn, close_db_connection


def get_data(table: str, previous_date: datetime.datetime):
    """Queries the database and returns data from table

    Args:
        table (str): name of table to query
        previous_date (datetime.datetime): date to filter query with

    Raises:
        DatabaseError: raises error related to the database
        Exception: raises anyother error

    Returns:
        list[dict]: list of dictionaries containing table data
    """
    conn = None
    try:
        # line 47 was giving false positive for bandit as table is only defined
        # inside handler function
        conn = create_conn()
        query = f"SELECT row_to_json({table}) FROM {table}"  # nosec

        if previous_date:
            query += f" WHERE last_updated > '{previous_date}';"

        rows = conn.run(query)
        data = [row[0] for row in rows]

        return data
    finally:
        if conn:
            close_db_connection(conn)


def put_object(
    client: boto3.client, data: list[dict], table: str, bucket: str, current_date: datetime.datetime
):
    """Puts data in s3 bucket

    Args:
        client (boto3.client): s3_client
        data (list[dict]): serialized data
        table (str): name of table
        bucket (str): name of bucket
        current_date (datetime.datetime): current date for file name

    Raises:
        Exception: Catches all exceptions

    Returns:
        str: the file path in the s3 bucket
    """

    data_bytes = json.dumps(data).encode("utf-8")
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")
    hour = current_date.strftime("%H")
    minute = current_date.strftime("%M")
    key = f"{table}/{year}/{month}/{day}/{hour}-{minute}-{table}.json"

    client.put_object(Bucket=bucket, Body=data_bytes, Key=key)
    return key


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


def put_parameter(client: boto3.client, current_date: datetime.datetime):
    """put parameter in parameter store

    Args:
        client (boto3.client): ssm_client
        current_date (datetime.datetime): current date

    Raises:
        Exception: catches all exceptions

    """
    client.put_parameter(
        Name="lambda_last_run", Value=current_date.strftime("%Y_%m_%d-%H_%M"), Overwrite=True, Type="String"
    )
