import pg8000.native
import os
import boto3
import datetime
from datetime import timezone
import pandas as pd
import io
from pg8000 import DatabaseError
import logging


logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def create_conn():
    return pg8000.native.Connection(
        user=os.getenv("W_USER"),
        password=os.getenv("W_PASSWORD"),
        database=os.getenv("W_DATABASE"),
        host=os.getenv("W_HOST"),
        port=int(os.getenv("W_PORT")),
    )


def close_db_connection(conn):
    conn.close()


def list_new_from_s3(
    client: boto3.client,
    last_run: datetime.datetime,
    bucket_name: str,
    folder_name: str,
) -> list[str]:
    """Lists all keys from s3 Bucket that have been added after the last run time
    Args:
        client (boto3.client): s3 Client
        last_run (datetime.datetime): Time the load lambda function was last run
        bucket_name (str): Name of s3 bucket
        folder_name (str): Used for prefix in list objects

    Returns:
        list[str]: List of s3 object keys
    """
    all_files = client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    if all_files["KeyCount"] == 0:
        return []
    if last_run is None:
        new_files = [file["Key"] for file in all_files["Contents"] if file["LastModified"]]
    else:
        last_run = last_run.replace(tzinfo=timezone.utc)
        new_files = [
            file["Key"] for file in all_files["Contents"] if file["LastModified"] > last_run
        ]
    return new_files


def get_parquet_files(client: boto3.client, file_keys: list, bucket_name: str) -> list[object]:
    """Retreives s3 object and converts to parquet file format for each file key

    Args:
        client (boto3.client): s3 Client
        file_keys (list): List of s3 object file keys
        bucket_name (str): s3 Bucket name containing file keys

    Returns:
        list[object]: List of parquet file objects
    """
    parquet_files = []
    for key in file_keys:
        s3_object = client.get_object(Bucket=bucket_name, Key=key)
        parquet_file = io.BytesIO(s3_object['Body'].read())
        parquet_files.append(parquet_file)
    return parquet_files


def write_to_database(table_name: str, parquet_file_list: list[object]) -> None:
    """Converts parquet file list to a pandas DataFrame, removes duplicates,
      then writes each row to database table

    Args:
        table_name (str): Database table to write to
        parquet_file list[object]: List of parquet files to write
    """
    conn = None
    try:
        conn = create_conn()
        df_list = []
        for parquet_file in parquet_file_list:
            df_list.append(pd.read_parquet(parquet_file))
        df = pd.concat(df_list).drop_duplicates()
        column_names = list(df.columns)
        insert_str = f"""
        INSERT INTO {table_name} ({", ".join(column_names)})
        VALUES (:{", :".join(column_names)})"""  # nosec
        duplicates = 0
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            try:
                conn.run(insert_str, **dict(row_dict))
            except DatabaseError as exc:
                if exc.args[0]['C'] == "23505":
                    duplicates += 1
                else:
                    raise
        logger.info(f"Succesfully added {df.shape[0] - duplicates} rows to {table_name}. {duplicates}"
                    " duplicates skipped")
    finally:
        if conn:
            close_db_connection(conn)


def get_parameter(client: boto3.client, parameter_name: str) -> str:
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


def put_parameter(client: boto3.client, current_date: datetime.datetime) -> str:
    """put parameter in parameter store

    Args:
        client (boto3.client): ssm_client
        current_date (datetime.datetime): current date

    Raises:
        Exception: catches all exceptions

    """
    client.put_parameter(
        Name="load_last_run",
        Value=current_date.strftime("%Y_%m_%d-%H_%M_%S"),
        Overwrite=True,
        Type="String",
    )
