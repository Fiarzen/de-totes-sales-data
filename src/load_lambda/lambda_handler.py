""""
A Python application that loads the data into a prepared
data warehouse at defined intervals.
Again the application should be adequately logged and monitored.

"""
import boto3
import logging
from pg8000 import DatabaseError
from botocore.exceptions import ClientError
from datetime import datetime as dt
try:
    from src.load_lambda.load_utils import (
        list_new_from_s3,
        write_to_database,
        get_parquet_files,
        get_parameter,
        put_parameter
    )
except Exception:
    from load_utils import (
        list_new_from_s3,
        write_to_database,
        get_parquet_files,
        get_parameter,
        put_parameter
    )


# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_data(event, context):
    """Loads data into the data warehouse."""
    try:
        logger.info("Started load data...")
        s3_client = boto3.client("s3")
        ssm_client = boto3.client("ssm")
        last_run = get_parameter(ssm_client, "load_last_run")
        if last_run == "None":
            last_run = None
        else:
            last_run = dt.strptime(last_run, "%Y_%m_%d-%H_%M_%S")

        processed_bucket = get_parameter(ssm_client, "processed_bucket_name")
        put_parameter(ssm_client, dt.now())

        folder_list = ["dim_date", "dim_counterparty", "dim_currency", "dim_design",
                       "dim_location", "dim_staff", "fact_sales_order"]
        for folder in folder_list:
            new_files = list_new_from_s3(s3_client, last_run, processed_bucket, folder)
            if new_files != []:
                logger.info(f"Found {len(new_files)} {folder} parquet files")
                new_parquet_files = get_parquet_files(s3_client, new_files, processed_bucket)
                try:
                    write_to_database(folder, new_parquet_files)
                    logger.info(f"Succesfully wrote {", ".join(new_files)} to {folder} table")
                except DatabaseError as e:
                    logger.exception(f"Database Error: {e}")
            elif new_files == []:
                logger.info(f"Found no new files in {folder}")
        return "Load function successfully ran."

    except ClientError as e:
        logger.exception(f"ClientError Error: {e}")
        return f"ClientError Error: {e}"
    except Exception as e:
        logger.exception(f"Data load failed: {e}")
        return f"Unexpected error {e}"
