import boto3
import logging
from datetime import datetime as dt
from pg8000 import DatabaseError

try:
    from src.extract_lambda.utils import get_data, put_object, get_parameter, put_parameter
except ImportError:
    from utils import get_data, put_object, get_parameter, put_parameter


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    ssm_client = boto3.client("ssm")
    try:
        previous_time = get_parameter(ssm_client, "lambda_last_run")

        if previous_time == "None":
            previous_time = None
        else:
            previous_time = dt.strptime(previous_time, "%Y_%m_%d-%H_%M")

        logger.info(f"previous time is {previous_time}")

        bucket_name = get_parameter(ssm_client, "ingestion_bucket_name")

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

        current_date = dt.now()

        for table in tables:
            table_data = get_data(table, previous_time)
            if table_data:
                response = put_object(s3_client, table_data, table, bucket_name, current_date)

                logger.info(f"Successfully put {len(table_data)} objects into {response}")
            else:
                logger.info(f"No new data for {table}")

        put_parameter(ssm_client, current_date)
        logger.info(f"Updated lambda last run to {current_date}")
        return "Successfully ran"
    except DatabaseError as e:
        logger.exception(f"Database Error: {e}")
        return f"Database Error: {e}"
    except Exception as e:
        logger.exception(f"Unexpected Error: {e}")
        return f"Unexpected Error: {e}"
