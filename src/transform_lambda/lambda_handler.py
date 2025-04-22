import boto3
import json
import logging
import pandas as pd
from datetime import datetime
from decimal import Decimal

from botocore.exceptions import ClientError

try:
    from src.transform_lambda.transform_helpers import transform_data, save_to_parquet
except ImportError:
    from transform_helpers import transform_data, save_to_parquet

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")


def lambda_handler(event, context):
    """Lambda function triggered by S3 event to process and transform ingested data."""
    try:
        # Validate event structure
        if "Records" not in event or not isinstance(event["Records"], list):
            raise ValueError("Invalid event structure: 'Records' field missing or invalid.")

        for record in event["Records"]:
            try:
                # Validate record structure
                bucket = record["s3"]["bucket"]["name"]

                key = record["s3"]["object"].get("key")  # Safely get 'key'
                response = s3_client.get_object(Bucket=bucket, Key=key)
                # Process the raw data
                data = response["Body"].read().decode("utf-8")
                raw_data = json.loads(data, parse_float=Decimal)

                # Transform and save data
                table_name = key.split("/")[0]
                transformed_df = transform_data(raw_data, table_name)

                if isinstance(transformed_df, pd.DataFrame):
                    current_date = datetime.now().strftime("%Y/%m/%d/%H_%M")
                    if table_name == "address":
                        parquet_key = f"dim_location/transformed/{current_date}-dim_location.parquet"
                    elif table_name in ["payment", "purchase_order"]:
                        parquet_key = f"""fact_{table_name}/transformed/
                                          {current_date}-fact_{table_name}.parquet"""
                    else:
                        parquet_key = f"dim_{table_name}/transformed/{current_date}-dim_{table_name}.parquet"
                    save_to_parquet(transformed_df, parquet_key, s3_client)

                    logger.info(f"Successfully processed {table_name} data to Parquet.")

                elif isinstance(transformed_df, list):
                    table_names = ["fact_sales_order", "dim_date"]
                    for i in range(len(transformed_df)):
                        current_date = datetime.now().strftime("%Y/%m/%d/%H_%M")
                        parquet_key = f"{table_names[i]}/transformed/{current_date}-{table_names[i]}.parquet"
                        save_to_parquet(transformed_df[i], parquet_key, s3_client)

                        logger.info(f"Successfully processed {table_names[i]} data to Parquet.")
                else:
                    raise ClientError

            except ClientError as botoErr:
                logger.error(f"Botocore error {botoErr}")
                raise

            except Exception as record_error:
                logger.exception(f"Error processing record {record}: {record_error}")
                continue

        return "Successfully ran"

    except Exception as e:
        logger.exception(f"Critical error processing event: {e}")
        raise e
