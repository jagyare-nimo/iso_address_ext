# fernego_etl.py

import sys
import boto3
import pandas as pd
import requests
import json
import os
from awsglue.utils import getResolvedOptions

# Parse arguments passed by AWS Glue
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'DDG_ENDPOINT',
    'BATCH_SIZE',
    'S3_OUTPUT_PATH',
    'S3_TRANSFORMATIONS_PATH'
])

# Expected environment variables passed from Glue job defaultArguments
SOURCE_BUCKET = os.environ.get('SOURCE_BUCKET')
SOURCE_FILE_KEY = os.environ.get('SOURCE_FILE_KEY')
DDG_ENDPOINT = args['DDG_ENDPOINT']
BATCH_SIZE = int(args['BATCH_SIZE'])


def fetch_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Fetch CSV file from S3 and return as a DataFrame."""
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(response['Body'])
    return df


def transform_row(row) -> dict:
    """Map a DataFrame row to the expected JSON structure."""
    return {
        "RequestId": row.get("RequestId"),
        "EID": str(row.get("EID")),
        "Active_Flag": int(row.get("Active_Flag", 0)),
        "AML_Enterprise_Risk_Rating": row.get("AML_Enterprise_Risk_Rating"),
        "AML_Last_Review_Date": row.get("AML_Last_Review_Date"),
        "AML_Due_Date": row.get("AML_Due_Date"),
        "AML_System_Name": row.get("AML_System_Name"),
        "AML_System_ID": str(row.get("AML_System_ID")),
        "Publish_Type": row.get("Publish_Type")
    }


def post_batches_to_ddg(df: pd.DataFrame, endpoint: str, batch_size: int = 100):
    """Post transformed data in batches to DDG endpoint."""
    batch = []
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        batch.append(transform_row(row))

        if len(batch) == batch_size:
            send_batch(batch, endpoint)
            print(f"Sent batch ending at row {i}")
            batch.clear()

    if batch:
        send_batch(batch, endpoint)
        print(f"Sent final batch of {len(batch)}")


def send_batch(batch: list, endpoint: str):
    """Send one batch of entities to DDG."""
    try:
        resp = requests.post(endpoint, json={"entities_list": batch}, timeout=20)
        resp.raise_for_status()
        print(f"Posted batch successfully, status: {resp.status_code}")
    except Exception as e:
        print(f"Failed to post batch: {e}")


def main():
    print(f"Starting job for {SOURCE_BUCKET}/{SOURCE_FILE_KEY}")
    if not SOURCE_BUCKET or not SOURCE_FILE_KEY:
        raise ValueError("SOURCE_BUCKET and SOURCE_FILE_KEY must be set as environment variables.")

    df = fetch_csv_from_s3(SOURCE_BUCKET, SOURCE_FILE_KEY)
    post_batches_to_ddg(df, DDG_ENDPOINT, BATCH_SIZE)
    print("Job completed successfully.")


if __name__ == '__main__':
    main()