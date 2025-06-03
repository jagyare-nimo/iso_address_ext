import sys
import boto3
import pandas as pd
import requests
import json
from awsglue.utils import getResolvedOptions

# Get arguments from Glue job config
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'DDG_ENDPOINT',
    'BATCH_SIZE',
    'S3_OUTPUT_PATH',
    'S3_TRANSFORMATIONS_PATH',
    'SOURCE_BUCKET',
    'SOURCE_FILE_KEY'
])

SOURCE_BUCKET = args['SOURCE_BUCKET']
SOURCE_FILE_KEY = args['SOURCE_FILE_KEY']
DDG_ENDPOINT = args['DDG_ENDPOINT']
BATCH_SIZE = int(args['BATCH_SIZE'])

def fetch_csv_from_s3(bucket, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(response['Body'])

def transform_row(row):
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

def send_batch(batch, endpoint):
    try:
        resp = requests.post(endpoint, json={"entities_list": batch}, timeout=20)
        resp.raise_for_status()
        print(f"Posted batch: status {resp.status_code}")
    except Exception as e:
        print(f"Failed to post batch: {e}")

def post_batches_to_ddg(df, endpoint, batch_size):
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

def main():
    print(f"Processing: s3://{SOURCE_BUCKET}/{SOURCE_FILE_KEY}")
    df = fetch_csv_from_s3(SOURCE_BUCKET, SOURCE_FILE_KEY)
    post_batches_to_ddg(df, DDG_ENDPOINT, BATCH_SIZE)
    print("Job finished.")

if __name__ == "__main__":
    main()