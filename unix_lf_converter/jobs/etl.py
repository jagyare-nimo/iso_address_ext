import sys
import boto3
import pandas as pd
import requests
import os
from awsglue.utils import getResolvedOptions

# Parse arguments passed from Glue job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'DDG_ENDPOINT',
    'BATCH_SIZE',
    'SOURCE_BUCKET',
    'SOURCE_FOLDER_PREFIX'
])

SOURCE_BUCKET = args['SOURCE_BUCKET']
FOLDER_PREFIX = args['SOURCE_FOLDER_PREFIX']
DDG_ENDPOINT = args['DDG_ENDPOINT']
BATCH_SIZE = int(args['BATCH_SIZE'])

def fetch_all_csvs(bucket, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix + '/')

    dfs = []
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'):
                print(f"Reading file: s3://{bucket}/{key}")
                response = s3.get_object(Bucket=bucket, Key=key)
                df = pd.read_csv(response['Body'])
                dfs.append(df)

    if not dfs:
        raise ValueError(f"No CSV files found in s3://{bucket}/{prefix}/")

    return pd.concat(dfs, ignore_index=True)

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
    print(f"Processing all CSVs under: s3://{SOURCE_BUCKET}/{FOLDER_PREFIX}/")
    df = fetch_all_csvs(SOURCE_BUCKET, FOLDER_PREFIX)
    post_batches_to_ddg(df, DDG_ENDPOINT, BATCH_SIZE)
    print("Glue job completed.")

if __name__ == "__main__":
    main()