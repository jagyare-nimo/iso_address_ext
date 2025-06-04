import json
import sys
from datetime import datetime

import boto3
import pandas as pd
import requests
from awsglue.utils import getResolvedOptions


# --------------------- Logging Setup ---------------------
def json_log(payload: dict, level="INFO"):
    log_entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": level,
        **payload
    }
    print(json.dumps(log_entry))


# --------------------- Env & Params ---------------------
try:
    args = getResolvedOptions(sys.argv, [
        'GLUE_JOB_NAME',
        'DDG_ENDPOINT',
        'BATCH_SIZE',
        'SOURCE_BUCKET',
        'SOURCE_FOLDER_PREFIX'
    ])

    GLUE_JOB_NAME = args['GLUE_JOB_NAME']
    DDG_ENDPOINT = args['DDG_ENDPOINT']
    BATCH_SIZE = int(args['BATCH_SIZE'])
    SOURCE_BUCKET = args['SOURCE_BUCKET']
    FOLDER_PREFIX = args['SOURCE_FOLDER_PREFIX']

except Exception as e:
    json_log({"error": "Failed to parse Glue arguments", "reason": str(e)}, level="CRITICAL")
    raise

# --------------------- Constants ---------------------
REQUIRED_COLUMNS = [
    "RequestId", "EID", "Active_Flag",
    "AML_Enterprise_Risk_Rating", "AML_Last_Review_Date",
    "AML_Due_Date", "AML_System_Name", "AML_System_ID", "Publish_Type"
]

# --------------------- S3 & Batching ---------------------
s3 = boto3.client('s3')


def validate_columns(df: pd.DataFrame, file_key: str):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        json_log({
            "error": "MissingRequiredColumns",
            "file": file_key,
            "missingColumns": missing
        }, level="ERROR")
        raise ValueError(f"File {file_key} is missing required columns: {missing}")


def fetch_all_csvs(bucket: str, prefix: str):
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix + '/')

    dataframes = []
    processed_keys = []

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.csv'):
                continue

            try:
                response = s3.get_object(Bucket=bucket, Key=key)
                df = pd.read_csv(response['Body'])

                if df.empty:
                    json_log({"warning": "EmptyCSVSkipped", "file": key})
                    continue

                validate_columns(df, key)

                dataframes.append(df)
                processed_keys.append(key)

                json_log({"info": "CSVLoaded", "file": key, "rows": len(df)})

            except Exception as e:
                json_log({"error": "FailedToReadCSV", "file": key, "reason": str(e)}, level="ERROR")

    if not dataframes:
        json_log({"error": "NoValidCSVsFound", "bucket": bucket, "prefix": prefix}, level="WARNING")
        raise ValueError(f"No valid CSVs found under s3://{bucket}/{prefix}/")

    return pd.concat(dataframes, ignore_index=True), processed_keys


def transform_row(row: pd.Series) -> dict:
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


def send_batch(batch: list, endpoint: str):
    try:
        resp = requests.post(endpoint, json={"entities_list": batch}, timeout=30)
        resp.raise_for_status()
        json_log({"status": "BatchPosted", "batchSize": len(batch), "statusCode": resp.status_code})
    except Exception as e:
        json_log({"error": "PostFailed", "reason": str(e)}, level="ERROR")


def post_batches_to_ddg(df: pd.DataFrame, endpoint: str, batch_size: int):
    batch = []
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        try:
            batch.append(transform_row(row))
        except Exception as e:
            json_log({"error": "RowTransformFailed", "rowNumber": i, "reason": str(e)}, level="ERROR")
            continue

        if len(batch) >= batch_size:
            send_batch(batch, endpoint)
            batch.clear()

    if batch:
        send_batch(batch, endpoint)


# --------------------- Archival ---------------------
def archive_files(bucket: str, keys: list, prefix: str):
    archived = []
    failed = []

    for key in keys:
        try:
            filename = key.split("/")[-1]
            archive_key = f"{prefix}/archive/{filename}"

            s3.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=archive_key
            )
            s3.delete_object(Bucket=bucket, Key=key)

            archived.append(archive_key)
        except Exception as e:
            json_log({"error": "ArchiveFailed", "file": key, "reason": str(e)}, level="ERROR")
            failed.append(key)

    json_log({
        "archivedCount": len(archived),
        "failedArchives": failed
    })


# --------------------- Main Glue Job ---------------------
def main():
    job_id = f"{GLUE_JOB_NAME}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    json_log({"action": "JobStart", "jobId": job_id, "bucket": SOURCE_BUCKET, "prefix": FOLDER_PREFIX})

    try:
        df, csv_keys = fetch_all_csvs(SOURCE_BUCKET, FOLDER_PREFIX)
        post_batches_to_ddg(df, DDG_ENDPOINT, BATCH_SIZE)
        archive_files(SOURCE_BUCKET, csv_keys, FOLDER_PREFIX)

        json_log({"status": "JobSuccess", "jobId": job_id, "rowCount": len(df)})
    except Exception as e:
        json_log({"status": "JobFailed", "jobId": job_id, "reason": str(e)}, level="CRITICAL")
        raise


if __name__ == "__main__":
    main()
