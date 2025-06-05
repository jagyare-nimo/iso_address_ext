# --- Original Imports + Async ---
import json
import sys
import uuid
import asyncio
from datetime import datetime
from io import StringIO

import boto3
import pandas as pd
import aiohttp
from awsglue.utils import getResolvedOptions


# --- Logging Helper ---
def json_log(payload: dict, level="INFO"):
    log_entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": level,
        **payload
    }
    print(json.dumps(log_entry))


# --- Parameter Parsing ---
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

# --- CSV Fields Constants ---
REQUIRED_COLUMNS = [
    "UEN",
    "Active_Flag",
    "AML_Enterprise_Risk_Rating",
    "AML_Last_Review_Date",
    "AML_Due_Date",
    "AML_System_Name",
    "AML_System_ID",
    "Publish_Type"
]

s3 = boto3.client('s3')


def validate_columns(df: pd.DataFrame, file_key: str):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        json_log({"error": "MissingRequiredColumns", "file": file_key, "missingColumns": missing}, level="ERROR")
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
                for chunk in pd.read_csv(response['Body'], chunksize=5000):
                    if chunk.empty:
                        json_log({"warning": "EmptyChunkSkipped", "file": key})
                        continue
                    validate_columns(chunk, key)
                    dataframes.append(chunk)
                processed_keys.append(key)
                json_log({"info": "CSVLoaded", "file": key})
            except Exception as e:
                json_log({"error": "FailedToReadCSV", "file": key, "reason": str(e)}, level="ERROR")

    if not dataframes:
        json_log({"error": "NoValidCSVsFound", "bucket": bucket, "prefix": prefix}, level="WARNING")
        raise ValueError(f"No valid CSVs found under s3://{bucket}/{prefix}/")

    return pd.concat(dataframes, ignore_index=True), processed_keys


def transform_row(row: pd.Series) -> dict:
    return {
        "RequestId": f"CRD-{uuid.uuid4()}",
        "UEN": row.get("UEN"),
        "Active_Flag": int(row.get("Active_Flag", 0)),
        "AML_Enterprise_Risk_Rating": row.get("AML_Enterprise_Risk_Rating"),
        "AML_Last_Review_Date": row.get("AML_Last_Review_Date"),
        "AML_Due_Date": row.get("AML_Due_Date"),
        "AML_System_Name": row.get("AML_System_Name"),
        "AML_System_ID": str(row.get("AML_System_ID")),
        "Publish_Type": row.get("Publish_Type")
    }


# --- Async HTTP with retry + backoff ---
# --- Async HTTP with retry + backoff ---
async def send_batch_async(session, batch: list, endpoint: str, max_retries: int = 2) -> tuple[bool, str]:
    payload = {"entities_list": batch}
    attempt = 0
    delay = 2  # initial backoff

    while attempt <= max_retries:
        try:
            async with session.post(endpoint, json=payload, timeout=30) as resp:
                if 200 <= resp.status < 300:
                    json_log({
                        "status": "BatchPosted",
                        "batchSize": len(batch),
                        "statusCode": resp.status,
                        "attempt": attempt + 1
                    })
                    return True, ""
                else:
                    reason = f"HTTP {resp.status} - {str(resp.reason).splitlines()[0].strip()}"
                    raise Exception(reason)
        except Exception as e:
            attempt += 1
            reason = f"HTTP {getattr(e, 'status', 'Unknown')} - {str(getattr(e, 'reason', str(e))).splitlines()[0].strip()}"
            json_log({
                "error": "PostFailed",
                "attempt": attempt,
                "batchSize": len(batch),
                "reason": reason
            }, level="ERROR")
            if attempt > max_retries:
                json_log({"critical": "MaxRetriesExceeded", "batchSize": len(batch)}, level="CRITICAL")
                return False, reason
            await asyncio.sleep(delay)
            delay *= 2

    # fallback return in case all retries are exhausted but not caught above
    return False, "Unknown failure"


# --- Write Failed Rows to S3 ---
def upload_failed_rows_to_s3(failed_rows: list, bucket: str, prefix: str):
    if not failed_rows:
        return

    df = pd.DataFrame(failed_rows)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    failed_key = f"{prefix}/failed/failed_rows_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.csv"
    s3.put_object(Bucket=bucket, Key=failed_key, Body=csv_buffer.getvalue())

    json_log({"info": "FailedRowsUploaded", "rowCount": len(failed_rows), "s3Key": failed_key})


# --- Success/Failure Tracking at Row Level to handle Clearer FailureReason ---
async def post_batches_with_success_tracking(df: pd.DataFrame, endpoint: str, batch_size: int) -> bool:
    batch_rows = []
    batch_payload = []
    failed_rows = []
    successful_rows = []

    async with aiohttp.ClientSession() as session:
        for i, (_, row) in enumerate(df.iterrows(), start=1):
            try:
                transformed = transform_row(row)
                batch_payload.append(transformed)
                row["RequestId"] = transformed["RequestId"]
                batch_rows.append(row)
            except Exception as e:
                row[
                    "FailureReason"] = f"HTTP {getattr(e, 'status', 'Unknown')} - {str(getattr(e, 'reason', str(e))).splitlines()[0].strip()}"
                failed_rows.append(row)
                continue

            if len(batch_payload) >= batch_size:
                success, failure_reason = await send_batch_async(session, batch_payload, endpoint)
                if success:
                    successful_rows.extend(batch_rows)
                else:
                    for r in batch_rows:
                        r["FailureReason"] = failure_reason
                        failed_rows.append(r)
                batch_payload.clear()
                batch_rows.clear()

        if batch_payload:
            success, failure_reason = await send_batch_async(session, batch_payload, endpoint)
            if success:
                successful_rows.extend(batch_rows)
            else:
                for r in batch_rows:
                    r["FailureReason"] = failure_reason
                    failed_rows.append(r)

    upload_failed_rows_to_s3(failed_rows, SOURCE_BUCKET, FOLDER_PREFIX)

    json_log({"summary": "BatchPostSummary", "totalRows": len(df), "successfulRows": len(successful_rows),
              "failedRows": len(failed_rows)})

    return len(failed_rows) == 0


# --- Archive CSVs ---
def archive_files(bucket: str, keys: list, prefix: str):
    archived = []
    failed = []

    for key in keys:
        try:
            filename = key.split("/")[-1]
            archive_key = f"{prefix}/archive/{filename}"
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=archive_key)
            s3.delete_object(Bucket=bucket, Key=key)
            archived.append(archive_key)
        except Exception as e:
            json_log({"error": "ArchiveFailed", "file": key, "reason": str(e)}, level="ERROR")
            failed.append(key)

    json_log({"archivedCount": len(archived), "failedArchives": failed})


def main():
    job_id = f"{GLUE_JOB_NAME}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    json_log({"action": "JobStart", "jobId": job_id, "bucket": SOURCE_BUCKET, "prefix": FOLDER_PREFIX})

    try:
        df, csv_keys = fetch_all_csvs(SOURCE_BUCKET, FOLDER_PREFIX)
        all_batches_successful = asyncio.run(post_batches_with_success_tracking(df, DDG_ENDPOINT, BATCH_SIZE))

        if all_batches_successful:
            archive_files(SOURCE_BUCKET, csv_keys, FOLDER_PREFIX)
        else:
            json_log({"warning": "SkippingArchival", "reason": "Some batches failed", "jobId": job_id})

        json_log({"status": "JobCompleted", "jobId": job_id, "rowCount": len(df)})
    except Exception as e:
        json_log({"status": "JobFailed", "jobId": job_id, "reason": str(e)}, level="CRITICAL")
        raise


if __name__ == "__main__":
    main()
