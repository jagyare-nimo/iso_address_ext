import asyncio
import json
import sys
import uuid

from datetime import datetime
from io import StringIO
from typing import cast
from pandas._typing import WriteBuffer
from io import StringIO, TextIOBase

import aiohttp
import boto3
import pandas as pd
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
    BATCH_SIZE = 2  # -- int(args['BATCH_SIZE'])
    SOURCE_BUCKET = args['SOURCE_BUCKET']
    FOLDER_PREFIX = args['SOURCE_FOLDER_PREFIX']
    MAX_RETRY = 2  # -- int(args['MAX_RETRY'])

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

# --- S3 init -----
s3 = boto3.client('s3')


def validate_columns(df: pd.DataFrame, file_key: str) -> bool:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        json_log({"error": "MissingRequiredColumns", "file": file_key, "missingColumns": missing}, level="ERROR")
        return False
    return True


def fetch_all_csvs(bucket: str, prefix: str):
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix + '/')

    dataframes = []
    processed_keys = []

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']

            if not key.endswith('.csv') or '/' in key[len(prefix) + 1:]:
                continue

            try:
                response = s3.get_object(Bucket=bucket, Key=key)

                for chunk in pd.read_csv(response['Body'], chunksize=5000):
                    if chunk.empty:
                        json_log({"warning": "EmptyChunkSkipped", "file": key})
                        continue

                    if not validate_columns(chunk, key):
                        json_log({"warning": "InvalidColumnsSkipped", "file": key})
                        break

                    chunk["source_file_key"] = key
                    dataframes.append(chunk)

                processed_keys.append(key)
                json_log({"info": "CSVLoaded", "file": key})

            except Exception as e:
                json_log({
                    "error": "FailedToReadCSV",
                    "file": key,
                    "reason": str(e)
                }, level="ERROR")

    if not dataframes:
        json_log({
            "warning": "NoValidCSVsFound",
            "bucket": bucket,
            "prefix": prefix,
            "message": "No valid CSVs found; continuing job without processing."
        }, level="WARNING")
        return pd.DataFrame(), []

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
async def send_batch_async(session, batch: list, endpoint: str, max_retries: int = 2) -> tuple[bool, str]:
    payload = {"entities_list": batch}
    json_log({"info": "PostingBatchPayload", "batchSize": len(batch), "batchPayload": payload, "endpoint": endpoint})
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
                        "attempt": attempt + 1,
                        "batchPayload": payload

                    })
                    return True, ""
                else:
                    reason = f"HTTP {resp.status} - {str(resp.reason).splitlines()[0].strip()}"
                    raise Exception(reason)
        except Exception as err_:
            attempt += 1
            raw_resp = str(getattr(err_, 'reason', '')).strip()
            short_resp = raw_resp.splitlines()[0].strip() if raw_resp else "Couldn't get response"
            reason = f"HTTP {getattr(err_, 'status', 'Unknown')} - {short_resp}"
            json_log({
                "error": "PostFailed",
                "attempt": attempt,
                "batchSize": len(batch),
                "reason": reason,
                "batchPayload": payload
            }, level="ERROR")
            if attempt > max_retries:
                json_log({"critical": "MaxRetriesExceeded", "batchSize": len(batch)}, level="CRITICAL")
                return False, reason
            await asyncio.sleep(delay)
            delay *= 2

    # fallback return in case all retries are exhausted but not caught above
    return False, "Unknown failure"


# --- Write Failed Rows to S3 ---
def upload_failed_rows_to_s3(failed_rows: list[dict], bucket: str, prefix: str):
    if not failed_rows:
        return

    df = pd.DataFrame(failed_rows)

    if "source_file_key" not in df.columns:
        json_log({"error": "Missing source_file_key in failed rows"}, level="ERROR")
        return

    for source_key, group_df in df.groupby("source_file_key"):
        try:
            buffer = cast(WriteBuffer[str], StringIO())
            group_df.to_csv(buffer, index=False)
            buffer.seek(0)

            original_filename = str(source_key).split("/")[-1].replace(".csv", "")
            timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
            failed_key = f"{prefix}/failed/{original_filename}_failed_rows_{timestamp}.csv"

            s3.put_object(Bucket=bucket, Key=failed_key, Body=buffer.getvalue())

            json_log({
                "uploadFailedRows": {
                    "info": "Failed rows uploaded",
                    "rowCount": len(group_df),
                    "s3Key": failed_key,
                    "originalFile": source_key
                }
            })
        except Exception as exception:
            json_log({
                "error": "FailedUploadFailedRows",
                "sourceFile": str(source_key),
                "reason": str(exception)
            }, level="ERROR")


# --- Success/Failure Tracking at Row Level to handle Clearer FailureReason ---
async def post_batches_with_success_tracking(df: pd.DataFrame, endpoint: str, batch_size: int) -> tuple[list, list]:
    batch_rows = []
    batch_payload = []
    failed_rows = []
    successful_rows = []
    failure_streak = 0
    max_failure_streak = MAX_RETRY

    async with aiohttp.ClientSession() as session:
        for i, (_, row) in enumerate(df.iterrows(), start=1):
            try:
                transformed = transform_row(row)
                batch_payload.append(transformed)
                row["RequestId"] = transformed["RequestId"]
                batch_rows.append(row)
            except Exception as err_:
                raw_resp = str(getattr(err_, 'reason', '')).strip()
                short_resp = raw_resp.splitlines()[0].strip() if raw_resp else "Couldn't get response"
                fail_resp = f"HTTP {getattr(err_, 'status', 'Unknown')} - {short_resp}"
                row["FailureReason"] = fail_resp
                failed_rows.append(row)
                continue

            if len(batch_payload) >= batch_size:
                success, failure_reason = await send_batch_async(session, batch_payload, endpoint)
                if success:
                    successful_rows.extend(batch_rows)
                    failure_streak = 0
                else:
                    for r in batch_rows:
                        r["FailureReason"] = failure_reason
                        failed_rows.append(r)
                    failure_streak += 1
                    if failure_streak >= max_failure_streak:
                        json_log({
                            "critical": "TooManyConsecutiveFailures",
                            "streak": failure_streak,
                            "message": "Aborting batch processing"
                        }, level="CRITICAL")
                        break
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

    json_log({
        "BatchPostSummary": {
            "summary": "BatchPostSummary",
            "totalRows": len(df),
            "successfulRows": len(successful_rows),
            "failedRows": len(failed_rows)
        }
    })

    return successful_rows, failed_rows


# --- Archive CSVs ---
def archive_files(bucket: str, keys: list, prefix: str):
    archived = []
    failed = []

    for key in keys:
        filename = key.split("/")[-1]
        archive_key = f"{prefix}/archive/{filename}"

        try:
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=archive_key)
            s3.delete_object(Bucket=bucket, Key=key)

            archived.append(archive_key)

            json_log({
                "archiveSummary": {
                    "attempted": len(keys),
                    "successfulCount": len(archived),
                    "failedCount": len(failed),
                    "failedFiles": failed
                }
            })

        except Exception as exception:
            json_log({"error": "ArchiveFailed", "file": key, "reason": str(exception)}, level="ERROR")
            failed.append(key)


def main():
    job_id = f"{GLUE_JOB_NAME}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    json_log({"action": "JobStart", "jobId": job_id,  "bucket": SOURCE_BUCKET, "prefix": FOLDER_PREFIX})

    try:
        df, csv_keys = fetch_all_csvs(SOURCE_BUCKET, FOLDER_PREFIX)
        successful_rows, failed_rows = asyncio.run(
            post_batches_with_success_tracking(df, DDG_ENDPOINT, BATCH_SIZE)
        )

        archive_files(SOURCE_BUCKET, csv_keys, FOLDER_PREFIX)

        if failed_rows:
            dict_failed_rows = [r.to_dict() for r in failed_rows]
            upload_failed_rows_to_s3(dict_failed_rows, SOURCE_BUCKET, FOLDER_PREFIX)

        json_log({
            "jobDetails": {
                "status": "Job Completed",
                "jobId": job_id,
                "rowCount": len(df),
                "successfulRows": len(successful_rows),
                "failedRows": len(failed_rows),
                "bucket": SOURCE_BUCKET,
                "prefix": FOLDER_PREFIX,
                "processedFiles": csv_keys
            }
        })

    except Exception as exception:
        json_log({
            "jobDetails": {
                "status": "Job Failed",
                "jobId": job_id,
                "bucket": SOURCE_BUCKET,
                "prefix": FOLDER_PREFIX,
                "reason": str(exception)
            }
        }, level="CRITICAL")
        raise


if __name__ == "__main__":
    main()
