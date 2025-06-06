import os
import boto3
import logging
import json
import uuid
import re
from urllib.parse import unquote_plus
from pathlib import Path
from datetime import datetime

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not logger.handlers:
    log_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(message)s')  # Keeps JSON as-is
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)


def json_log(payload: dict, level=logging.INFO):
    log_entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": logging.getLevelName(level),
        **payload
    }
    logger.log(level, json.dumps(log_entry))


# AWS Client
glue_client = boto3.client("glue")


def get_env_var(key: str, required: bool = True, default: str = None) -> str:
    val = os.environ.get(key, default)
    if required and not val:
        json_log({
            "error": "MissingEnvironmentVariable",
            "missingKey": key
        }, level=logging.CRITICAL)
        raise RuntimeError(f"Missing required environment variable: {key}")
    return val


# Config
GLUE_JOB_NAME = get_env_var("GLUE_JOB_NAME")
DDG_ENDPOINT = get_env_var("DDG_ENDPOINT")
BATCH_SIZE = get_env_var("BATCH_SIZE", required=False, default="100")
SOURCE_FOLDER_PREFIX = get_env_var("SOURCE_FOLDER_PREFIX", required=True)


def is_valid_folder(folder_prefix: str, correlation_id: str) -> bool:
    """
    Validates that the folder path is exactly: {SOURCE_FOLDER_PREFIX}/date=YYYY-MM-DD
    And that the date equals today (UTC).
    """
    escaped_prefix = re.escape(SOURCE_FOLDER_PREFIX)
    pattern = rf"^{escaped_prefix}/date=(\d{{4}}-\d{{2}}-\d{{2}})$"
    match = re.match(pattern, folder_prefix)

    if not match:
        json_log({
            "warning": "InvalidFolderFormat",
            "folderPrefix": folder_prefix,
            "expectedPattern": f"{SOURCE_FOLDER_PREFIX}/date=YYYY-MM-DD",
            "correlationId": correlation_id
        }, level=logging.WARNING)
        return False

    date_str = match.group(1)
    try:
        folder_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as ve:
        json_log({
            "warning": "DateParsingError",
            "folderPrefix": folder_prefix,
            "date": date_str,
            "correlationId": correlation_id,
            "reason": str(ve)
        }, level=logging.WARNING)
        return False

    today = datetime.utcnow().date()
    if folder_date != today:
        json_log({
            "warning": "DateMismatch",
            "folderPrefix": folder_prefix,
            "dateFound": date_str,
            "expectedDate": str(today),
            "correlationId": correlation_id
        }, level=logging.WARNING)
        return False

    return True


def trigger_glue_job(bucket: str, folder_prefix: str, correlation_id: str) -> str:
    try:
        json_log({
            "action": "TriggeringGlueJob",
            "bucket": bucket,
            "folderPrefix": folder_prefix,
            "jobName": GLUE_JOB_NAME,
            "correlationId": correlation_id
        })

        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--DDG_ENDPOINT": DDG_ENDPOINT,
                "--BATCH_SIZE": BATCH_SIZE,
                "--SOURCE_BUCKET": bucket,
                "--SOURCE_FOLDER_PREFIX": folder_prefix
            }
        )

        job_run_id = response["JobRunId"]

        json_log({
            "status": "GlueJobStarted",
            "jobRunId": job_run_id,
            "folderPrefix": folder_prefix,
            "correlationId": correlation_id
        })

        return job_run_id

    except Exception as e:
        json_log({
            "error": "GlueJobTriggerFailed",
            "reason": str(e),
            "jobName": GLUE_JOB_NAME,
            "bucket": bucket,
            "folderPrefix": folder_prefix,
            "correlationId": correlation_id
        }, level=logging.ERROR)
        raise


def lambda_handler(event, context):
    correlation_id = str(uuid.uuid4())
    triggered_jobs = []
    seen_folders = set()

    try:
        records = event.get("Records", [])
        if not records:
            response = {"statusCode": 400, "body": "No records in S3 event."}
            json_log({**response, "correlationId": correlation_id}, level=logging.WARNING)
            return response

        for record in records:
            s3_info = record.get("s3", {})
            bucket = s3_info.get("bucket", {}).get("name")
            key = unquote_plus(s3_info.get("object", {}).get("key", ""))

            if not bucket or not key:
                json_log({
                    "warning": "MissingS3BucketOrKey",
                    "record": record,
                    "correlationId": correlation_id
                }, level=logging.WARNING)
                continue

            folder_prefix = str(Path(key).parent)

            # Avoid re-processing the same folder
            folder_id = f"{bucket}/{folder_prefix}"
            if folder_id in seen_folders:
                continue
            seen_folders.add(folder_id)

            try:
                if not is_valid_folder(folder_prefix, correlation_id):
                    continue
            except ValueError as e:
                json_log({
                    "warning": "FolderValidationFailed",
                    "folderPrefix": folder_prefix,
                    "correlationId": correlation_id,
                    "reason": str(e)
                }, level=logging.WARNING)
                continue

            try:
                job_run_id = trigger_glue_job(bucket, folder_prefix, correlation_id)
                triggered_jobs.append({
                    "folder": folder_prefix,
                    "jobRunId": job_run_id
                })
            except Exception:
                continue

        response = {
            "statusCode": 200 if triggered_jobs else 207,
            "body": f"{len(triggered_jobs)} Glue job(s) triggered." if triggered_jobs else "No Glue job was triggered.",
            "details": triggered_jobs
        }

        json_log({**response, "correlationId": correlation_id})
        return response

    except Exception as e:
        error_response = {
            "statusCode": 500,
            "body": "Lambda execution failed",
            "error": str(e)
        }
        json_log({**error_response, "correlationId": correlation_id}, level=logging.ERROR)
        return error_response
