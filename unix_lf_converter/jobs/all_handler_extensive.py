import os
import boto3
import logging
import json
import uuid
from urllib.parse import unquote_plus

# Initialize AWS client
glue_client = boto3.client('glue')

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(message)s')  # Keep logs JSON-compatible
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


def json_log(payload: dict, level: str = "INFO"):
    log_entry = {
        "timestamp": uuid.uuid1().time,  # Ensures traceable ordering
        "level": level,
        **payload
    }
    logger.log(getattr(logging, level.upper(), logging.INFO), json.dumps(log_entry))


# Environment config
env = os.environ.get("ENV", "dev").lower()
glue_job_prefixes = {
    "dev": "crd",
    "uat": "crd-dev",
    "prod": "crd-prod"
}

source_to_job_suffix = {
    "apms": "apms-entity-details-glue-job",
    "fenergo": "fenergo-entity-ddg-transform-job"
}


def trigger_glue_job(job_name: str, s3_uri: str, correlation_id: str):
    try:
        json_log({
            "action": "StartGlueJob",
            "jobName": job_name,
            "s3FileUri": s3_uri,
            "correlationId": correlation_id
        })

        run_id = glue_client.start_job_run(
            JobName=job_name,
            Arguments={"--S3_FILE_URI": s3_uri}
        )["JobRunId"]

        json_log({
            "status": "GlueJobStarted",
            "jobName": job_name,
            "runId": run_id,
            "correlationId": correlation_id
        })
        return run_id

    except Exception as e:
        json_log({
            "error": "GlueJobStartFailed",
            "jobName": job_name,
            "reason": str(e),
            "correlationId": correlation_id
        }, level="ERROR")
        raise


def lambda_handler(event, context):
    correlation_id = str(uuid.uuid4())

    try:
        record = event.get("Records", [])[0]
        bucket = record.get("s3", {}).get("bucket", {}).get("name")
        key = unquote_plus(record.get("s3", {}).get("object", {}).get("key", ""))

        if not bucket or not key:
            raise ValueError("Missing S3 bucket or object key in event")

        s3_file_uri = f"s3://{bucket}/{key}"
        parts = key.split('/')

        if len(parts) < 3:
            raise ValueError(f"Unexpected S3 key format: {key}")

        source_name = parts[1].lower()
        job_prefix = glue_job_prefixes.get(env)

        if not job_prefix:
            raise ValueError(f"Unsupported environment: {env}")

        job_suffix = source_to_job_suffix.get(source_name)
        if not job_suffix:
            json_log({
                "warning": "UnsupportedSource",
                "source": source_name,
                "correlationId": correlation_id
            }, level="WARNING")
            return {
                "statusCode": 400,
                "body": f"Unsupported source: {source_name}"
            }

        glue_job_name = f"{job_prefix}-{job_suffix}"
        trigger_glue_job(glue_job_name, s3_file_uri, correlation_id)

        return {
            "statusCode": 200,
            "body": f"Glue job '{glue_job_name}' triggered successfully.",
            "correlationId": correlation_id
        }

    except Exception as e:
        json_log({
            "error": "LambdaFailure",
            "reason": str(e),
            "correlationId": correlation_id
        }, level="CRITICAL")
        return {
            "statusCode": 500,
            "body": "Lambda handler failed to execute",
            "correlationId": correlation_id
        }