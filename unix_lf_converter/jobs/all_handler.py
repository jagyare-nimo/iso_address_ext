import os
import boto3
import logging
from urllib.parse import unquote_plus

# Initialize clients
glue_client = boto3.client('glue')

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Read environment name (e.g., "dev", "uat", "prod")
env = os.environ.get("ENV", "dev")

# Job prefixes mapped to environment
glue_job_prefixes = {
    "dev": "crd",
    "uat": "crd-dev",
    "prod": "crd-prod"
}


def trigger_glue_job(glue_job_name, s3_file_uri):
    try:
        logger.info(f"Triggering Glue JOB: {glue_job_name}")
        run_id = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments={
                "--S3_FILE_URI": s3_file_uri
            }
        )['JobRunId']

        logger.info(f"Glue Job {glue_job_name} started with RunId: {run_id}")
    except Exception as exception:
        logger.error(f"Failed to trigger Glue Job {glue_job_name}: {str(exception)}")
        raise


def lambda_handler(event, context):
    try:
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        object_key = unquote_plus(record['s3']['object']['key'])

        logger.info(f"S3 File Uploaded: s3://{bucket_name}/{object_key}")

        s3_file_uri = f"s3://{bucket_name}/{object_key}"

        # Extract 'FENERGO' from 'entity/FENERGO/date=.../file.csv'
        parts = object_key.split('/')
        if len(parts) < 3:
            raise ValueError(f"Unexpected S3 key format: {object_key}")

        source_name = parts[1].lower()
        job_prefix = glue_job_prefixes.get(env)

        logger.info(f"Environment: {env}, Source: {source_name}, Prefix: {job_prefix}")

        # Determine which job to run
        if source_name == "apms":
            glue_job_name = f"{job_prefix}-apms-entity-details-glue-job"
        elif source_name == "fenergo":
            glue_job_name = f"{job_prefix}-fenergo-entity-ddg-transform-job"
        else:
            return {
                'statusCode': 400,
                'body': f"Unsupported source: {source_name}"
            }

        trigger_glue_job(glue_job_name, s3_file_uri)

        return {
            'statusCode': 200,
            'body': f"Glue job '{glue_job_name}' triggered successfully."
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': 'Lambda handler failed to execute'
        }
