import boto3
import os
import os.path

def main(event, context):
    glue = boto3.client('glue')
    job_name = os.environ['GLUE_JOB_NAME']
    ddg_endpoint = os.environ['DDG_ENDPOINT']
    batch_size = os.environ.get('BATCH_SIZE', '100')

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Extract folder prefix to process all CSVs under that folder
        folder_prefix = os.path.dirname(key)  # e.g., 'entity/FENERGO/date=2025-06-02'

        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                '--DDG_ENDPOINT': ddg_endpoint,
                '--BATCH_SIZE': batch_size,
                '--SOURCE_BUCKET': bucket,
                '--SOURCE_FOLDER_PREFIX': folder_prefix
            }
        )
        print(f"Started Glue job {response['JobRunId']} for folder: {folder_prefix}")