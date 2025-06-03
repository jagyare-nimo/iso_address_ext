import boto3
import os


def main(event, context):
    glue = boto3.client('glue')
    job_name = os.environ['GLUE_JOB_NAME']
    ddg_endpoint = os.environ['DDG_ENDPOINT']
    batch_size = os.environ.get('BATCH_SIZE', '100')

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        if key.endswith('.csv'):
            response = glue.start_job_run(
                JobName=job_name,
                Arguments={
                    '--DDG_ENDPOINT': ddg_endpoint,
                    '--BATCH_SIZE': batch_size
                },
                Environment={
                    'SOURCE_BUCKET': bucket,
                    'SOURCE_FILE_KEY': key
                }
            )
            print(f"Started Glue job {response['JobRunId']} for file: {key}")
