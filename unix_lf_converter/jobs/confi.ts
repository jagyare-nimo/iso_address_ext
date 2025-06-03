// GlueStack.ts - Hybrid Approach with CDK-Defined Glue Job Only
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';

export class GlueStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const jobRole = iam.Role.fromRoleArn(
      this,
      'FenergoGlueJobRole',
      'arn:aws:iam::30574743344:role/81774/CRCDKSecurityResources/crd-rawzone-glue-role'
    );

    new glue.CfnJob(this, 'FenergoEntityRawzoneGlueJob', {
      jobname: 'fenergo-entity-rawzone-ddg-transform-job',
      jobrole: jobRole.roleArn,
      securityConfiguration: 'fenergoEntityRawzoneGlueSecurityConfig',
      command: {
        name: 'glueetl',
        scriptLocation: 's3://fenergo-entity-rawzone/scripts/fenergo_entity_rawzone_ddg_transform.py',
        pythonVersion: '3',
      },
      connections: ['FenergoEntityRawzoneGlueConnection'],
      description: 'Fenergo Entity Rawzone ETL Job: Parse, transform, and push entities to DDG endpoint.',
      maxRetries: 0,
      numberOfWorkers: 4,
      workerType: 'G.1X',
      executionProperty: {
        maxConcurrentRuns: 2,
      },
      timeout: 2880,
      notifyDelayAfter: 2,
      glueVersion: '3.0',
      logUri: 's3://fenergo-entity-rawzone/glue-logs/',
      defaultArguments: {
        '--job-bookmark-option': 'job-bookmark-disable',
        '--job-language': 'python',
        '--enable-metrics': 'true',
        '--enable-continuous-cloudwatch-log': 'true',
        '--extra-py-files': 's3://fenergo-entity-rawzone/glue-assets/scripts/glue_utilities.py',
        '--S3_OUTPUT_PATH': 's3://fenergo-entity-processed/',
        '--S3_TRANSFORMATIONS_PATH': 's3://fenergo-entity-rawzone/glue-assets/mappings/entity-transformations.json',
        '--DDG_ENDPOINT': 'https://your-ddg-endpoint/api/entity/batch',
        '--BATCH_SIZE': '100'
      },
    });
  }
}