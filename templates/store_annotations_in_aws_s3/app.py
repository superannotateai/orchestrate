"""
Python Version Compatibility: 3.8, 3.9, 3.10, 3.11
Dependencies:
    superannotate library, version 4.4.19 or higher
    boto3 library, version 1.28.0 or higher

This script sets up a connection to AWS based on credentials and stores the annotation JSON file in a specified location.

Before running the script, make sure to set the following environment variables:
- SA_TOKEN: The SuperAnnotate SDK token. With this key, the client will be automatically initialized.
- EXTERNAL_ID: todo define
- ROLE_ARN: role ARN
- BUCKET_NAME: S3 bucket name
- AWS_ACCESS_KEY_ID: The access key for your AWS account. With this key, the boto3 client will be automatically initialized.
- AWS_SECRET_ACCESS_KEY: The secret key for your AWS account. With this key, the boto3 client will be automatically initialized.

You can define key-value variables from the Secrets page of the Actions tab in Orchestrate. You can then mount this secret to a custom action in your pipeline.

Please refer to the documentation for more details: https://doc.superannotate.com/docs/create-automation#secrets.

The `handler` function triggers the script upon an event [Item annotation status updated].
"""

import os
import json
import random
import logging
from datetime import datetime

import boto3
from superannotate import SAClient

logging.basicConfig(level=logging.INFO)


def generateSessionId():
    return f'id_{round(random.random() * 1000000000)}_{datetime.now().strftime("%d_%m_%Y_%H-%M-%S")}'


sa = SAClient()

EXTERNAL_ID = os.environ.get("EXTERNAL_ID")
ROLE_ARN = os.environ.get("ROLE_ARN")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
PREFIX = 'outputs/'


def handler(metadata, context):
    # Get item info
    current_state = context['after']
    item_name = current_state['name']
    project_name = current_state['project_name']

    project_id, folder_id = current_state['project_id'], current_state['folder_id']
    folder_name = sa.get_folder_by_id(project_id, folder_id)['name']

    project_path = os.path.join(project_name, folder_name)

    # Get Annotation for item
    annotation_data = sa.get_annotations(project=project_path, items=[item_name])

    # Set up Boto3/AWS credentials
    sess = boto3.Session()
    sts_connection = sess.client('sts')

    assume_role_object = sts_connection.assume_role(
        RoleArn=ROLE_ARN,
        RoleSessionName=generateSessionId(),
        DurationSeconds=3600,
        ExternalId=EXTERNAL_ID)
    credentials = assume_role_object['Credentials']

    session = boto3.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
    s3 = session.resource('s3')

    # Create S3 object and upload JSON to s3
    # Take only the item name without the extension
    item_out_name = os.path.splitext(item_name)[0]

    annot_json = annotation_data[0]

    logging.info(f'item_out_name: {item_out_name}')
    s3_object = s3.Object(BUCKET_NAME, PREFIX + item_out_name + ".json")
    status = s3_object.put(Body=json.dumps(annot_json))
    logging.info(status['ResponseMetadata']['HTTPStatusCode'])
    return status['ResponseMetadata']['HTTPStatusCode']
