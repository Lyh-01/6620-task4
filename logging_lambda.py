import boto3
import json
import os

logs = boto3.client('logs')
log_group_name = '/aws/lambda/logging-lambda'

def lambda_handler(event, context):
    # Extract object name and size_delta from the event
    s3_event = json.loads(event['Records'][0]['body'])
    object_name = s3_event['object_name']
    size_delta = s3_event['size_delta']

    # Log to CloudWatch
    log_message = {
        "object_name": object_name,
        "size_delta": size_delta
    }

    # Put log message
    logs.put_log_events(
        logGroupName=log_group_name,
        logStreamName='S3EventStream',
        logEvents=[{
            'timestamp': int(round(time.time() * 1000)),
            'message': json.dumps(log_message)
        }]
    )

    return {
        'statusCode': 200,
        'body': 'Log recorded'
    }
