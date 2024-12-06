import boto3
import time

s3 = boto3.client('s3')
bucket_name = 'testbucket2270'

def lambda_handler(event, context):
    # List all objects in the bucket to find the largest one
    response = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        largest_object = max(response['Contents'], key=lambda x: x['Size'])
        largest_object_key = largest_object['Key']

        # Delete the largest object
        s3.delete_object(Bucket=bucket_name, Key=largest_object_key)
        print(f"Deleted {largest_object_key} (size: {largest_object['Size']} bytes)")

    return {
        'statusCode': 200,
        'body': 'Cleaner lambda executed successfully'
    }
