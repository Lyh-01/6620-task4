import boto3
from datetime import datetime

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('S3-object-size-history')
queue_url = os.environ['QUEUE_URL']

def lambda_handler(event, context):
    bucket_name = 'testbucket2270'

    # List all objects in the bucket
    response = s3.list_objects_v2(Bucket=bucket_name)
    total_size = 0
    total_objects = 0

    if 'Contents' in response:
        for obj in response['Contents']:
            total_size += obj['Size']
            total_objects += 1

    # Store in DynamoDB
    timestamp = datetime.utcnow().isoformat()
    try:
        print(f"Putting item in DynamoDB: bucket_name={bucket_name}, total_size={total_size}, total_objects={total_objects}, timestamp={timestamp}")
        table.put_item(
            Item={
                'bucket_name': bucket_name,
                'timestamp': timestamp,
                'total_size': total_size,
                'total_objects': total_objects
            }
        )
        print("Item successfully written to DynamoDB.")
    except Exception as e:
        print(f"Error writing to DynamoDB: {e}")

    return {
        'statusCode': 200,
        'body': f'Total size: {total_size}, Total objects: {total_objects}, Timestamp: {timestamp}'
    }
