import boto3
import time
import os

s3 = boto3.client('s3')
sns_topic_arn = os.environ['SNS_TOPIC_ARN']

def lambda_handler(event, context):
    # Create object 'assignment1.txt'
    s3.put_object(Bucket='testbucket2270', Key='assignment1.txt', Body='Empty Assignment 1')
    time.sleep(2)

    # Create object 'assignment2.txt'
    s3.put_object(Bucket='testbucket2270', Key='assignment2.txt', Body='Empty Assignment 2222222222')
    time.sleep(2)

    # Publish event to SNS
    sns = boto3.client('sns')
    sns.publish(TopicArn=sns_topic_arn, Message="Object created: assignment2.txt", Subject="S3 Event")

    time.sleep(2)

    # Create object 'assignment3.txt'
    s3.put_object(Bucket='testbucket2270', Key='assignment3.txt', Body='33')
    time.sleep(2)

    return {
        'statusCode': 200,
        'body': 'Driver Lambda executed successfully!'
    }
