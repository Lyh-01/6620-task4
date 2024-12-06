from aws_cdk import (
    core,
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_sns as sns,
    aws_sqs as sqs,
    aws_iam as iam,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_s3_notifications as s3_notifications,
    aws_events as events,
    aws_events_targets as targets,
    aws_sns_subscriptions as subs
)

class S3FanoutStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # S3 Bucket
        bucket = s3.Bucket(self, 'TestBucket2270')

        # SNS Topic for fanout pattern
        sns_topic = sns.Topic(self, 'S3EventTopic')

        # SQS Queues for consumers
        size_tracking_queue = sqs.Queue(self, 'SizeTrackingQueue')
        logging_queue = sqs.Queue(self, 'LoggingQueue')

        # Subscribe SQS queues to SNS Topic
        sns_topic.add_subscription(subs.SqsSubscription(size_tracking_queue))
        sns_topic.add_subscription(subs.SqsSubscription(logging_queue))

        # Lambda Functions
        size_tracking_lambda = self.create_lambda('SizeTrackingLambda', 'size_tracking_lambda', size_tracking_queue)
        logging_lambda = self.create_lambda('LoggingLambda', 'logging_lambda', logging_queue)
        cleaner_lambda = self.create_lambda('CleanerLambda', 'cleaner_lambda')

        # Permissions
        bucket.grant_read_write(size_tracking_lambda)
        bucket.grant_read_write(cleaner_lambda)

        # CloudWatch Logs for Logging Lambda
        log_group = logs.LogGroup(self, 'LoggingLambdaLogGroup')

        # CloudWatch Metric Filter for object size delta
        metric_filter = logs.MetricFilter(
            self, 'MetricFilter',
            log_group=log_group,
            filter_pattern=logs.FilterPattern.all_terms('object_name', 'size_delta'),
            metric_namespace='Assignment4App',
            metric_name='TotalObjectSize',
            metric_value="$.size_delta"
        )

        # CloudWatch Alarm for the TotalObjectSize metric
        alarm = cloudwatch.Alarm(
            self, 'TotalObjectSizeAlarm',
            metric=metric_filter.metric(),
            threshold=20,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD
        )

        # Trigger Cleaner Lambda when alarm fires
        alarm.add_alarm_action(cw_actions.LambdaFunction(cleaner_lambda))

        # EventBridge rule to trigger the Driver Lambda (for testing)
        driver_lambda = self.create_driver_lambda('DriverLambda', sns_topic.topic_arn)
        rule = events.Rule(self, 'DriverRule', event_pattern=events.EventPattern(
            source=["aws.lambda"],
            detail_type=["AWS API Call via CloudTrail"],
            detail={"eventSource": ["s3.amazonaws.com"]}
        ))
        rule.add_target(targets.LambdaFunction(driver_lambda))

    def create_lambda(self, id: str, handler: str, queue=None):
        """ Helper function to create lambda """
        lambda_role = iam.Role(self, f'{id}Role', assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'))
        if queue:
            queue.grant_consume_messages(lambda_role)
        return lambda_.Function(
            self, id,
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler=f'{handler}.lambda_handler',
            code=lambda_.Code.from_asset('lambda'),
            environment={
                'QUEUE_URL': queue.queue_url if queue else '',
            },
            role=lambda_role
        )

    def create_driver_lambda(self, id: str, sns_topic_arn):
        """ Create the Driver Lambda """
        driver_role = iam.Role(self, 'DriverLambdaRole', assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'))
        return lambda_.Function(
            self, id,
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler='driver_lambda.lambda_handler',
            code=lambda_.Code.from_asset('lambda'),
            environment={
                'SNS_TOPIC_ARN': sns_topic_arn
            },
            role=driver_role
        )
