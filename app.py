#!/usr/bin/env python3
from aws_cdk import core
from s3_fanout_stack import S3FanoutStack

app = core.App()

# Define the stack and specify the environment (AWS region, account, etc.)
S3FanoutStack(app, "S3FanoutStack", env=core.Environment(account="376129856599", region="us-east-1"))

# Synthesize the CloudFormation template
app.synth()