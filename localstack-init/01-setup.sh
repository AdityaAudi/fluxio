#!/bin/bash
# Runs automatically when LocalStack is ready.
# Creates the DynamoDB table and SQS FIFO queue fluxio needs.

echo ">>> Creating fluxio DynamoDB table..."
awslocal dynamodb create-table \
  --table-name fluxio_workflows \
  --billing-mode PAY_PER_REQUEST \
  --attribute-definitions \
    AttributeName=PK,AttributeType=S \
    AttributeName=SK,AttributeType=S \
  --key-schema \
    AttributeName=PK,KeyType=HASH \
    AttributeName=SK,KeyType=RANGE

echo ">>> Creating fluxio SQS FIFO queue..."
awslocal sqs create-queue \
  --queue-name fluxio.fifo \
  --attributes \
    FifoQueue=true \
    ContentBasedDeduplication=false \
    VisibilityTimeout=60 \
    RedrivePolicy='{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:fluxio-dlq.fifo","maxReceiveCount":"3"}'

echo ">>> Creating fluxio DLQ..."
awslocal sqs create-queue \
  --queue-name fluxio-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

echo ">>> LocalStack fluxio resources ready."
awslocal dynamodb list-tables
awslocal sqs list-queues
