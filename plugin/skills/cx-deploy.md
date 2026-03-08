---
name: cx-deploy
description: "Deploy the CX Intelligence Platform to AWS using CloudFormation. Use when the user asks to deploy, provision AWS resources, or manage infrastructure."
---

# AWS Deployment

Deploy the Nexus CX Intelligence Platform to AWS using the CloudFormation templates in the project's `/cloudformation/` directory.

## Prerequisites

Before deploying, verify:

```bash
# AWS CLI is configured
aws sts get-caller-identity

# Check current region
aws configure get region
```

Required IAM permissions:
- CloudFormation (create/update/delete stacks)
- Lambda (create functions)
- API Gateway (create REST APIs)
- DynamoDB (create tables)
- Kinesis (create streams)
- SQS (create queues)
- SNS (create topics)
- IAM (create roles and policies for the stack)
- CloudWatch (create log groups, alarms, dashboards)
- S3 (create buckets for artifacts)

## Stack Architecture

The `main.yaml` template deploys the full platform as nested stacks:

| Layer | AWS Service | Purpose |
|-------|------------|---------|
| Event Fabric | Kinesis Data Streams + Lambda | Real-time event ingestion and fan-out |
| Classifier | Lambda + DynamoDB | Intent classification (TF-IDF model in Lambda layer) |
| Journey Engine | DynamoDB + Lambda | State machine tracking with DDB Streams |
| NBA Engine | Lambda + SQS | Next-best-action rules engine with async processing |
| Analytics | Kinesis Analytics + CloudWatch | Real-time metrics, dashboards, alarms |
| API Layer | API Gateway + Lambda | REST API for external consumers |
| Dashboard | S3 + CloudFront | Static dashboard UI hosting |

## Deploy

### Development Environment

```bash
cd ~/omnichannel-cx-platform

aws cloudformation deploy \
  --template-file cloudformation/main.yaml \
  --stack-name cx-platform-dev \
  --parameter-overrides \
    Environment=dev \
    KinesisShardCount=1 \
    DynamoDBReadCapacity=5 \
    DynamoDBWriteCapacity=5 \
  --capabilities CAPABILITY_NAMED_IAM \
  --tags Project=cx-platform Environment=dev Owner=chad
```

### Staging Environment

```bash
aws cloudformation deploy \
  --template-file cloudformation/main.yaml \
  --stack-name cx-platform-staging \
  --parameter-overrides \
    Environment=staging \
    KinesisShardCount=2 \
    DynamoDBReadCapacity=25 \
    DynamoDBWriteCapacity=25 \
  --capabilities CAPABILITY_NAMED_IAM \
  --tags Project=cx-platform Environment=staging Owner=chad
```

### Production Environment

```bash
aws cloudformation deploy \
  --template-file cloudformation/main.yaml \
  --stack-name cx-platform-prod \
  --parameter-overrides \
    Environment=prod \
    KinesisShardCount=4 \
    DynamoDBReadCapacity=100 \
    DynamoDBWriteCapacity=100 \
  --capabilities CAPABILITY_NAMED_IAM \
  --tags Project=cx-platform Environment=prod Owner=chad
```

## Post-Deployment Validation

After deployment completes, validate the stack:

```bash
# Check stack status
aws cloudformation describe-stacks --stack-name cx-platform-dev --query 'Stacks[0].StackStatus'

# Get stack outputs (API URL, dashboard URL, etc.)
aws cloudformation describe-stacks --stack-name cx-platform-dev \
  --query 'Stacks[0].Outputs' --output table

# Test the API endpoint (from stack outputs)
API_URL=$(aws cloudformation describe-stacks --stack-name cx-platform-dev \
  --query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' --output text)

curl -s "$API_URL/health" | jq .

# Test classification
curl -s -X POST "$API_URL/classify" \
  -H 'Content-Type: application/json' \
  -d '{"text": "I want a refund for the duplicate charge"}' | jq .

# Verify Kinesis stream
aws kinesis describe-stream --stream-name cx-events-dev --query 'StreamDescription.StreamStatus'

# Check DynamoDB tables
aws dynamodb list-tables --query 'TableNames[?starts_with(@, `cx-`)]'
```

## Cost Estimation

| Environment | Estimated Monthly Cost | Notes |
|-------------|----------------------|-------|
| Dev | $15-30 | Minimal throughput, on-demand where possible |
| Staging | $50-100 | Moderate throughput for testing |
| Production | $200-500+ | Depends on event volume, scales with Kinesis shards |

Primary cost drivers:
- **Kinesis Data Streams**: $0.015/shard-hour (~$11/shard/month)
- **Lambda**: Free tier covers most dev usage; $0.20 per 1M requests beyond
- **DynamoDB**: On-demand pricing recommended for dev/staging; provisioned for prod
- **API Gateway**: $3.50 per 1M API calls
- **CloudWatch**: Minimal for logs/metrics, dashboards are $3/month each

## Stack Management

```bash
# View stack events (useful for debugging deployment issues)
aws cloudformation describe-stack-events --stack-name cx-platform-dev \
  --query 'StackEvents[0:10].[Timestamp,ResourceStatus,ResourceType,LogicalResourceId]' \
  --output table

# Update stack (same deploy command — CloudFormation creates a changeset)
aws cloudformation deploy --template-file cloudformation/main.yaml \
  --stack-name cx-platform-dev --capabilities CAPABILITY_NAMED_IAM

# Delete stack
aws cloudformation delete-stack --stack-name cx-platform-dev

# Monitor deletion
aws cloudformation wait stack-delete-complete --stack-name cx-platform-dev
```

## Troubleshooting

- **ROLLBACK_COMPLETE**: Check stack events for the first CREATE_FAILED resource. Common causes: IAM permission issues, resource name conflicts, or service limits.
- **Nested stack failures**: The error will reference a nested stack ARN. Describe that specific stack for detailed events.
- **Lambda cold starts**: First classification request after deploy may take 3-5 seconds. Subsequent requests use warm containers.
- **Kinesis throttling**: If you see ProvisionedThroughputExceededException, increase shard count.
