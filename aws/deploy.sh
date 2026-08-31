#!/usr/bin/env bash
# Build the API image, push it to ECR, and trigger an App Runner deployment.
#
# This script is NOT run automatically by anything in this repo — it requires your
# own AWS credentials (`aws configure` or exported AWS_* env vars) and will create
# real, billable AWS resources the first time you `terraform apply`. Read it before
# running it.
#
# Prerequisites:
#   1. terraform apply  (from aws/terraform/) — provisions the ECR repo, S3 bucket,
#      IAM roles, and App Runner service. Run once, and again after any .tf change.
#   2. Docker installed and running locally.
#   3. AWS credentials with ECR push + App Runner deploy permissions.
#
# Usage:
#   cd aws && ./deploy.sh

set -euo pipefail

cd "$(dirname "$0")/.."   # repo root

REGION="${AWS_REGION:-us-east-1}"
PROJECT_NAME="${PROJECT_NAME:-supply-chain-decision-engine}"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REPO="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${PROJECT_NAME}-api"

echo "==> Building API image"
docker build -f docker/Dockerfile.api -t "${PROJECT_NAME}-api:latest" .

echo "==> Authenticating Docker to ECR"
aws ecr get-login-password --region "${REGION}" \
  | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

echo "==> Tagging and pushing image"
docker tag "${PROJECT_NAME}-api:latest" "${ECR_REPO}:latest"
docker push "${ECR_REPO}:latest"

echo "==> Exporting current data layer to S3"
BUCKET=$(cd aws/terraform && terraform output -raw data_bucket_name)
python -m scripts.export_to_s3 --dest "s3://${BUCKET}"

echo "==> Triggering App Runner deployment"
SERVICE_ARN=$(aws apprunner list-services --region "${REGION}" \
  --query "ServiceSummaryList[?ServiceName=='${PROJECT_NAME}-api'].ServiceArn" --output text)
aws apprunner start-deployment --region "${REGION}" --service-arn "${SERVICE_ARN}"

echo "==> Done. Service URL:"
(cd aws/terraform && terraform output -raw api_service_url)
echo
