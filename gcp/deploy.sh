#!/usr/bin/env bash
# Build the API image, push it to Artifact Registry, and deploy to Cloud Run.
#
# This script is NOT run automatically by anything in this repo — it requires your
# own GCP project + credentials (`gcloud auth login`) and will create real,
# billable GCP resources the first time you `terraform apply`. Read it before
# running it.
#
# Prerequisites:
#   1. terraform apply  (from gcp/terraform/) — provisions the Artifact Registry
#      repo, Cloud Run service, and IAM binding. Run once, and again after any
#      .tf change.
#   2. Docker installed and running locally.
#   3. gcloud CLI authenticated (`gcloud auth login && gcloud config set project <id>`).
#   4. An AWS IAM user (read-only, scoped to the aws/terraform data bucket) whose
#      keys are passed to `terraform apply` as aws_access_key_id/aws_secret_access_key
#      — this Cloud Run service reads the data layer from AWS S3, not GCS. See
#      main.tf and DECISIONS.md for why.
#
# Usage:
#   cd gcp && ./deploy.sh

set -euo pipefail

cd "$(dirname "$0")/.."   # repo root

REGION="${GCP_REGION:-us-central1}"
PROJECT_NAME="${PROJECT_NAME:-supply-chain-decision-engine}"
PROJECT_ID=$(gcloud config get-value project)

REPO="${REGION}-docker.pkg.dev/${PROJECT_ID}/${PROJECT_NAME}-api"

echo "==> Building API image (same Dockerfile.api used for AWS — see docker/)"
docker build -f docker/Dockerfile.api -t "${PROJECT_NAME}-api:latest" .

echo "==> Authenticating Docker to Artifact Registry"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

echo "==> Tagging and pushing image"
docker tag "${PROJECT_NAME}-api:latest" "${REPO}/api:latest"
docker push "${REPO}/api:latest"

echo "==> Exporting current data layer to S3 (same bucket AWS App Runner reads from)"
BUCKET=$(cd aws/terraform && terraform output -raw data_bucket_name)
python -m scripts.export_to_s3 --dest "s3://${BUCKET}"

echo "==> Deploying to Cloud Run"
gcloud run deploy "${PROJECT_NAME}-api" \
  --image "${REPO}/api:latest" \
  --region "${REGION}" \
  --platform managed

echo "==> Done. Service URL:"
(cd gcp/terraform && terraform output -raw cloud_run_url)
echo
