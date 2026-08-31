# GCP Deployment — FastAPI on Cloud Run

Deploys the same containerized FastAPI service from `aws/` to **Cloud Run**, GCP's
managed-container platform — the specific claim this phase exists to make true (see
`DECISIONS.md`, Phase 4: this repo had zero GCP deployment artifacts before this
phase, despite the resume claiming "FastAPI on Cloud Run").

> **This infrastructure has not been applied to a live GCP project.** The Terraform
> in `terraform/` was formatted (`terraform fmt`) and validated
> (`terraform init && terraform validate`) against the real Google provider schema —
> confirming the HCL is syntactically and semantically correct — but `plan`/`apply`
> were not run, because no GCP project or credentials were available in the
> environment this was built in. Run it yourself against your own GCP project.

## Architecture — and why it's cross-cloud on purpose

```
aws/terraform  → S3 bucket (data layer: Parquet + DuckDB snapshot)
                       ↓ DUCKDB_S3_URI (same mechanism as AWS App Runner)
gcp/terraform  → Cloud Run service (same docker/Dockerfile.api image as AWS)
                       ↓
                 FastAPI  (/health, /suppliers, /decisions/ask)
```

This Cloud Run service reads its data from the **same AWS S3 bucket** provisioned in
`aws/terraform/`, via the identical `docker/fetch_db.py` cold-start fetch used by the
AWS App Runner deployment — it does **not** maintain a separate GCS copy of the data
layer. That's a deliberate design choice: it means the exact same Docker image runs
unmodified on both clouds, which is a much stronger "this is genuinely portable"
claim than two parallel, independently-drifting data copies would be. The tradeoff —
a GCP-hosted container depending on AWS credentials and cross-cloud network egress —
is named directly in `DECISIONS.md`, not hidden.

## One-time setup

```bash
# 0. Prerequisite: aws/terraform has already been applied (see ../aws/README.md) —
#    this deployment reads DUCKDB_S3_URI from that bucket.

# 1. Authenticate and set your project
gcloud auth login
gcloud config set project <your-gcp-project-id>

# 2. Provision Cloud Run + Artifact Registry
cd gcp/terraform
terraform init
terraform apply \
  -var="gcp_project_id=<your-gcp-project-id>" \
  -var="anthropic_api_key=$ANTHROPIC_API_KEY" \
  -var="aws_access_key_id=$AWS_ACCESS_KEY_ID" \
  -var="aws_secret_access_key=$AWS_SECRET_ACCESS_KEY" \
  -var="duckdb_s3_uri=s3://$(cd ../../aws/terraform && terraform output -raw data_bucket_name)/supply_chain.duckdb"
```

As with the AWS setup, the first `apply` creates a Cloud Run service pointing at an
Artifact Registry image that doesn't exist yet — it will report a failed revision
until the first image push (next step). Normal, not a bug.

```bash
# 3. Build, push, and deploy the API image
cd ../..              # repo root
./gcp/deploy.sh
```

## Cost

Cloud Run bills per-request/per-second of actual CPU time with a genuine
scale-to-zero floor (unlike App Runner's always-provisioned minimum) — for a
low-traffic demo service, Cloud Run is typically the cheaper of the two when mostly
idle. Check current GCP pricing before leaving this running indefinitely.
`terraform destroy` tears everything down.

## Manual alternative (no Terraform)

1. **Artifact Registry**: `gcloud artifacts repositories create <project>-api --repository-format=docker --location=<region>`
2. Build, tag, push:
   ```bash
   docker build -f docker/Dockerfile.api -t <region>-docker.pkg.dev/<project-id>/<project>-api/api:latest .
   gcloud auth configure-docker <region>-docker.pkg.dev
   docker push <region>-docker.pkg.dev/<project-id>/<project>-api/api:latest
   ```
3. **Cloud Run**: `gcloud run deploy <project>-api --image <the-image-above> --region <region> --allow-unauthenticated --set-env-vars DUCKDB_S3_URI=s3://<bucket>/supply_chain.duckdb,AWS_REGION=<region>,AWS_ACCESS_KEY_ID=...,AWS_SECRET_ACCESS_KEY=...,ANTHROPIC_API_KEY=...`
