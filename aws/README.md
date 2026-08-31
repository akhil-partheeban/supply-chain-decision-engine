# AWS Deployment

Deploys two things: the data layer to **S3** (Parquet exports of every bronze/silver/
gold table, plus a DuckDB snapshot), and the FastAPI service to **App Runner** (AWS's
managed-container equivalent of GCP Cloud Run) as a Docker image hosted in **ECR**.

> **This infrastructure has not been applied to a live AWS account.** The Terraform in
> `terraform/` was written, formatted (`terraform fmt`), and validated
> (`terraform init && terraform validate`) against the real AWS provider schema — that
> confirms the HCL is syntactically and semantically correct — but `terraform plan`/
> `apply` were not run, because no AWS credentials were available in the environment
> this was built in. Applying this will create real, billable resources in your AWS
> account; run it yourself (or hand me credentials and ask explicitly) rather than
> assuming it's already live. See `DECISIONS.md` (Phase 3) for the full reasoning.

## Architecture

```
scripts/export_to_s3.py
    ↓ (Parquet per table + DuckDB snapshot)
S3 bucket (supply-chain-decision-engine-data-<account-id>)
    ↓ (DUCKDB_S3_URI, fetched by docker/fetch_db.py at container startup)
App Runner service (docker/Dockerfile.api, image in ECR)
    ↓
FastAPI  (/health, /suppliers, /decisions/ask)
```

The Streamlit dashboard and `docker compose` setup are **not** part of this AWS
deployment — they're unaffected by anything here, and continue to run exactly as
documented in the main README (local dev) or on Streamlit Community Cloud.

## One-time setup

```bash
# 1. Provision the AWS resources (S3 bucket, ECR repo, IAM roles, App Runner service)
cd aws/terraform
terraform init
terraform plan   -var="anthropic_api_key=$ANTHROPIC_API_KEY"
terraform apply  -var="anthropic_api_key=$ANTHROPIC_API_KEY"
```

The very first `apply` creates an App Runner service pointing at an ECR repository
that doesn't have an image in it yet — App Runner will report a failed deployment
until you push an image (next step). This ordering (infra first, image second) is
normal for this kind of setup and not a bug.

```bash
# 2. Build, push, and deploy the API image + current data snapshot
cd ../..              # repo root
./aws/deploy.sh
```

`deploy.sh` builds `docker/Dockerfile.api`, pushes it to the ECR repo Terraform just
created, exports the current gold layer to the S3 bucket via
`scripts/export_to_s3.py`, and triggers an App Runner deployment.

## Day-to-day: refreshing the data layer

Whenever the local pipeline is re-run (`dbt build` produces new gold tables), push the
refresh to S3 and let the API pick it up on its next cold start or an explicit
redeploy:

```bash
python -m scripts.export_to_s3 --dest s3://$(cd aws/terraform && terraform output -raw data_bucket_name)
aws apprunner start-deployment --service-arn <service-arn-from-terraform-output>
```

`auto_deployments_enabled` is deliberately `false` in the Terraform config — pushing a
new image or a new data snapshot does not silently redeploy the running service. See
DECISIONS.md for why.

## Cost

App Runner at the smallest size (0.25 vCPU / 0.5 GB, `variables.tf` defaults) bills
per-second while running plus a (small) provisioned-but-idle rate; an S3 bucket this
size (~100-200MB) costs a fraction of a cent per month in storage. Neither is free-tier
guaranteed forever — check current AWS pricing before leaving this running
indefinitely. `terraform destroy` tears everything down.

## Manual alternative (no Terraform)

If you'd rather click through the console or don't want to install Terraform:

1. **S3**: create a bucket, block all public access, enable versioning.
2. **ECR**: create a private repository named `<project>-api`.
3. Build and push the image manually:
   ```bash
   docker build -f docker/Dockerfile.api -t <ecr-repo-url>:latest .
   aws ecr get-login-password | docker login --username AWS --password-stdin <account-id>.dkr.ecr.<region>.amazonaws.com
   docker push <ecr-repo-url>:latest
   ```
4. **App Runner**: create a service from the ECR image, port `8000`, health check
   path `/health`, environment variables `DUCKDB_S3_URI=s3://<bucket>/supply_chain.duckdb`,
   `AWS_REGION=<region>`, `ANTHROPIC_API_KEY=<key>`. Attach an instance role with
   `s3:GetObject`/`s3:ListBucket` scoped to the bucket (see `main.tf` for the exact
   policy document if replicating by hand).
5. Run `python -m scripts.export_to_s3 --dest s3://<bucket>` to populate the data
   layer before the service's first request.
