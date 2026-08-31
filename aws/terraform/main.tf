data "aws_caller_identity" "current" {}

# ── S3 data layer ────────────────────────────────────────────────────────────────
# Bucket name includes the account ID because S3 bucket names are globally unique
# across all of AWS, not just this account.
resource "aws_s3_bucket" "data_layer" {
  bucket = "${var.project_name}-data-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "data_layer" {
  bucket = aws_s3_bucket.data_layer.id
  versioning_configuration {
    # Versioning, not lifecycle rules, is the mechanism keeping old DuckDB snapshots
    # recoverable — every scripts/export_to_s3.py run overwrites the same key, and
    # versioning is what turns that into "keep history" instead of "destructive
    # overwrite."
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_layer" {
  bucket = aws_s3_bucket.data_layer.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_layer" {
  bucket = aws_s3_bucket.data_layer.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── ECR: where the API image lives ────────────────────────────────────────────────
resource "aws_ecr_repository" "api" {
  name                 = "${var.project_name}-api"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# ── IAM: App Runner's two distinct roles ──────────────────────────────────────────
# App Runner has two separate IAM roles with different trust principals, which is a
# common point of confusion: the "access role" is assumed by App Runner's build
# service to pull the image from ECR; the "instance role" is assumed by the running
# container itself and grants the permissions the application needs at runtime (here,
# reading the DuckDB snapshot from S3). Conflating them would either over-grant the
# build step S3 access it doesn't need, or under-grant the running container ECR
# access it doesn't need either — neither should have both.

data "aws_iam_policy_document" "apprunner_ecr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["build.apprunner.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "apprunner_ecr_access" {
  name               = "${var.project_name}-apprunner-ecr-access"
  assume_role_policy = data.aws_iam_policy_document.apprunner_ecr_assume.json
}

resource "aws_iam_role_policy_attachment" "apprunner_ecr_access" {
  role       = aws_iam_role.apprunner_ecr_access.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess"
}

data "aws_iam_policy_document" "apprunner_instance_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["tasks.apprunner.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "apprunner_instance" {
  name               = "${var.project_name}-apprunner-instance"
  assume_role_policy = data.aws_iam_policy_document.apprunner_instance_assume.json
}

# Scoped to exactly this bucket, read-only, no write/delete — the running API
# container should never be able to modify the data layer it's serving from. Writes
# to this bucket happen only from scripts/export_to_s3.py, run by a human or a CI
# job with its own separate credentials, not by the API's instance role.
data "aws_iam_policy_document" "s3_read_data_layer" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.data_layer.arn}/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data_layer.arn]
  }
}

resource "aws_iam_role_policy" "apprunner_instance_s3_read" {
  name   = "${var.project_name}-s3-read"
  role   = aws_iam_role.apprunner_instance.id
  policy = data.aws_iam_policy_document.s3_read_data_layer.json
}

# ── App Runner: the containerized FastAPI service ────────────────────────────────
# App Runner is AWS's direct equivalent of GCP Cloud Run — a fully managed container
# platform with no cluster, load balancer, or VPC to provision, HTTPS by default, and
# scale-to-zero-adjacent billing. It's the natural AWS counterpart to the "FastAPI on
# Cloud Run" resume claim this project is reconciling (see DECISIONS.md, Phase 4) —
# same category of service, different cloud.
resource "aws_apprunner_service" "api" {
  service_name = "${var.project_name}-api"

  source_configuration {
    authentication_configuration {
      access_role_arn = aws_iam_role.apprunner_ecr_access.arn
    }

    image_repository {
      image_identifier      = "${aws_ecr_repository.api.repository_url}:latest"
      image_repository_type = "ECR"

      image_configuration {
        port = "8000"
        runtime_environment_variables = {
          DUCKDB_S3_URI     = "s3://${aws_s3_bucket.data_layer.bucket}/${var.duckdb_snapshot_key}"
          DUCKDB_PATH       = "/app/data/duckdb/supply_chain.duckdb"
          AWS_REGION        = var.aws_region
          ANTHROPIC_API_KEY = var.anthropic_api_key
        }
      }
    }

    # Deliberately off: a new `docker push` to :latest should not silently redeploy a
    # running service. Redeploys are triggered explicitly (see aws/deploy.sh), which
    # is the safer default for something that fetches a specific data snapshot on
    # startup — an unplanned redeploy would also silently repull whatever the
    # DUCKDB_S3_URI object currently is, which may not match what was tested.
    auto_deployments_enabled = false
  }

  instance_configuration {
    cpu               = var.cpu
    memory            = var.memory
    instance_role_arn = aws_iam_role.apprunner_instance.arn
  }

  health_check_configuration {
    protocol            = "HTTP"
    path                = "/health"
    interval            = 10
    timeout             = 5
    healthy_threshold   = 1
    unhealthy_threshold = 5
  }

  tags = {
    Project = var.project_name
  }
}
