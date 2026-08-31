# ── Artifact Registry: where the API image lives (GCP's ECR equivalent) ──────────
resource "google_artifact_registry_repository" "api" {
  location      = var.gcp_region
  repository_id = "${var.project_name}-api"
  format        = "DOCKER"
}

# ── Cloud Run: the containerized FastAPI service ──────────────────────────────────
# This is literally the resume claim being reconciled ("FastAPI on Cloud Run") —
# see DECISIONS.md, Phase 4, for why nothing here existed before this phase despite
# that claim.
#
# Deliberately reads its data from the SAME AWS S3 bucket provisioned in
# aws/terraform/ (via DUCKDB_S3_URI + docker/fetch_db.py), rather than mirroring the
# data layer into a separate GCS bucket. This is the same container image deployed
# unmodified to both aws_apprunner_service.api and this resource — proving the
# compute layer is genuinely portable across clouds, decoupled from where the data
# layer happens to live, rather than maintaining two parallel, drifting copies of
# the data layer just so each cloud has its own. The cost is that this Cloud Run
# service depends on AWS credentials and AWS network egress even though it's
# running on GCP — a deliberate, named tradeoff, not an oversight (see DECISIONS.md).
resource "google_cloud_run_v2_service" "api" {
  name     = "${var.project_name}-api"
  location = var.gcp_region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    containers {
      # :latest is pushed out-of-band by gcp/deploy.sh, not managed by Terraform —
      # re-running `terraform apply` after a manual deploy will not roll it back
      # because the tag itself never changes from Terraform's point of view.
      image = "${var.gcp_region}-docker.pkg.dev/${var.gcp_project_id}/${google_artifact_registry_repository.api.repository_id}/api:latest"

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = var.cpu
          memory = var.memory
        }
      }

      env {
        name  = "DUCKDB_S3_URI"
        value = var.duckdb_s3_uri
      }
      env {
        name  = "AWS_REGION"
        value = var.aws_region
      }
      env {
        name  = "AWS_ACCESS_KEY_ID"
        value = var.aws_access_key_id
      }
      env {
        name  = "AWS_SECRET_ACCESS_KEY"
        value = var.aws_secret_access_key
      }
      env {
        name  = "ANTHROPIC_API_KEY"
        value = var.anthropic_api_key
      }
      env {
        name  = "DUCKDB_PATH"
        value = "/app/data/duckdb/supply_chain.duckdb"
      }

      startup_probe {
        http_get {
          path = "/health"
        }
        initial_delay_seconds = 5
        period_seconds        = 5
        failure_threshold     = 6
      }
    }
  }
}

# Cloud Run requires an explicit IAM binding for unauthenticated (public) access —
# unlike App Runner, which is public by default. Without this, every request gets a
# 403 regardless of application-level auth.
resource "google_cloud_run_v2_service_iam_member" "public_access" {
  location = google_cloud_run_v2_service.api.location
  name     = google_cloud_run_v2_service.api.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
