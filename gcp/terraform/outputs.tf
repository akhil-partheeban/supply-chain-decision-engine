output "cloud_run_url" {
  description = "Public HTTPS URL of the deployed FastAPI service."
  value       = google_cloud_run_v2_service.api.uri
}

output "artifact_registry_repo" {
  description = "Push images here: <region>-docker.pkg.dev/<project>/<repo>/api:latest"
  value       = "${var.gcp_region}-docker.pkg.dev/${var.gcp_project_id}/${google_artifact_registry_repository.api.repository_id}"
}
