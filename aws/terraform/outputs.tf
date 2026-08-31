output "api_service_url" {
  description = "Public HTTPS URL of the deployed FastAPI service."
  value       = "https://${aws_apprunner_service.api.service_url}"
}

output "ecr_repository_url" {
  description = "Push images here: docker push <ecr_repository_url>:latest"
  value       = aws_ecr_repository.api.repository_url
}

output "data_bucket_name" {
  description = "S3 bucket for the exported data layer — target for scripts/export_to_s3.py --dest s3://<data_bucket_name>"
  value       = aws_s3_bucket.data_layer.bucket
}
