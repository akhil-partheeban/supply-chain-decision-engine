variable "gcp_project_id" {
  description = "GCP project ID to deploy into."
  type        = string
}

variable "gcp_region" {
  description = "GCP region for Artifact Registry + Cloud Run."
  type        = string
  default     = "us-central1"
}

variable "project_name" {
  description = "Prefix applied to resource names."
  type        = string
  default     = "supply-chain-decision-engine"
}

variable "anthropic_api_key" {
  description = "API key for the Claude-powered decision agent. Passed via -var or TF_VAR_anthropic_api_key, never committed. See DECISIONS.md for why this isn't in Secret Manager yet (same deferred-scope reasoning as the AWS side)."
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_access_key_id" {
  description = "AWS credentials so this GCP-hosted container can read the data layer from S3 (see main.tf comment on why Cloud Run reads cross-cloud from AWS S3 rather than a separate GCS copy). Scope this to an IAM user with read-only access to exactly the data bucket from aws/terraform."
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_secret_access_key" {
  type      = string
  sensitive = true
  default   = ""
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "duckdb_s3_uri" {
  description = "s3://<bucket-from-aws-terraform-output>/supply_chain.duckdb"
  type        = string
  default     = ""
}

variable "cpu" {
  type    = string
  default = "1"
}

variable "memory" {
  type    = string
  default = "512Mi"
}
