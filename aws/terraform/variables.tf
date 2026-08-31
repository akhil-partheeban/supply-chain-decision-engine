variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefix applied to every resource name (bucket, ECR repo, App Runner service)."
  type        = string
  default     = "supply-chain-decision-engine"
}

variable "duckdb_snapshot_key" {
  description = "Key (path) within the data bucket where the DuckDB snapshot is uploaded by scripts/export_to_s3.py, and where the API container fetches it from on startup."
  type        = string
  default     = "supply_chain.duckdb"
}

variable "anthropic_api_key" {
  description = "API key for the Claude-powered decision agent, injected as a container environment variable. Passed via -var or TF_VAR_anthropic_api_key, never committed. A production setup would source this from AWS Secrets Manager instead of a plain App Runner env var — see DECISIONS.md for why that's deferred here."
  type        = string
  sensitive   = true
  default     = ""
}

variable "cpu" {
  description = "App Runner vCPU allocation. 0.25 vCPU is the smallest tier and is enough for a single-container FastAPI service backed by DuckDB (no separate database server to size for)."
  type        = string
  default     = "0.25 vCPU"
}

variable "memory" {
  description = "App Runner memory allocation. 0.5 GB is App Runner's minimum paired with 0.25 vCPU; bump this if the DuckDB snapshot grows well past its current ~100MB, since DuckDB memory-maps the file."
  type        = string
  default     = "0.5 GB"
}
