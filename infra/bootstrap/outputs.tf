# STUB (test commit): every output is null.

output "state_bucket" {
  description = "Name of the Terraform state bucket."
  value       = null
}

output "artifacts_bucket" {
  description = "Name of the release artifacts bucket."
  value       = null
}

output "artifacts_bucket_arn" {
  description = "ARN of the artifacts bucket."
  value       = null
}

output "artifacts_node_object_arn_pattern" {
  description = "Object ARN pattern for the node binaries (D4 grants s3:GetObject on it)."
  value       = null
}

output "artifacts_gateway_object_arn_pattern" {
  description = "Object ARN pattern for the gateway binaries (D5 grants s3:GetObject on it)."
  value       = null
}
