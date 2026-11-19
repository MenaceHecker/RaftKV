output "state_bucket" {
  description = "Name of the Terraform state bucket. Put it in infra/envs/demo/backend.hcl."
  value       = local.buckets.state
}

output "artifacts_bucket" {
  description = "Name of the release artifacts bucket. Pass it to scripts/release-upload.sh as RELEASE_BUCKET."
  value       = local.buckets.artifacts
}

output "artifacts_bucket_arn" {
  description = "ARN of the artifacts bucket."
  value       = "arn:aws:s3:::${local.buckets.artifacts}"
}

# The key layout scripts/release-upload.sh writes. D4 grants the node role s3:GetObject on the
# first pattern only, D5 grants the gateway role the second only. Nothing here grants anything.
output "artifacts_node_object_arn_pattern" {
  description = "Object ARN pattern of the node binaries (artifacts/raftkvd/<version>/...). D4 grants s3:GetObject on it."
  value       = "arn:aws:s3:::${local.buckets.artifacts}/artifacts/raftkvd/*"
}

output "artifacts_gateway_object_arn_pattern" {
  description = "Object ARN pattern of the gateway binaries (artifacts/gateway/<version>/...). D5 grants s3:GetObject on it."
  value       = "arn:aws:s3:::${local.buckets.artifacts}/artifacts/gateway/*"
}
