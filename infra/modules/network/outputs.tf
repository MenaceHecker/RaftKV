# STUB (test commit): every output is null.

output "vpc_id" {
  description = "VPC id."
  value       = null
}

output "public_subnet_id" {
  description = "Public subnet id (gateway)."
  value       = null
}

output "private_subnet_ids" {
  description = "Private subnet ids (nodes), keyed by index."
  value       = null
}

output "private_route_table_ids" {
  description = "Private route table ids, keyed by index."
  value       = null
}

output "sg_nodes_id" {
  description = "Security group of the Raft nodes."
  value       = null
}

output "sg_gateway_id" {
  description = "Security group of the gateway."
  value       = null
}

output "sg_eice_id" {
  description = "Security group of the Instance Connect Endpoint."
  value       = null
}

output "s3_endpoint_prefix_list_id" {
  description = "Prefix list of the S3 gateway endpoint."
  value       = null
}

output "eice_id" {
  description = "Instance Connect Endpoint id (null when ssh_debug is false)."
  value       = null
}
