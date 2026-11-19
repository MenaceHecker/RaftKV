output "vpc_id" {
  description = "VPC id."
  value       = module.network.vpc_id
}

output "public_subnet_id" {
  description = "Public subnet id (gateway, D5)."
  value       = module.network.public_subnet_id
}

output "private_subnet_ids" {
  description = "Private subnet ids (Raft nodes, D4), keyed by index."
  value       = module.network.private_subnet_ids
}

output "private_route_table_ids" {
  description = "Private route table ids, keyed by index."
  value       = module.network.private_route_table_ids
}

output "sg_nodes_id" {
  description = "Security group of the Raft nodes."
  value       = module.network.sg_nodes_id
}

output "sg_gateway_id" {
  description = "Security group of the gateway."
  value       = module.network.sg_gateway_id
}

output "sg_eice_id" {
  description = "Security group of the Instance Connect Endpoint."
  value       = module.network.sg_eice_id
}

output "s3_endpoint_prefix_list_id" {
  description = "Prefix list of the S3 gateway endpoint."
  value       = module.network.s3_endpoint_prefix_list_id
}

output "eice_id" {
  description = "Instance Connect Endpoint id (null when ssh_debug is false)."
  value       = module.network.eice_id
}

output "domain" {
  description = "Public domain (placeholder, to be confirmed). The gateway is api.<domain>."
  value       = var.domain
}

output "node_instance_ids" {
  description = "Node instance ids keyed by node id: the arguments of infra/scripts/push-node-tokens.sh, in order."
  value       = module.compute.node_instance_ids
}

output "node_private_ips" {
  description = "Fixed private addresses of the nodes keyed by node id."
  value       = module.compute.node_private_ips
}

output "gateway_instance_id" {
  description = "Gateway instance id."
  value       = module.compute.gateway_instance_id
}

output "gateway_public_ip" {
  description = "Elastic IP of the gateway: create the DNS A record for api.<domain> pointing here (a human does this)."
  value       = module.compute.gateway_public_ip
}
