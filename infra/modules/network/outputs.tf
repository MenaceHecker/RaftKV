output "vpc_id" {
  description = "VPC id."
  value       = aws_vpc.this.id
}

output "public_subnet_id" {
  description = "Public subnet id (gateway)."
  value       = aws_subnet.public.id
}

output "private_subnet_ids" {
  description = "Private subnet ids (nodes), keyed by index."
  value       = { for k, s in aws_subnet.private : k => s.id }
}

output "private_route_table_ids" {
  description = "Private route table ids, keyed by index."
  value       = { for k, rt in aws_route_table.private : k => rt.id }
}

output "sg_nodes_id" {
  description = "Security group of the Raft nodes."
  value       = aws_security_group.nodes.id
}

output "sg_gateway_id" {
  description = "Security group of the gateway."
  value       = aws_security_group.gateway.id
}

output "sg_eice_id" {
  description = "Security group of the Instance Connect Endpoint."
  value       = aws_security_group.eice.id
}

output "s3_endpoint_prefix_list_id" {
  description = "Prefix list of the S3 gateway endpoint."
  value       = aws_vpc_endpoint.s3.prefix_list_id
}

output "eice_id" {
  description = "Instance Connect Endpoint id (null when ssh_debug is false)."
  value       = one(aws_ec2_instance_connect_endpoint.this[*].id)
}
