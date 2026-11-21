output "node_instance_ids" {
  description = "Node instance ids keyed by node id (\"1\", \"2\", \"3\"): the input of infra/scripts/push-node-tokens.sh, in node order."
  value       = { for k in local.node_keys : tostring(local.nodes[k].id) => aws_instance.node[k].id }
}

output "node_private_ips" {
  description = "Fixed private addresses of the nodes keyed by node id."
  value       = { for k in local.node_keys : tostring(local.nodes[k].id) => local.nodes[k].ip }
}

output "gateway_instance_id" {
  description = "Gateway instance id."
  value       = aws_instance.gateway.id
}

output "gateway_public_ip" {
  description = "Elastic IP of the gateway: the target of the DNS A record for gateway_domain (a human adds it)."
  value       = aws_eip.gateway.public_ip
}
