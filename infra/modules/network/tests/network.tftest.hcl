# Offline tests for the network module. mock_provider "aws" means no credentials and no AWS
# calls; every run is command = plan. Computed ids are unknown at plan time, so each one the
# assertions compare is given a value with override_during = plan.

mock_provider "aws" {
  # Deliberately unsorted and with a fourth zone: the module must sort and take the first three.
  mock_data "aws_availability_zones" {
    defaults = {
      names = ["us-east-1d", "us-east-1a", "us-east-1c", "us-east-1b"]
    }
  }
  mock_data "aws_region" {
    defaults = {
      name = "us-east-1"
    }
  }
}

override_resource {
  target          = aws_vpc.this
  override_during = plan
  values          = { id = "vpc-test" }
}
override_resource {
  target          = aws_internet_gateway.this
  override_during = plan
  values          = { id = "igw-test" }
}
override_resource {
  target          = aws_subnet.public
  override_during = plan
  values          = { id = "subnet-pub" }
}
override_resource {
  target          = aws_route_table.public
  override_during = plan
  values          = { id = "rtb-pub" }
}
override_resource {
  target          = aws_subnet.private["0"]
  override_during = plan
  values          = { id = "subnet-p0" }
}
override_resource {
  target          = aws_subnet.private["1"]
  override_during = plan
  values          = { id = "subnet-p1" }
}
override_resource {
  target          = aws_subnet.private["2"]
  override_during = plan
  values          = { id = "subnet-p2" }
}
override_resource {
  target          = aws_route_table.private["0"]
  override_during = plan
  values          = { id = "rtb-p0" }
}
override_resource {
  target          = aws_route_table.private["1"]
  override_during = plan
  values          = { id = "rtb-p1" }
}
override_resource {
  target          = aws_route_table.private["2"]
  override_during = plan
  values          = { id = "rtb-p2" }
}
override_resource {
  target          = aws_vpc_endpoint.s3
  override_during = plan
  values = {
    id             = "vpce-s3"
    prefix_list_id = "pl-s3"
  }
}
override_resource {
  target          = aws_security_group.nodes
  override_during = plan
  values          = { id = "sg-nodes" }
}
override_resource {
  target          = aws_security_group.gateway
  override_during = plan
  values          = { id = "sg-gateway" }
}
override_resource {
  target          = aws_security_group.eice
  override_during = plan
  values          = { id = "sg-eice" }
}
override_resource {
  target          = aws_ec2_instance_connect_endpoint.this[0]
  override_during = plan
  values          = { id = "eice-test" }
}

run "private_subnets_in_three_distinct_azs" {
  command = plan

  assert {
    condition     = length(aws_subnet.private) == 3
    error_message = "expected exactly three private subnets"
  }
  assert {
    condition     = length(distinct([for s in aws_subnet.private : s.availability_zone])) == 3
    error_message = "the three private subnets must be in three DISTINCT availability zones"
  }
  assert {
    condition     = [for k in ["0", "1", "2"] : aws_subnet.private[k].availability_zone] == ["us-east-1a", "us-east-1b", "us-east-1c"]
    error_message = "private subnets must take the first three SORTED zone names, in order"
  }
  assert {
    condition     = [for k in ["0", "1", "2"] : aws_subnet.private[k].cidr_block] == ["10.20.1.0/24", "10.20.2.0/24", "10.20.3.0/24"]
    error_message = "private subnet CIDRs must be 10.20.1.0/24, 10.20.2.0/24, 10.20.3.0/24"
  }
  assert {
    condition     = alltrue([for s in aws_subnet.private : s.map_public_ip_on_launch == false])
    error_message = "nodes must never get a public IP"
  }
}

run "fewer_than_three_azs_is_rejected" {
  command = plan

  override_data {
    target = data.aws_availability_zones.available
    values = { names = ["us-east-1b", "us-east-1a"] }
  }

  expect_failures = [aws_subnet.private]
}

run "public_subnet_cidr_and_igw_route" {
  command = plan

  assert {
    condition     = aws_subnet.public.cidr_block == "10.20.0.0/24"
    error_message = "public subnet must be 10.20.0.0/24"
  }
  assert {
    condition     = aws_subnet.public.availability_zone == "us-east-1a"
    error_message = "public subnet must be in AZ-a (first sorted zone)"
  }
  assert {
    condition     = aws_route.public_internet.destination_cidr_block == "0.0.0.0/0" && aws_route.public_internet.gateway_id == "igw-test"
    error_message = "the public route table must send 0.0.0.0/0 to the internet gateway"
  }
  assert {
    condition     = aws_route.public_internet.route_table_id == "rtb-pub"
    error_message = "the internet route must be on the PUBLIC route table only"
  }
  assert {
    condition     = aws_route_table_association.public.subnet_id == "subnet-pub" && aws_route_table_association.public.route_table_id == "rtb-pub"
    error_message = "the public subnet must use the public route table"
  }
}

run "private_route_tables_have_no_default_route" {
  command = plan

  assert {
    condition     = length(aws_route_table.private) == 3
    error_message = "expected one route table per private subnet"
  }
  assert {
    condition     = alltrue([for rt in aws_route_table.private : length(rt.route) == 0])
    error_message = "private route tables must carry NO routes (no igw, no nat, no 0.0.0.0/0)"
  }
  assert {
    condition     = alltrue([for k in ["0", "1", "2"] : aws_route_table_association.private[k].route_table_id == "rtb-p${k}" && aws_route_table_association.private[k].subnet_id == "subnet-p${k}"])
    error_message = "each private subnet must be associated with its own private route table"
  }
}

run "no_nat_gateway_resource" {
  command = plan

  # The module declares no aws_nat_gateway at all; `check.sh --hcl` greps the source for the
  # resource type. Here: the only route anywhere in the module is the public one, it is not a
  # NAT route, and no private route table can hold one.
  assert {
    condition     = aws_route.public_internet.nat_gateway_id == null
    error_message = "the public route must not target a NAT gateway"
  }
  assert {
    condition     = alltrue([for rt in aws_route_table.private : length(rt.route) == 0]) && length(aws_route_table.public.route) == 0
    error_message = "route tables must hold no inline routes (the only route is aws_route.public_internet)"
  }
}

run "vpc_cidr_is_10_20_0_0_16" {
  command = plan

  assert {
    condition     = aws_vpc.this.cidr_block == "10.20.0.0/16"
    error_message = "VPC must be 10.20.0.0/16"
  }
  assert {
    condition     = aws_vpc.this.enable_dns_support == true && aws_vpc.this.enable_dns_hostnames == true
    error_message = "VPC needs DNS support and DNS hostnames"
  }
}

run "s3_endpoint_is_gateway_type_on_all_private_rts" {
  command = plan

  assert {
    condition     = aws_vpc_endpoint.s3.vpc_endpoint_type == "Gateway"
    error_message = "the S3 endpoint must be a Gateway endpoint (free)"
  }
  assert {
    condition     = aws_vpc_endpoint.s3.service_name == "com.amazonaws.us-east-1.s3"
    error_message = "wrong S3 service name"
  }
  assert {
    condition     = aws_vpc_endpoint.s3.route_table_ids == toset(["rtb-p0", "rtb-p1", "rtb-p2"])
    error_message = "the S3 endpoint must be attached to all three private route tables and not the public one"
  }
}

run "sg_nodes_ingress_matches_table" {
  command = plan

  assert {
    condition     = toset(keys(aws_vpc_security_group_ingress_rule.nodes)) == toset(["raft_from_nodes", "raft_from_gateway", "admin_from_gateway", "metrics_from_gateway", "ssh_from_eice"])
    error_message = "sg_nodes ingress must be exactly: raft/client 7000 from nodes and gateway, admin 9000 and metrics 2112 from gateway, ssh 22 from eice"
  }
  assert {
    condition = alltrue([
      for r in values(aws_vpc_security_group_ingress_rule.nodes) :
      r.cidr_ipv4 == null && r.cidr_ipv6 == null && r.prefix_list_id == null && r.ip_protocol == "tcp" && r.from_port == r.to_port && r.security_group_id == "sg-nodes"
    ])
    error_message = "every sg_nodes ingress rule must be one tcp port from a security group reference, never a CIDR"
  }
  assert {
    condition = (
      aws_vpc_security_group_ingress_rule.nodes["raft_from_nodes"].from_port == 7000 && aws_vpc_security_group_ingress_rule.nodes["raft_from_nodes"].referenced_security_group_id == "sg-nodes" &&
      aws_vpc_security_group_ingress_rule.nodes["raft_from_gateway"].from_port == 7000 && aws_vpc_security_group_ingress_rule.nodes["raft_from_gateway"].referenced_security_group_id == "sg-gateway" &&
      aws_vpc_security_group_ingress_rule.nodes["admin_from_gateway"].from_port == 9000 && aws_vpc_security_group_ingress_rule.nodes["admin_from_gateway"].referenced_security_group_id == "sg-gateway" &&
      aws_vpc_security_group_ingress_rule.nodes["metrics_from_gateway"].from_port == 2112 && aws_vpc_security_group_ingress_rule.nodes["metrics_from_gateway"].referenced_security_group_id == "sg-gateway" &&
      aws_vpc_security_group_ingress_rule.nodes["ssh_from_eice"].from_port == 22 && aws_vpc_security_group_ingress_rule.nodes["ssh_from_eice"].referenced_security_group_id == "sg-eice"
    )
    error_message = "sg_nodes ports or sources differ from the table"
  }
  assert {
    condition     = !contains([for r in values(aws_vpc_security_group_ingress_rule.nodes) : r.from_port], 7001)
    error_message = "there is no separate 7001 port: Raft and clients share the one --listen port"
  }
  assert {
    condition     = toset(keys(aws_vpc_security_group_egress_rule.nodes)) == toset(["vpc", "s3"])
    error_message = "sg_nodes egress must be exactly: the VPC CIDR and the S3 prefix list"
  }
  assert {
    condition = (
      aws_vpc_security_group_egress_rule.nodes["vpc"].cidr_ipv4 == "10.20.0.0/16" && aws_vpc_security_group_egress_rule.nodes["vpc"].ip_protocol == "tcp" &&
      aws_vpc_security_group_egress_rule.nodes["vpc"].from_port == 0 && aws_vpc_security_group_egress_rule.nodes["vpc"].to_port == 65535 &&
      aws_vpc_security_group_egress_rule.nodes["s3"].prefix_list_id == "pl-s3" && aws_vpc_security_group_egress_rule.nodes["s3"].ip_protocol == "tcp" &&
      aws_vpc_security_group_egress_rule.nodes["s3"].from_port == 443 && aws_vpc_security_group_egress_rule.nodes["s3"].to_port == 443
    )
    error_message = "sg_nodes egress: tcp all ports to the VPC CIDR, tcp 443 to the S3 gateway-endpoint prefix list, nothing else"
  }
}

run "sg_gateway_ingress_only_80_443_world" {
  command = plan

  assert {
    condition     = toset(keys(aws_vpc_security_group_ingress_rule.gateway_world)) == toset(["80", "443"])
    error_message = "sg_gateway must accept only 80 and 443 (no 8080, no SSH)"
  }
  assert {
    condition = alltrue([
      for k, r in aws_vpc_security_group_ingress_rule.gateway_world :
      r.cidr_ipv4 == "0.0.0.0/0" && r.ip_protocol == "tcp" && r.from_port == tonumber(k) && r.to_port == tonumber(k) && r.security_group_id == "sg-gateway"
    ])
    error_message = "gateway world rules must be tcp, one port each, from 0.0.0.0/0, on sg_gateway"
  }
  assert {
    condition     = toset(keys(aws_vpc_security_group_egress_rule.gateway)) == toset(["https", "http", "vpc"])
    error_message = "sg_gateway egress must be exactly: tcp 443 and 80 to the world and everything to the VPC CIDR"
  }
  assert {
    condition = (
      aws_vpc_security_group_egress_rule.gateway["https"].cidr_ipv4 == "0.0.0.0/0" && aws_vpc_security_group_egress_rule.gateway["https"].ip_protocol == "tcp" &&
      aws_vpc_security_group_egress_rule.gateway["https"].from_port == 443 && aws_vpc_security_group_egress_rule.gateway["https"].to_port == 443 &&
      aws_vpc_security_group_egress_rule.gateway["http"].cidr_ipv4 == "0.0.0.0/0" && aws_vpc_security_group_egress_rule.gateway["http"].ip_protocol == "tcp" &&
      aws_vpc_security_group_egress_rule.gateway["http"].from_port == 80 && aws_vpc_security_group_egress_rule.gateway["http"].to_port == 80 &&
      aws_vpc_security_group_egress_rule.gateway["vpc"].cidr_ipv4 == "10.20.0.0/16" && aws_vpc_security_group_egress_rule.gateway["vpc"].ip_protocol == "-1"
    )
    error_message = "sg_gateway egress values differ from the table"
  }
}

run "sg_eice_egress_22_only" {
  command = plan

  assert {
    condition     = toset(keys(aws_vpc_security_group_egress_rule.eice)) == toset(["nodes_ssh"])
    error_message = "sg_eice must have exactly one egress rule"
  }
  assert {
    condition = (
      aws_vpc_security_group_egress_rule.eice["nodes_ssh"].ip_protocol == "tcp" &&
      aws_vpc_security_group_egress_rule.eice["nodes_ssh"].from_port == 22 && aws_vpc_security_group_egress_rule.eice["nodes_ssh"].to_port == 22 &&
      aws_vpc_security_group_egress_rule.eice["nodes_ssh"].referenced_security_group_id == "sg-nodes" &&
      aws_vpc_security_group_egress_rule.eice["nodes_ssh"].cidr_ipv4 == null &&
      aws_vpc_security_group_egress_rule.eice["nodes_ssh"].security_group_id == "sg-eice"
    )
    error_message = "sg_eice may reach sg_nodes tcp/22 only (NOT the gateway)"
  }
}

run "eice_in_private_subnet" {
  command = plan

  assert {
    condition     = length(aws_ec2_instance_connect_endpoint.this) == 1
    error_message = "ssh_debug = true must create the Instance Connect Endpoint"
  }
  assert {
    condition     = aws_ec2_instance_connect_endpoint.this[0].subnet_id == "subnet-p0"
    error_message = "the endpoint must sit in private subnet 1, not the public one"
  }
  assert {
    condition     = aws_ec2_instance_connect_endpoint.this[0].preserve_client_ip == false
    error_message = "preserve_client_ip must be false (the node SG admits sg_eice, not the operator IP)"
  }
  assert {
    condition     = aws_ec2_instance_connect_endpoint.this[0].security_group_ids == toset(["sg-eice"])
    error_message = "the endpoint must use only sg_eice"
  }
}

run "ssh_debug_false_removes_every_ssh_path" {
  command = plan

  variables {
    ssh_debug = false
  }

  assert {
    condition     = length(aws_ec2_instance_connect_endpoint.this) == 0
    error_message = "ssh_debug = false must not create the endpoint"
  }
  assert {
    condition     = !contains(keys(aws_vpc_security_group_ingress_rule.nodes), "ssh_from_eice") && length(aws_vpc_security_group_egress_rule.eice) == 0
    error_message = "ssh_debug = false must remove the port 22 rules on both sides"
  }
}

run "outputs_expose_ids_by_index" {
  command = plan

  assert {
    condition     = output.vpc_id == "vpc-test" && output.public_subnet_id == "subnet-pub"
    error_message = "vpc_id / public_subnet_id output wrong"
  }
  assert {
    condition     = output.private_subnet_ids == { "0" = "subnet-p0", "1" = "subnet-p1", "2" = "subnet-p2" }
    error_message = "private_subnet_ids must be a map keyed by index"
  }
  assert {
    condition     = output.private_route_table_ids == { "0" = "rtb-p0", "1" = "rtb-p1", "2" = "rtb-p2" }
    error_message = "private_route_table_ids must be a map keyed by index"
  }
  assert {
    condition     = output.sg_nodes_id == "sg-nodes" && output.sg_gateway_id == "sg-gateway" && output.sg_eice_id == "sg-eice"
    error_message = "security group id outputs wrong"
  }
  assert {
    condition     = output.s3_endpoint_prefix_list_id == "pl-s3" && output.eice_id == "eice-test"
    error_message = "s3_endpoint_prefix_list_id / eice_id output wrong"
  }
}
