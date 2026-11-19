# STUB (test commit): wrong on purpose, see main.tf.

resource "aws_security_group" "nodes" {
  name   = "stub-nodes"
  vpc_id = aws_vpc.this.id
}

resource "aws_security_group" "gateway" {
  name   = "stub-gateway"
  vpc_id = aws_vpc.this.id
}

resource "aws_security_group" "eice" {
  name   = "stub-eice"
  vpc_id = aws_vpc.this.id
}

resource "aws_vpc_security_group_ingress_rule" "nodes" {
  for_each          = toset(["raft_from_nodes"])
  security_group_id = aws_security_group.nodes.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_vpc_security_group_egress_rule" "nodes" {
  for_each          = toset(["vpc"])
  security_group_id = aws_security_group.nodes.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_vpc_security_group_ingress_rule" "gateway_world" {
  for_each          = toset(["22"])
  security_group_id = aws_security_group.gateway.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_vpc_security_group_egress_rule" "gateway" {
  for_each          = toset(["all"])
  security_group_id = aws_security_group.gateway.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_vpc_security_group_egress_rule" "eice" {
  for_each          = toset(["all"])
  security_group_id = aws_security_group.eice.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}
