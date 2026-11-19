# Network (RUNBOOK 10.1, 10.3 D2): one public subnet for the gateway, three private subnets
# (one Raft node each, three DISTINCT AZs), no NAT gateway, no public IP on any node.
#
# Design rules enforced by tests/network.tftest.hcl and infra/policy/check.sh:
#   * the ONLY route to the internet is aws_route.public_internet on the public route table;
#   * private route tables carry no routes at all (route = [] also removes any route added by hand);
#   * nodes reach S3 (binary download) through the free S3 gateway endpoint, nothing else;
#   * debug access is an EC2 Instance Connect Endpoint, so no node needs a public IP or a bastion.

data "aws_availability_zones" "available" {
  state = "available"

  # Regular AZs only: no Local Zones or Wavelength Zones.
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

data "aws_region" "current" {}

locals {
  # Sorted so the AZ-a/b/c assignment is stable between plans.
  azs = sort(data.aws_availability_zones.available.names)

  # try(): with too few zones the lookup must not fail before the precondition on
  # aws_subnet.private can explain why.
  private_subnets = { for i, cidr in var.private_cidrs : tostring(i) => { cidr = cidr, az = try(local.azs[i], "none-available") } }
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = var.name }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = { Name = var.name }
}

# --- public (gateway) ---------------------------------------------------------------------

resource "aws_subnet" "public" {
  vpc_id            = aws_vpc.this.id
  cidr_block        = var.public_cidr
  availability_zone = local.azs[0]

  # The gateway gets an Elastic IP (D5); no automatic public IPv4 (it bills per hour).
  map_public_ip_on_launch = false

  tags = { Name = "${var.name}-public-${local.azs[0]}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  tags = { Name = "${var.name}-public" }
}

# The single route to the internet in this module.
resource "aws_route" "public_internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# --- private (Raft nodes) -----------------------------------------------------------------

resource "aws_subnet" "private" {
  for_each = local.private_subnets

  vpc_id                  = aws_vpc.this.id
  cidr_block              = each.value.cidr
  availability_zone       = each.value.az
  map_public_ip_on_launch = false

  tags = { Name = "${var.name}-private-${each.value.az}" }

  lifecycle {
    # Three nodes in three distinct AZs, or the cluster does not survive losing a zone.
    precondition {
      condition     = length(local.azs) >= var.az_count
      error_message = "this region offers ${length(local.azs)} availability zone(s); ${var.az_count} distinct zones are required, one per Raft node."
    }
  }
}

# No route blocks and no aws_route for these tables: they have no default route, so they cannot
# reach the internet, an internet gateway or a NAT. `route = []` makes Terraform own the (empty)
# route list, so a route added by hand shows up as drift.
resource "aws_route_table" "private" {
  for_each = local.private_subnets

  vpc_id = aws_vpc.this.id
  route  = []

  tags = { Name = "${var.name}-private-${each.value.az}" }
}

resource "aws_route_table_association" "private" {
  for_each = local.private_subnets

  subnet_id      = aws_subnet.private[each.key].id
  route_table_id = aws_route_table.private[each.key].id
}

# Gateway endpoints are free and work by adding a prefix-list route to the associated route
# tables. Only the private ones: nodes pull their release binary from S3 this way. The ACM,
# SSM and EC2 Instance Connect paths are NOT added (interface endpoints cost money).
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [for rt in aws_route_table.private : rt.id]

  tags = { Name = "${var.name}-s3" }
}

# --- debug access -------------------------------------------------------------------------

# EC2 Instance Connect Endpoint: SSH to a private node without a public IP, a bastion or a NAT.
# One endpoint in private subnet 1 is enough to reach all three subnets of the VPC. It has its
# own security group (sg_eice, sg.tf) that may open port 22 to sg_nodes and nowhere else.
# preserve_client_ip = false so the node sees the endpoint's private address, which sg_nodes
# admits by security group reference.
resource "aws_ec2_instance_connect_endpoint" "this" {
  count = var.ssh_debug ? 1 : 0

  subnet_id          = aws_subnet.private["0"].id
  security_group_ids = [aws_security_group.eice.id]
  preserve_client_ip = false

  tags = { Name = var.name }
}
