# STUB (test commit): every resource the tests reference exists but carries deliberately wrong
# values, so the tests fail on their assertions. The real configuration replaces this.

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_region" "current" {}

locals {
  azs = sort(data.aws_availability_zones.available.names)
}

resource "aws_vpc" "this" {
  cidr_block = "10.0.0.0/16"
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
}

resource "aws_subnet" "public" {
  vpc_id            = aws_vpc.this.id
  cidr_block        = "10.0.9.0/24"
  availability_zone = local.azs[1]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
}

resource "aws_route" "public_internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "10.0.0.0/8"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_subnet" "private" {
  for_each          = toset(["0", "1", "2"])
  vpc_id            = aws_vpc.this.id
  cidr_block        = "10.0.${each.key}.0/24"
  availability_zone = local.azs[0]
}

resource "aws_route_table" "private" {
  for_each = toset(["0", "1", "2"])
  vpc_id   = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
}

resource "aws_route_table_association" "private" {
  for_each       = toset(["0", "1", "2"])
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Interface"
}

resource "aws_ec2_instance_connect_endpoint" "this" {
  count              = 1
  subnet_id          = aws_subnet.public.id
  preserve_client_ip = true
  security_group_ids = []
}
