# The gateway: one instance in the public subnet with an Elastic IP. Its security group (the
# network module's sg_gateway) allows 80 and 443 from the internet and nothing else in: no SSH, so
# no Instance Connect shell either. Debug it with the console or by replacing it.

resource "aws_ebs_volume" "gateway" {
  availability_zone = data.aws_subnet.public.availability_zone
  type              = "gp3"
  size              = 2
  encrypted         = true

  tags = { Name = "${var.name}-gateway-data" }
}

resource "aws_instance" "gateway" {
  ami           = data.aws_ssm_parameter.al2023_arm64.insecure_value
  instance_type = "t4g.micro"

  subnet_id = var.public_subnet_id
  # The public address is the Elastic IP below; no automatic public IPv4 (it bills per hour).
  associate_public_ip_address = false
  vpc_security_group_ids      = [var.sg_gateway_id]
  iam_instance_profile        = aws_iam_instance_profile.gateway.name

  credit_specification {
    cpu_credits = "standard"
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 8
    encrypted             = true
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/userdata-gateway.sh.tftpl", {
    region       = local.region
    bucket       = var.artifact_bucket
    binary_key   = local.gateway_binary_key
    sha256       = var.release_sha256.gateway
    fetch_script = file("${path.module}/../../scripts/fetch-gateway-tokens.sh")
    unit = templatefile("${path.module}/gateway.service.tftpl", {
      region      = local.region
      domain      = var.gateway_domain
      nodes       = local.gateway_nodes
      origin      = var.dashboard_origin
      tls_staging = var.tls_staging
    })
  })
  # The gateway keeps no state of its own on the root disk (registry and certificates are on the
  # separate volume), so a changed release or flag may replace it; the Elastic IP follows.
  user_data_replace_on_change = true

  tags = { Name = "${var.name}-gateway" }
}

resource "aws_volume_attachment" "gateway" {
  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.gateway.id
  instance_id = aws_instance.gateway.id

  stop_instance_before_detaching = true
}

resource "aws_eip" "gateway" {
  domain = "vpc"

  tags = { Name = "${var.name}-gateway" }
}

resource "aws_eip_association" "gateway" {
  allocation_id = aws_eip.gateway.id
  instance_id   = aws_instance.gateway.id
}
