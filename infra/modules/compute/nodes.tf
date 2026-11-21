# The three Raft nodes: one per private subnet, fixed address .10, no public IP, no NAT.
#
# Replacing a node instance wipes its WAL and its vote. So `ami` and `user_data` are in
# ignore_changes: editing the unit, the flags or the release never replaces an instance; those
# changes go through the D9 rolling update, one node at a time.

resource "aws_ebs_volume" "node" {
  for_each = local.nodes

  availability_zone = each.value.az
  type              = "gp3"
  size              = 4
  encrypted         = true

  tags = { Name = "${var.name}-node-${each.value.id}-data" }
}

resource "aws_instance" "node" {
  for_each = local.nodes

  ami           = data.aws_ssm_parameter.al2023_arm64.insecure_value
  instance_type = "t4g.micro"

  subnet_id                   = each.value.subnet_id
  private_ip                  = each.value.ip
  associate_public_ip_address = false
  vpc_security_group_ids      = [var.sg_nodes_id]
  iam_instance_profile        = aws_iam_instance_profile.node.name

  # "unlimited" is the t4g default and can bill surplus CPU credits.
  credit_specification {
    cpu_credits = "standard"
  }

  # IMDSv2 only, and a hop limit of 1 so no container or forwarded request can reach it.
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

  user_data = templatefile("${path.module}/userdata-node.sh.tftpl", {
    node_id    = each.value.id
    region     = local.region
    bucket     = var.artifact_bucket
    binary_key = local.node_binary_key
    sha256     = var.release_sha256.node
    unit = templatefile("${path.module}/raftkvd.service.tftpl", {
      node_id      = each.value.id
      peers        = local.peers
      raft_port    = local.raft_port
      admin_port   = local.admin_port
      metrics_port = local.metrics_port
    })
  })
  user_data_replace_on_change = false

  tags = { Name = "${var.name}-node-${each.value.id}" }

  lifecycle {
    ignore_changes = [ami, user_data]
  }
}

resource "aws_volume_attachment" "node" {
  for_each = local.nodes

  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.node[each.key].id
  instance_id = aws_instance.node[each.key].id

  # Detaching a mounted volume from a running instance can hang; stop it first.
  stop_instance_before_detaching = true
}
