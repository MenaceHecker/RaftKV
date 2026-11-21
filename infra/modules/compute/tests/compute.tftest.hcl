# Offline tests for the compute module (RUNBOOK 10.3 D4, docs/tasks/D4D6.md). mock_provider "aws"
# means no credentials and no AWS calls; every run is command = plan. Computed ids are unknown at
# plan time; nothing an assertion needs is unknown, because user-data and the unit never depend on
# a computed id (that is also what lets infra/policy/check.sh read user-data from a real plan).

mock_provider "aws" {
  mock_data "aws_region" {
    defaults = { name = "us-east-1" }
  }
  mock_data "aws_caller_identity" {
    defaults = { account_id = "111111111111" }
  }
  # The public AL2023 AMI parameter is the only SSM data source the module reads.
  mock_data "aws_ssm_parameter" {
    defaults = {
      insecure_value = "ami-0123456789abcdef0"
      value          = "ami-0123456789abcdef0"
    }
  }
}

variables {
  private_subnet_ids = { "0" = "subnet-p0", "1" = "subnet-p1", "2" = "subnet-p2" }
  public_subnet_id   = "subnet-pub"
  sg_nodes_id        = "sg-nodes"
  sg_gateway_id      = "sg-gateway"
  artifact_bucket    = "raftkv-artifacts-test"
  release_version    = "v0.0.0-test"
  release_sha256 = {
    node    = "1111111111111111111111111111111111111111111111111111111111111111"
    gateway = "2222222222222222222222222222222222222222222222222222222222222222"
  }
  gateway_domain   = "api.example.test"
  dashboard_origin = "https://dash.example.test"
}

override_data {
  target = data.aws_subnet.private["0"]
  values = { cidr_block = "10.20.1.0/24", availability_zone = "us-east-1a" }
}
override_data {
  target = data.aws_subnet.private["1"]
  values = { cidr_block = "10.20.2.0/24", availability_zone = "us-east-1b" }
}
override_data {
  target = data.aws_subnet.private["2"]
  values = { cidr_block = "10.20.3.0/24", availability_zone = "us-east-1c" }
}

run "nodes_hardening" {
  command = plan

  assert {
    condition     = length(aws_instance.node) == 3
    error_message = "expected exactly 3 node instances"
  }
  assert {
    condition     = alltrue([for n in aws_instance.node : n.instance_type == "t4g.micro"])
    error_message = "nodes must be t4g.micro"
  }
  assert {
    condition = (aws_instance.node["0"].private_ip == "10.20.1.10" && aws_instance.node["1"].private_ip == "10.20.2.10"
    && aws_instance.node["2"].private_ip == "10.20.3.10")
    error_message = "nodes must have the fixed private addresses .10 of their own subnet"
  }
  assert {
    condition = (aws_instance.node["0"].subnet_id == "subnet-p0" && aws_instance.node["1"].subnet_id == "subnet-p1"
    && aws_instance.node["2"].subnet_id == "subnet-p2")
    error_message = "node N+1 must live in private subnet N"
  }
  assert {
    condition     = alltrue([for n in aws_instance.node : n.associate_public_ip_address == false])
    error_message = "no node may have a public IP"
  }
  assert {
    condition     = alltrue([for n in aws_instance.node : n.vpc_security_group_ids == toset(["sg-nodes"])])
    error_message = "nodes use the network module's sg_nodes and nothing else"
  }
  assert {
    condition     = alltrue([for n in aws_instance.node : n.metadata_options[0].http_tokens == "required" && n.metadata_options[0].http_put_response_hop_limit == 1])
    error_message = "IMDSv2 must be required with hop limit 1"
  }
  assert {
    condition     = alltrue([for n in aws_instance.node : n.credit_specification[0].cpu_credits == "standard"])
    error_message = "cpu_credits must be standard (unlimited can bill surplus CPU)"
  }
  assert {
    condition     = alltrue([for n in aws_instance.node : n.root_block_device[0].encrypted == true && n.root_block_device[0].volume_type == "gp3"])
    error_message = "the root volume must be encrypted gp3"
  }
  assert {
    condition     = length(aws_ebs_volume.node) == 3 && alltrue([for v in aws_ebs_volume.node : v.encrypted == true && v.type == "gp3"])
    error_message = "each node needs a separate encrypted gp3 data volume"
  }
  assert {
    condition     = alltrue([for v in aws_ebs_volume.node : v.availability_zone == "us-east-1a" || v.availability_zone == "us-east-1b" || v.availability_zone == "us-east-1c"])
    error_message = "a data volume must be in the zone of its subnet"
  }
}

run "gateway_hardening" {
  command = plan

  assert {
    condition     = aws_instance.gateway.instance_type == "t4g.micro" && aws_instance.gateway.subnet_id == "subnet-pub"
    error_message = "the gateway is a t4g.micro in the public subnet"
  }
  assert {
    condition     = aws_instance.gateway.vpc_security_group_ids == toset(["sg-gateway"])
    error_message = "the gateway uses the network module's sg_gateway (80 and 443 only, no SSH)"
  }
  assert {
    condition     = aws_instance.gateway.associate_public_ip_address == false
    error_message = "the gateway's public address is the Elastic IP, not an automatic public IPv4"
  }
  assert {
    condition     = aws_instance.gateway.metadata_options[0].http_tokens == "required" && aws_instance.gateway.metadata_options[0].http_put_response_hop_limit == 1
    error_message = "IMDSv2 must be required with hop limit 1"
  }
  assert {
    condition     = aws_instance.gateway.credit_specification[0].cpu_credits == "standard"
    error_message = "cpu_credits must be standard"
  }
  assert {
    condition     = aws_instance.gateway.root_block_device[0].encrypted == true && aws_instance.gateway.root_block_device[0].volume_type == "gp3"
    error_message = "the root volume must be encrypted gp3"
  }
  assert {
    condition     = aws_ebs_volume.gateway.encrypted == true && aws_ebs_volume.gateway.type == "gp3" && aws_ebs_volume.gateway.size == 2
    error_message = "the registry and certificate volume is an encrypted 2 GiB gp3"
  }
  assert {
    condition     = aws_eip.gateway.domain == "vpc"
    error_message = "the gateway needs an Elastic IP"
  }
}

run "iam_exact" {
  command = plan

  assert {
    condition     = length(jsondecode(aws_iam_role_policy.node.policy).Statement) == 1
    error_message = "the node role has exactly one statement"
  }
  assert {
    condition = (jsondecode(aws_iam_role_policy.node.policy).Statement[0].Effect == "Allow"
      && jsondecode(aws_iam_role_policy.node.policy).Statement[0].Action == ["s3:GetObject"]
    && jsondecode(aws_iam_role_policy.node.policy).Statement[0].Resource == ["arn:aws:s3:::raftkv-artifacts-test/artifacts/raftkvd/*"])
    error_message = "the node role may only s3:GetObject on the node artifacts prefix"
  }
  assert {
    condition     = !strcontains(aws_iam_role_policy.node.policy, "ssm") && !strcontains(aws_iam_role_policy.node.policy, "kms")
    error_message = "nodes get no SSM and no KMS permission (tokens are pushed by the human)"
  }
  assert {
    condition     = length(jsondecode(aws_iam_role_policy.gateway.policy).Statement) == 2
    error_message = "the gateway role has exactly two statements"
  }
  assert {
    condition = (jsondecode(aws_iam_role_policy.gateway.policy).Statement[0].Action == ["s3:GetObject"]
    && jsondecode(aws_iam_role_policy.gateway.policy).Statement[0].Resource == ["arn:aws:s3:::raftkv-artifacts-test/artifacts/gateway/*"])
    error_message = "the gateway role may only s3:GetObject on the gateway artifacts prefix"
  }
  assert {
    condition = (jsondecode(aws_iam_role_policy.gateway.policy).Statement[1].Action == ["ssm:GetParameter"]
      && jsondecode(aws_iam_role_policy.gateway.policy).Statement[1].Resource == [
        "arn:aws:ssm:us-east-1:111111111111:parameter/raftkv/admin-token",
        "arn:aws:ssm:us-east-1:111111111111:parameter/raftkv/node-admin-token",
    ])
    error_message = "the gateway role may only ssm:GetParameter on exactly the admin-token and node-admin-token ARNs"
  }
  assert {
    condition     = !strcontains(aws_iam_role_policy.gateway.policy, "cluster-token") && !strcontains(aws_iam_role_policy.gateway.policy, "kms")
    error_message = "no instance may read the cluster token; no KMS permission is granted"
  }
  assert {
    condition = alltrue(flatten([
      for p in [aws_iam_role_policy.node.policy, aws_iam_role_policy.gateway.policy] : [
        for st in jsondecode(p).Statement : concat([for a in st.Action : !strcontains(a, "*")], [for r in st.Resource : !strcontains(replace(r, "/*", ""), "*")])
      ]
    ]))
    error_message = "no wildcard action or resource"
  }
  assert {
    condition = (jsondecode(aws_iam_role.node.assume_role_policy).Statement[0].Principal.Service == "ec2.amazonaws.com"
    && jsondecode(aws_iam_role.gateway.assume_role_policy).Statement[0].Principal.Service == "ec2.amazonaws.com")
    error_message = "only EC2 may assume the instance roles"
  }
}

run "userdata_no_secrets" {
  command = plan

  assert {
    condition = alltrue([for n in aws_instance.node : alltrue([
      for bad in ["Bearer", "put-parameter", "--with-decryption", "--value", "BEGIN PRIVATE KEY", "AKIA"] : !strcontains(n.user_data, bad)
    ])])
    error_message = "node user-data holds no token material and no SSM call"
  }
  assert {
    condition = alltrue([
      for bad in ["Bearer", "put-parameter", "--value", "BEGIN PRIVATE KEY", "AKIA"] : !strcontains(aws_instance.gateway.user_data, bad)
    ])
    error_message = "gateway user-data holds no token material"
  }
  assert {
    condition     = alltrue([for n in aws_instance.node : !can(regex("[A-Za-z0-9+/=]{40,}", replace(replace(n.user_data, var.release_sha256.node, ""), var.release_sha256.gateway, "")))])
    error_message = "node user-data contains a long base64-like literal (a token?)"
  }
  assert {
    condition     = !can(regex("[A-Za-z0-9+/=]{40,}", replace(replace(aws_instance.gateway.user_data, var.release_sha256.node, ""), var.release_sha256.gateway, "")))
    error_message = "gateway user-data contains a long base64-like literal (a token?)"
  }
  assert {
    condition     = alltrue([for n in aws_instance.node : strcontains(n.user_data, "sha256sum -c") && !strcontains(n.user_data, "set -x")])
    error_message = "nodes verify the download with sha256sum -c and never trace"
  }
  assert {
    condition     = strcontains(aws_instance.gateway.user_data, "sha256sum -c") && !strcontains(aws_instance.gateway.user_data, "set -x")
    error_message = "the gateway verifies the download with sha256sum -c and never traces"
  }
  assert {
    condition     = alltrue([for n in aws_instance.node : strcontains(n.user_data, "1111111111111111111111111111111111111111111111111111111111111111  ") && !strcontains(n.user_data, "2222222222222222222222222222222222222222222222222222222222222222")])
    error_message = "the node checks the node sha256 from the variable (and does not know the gateway's)"
  }
  assert {
    condition     = strcontains(aws_instance.gateway.user_data, "2222222222222222222222222222222222222222222222222222222222222222  ")
    error_message = "the gateway checks the gateway sha256 from the variable"
  }
  assert {
    condition     = alltrue([for n in aws_instance.node : !strcontains(n.user_data, ".sha256")])
    error_message = "the expected hash must not come from a file in the bucket"
  }
  assert {
    condition     = alltrue([for n in aws_instance.node : strcontains(n.user_data, "systemctl enable raftkvd") && !strcontains(n.user_data, "systemctl start") && !strcontains(n.user_data, "--now")])
    error_message = "nodes enable the unit but do not start it (the tokens are not there yet)"
  }
  assert {
    condition     = !strcontains(aws_instance.gateway.user_data, "cluster-token") && !strcontains(aws_instance.gateway.user_data, "cluster.token")
    error_message = "the gateway never touches the cluster token"
  }
  assert {
    condition     = strcontains(aws_instance.gateway.user_data, "--with-decryption") && strcontains(aws_instance.gateway.user_data, "umask 077")
    error_message = "the gateway pre-start script reads its two parameters under umask 077 (names only, no values)"
  }
}

run "unit_flags_nodes" {
  command = plan

  assert {
    condition = alltrue([for n in aws_instance.node : alltrue([
      for f in [
        "ExecStart=/usr/local/bin/raftkvd",
        "--listen :7000",
        "--data-dir /var/lib/raftkv",
        "--fsync=true",
        "--admin-listen :9000",
        "--admin-token-file /etc/raftkv/node-admin.token",
        "--admin-chaos",
        "--cluster-token-file /etc/raftkv/cluster.token",
        "--metrics-listen :2112",
        "--peers 1=10.20.1.10:7000,2=10.20.2.10:7000,3=10.20.3.10:7000",
        "Restart=always",
        "RestartSec=8",
        "NoNewPrivileges=true",
        "ProtectSystem=strict",
        "ReadWritePaths=/var/lib/raftkv",
        "LimitNOFILE=65536",
        "User=raftkv",
        "ConditionPathExists=/etc/raftkv/node-admin.token",
        "ConditionPathExists=/etc/raftkv/cluster.token",
      ] : strcontains(n.user_data, f)
    ])])
    error_message = "a node unit flag or setting is missing"
  }
  assert {
    condition = ((strcontains(aws_instance.node["0"].user_data, "--id 1\n") || strcontains(aws_instance.node["0"].user_data, "--id 1 "))
      && (strcontains(aws_instance.node["1"].user_data, "--id 2\n") || strcontains(aws_instance.node["1"].user_data, "--id 2 "))
    && (strcontains(aws_instance.node["2"].user_data, "--id 3\n") || strcontains(aws_instance.node["2"].user_data, "--id 3 ")))
    error_message = "each node passes its own --id (subnet index + 1)"
  }
  assert {
    condition     = alltrue([for n in aws_instance.node : !strcontains(n.user_data, "--tick") && !strcontains(n.user_data, "--election-ticks") && !strcontains(n.user_data, "--heartbeat-ticks")])
    error_message = "no timing flags: the tested defaults (100 ms tick, 10 election ticks, 1 heartbeat tick) stay"
  }
  assert {
    condition     = alltrue([for n in aws_instance.node : !strcontains(n.user_data, "7001") && !strcontains(n.user_data, "--fsync=false")])
    error_message = "no port 7001 and never fsync off"
  }
}

run "unit_flags_gateway" {
  command = plan

  assert {
    condition = alltrue([
      for f in [
        "ExecStart=/usr/local/bin/gateway",
        "--listen :443",
        "--tls-domain api.example.test",
        "--tls-http-listen :80",
        "--tls-cache-dir /var/lib/raftkv-gateway/autocert",
        "--state-dir /var/lib/raftkv-gateway/state",
        "--admin-token-file /run/raftkv-gateway/admin.token",
        "--node-token-file /run/raftkv-gateway/node.token",
        "--dashboard-origin https://dash.example.test",
        "AmbientCapabilities=CAP_NET_BIND_SERVICE",
        "ExecStartPre=+/usr/local/sbin/fetch-gateway-tokens.sh",
        "RuntimeDirectory=raftkv-gateway",
        "RuntimeDirectoryMode=0700",
        "User=raftkv-gw",
        "Restart=always",
        "NoNewPrivileges=true",
        "ProtectSystem=strict",
        "ReadWritePaths=/var/lib/raftkv-gateway",
        "LimitNOFILE=65536",
      ] : strcontains(aws_instance.gateway.user_data, f)
    ])
    error_message = "a gateway unit flag or setting is missing"
  }
  assert {
    condition     = !strcontains(aws_instance.gateway.user_data, "--tls-staging")
    error_message = "--tls-staging is off unless the human sets tls_staging"
  }
}

run "gateway_tls_staging_opt_in" {
  command = plan

  variables {
    tls_staging = true
  }

  assert {
    condition     = strcontains(aws_instance.gateway.user_data, "--tls-staging")
    error_message = "tls_staging = true must add --tls-staging"
  }
}

run "gateway_nodes_flag" {
  command = plan

  assert {
    condition = strcontains(aws_instance.gateway.user_data,
    "--nodes 1=us-east-1a=10.20.1.10:7000=10.20.1.10:9000,2=us-east-1b=10.20.2.10:7000=10.20.2.10:9000,3=us-east-1c=10.20.3.10:7000=10.20.3.10:9000")
    error_message = "--nodes must be id=az=addr=adminaddr for the three fixed addresses, ports 7000 and 9000"
  }
}

run "node_lifecycle_never_replaces" {
  command = plan

  # ignore_changes = [ami, user_data] cannot be read from a plan; the policy script checks the
  # source (check.sh --hcl). Here: the outputs the push script needs exist for all three nodes.
  assert {
    condition     = length(output.node_private_ips) == 3 && output.node_private_ips["2"] == "10.20.2.10"
    error_message = "node_private_ips must be keyed by node id with the fixed addresses"
  }
}

run "release_sha256_must_be_hex" {
  command = plan

  variables {
    release_sha256 = { node = "not-a-hash", gateway = "2222222222222222222222222222222222222222222222222222222222222222" }
  }

  expect_failures = [var.release_sha256]
}

run "release_version_single_segment" {
  command = plan

  variables {
    release_version = "v1/../../etc"
  }

  expect_failures = [var.release_version]
}
