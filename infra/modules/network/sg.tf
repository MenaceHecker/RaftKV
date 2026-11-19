# Security groups. A HUMAN MUST READ THIS FILE BEFORE APPLY (RUNBOOK 10.4); the port table is
# also in infra/README.md and in the pull request.
#
# Ports, from the code (cmd/raftkv-server/main.go, docs/gateway.md), not from the runbook:
#   * ONE shared gRPC listener (--listen) carries Raft, KVService and ClusterService. The
#     runbook's separate 7000 (raft) and 7001 (client) collapse into one: tcp/7000.
#   * AdminService has its own listener (--admin-listen): tcp/9000, bearer-token protected.
#   * Prometheus metrics (--metrics-listen): tcp/2112. Default is OFF and nothing scrapes it yet;
#     the rule is kept so D4 can turn it on without touching the network.
#   * sshd on tcp/22, reachable only through the Instance Connect Endpoint.
#
# Residual risk the human accepts: the shared 7000 listener is unauthenticated inside the VPC
# (docs/gateway.md, T3.18), so letting sg_gateway reach it also lets the gateway host call
# AddNode/RemoveNode. Compromise of the gateway therefore reaches cluster membership.
#
# Rules are separate aws_vpc_security_group_*_rule resources (never inline blocks), so sg_nodes
# can reference itself and sg_gateway without a dependency cycle, and every rule is its own
# reviewable line. Security groups do not filter traffic to the VPC DNS resolver, the
# metadata service or the Amazon Time Sync address, so the node egress below needs no DNS/NTP rule.

resource "aws_security_group" "nodes" {
  name        = "${var.name}-nodes"
  description = "Raft nodes: raft+client 7000, admin 9000, metrics 2112, ssh 22 via EICE"
  vpc_id      = aws_vpc.this.id

  tags = { Name = "${var.name}-nodes" }
}

resource "aws_security_group" "gateway" {
  name        = "${var.name}-gateway"
  description = "Gateway: 80 and 443 from the internet, no SSH"
  vpc_id      = aws_vpc.this.id

  tags = { Name = "${var.name}-gateway" }
}

resource "aws_security_group" "eice" {
  name        = "${var.name}-eice"
  description = "EC2 Instance Connect Endpoint: egress to sg_nodes port 22 only"
  vpc_id      = aws_vpc.this.id

  tags = { Name = "${var.name}-eice" }
}

# --- sg_nodes ingress: security group references only, never a CIDR -----------------------

locals {
  nodes_ingress = merge(
    {
      # The shared --listen port: Raft between nodes, KVService/ClusterService from the gateway.
      raft_from_nodes   = { port = 7000, source = aws_security_group.nodes.id, description = "raft and client, between nodes" }
      raft_from_gateway = { port = 7000, source = aws_security_group.gateway.id, description = "raft/client port, from the gateway (KVService, ClusterService)" }
      # --admin-listen: the gateway dials it for Status and the chaos RPCs.
      admin_from_gateway = { port = 9000, source = aws_security_group.gateway.id, description = "AdminService, from the gateway" }
      # --metrics-listen. Nothing scrapes it yet (default OFF); kept per RUNBOOK 10.4.
      metrics_from_gateway = { port = 2112, source = aws_security_group.gateway.id, description = "prometheus metrics, from the gateway (nothing scrapes it yet)" }
    },
    var.ssh_debug ? {
      ssh_from_eice = { port = 22, source = aws_security_group.eice.id, description = "ssh, from the Instance Connect Endpoint only" }
    } : {},
  )
}

resource "aws_vpc_security_group_ingress_rule" "nodes" {
  for_each = local.nodes_ingress

  security_group_id            = aws_security_group.nodes.id
  referenced_security_group_id = each.value.source
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
  description                  = each.value.description

  tags = { Name = "${var.name}-nodes-${each.key}" }
}

# --- sg_nodes egress: the VPC and the S3 gateway endpoint, nothing else -------------------

resource "aws_vpc_security_group_egress_rule" "nodes" {
  for_each = {
    vpc = { cidr = var.vpc_cidr, prefix_list = null, from = 0, to = 65535, description = "peers and gateway inside the VPC, any tcp port" }
    s3  = { cidr = null, prefix_list = aws_vpc_endpoint.s3.prefix_list_id, from = 443, to = 443, description = "S3 through the gateway endpoint (binary download), https only" }
  }

  security_group_id = aws_security_group.nodes.id
  cidr_ipv4         = each.value.cidr
  prefix_list_id    = each.value.prefix_list
  ip_protocol       = "tcp"
  from_port         = each.value.from
  to_port           = each.value.to
  description       = each.value.description

  tags = { Name = "${var.name}-nodes-egress-${each.key}" }
}

# --- sg_gateway ---------------------------------------------------------------------------

# The only world-open ingress in the whole network: 80 (ACME http-01 and the https redirect)
# and 443 (TLS). Not 8080: the gateway serves plain HTTP on :8080 today, but TLS on 80/443 is
# task D6; until then the gateway is simply not reachable from the internet. No SSH.
# infra/policy/check.sh recognises this resource name (gateway_world) as the one allowed to
# take 0.0.0.0/0, on tcp 80 or 443 only.
resource "aws_vpc_security_group_ingress_rule" "gateway_world" {
  for_each = toset(["80", "443"])

  security_group_id = aws_security_group.gateway.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = tonumber(each.key)
  to_port           = tonumber(each.key)
  description       = "${each.key} from the internet"

  tags = { Name = "${var.name}-gateway-in-${each.key}" }
}

# Egress is restricted instead of "all" (RUNBOOK 10.4 allows all; the human chose to restrict it):
# the gateway needs HTTPS (ACME certificate issuance, SSM Parameter Store, S3), HTTP (plain-http
# fetches such as certificate revocation lists), and everything inside the VPC to reach the nodes
# on 7000 and 9000. Nothing else leaves: no SSH, no SMTP, no raw TCP/UDP to the internet, which
# limits what a compromised gateway can exfiltrate or attack. If D5/D6 find another outbound need
# it is added here, deliberately, not by opening everything.
resource "aws_vpc_security_group_egress_rule" "gateway" {
  for_each = {
    https = { cidr = "0.0.0.0/0", protocol = "tcp", from = 443, to = 443, description = "https to the internet (ACME, SSM, S3)" }
    http  = { cidr = "0.0.0.0/0", protocol = "tcp", from = 80, to = 80, description = "http to the internet (ACME, package mirrors)" }
    vpc   = { cidr = var.vpc_cidr, protocol = "-1", from = null, to = null, description = "everything inside the VPC (nodes 7000, 9000)" }
  }

  security_group_id = aws_security_group.gateway.id
  cidr_ipv4         = each.value.cidr
  ip_protocol       = each.value.protocol
  from_port         = each.value.from
  to_port           = each.value.to
  description       = each.value.description

  tags = { Name = "${var.name}-gateway-egress-${each.key}" }
}

# --- sg_eice ------------------------------------------------------------------------------

# The endpoint may open ssh to the nodes and nowhere else. RUNBOOK 10.4 also lists sg_gateway
# here, but the gateway has "no SSH" ingress; the human chose no SSH to the gateway, so that
# rule does not exist.
resource "aws_vpc_security_group_egress_rule" "eice" {
  for_each = var.ssh_debug ? toset(["nodes_ssh"]) : toset([])

  security_group_id            = aws_security_group.eice.id
  referenced_security_group_id = aws_security_group.nodes.id
  ip_protocol                  = "tcp"
  from_port                    = 22
  to_port                      = 22
  description                  = "ssh to the nodes only"

  tags = { Name = "${var.name}-eice-egress-nodes-ssh" }
}
