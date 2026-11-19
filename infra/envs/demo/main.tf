# Demo environment: the network (D2) and the compute stack (D4: three Raft nodes, the gateway, their
# IAM roles, an Elastic IP). The DNS A record and the three SSM SecureStrings are NOT Terraform:
# the human creates them (infra/README.md). The network is applied FIRST (README step 4) and the
# compute stack second: the compute module reads the subnets, and its user-data must be fully known
# at plan time for infra/policy/check.sh to read it from the plan.
# The network costs nothing (no NAT; the S3 gateway endpoint and the Instance Connect Endpoint are
# free); the compute stack is about USD 31 to 35 a month (RUNBOOK 10.2).

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "raftkv"
      Env       = "demo"
      ManagedBy = "terraform"
    }
  }
}

module "network" {
  source = "../../modules/network"

  name      = var.name
  ssh_debug = var.ssh_debug
}

module "compute" {
  source = "../../modules/compute"

  name               = var.name
  private_subnet_ids = module.network.private_subnet_ids
  public_subnet_id   = module.network.public_subnet_id
  sg_nodes_id        = module.network.sg_nodes_id
  sg_gateway_id      = module.network.sg_gateway_id

  artifact_bucket = var.artifact_bucket
  release_version = var.release_version
  release_sha256  = var.release_sha256

  # The gateway is api.<domain>, the dashboard (Vercel) is <domain> (RUNBOOK 10.3 D6). TO BE CONFIRMED.
  gateway_domain   = "api.${var.domain}"
  dashboard_origin = "https://${var.domain}"
  tls_staging      = var.tls_staging
}
