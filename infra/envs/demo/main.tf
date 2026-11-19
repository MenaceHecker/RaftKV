# Demo environment. D2 wires the network module only; compute (D4), the gateway (D5) and DNS/TLS
# (D6) are later tasks that add to this file. Nothing here costs money: no NAT, no instances,
# the S3 gateway endpoint and the Instance Connect Endpoint are free.

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
