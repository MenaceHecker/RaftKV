# tflint configuration for everything under infra/. CI runs it (see .github/workflows/infra.yml);
# it was not installed on the machine that wrote this, so it has not been run locally.

config {
  call_module_type = "local"
}

plugin "terraform" {
  enabled = true
  preset  = "recommended"
}

plugin "aws" {
  enabled = true
  version = "0.50.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}
