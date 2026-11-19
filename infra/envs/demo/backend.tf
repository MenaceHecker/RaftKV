# Remote state in the S3 bucket that infra/bootstrap created, with native S3 locking
# (use_lockfile, Terraform >= 1.10; no DynamoDB table). The bucket and region are NOT written
# here: they come from a gitignored backend.hcl (see backend.hcl.example):
#
#   terraform init -backend-config=backend.hcl
terraform {
  backend "s3" {
    key          = "envs/demo/terraform.tfstate"
    encrypt      = true
    use_lockfile = true
  }
}
