# RaftKV infrastructure (Terraform)

Everything here is **written by an agent and applied by a human** (RUNBOOK 2.9). No agent and no CI job
runs `terraform apply`, `destroy` or `plan` against AWS, creates IAM users or keys, or holds AWS
credentials. CI only runs offline checks (`.github/workflows/infra.yml`).

| Directory | What | State |
|---|---|---|
| `bootstrap/` | state bucket, artifacts bucket, monthly budget (credits excluded), cost-anomaly alert | **local** (gitignored): the state bucket cannot hold the state that creates it |
| `modules/network/` | VPC, one public and three private subnets, S3 gateway endpoint, EC2 Instance Connect Endpoint, three security groups | none (module) |
| `envs/demo/` | the demo environment; wires the network module (compute, gateway, DNS come in D4, D5, D6) | S3 backend with `use_lockfile` |
| `policy/` | `check.sh` (offline policy rules over plan JSON or the sources) and its self-test | none |

No NAT gateway, no instances, no public IPs on nodes. The S3 gateway endpoint and the Instance
Connect Endpoint are free.

## Steps for the human, in order

1. **D0 first (not Terraform).** Root account MFA; an SSO / IAM Identity Center profile for daily use
   (`aws sso login --profile <profile>`); no long-lived access keys anywhere; region `us-east-1`. In
   *Billing* write down the **credit balance and its expiry date**. If the credits expire before the
   demo window, **stop and tell the planner**. Repeat this check before the D4 apply.
2. **Read before applying:** this README (the port and security group tables below),
   `modules/network/sg.tf` line by line, `bootstrap/main.tf` and `bootstrap/budgets.tf`, and the
   PR description (security group table, IAM and bucket policy summary).
3. **Bootstrap.** Choose the alert email (it is a required variable with no default; it is never
   committed) and apply with your profile:
   ```
   export AWS_PROFILE=<profile>
   export TF_VAR_alert_email=<you@example.com>
   cd infra/bootstrap
   terraform init
   terraform plan -out=bootstrap.plan
   ```
   Read the plan: budget limit `40` and `include_credit = false`, two buckets named
   `raftkv-tfstate-<account>-us-east-1` and `raftkv-artifacts-<account>-us-east-1`, a bucket
   policy that only **denies** non-TLS requests, **no IAM user, key or role**. Then
   `terraform apply bootstrap.plan`. AWS emails a subscription confirmation to the alert address:
   **confirm it**, or no alert is delivered. Keep `infra/bootstrap/terraform.tfstate` (gitignored)
   somewhere safe; teardown needs it.
4. **Demo network.**
   ```
   cd ../envs/demo
   cp backend.hcl.example backend.hcl       # gitignored; set bucket = output state_bucket of bootstrap
   terraform init -backend-config=backend.hcl
   terraform plan -out=demo.plan
   ```
   Read the plan (33 resources: VPC, internet gateway, 4 subnets, 4 route tables with 4
   associations, 1 route (public only), S3 endpoint, Instance Connect Endpoint, 3 security groups and
   13 rules; **no** `aws_nat_gateway` and no route on the private route tables). Then `terraform apply demo.plan`. Cost: about $0 (no NAT,
   no compute; confirm in the AWS Pricing Calculator).
5. **Upload a release** (task D3, `scripts/release-upload.sh`) into `s3://<artifacts bucket>/artifacts/raftkvd/<version>/`
   and `.../artifacts/gateway/<version>/`. Nothing reads them yet (D4).
6. **Teardown, in this order.**
   1. `terraform destroy` in `infra/envs/demo` (and later environments) first.
   2. Empty both buckets **including all object versions and delete markers** (S3 console, *Empty*).
      The buckets have `force_destroy = false` on purpose, and versioning keeps old objects.
   3. `terraform destroy` in `infra/bootstrap`.
   Never delete the state bucket while `envs/demo` state still lives in it.

Deleting `backend.hcl`, `*.tfstate`, `*.plan` or `*.tfvars` from your disk is safe; none of them is
in git (see `.gitignore`).

## What you must supply (nothing is committed)

| Placeholder | Where | Notes |
|---|---|---|
| `alert_email` | `TF_VAR_alert_email` or `-var` at bootstrap apply | required, no default; budget and anomaly alerts |
| `AWS_PROFILE` | your shell | SSO profile; never an access key |
| bucket name for the backend | `envs/demo/backend.hcl` | `terraform output state_bucket` from bootstrap |
| `domain` | `envs/demo` variable, default `raftkv.tushar25.com` | **to be confirmed**; unused until D6 |
| `/raftkv/admin-token` | SSM Parameter Store, created by hand | D4/D5, not Terraform |

## Port and security group table

Ports come from the code (`cmd/raftkv-server/main.go`, `docs/gateway.md`), not from RUNBOOK 10.4,
which assumes a separate Raft port. Nodes run **one** gRPC listener (`--listen`) for Raft, KVService
and ClusterService; AdminService is on `--admin-listen`; metrics on `--metrics-listen` (default off).

| Port | Process | Meaning |
|---|---|---|
| tcp/7000 | node `--listen` | Raft between nodes, KVService and ClusterService from the gateway (D4 passes `--listen <private ip>:7000`) |
| tcp/9000 | node `--admin-listen` | AdminService (bearer token), dialled by the gateway |
| tcp/2112 | node `--metrics-listen` | Prometheus metrics; **nothing scrapes it yet**, rule kept per RUNBOOK |
| tcp/22 | sshd | only through the Instance Connect Endpoint |
| tcp/80, 443 | gateway | ACME and TLS (D6; the gateway serves plain `:8080` today, which is not opened) |

| Group | Direction | Rule | Source / destination |
|---|---|---|---|
| `sg_nodes` | in | tcp 7000 | `sg_nodes` |
| `sg_nodes` | in | tcp 7000 | `sg_gateway` |
| `sg_nodes` | in | tcp 9000 | `sg_gateway` |
| `sg_nodes` | in | tcp 2112 | `sg_gateway` |
| `sg_nodes` | in | tcp 22 | `sg_eice` (only when `ssh_debug = true`) |
| `sg_nodes` | out | tcp 0-65535 | `10.20.0.0/16` |
| `sg_nodes` | out | tcp 443 | S3 gateway-endpoint prefix list |
| `sg_gateway` | in | tcp 80 | `0.0.0.0/0` |
| `sg_gateway` | in | tcp 443 | `0.0.0.0/0` |
| `sg_gateway` | out | tcp 443 | `0.0.0.0/0` (ACME, SSM, S3) |
| `sg_gateway` | out | tcp 80 | `0.0.0.0/0` |
| `sg_gateway` | out | all protocols | `10.20.0.0/16` |
| `sg_eice` | in | none | |
| `sg_eice` | out | tcp 22 | `sg_nodes` (only when `ssh_debug = true`) |

Differences from RUNBOOK 10.4, all chosen by the human:

- one node port (7000) instead of 7000 and 7001, because the code has one shared listener;
- `sg_gateway` egress is restricted to tcp 443, tcp 80 and the VPC instead of "all", to limit what a
  compromised gateway can reach; if D5 or D6 need another outbound path it is added deliberately;
- no SSH to the gateway: the `sg_eice` to `sg_gateway:22` rule does not exist;
- `sg_nodes` egress to S3 is tcp 443 only (the card said all ports to the prefix list; https is all S3 needs).

DNS (VPC resolver), the metadata service and Amazon Time Sync are not filtered by security groups,
so the nodes need no extra egress rule for them.

**Accepted residual risk.** The shared 7000 listener is unauthenticated inside the VPC (`docs/gateway.md`,
T3.18), so `sg_gateway` reaching it also lets the gateway host call AddNode and RemoveNode. A
compromised gateway can change cluster membership. D4 should pass `--cluster-token-file` to the nodes
(and the gateway needs a node-token file); that is D4/D5, not this change.

## IAM and bucket policies in this change

There are **no IAM users, access keys or roles** here (`policy/check.sh` fails the build if one appears).
The only policy documents are the two S3 bucket policies, each a single statement:

| Policy | Effect | Principal | Action | Resource | Condition |
|---|---|---|---|---|---|
| state bucket | Deny | `*` | `s3:*` | the bucket and `bucket/*` (explicit ARNs) | `aws:SecureTransport = false` |
| artifacts bucket | Deny | `*` | `s3:*` | the bucket and `bucket/*` (explicit ARNs) | `aws:SecureTransport = false` |

`s3:*` appears only on that TLS-only Deny, which can never grant access. The artifacts bucket grants
nothing to anyone; D4 will give the node role `s3:GetObject` on
`arn:aws:s3:::<bucket>/artifacts/raftkvd/*` and D5 the gateway role the same on `artifacts/gateway/*`
(outputs `artifacts_node_object_arn_pattern`, `artifacts_gateway_object_arn_pattern`). A trailing
`/*` on a named bucket's key prefix is the only wildcard `check.sh` tolerates in a Resource, because
S3 object access cannot be expressed otherwise.

Both buckets: versioning on, SSE-S3, all four public-access-block flags on, ACLs disabled (bucket
owner enforced), noncurrent versions expire after 90 days, `force_destroy = false`.

## Offline checks (what CI runs; you can run them too)

```
terraform fmt -check -recursive infra
for d in bootstrap modules/network envs/demo; do (cd infra/$d && terraform init -backend=false -lockfile=readonly && terraform validate); done
(cd infra/bootstrap && terraform test); (cd infra/modules/network && terraform test)   # mock provider, no credentials
infra/policy/selftest.sh && infra/policy/check.sh --hcl infra
```

`terraform plan` and `terraform show -json` need AWS credentials, so they are **not** run in CI or by
the agent. `policy/check.sh <plan.json>` is the same rule set for the plan JSON you can produce
yourself: `terraform plan -out=p && terraform show -json p > p.json && infra/policy/check.sh p.json`
(run it before every apply; it also fails a plan whose policy text is unknown at plan time).
tflint runs in CI only. checkov and tfsec are not used.

## Cost

Network: about $0 (no NAT gateway, no public IPv4, free S3 gateway endpoint and Instance Connect
Endpoint). Buckets and budgets: cents. Check current AWS Budgets and Cost Explorer pricing for your account. The ceiling
and teardown policy for the whole demo is RUNBOOK 10.9: USD 40 per month gross; the budget alerts
at 20, 30, 40 (actual) and at a 100 percent forecast, and excludes credits so it shows gross burn.
Verify everything in the AWS Pricing Calculator before D4.

## Notes for later tasks

- D4: artifacts key layout is `artifacts/raftkvd/<version>/{raftkv-server-linux-arm64,.sha256,MANIFEST}`.
  The nodes need `--cluster-token-file` and `--admin-token-file`, and `--admin-chaos` for the chaos
  buttons; the user-data translates the runbook's millisecond settings to `--tick` counts.
- D5: the gateway needs a node-token file as well as the admin token, and reads
  `artifacts/gateway/<version>/`. Its security group already allows 80/443 in and nodes out.
- Region and zones: `us-east-1`; the module takes the first three sorted available zones and fails
  (precondition) if the region offers fewer than three.
