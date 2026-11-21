# RaftKV infrastructure (Terraform)

Everything here is **written by an agent and applied by a human** (RUNBOOK 2.9). No agent and no CI job
runs `terraform apply`, `destroy` or `plan` against AWS, creates IAM users or keys, or holds AWS
credentials. CI only runs offline checks (`.github/workflows/infra.yml`).

| Directory | What | State |
|---|---|---|
| `bootstrap/` | state bucket, artifacts bucket, monthly budget (credits excluded), cost-anomaly alert | **local** (gitignored): the state bucket cannot hold the state that creates it |
| `modules/network/` | VPC, one public and three private subnets, S3 gateway endpoint, EC2 Instance Connect Endpoint, three security groups | none (module) |
| `modules/compute/` | D4: three Raft nodes (t4g.micro, fixed `.10` addresses), the gateway (t4g.micro, Elastic IP), their instance roles, systemd units, user-data | none (module) |
| `envs/demo/` | the demo environment; wires the network and compute modules | S3 backend with `use_lockfile` |
| `scripts/` | `push-node-tokens.sh` and `fetch-gateway-tokens.sh` (the token flow) and their stub-`aws` tests | none |
| `policy/` | `check.sh` (offline policy rules over plan JSON or the sources) and its self-test | none |

No NAT gateway and no public IPs on nodes. The S3 gateway endpoint and the Instance
Connect Endpoint are free; the compute stack is not (see Cost).

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
5. **Create the three tokens (out of band, never in Terraform, never inline).** They are SSM
   SecureStrings you create once, from a 0600 temp file (never `--value <token>`: it lands in shell
   history and `ps`). Do this for each of the three names; `shred -u` where available, else `rm -P` (macOS):
   ```
   export AWS_PROFILE=<profile>
   for name in admin-token node-admin-token cluster-token; do
     ( umask 077; f=$(mktemp); openssl rand -base64 32 | tr -d '\n' > "$f"
       aws ssm put-parameter --name "/raftkv/$name" --type SecureString --value "file://$f"
       shred -u "$f" 2>/dev/null || rm -P "$f" )
   done
   ```
   The three values must be **different** (the gateway refuses equal admin and node tokens).
   `/raftkv/admin-token` is the HTTP admin Bearer you type in the dashboard; `/raftkv/node-admin-token`
   is the machine token shared by the gateway (`--node-token-file`) and every node (`--admin-token-file`);
   `/raftkv/cluster-token` is the nodes' `--cluster-token-file` (AddNode and RemoveNode).
6. **Upload a release** (D3). On the build machine: `make release`, then
   `RELEASE_AWS_PROFILE=<profile> RELEASE_BUCKET=<artifacts bucket> make release-upload RELEASE_DIR=dist/<version>`.
   Copy the two hashes from `dist/<version>/raftkv-server-linux-arm64.sha256` and
   `gateway-linux-arm64.sha256` (64 hex characters each); Terraform pins them, the instances check the
   download against them, never against a hash stored in the bucket.
7. **Compute: plan, read, apply.** In `infra/envs/demo`, supply (nothing is committed; use a
   gitignored `demo.tfvars` or `TF_VAR_*`): `artifact_bucket`, `release_version`,
   `release_sha256 = { node = "<hex>", gateway = "<hex>" }`, `tls_staging = true` (first run), and
   confirm `domain`. Repeat the credit balance check (step 1), then:
   ```
   terraform plan -out=demo.plan
   terraform show -json demo.plan > demo.plan.json && ../../policy/check.sh demo.plan.json
   ```
   Read the plan against the tables in this file (IAM, security groups, user-data below) and in the
   PR; `check.sh` must exit 0. Then `terraform apply demo.plan` (human only). The expected additions:
   2 IAM roles, 2 role policies, 2 instance profiles, 4 instances, 4 data volumes + 4 attachments, 1
   Elastic IP + association. Note the outputs `node_instance_ids` and `gateway_public_ip`.
8. **DNS (human).** Create the A record `api.<domain>` pointing at `gateway_public_ip`, wait for it
   to propagate (the certificate authority connects to that name).
9. **Push the node tokens, start the nodes (human, once per node replacement).** The nodes have no SSM
   access and no NAT, so you push the two node-side tokens over EC2 Instance Connect Endpoint; until you
   do, the node units are skipped (fail closed, no crash loop):
   ```
   export AWS_PROFILE=<profile>
   infra/scripts/push-node-tokens.sh $(terraform -chdir=infra/envs/demo output -json node_instance_ids | jq -r '.["1"], .["2"], .["3"]')
   ```
   It reads `/raftkv/node-admin-token` and `/raftkv/cluster-token` with your profile, writes
   `/etc/raftkv/node-admin.token` and `/etc/raftkv/cluster.token` (mode 0600, owner `raftkv`) on each
   node through ssh's stdin, and starts `raftkvd`. A token is never on a command line, never echoed,
   never in a temp file. Your own SSO role needs `ssm:GetParameter` (with decrypt) on the two names and
   `ec2-instance-connect:SendSSHPublicKey` and `ec2-instance-connect:OpenTunnel`. It needs the AWS CLI v2 (`aws ec2-instance-connect ssh --connection-type eice`).
   **Unverified:** whether that command forwards a remote command and stdin the way the script uses it;
   the stub tests cannot show it. If the first real run fails there, stop and tell the planner.
10. **The gateway needs no push.** It reads `/raftkv/admin-token` and `/raftkv/node-admin-token` itself
    on every start (`ExecStartPre=+`, below) and comes up once they exist and the instance role may read
    them. Check `curl -sk https://api.<domain>/healthz` (staging certificate: `-k`).
11. **First run with the staging CA, then production.** The gateway unit has no `--tls-staging` unless
    Terraform says so: `tls_staging = true` adds it, which makes the first certificate a Let's Encrypt
    staging one (browsers distrust it; no production rate limit is burned, and its cache is a separate
    `staging/` directory so it is never served later). Once `https://api.<domain>` answers, set
    `tls_staging = false` and apply again. That changes the user-data, which **replaces the gateway
    instance** (its volume and Elastic IP are kept, the DNS record stays valid).
12. **D8 verification** (below), then the demo. Rolling update, pause and teardown are D9.
13. **Teardown, in this order.**
    1. `terraform destroy` in `infra/envs/demo` (and later environments) first.
    2. Empty both buckets **including all object versions and delete markers** (S3 console, *Empty*).
       The buckets have `force_destroy = false` on purpose, and versioning keeps old objects.
    3. `terraform destroy` in `infra/bootstrap`.
    4. Delete the three SSM parameters by hand (`aws ssm delete-parameter --name /raftkv/<name>`).
    Never delete the state bucket while `envs/demo` state still lives in it.

Deleting `backend.hcl`, `*.tfstate`, `*.plan` or `*.tfvars` from your disk is safe; none of them is
in git (see `.gitignore`).

## What you must supply (nothing is committed)

| Placeholder | Where | Notes |
|---|---|---|
| `alert_email` | `TF_VAR_alert_email` or `-var` at bootstrap apply | required, no default; budget and anomaly alerts |
| `AWS_PROFILE` | your shell | SSO profile; never an access key |
| bucket name for the backend | `envs/demo/backend.hcl` | `terraform output state_bucket` from bootstrap |
| `domain` | `envs/demo` variable, default `raftkv.tushar25.com` | **placeholder, to be confirmed**; the gateway is `api.<domain>`, the dashboard origin `https://<domain>` |
| `artifact_bucket` | `envs/demo` variable, no default | `terraform output artifacts_bucket` from bootstrap |
| `release_version` | `envs/demo` variable, no default | the version `make release` printed |
| `release_sha256` | `envs/demo` variable, no default | `{ node, gateway }`, 64 hex each, from `dist/<version>/*.sha256` |
| `tls_staging` | `envs/demo` variable, default `false` | `true` for the first run only |
| DNS A record `api.<domain>` | your DNS provider | points at output `gateway_public_ip`; not Terraform |
| `/raftkv/admin-token`, `/raftkv/node-admin-token`, `/raftkv/cluster-token` | SSM Parameter Store, created by hand (step 5) | never in Terraform, git, user-data or state |

## Port and security group table

Ports come from the code (`cmd/raftkv-server/main.go`, `docs/gateway.md`), not from RUNBOOK 10.4,
which assumes a separate Raft port. Nodes run **one** gRPC listener (`--listen`) for Raft, KVService
and ClusterService; AdminService is on `--admin-listen`; metrics on `--metrics-listen` (default off).

| Port | Process | Meaning |
|---|---|---|
| tcp/7000 | node `--listen` | Raft between nodes, KVService and ClusterService from the gateway (D4 passes `--listen :7000`; the node has one network interface) |
| tcp/9000 | node `--admin-listen` | AdminService (bearer token), dialled by the gateway |
| tcp/2112 | node `--metrics-listen` | Prometheus metrics; **nothing scrapes it yet**, rule kept per RUNBOOK |
| tcp/22 | sshd | only through the Instance Connect Endpoint |
| tcp/80, 443 | gateway | ACME (TLS-ALPN on 443), the 308 redirect on 80; `--listen :443 --tls-http-listen :80` (D4 unit) |

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
compromised gateway can change cluster membership. D4 reduces it: the nodes run with `--cluster-token-file`,
the cluster token is readable by no instance (not by the gateway role) and lives only on the nodes and
in your hands, so AddNode and RemoveNode need a token the gateway host does not have.

## IAM and bucket policies in this change

No IAM users or access keys (`policy/check.sh` fails the build if one appears). Two instance roles,
each with ONE inline policy written with `jsonencode` of literal values (so tests and `check.sh` can
read them from a plan) and **no managed policy attached**. Assume-role: only `ec2.amazonaws.com`.

| Role | Effect | Action | Resource |
|---|---|---|---|
| node | Allow | `s3:GetObject` | `arn:aws:s3:::<artifacts bucket>/artifacts/raftkvd/*` |
| gateway | Allow | `s3:GetObject` | `arn:aws:s3:::<artifacts bucket>/artifacts/gateway/*` |
| gateway | Allow | `ssm:GetParameter` | `arn:aws:ssm:<region>:<account>:parameter/raftkv/admin-token` and `.../raftkv/node-admin-token` (exactly these two) |

Nodes have **no** SSM and **no** KMS permission; **no instance can read `/raftkv/cluster-token`**.
`kms:Decrypt` is not granted: the SecureStrings are expected to use the default `aws/ssm` key.
**Open item for the human (unverified, D8):** whether the gateway can decrypt with only
`ssm:GetParameter` on that key. If the first boot logs an `AccessDenied` or `KMS` error from
`fetch-gateway-tokens.sh` (`journalctl -u gateway`), the fix is `kms:Decrypt` on that ONE key ARN
(a deliberate change in `modules/compute/iam.tf` and in `policy/check.sh`), never a wildcard.

The two S3 bucket policies from bootstrap are unchanged (a single `Deny` on non-TLS requests each).
`s3:*` appears only on that TLS-only Deny, which can never grant access. A trailing `/*` on a named
bucket's key prefix is the only wildcard `check.sh` tolerates in a Resource.

Both buckets: versioning on, SSE-S3, all four public-access-block flags on, ACLs disabled (bucket
owner enforced), noncurrent versions expire after 90 days, `force_destroy = false`.

## Compute (D4)

| What | Node (x3) | Gateway |
|---|---|---|
| Instance | t4g.micro, AL2023 arm64 (public SSM AMI parameter), `cpu_credits = "standard"` | same |
| Placement | private subnet N, fixed address `10.20.N.10`, no public IP | public subnet, Elastic IP (no automatic public IP) |
| Metadata | IMDSv2 required, hop limit 1 | same |
| Disks | root 8 GiB gp3 encrypted; data 4 GiB gp3 encrypted at `/var/lib/raftkv` | root 8 GiB gp3 encrypted; data 2 GiB gp3 encrypted at `/var/lib/raftkv-gateway` (`state/` registry, `autocert/` cache, both 0700) |
| Security group | `sg_nodes` (unchanged from D2) | `sg_gateway` (unchanged from D2: 80, 443 in; no SSH) |
| Binary | `/usr/local/bin/raftkvd` from `artifacts/raftkvd/<v>/raftkv-server-linux-arm64` | `/usr/local/bin/gateway` from `artifacts/gateway/<v>/gateway-linux-arm64` |
| Unit | `raftkvd.service`, user `raftkv` | `gateway.service`, user `raftkv-gw` |
| On changes | `ignore_changes = [ami, user_data]`: never replaced by `apply` (a replacement wipes the WAL and the vote); config edits go through D9 | user-data change replaces it (volume and Elastic IP kept) |

**What is in user-data (it is readable from instance metadata and the console, so it is non-secret):**
the region, bucket name, object key, the expected sha256 (from the Terraform variable), disk setup,
the two unit files, and for the gateway the text of `fetch-gateway-tokens.sh` (parameter NAMES only).
No token, no password, no key, no `put-parameter`. Nodes' user-data has no SSM call at all, and it
enables `raftkvd` but does not start it.

**Token flow, end to end.**
1. You create three SecureStrings in SSM (step 5), from a 0600 temp file.
2. Gateway: on each start systemd runs `ExecStartPre=+/usr/local/sbin/fetch-gateway-tokens.sh` as root.
   With the instance profile it reads `/raftkv/admin-token` and `/raftkv/node-admin-token` (the aws
   call carries only the names; stdout goes straight into a file), under `umask 077`, into the tmpfs
   `RuntimeDirectory=/run/raftkv-gateway` (0700), chowned to `raftkv-gw`, mode 0600. Nothing reaches
   disk or the journal. If a read fails, the unit does not start and no token file is left.
3. Nodes: no SSM access. `push-node-tokens.sh` (step 9) reads `/raftkv/node-admin-token` and
   `/raftkv/cluster-token` on your machine with your profile and writes them through EC2 Instance
   Connect Endpoint. Until both files exist the unit is skipped (`ConditionPathExists`).
4. The same `node-admin-token` value is the gateway's `--node-token-file` and every node's
   `--admin-token-file`; `cluster-token` is only on the nodes (and in your hands for `raftkvctl`).

**Flags** (the unit templates are checked against the real binaries by `internal/infradrift`):
node `--id N --peers 1=10.20.1.10:7000,2=10.20.2.10:7000,3=10.20.3.10:7000 --listen :7000 --data-dir
/var/lib/raftkv --fsync=true --admin-listen :9000 --admin-token-file /etc/raftkv/node-admin.token
--admin-chaos --cluster-token-file /etc/raftkv/cluster.token --metrics-listen :2112`; gateway
`--listen :443 --tls-domain api.<domain> --tls-http-listen :80 --tls-cache-dir
/var/lib/raftkv-gateway/autocert --state-dir /var/lib/raftkv-gateway/state --admin-token-file
/run/raftkv-gateway/admin.token --node-token-file /run/raftkv-gateway/node.token --nodes
1=<az>=10.20.1.10:7000=10.20.1.10:9000,... --dashboard-origin https://<domain>`, with
`AmbientCapabilities=CAP_NET_BIND_SERVICE`. `--fsync` stays on (RUNBOOK 7.3).

**Election timing differs from the RUNBOOK on purpose.** No timing flags are passed: the tested
defaults (100 ms tick, 10 election ticks, 1 heartbeat tick) give election timeouts of 1.0 to 1.9 s. The
RUNBOOK's 600 to 1200 ms (the card suggested `--tick=50ms --election-ticks=12 --heartbeat-ticks=2`)
needs the chaos and simulator suites re-tuned first; it is not applied here.

**RUNBOOK text that is now out of date** (RUNBOOK.md is not edited here; fix it in a docs change): 10.4
`ssm:GetParameter` on ONE parameter (it is two) and "nodes: s3 only" (also true, plus the human token
push); 10.4 port 7001 (does not exist; one shared 7000); 10.5 `/etc/raftkv/config.yaml`, `raftkvd
--config`, `client_addr`, `read_mode` (the real binary is `raftkv-server`, installed as `raftkvd`, flags
only, no config file, no `read_mode`); 10.5 election 600 to 1200 ms.

## What D8 must verify (nothing below has been run on a real instance)

- `Admin.Crash` then systemd restart: the process exits with status 137 after the reply
  (`internal/transport/admin.go`); confirm `Restart=always` restarts after 137 (some setups treat
  signal-like codes differently), the downtime is about `RestartSec=8` plus start plus an election of
  1.0 to 1.9 s: `systemctl show raftkvd -p NRestarts,ExecMainStartTimestamp`, `journalctl -u raftkvd`
  over EICE.
- CPU credits: watch `CPUCreditBalance` on all four instances while the chaos suite runs; `standard`
  throttles instead of billing, so leader flapping under throttling is possible.
- The KMS question above (the gateway decrypting with only `ssm:GetParameter`).
- The `aws ec2-instance-connect ssh` remote-command and stdin behaviour used by `push-node-tokens.sh`.
- `ExecStartPre=+` with `RuntimeDirectory` ownership: the gateway process (`raftkv-gw`) can read
  the two token files; `curl https://api.<domain>/healthz`.
- Data volume discovery: user-data takes the one NVMe disk that is not the root disk; on a different
  instance type or an extra instance-store disk it refuses (exactly one data disk expected).
- The three token files are mode 0600 on every node: `stat -c %a /etc/raftkv/*.token`.

## Cost

Network: about $0. Compute, from RUNBOOK 10.2: 3 x t4g.micro about $18.40, gateway about $6.10,
Elastic IP about $3.65, EBS about $2.60 for the RUNBOOK's four 8 GB volumes, plus S3, SSM and transfer
about $1 to 2: **about $31 to $35 a month**. This design adds four small data volumes (3 x 4 GiB and
2 GiB, about $1 more at gp3 prices): still within the **USD 40 ceiling** (RUNBOOK 10.9). Verify in the
AWS Pricing Calculator; the budget alerts at 20, 30, 40 actual and 100 percent forecast.

## NOT automated (you do these)

- Creating the three SSM SecureStrings (and deleting them at teardown).
- Pushing the node tokens and starting the nodes (`push-node-tokens.sh`), again after every node replacement.
- The DNS A record for `api.<domain>`, and confirming the domain placeholder.
- Uploading the release (`make release-upload`) and copying its two sha256 hashes into the variables.
- Running `terraform plan`, reading it, `check.sh` on its JSON, and `terraform apply` / `destroy`.
- Flipping `tls_staging` back to `false` after the staging run.
- Verifying the KMS question and D8 above; config or release updates on the nodes (D9).
- Linking the dashboard (Vercel, D7). Node-to-node and node-to-gateway TLS (not implemented).

## Offline checks (what CI runs; you can run them too)

```
terraform fmt -check -recursive infra
for d in bootstrap modules/network modules/compute envs/demo; do (cd infra/$d && terraform init -backend=false -lockfile=readonly && terraform validate); done
(cd infra/bootstrap && terraform test); (cd infra/modules/network && terraform test); (cd infra/modules/compute && terraform test)   # mock provider, no credentials
bash infra/scripts/fetch-gateway-tokens_test.sh; bash infra/scripts/push-node-tokens_test.sh   # stub aws and ssh
go test ./internal/infradrift/   # unit templates vs the real binaries' flags
infra/policy/selftest.sh && infra/policy/check.sh --hcl infra
```

`terraform plan` and `terraform show -json` need AWS credentials, so they are **not** run in CI or by
the agent. `policy/check.sh <plan.json>` is the same rule set for the plan JSON you can produce
yourself: `terraform plan -out=p && terraform show -json p > p.json && infra/policy/check.sh p.json`
(run it before every apply; it also fails a plan whose policy text is unknown at plan time).
tflint runs in CI only. checkov and tfsec are not used.

## Notes for later tasks

- D9: rolling update, pause and teardown scripts. Node config or release changes are NOT applied by
  `terraform apply` (`ignore_changes`); D9 does them one node at a time.
- Region and zones: `us-east-1`; the network module takes the first three sorted available zones and
  fails (precondition) if the region offers fewer than three.
