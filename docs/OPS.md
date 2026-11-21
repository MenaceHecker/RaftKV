# Operating the demo (D9)

Human-run notes for the AWS demo built by `infra/` (see `infra/README.md`). Every command is run by
you, with your SSO profile (`export AWS_PROFILE=<profile>`), never in CI and never with static keys.
Instance ids: `terraform output -json node_instance_ids` (node order 1, 2, 3) and the gateway id from
the same environment. Nothing here prints a token; do not paste one into a ticket or a shell history.

## Routine operations

| Task | How |
|---|---|
| Update the node binary | `scripts/rolling-update.sh --bucket <artifacts bucket> --dist dist/<version> <node1> <node2> <node3>`; add `--dry-run` first to read the plan |
| Restart the nodes one by one (config or token change) | `scripts/rolling-update.sh --restart-only <node1> <node2> <node3>` |
| Downgrade the nodes | rerun the same script with the OLD `dist/<version>` directory (nodes already on it are skipped) |
| Update the gateway | no script, see below |
| Pause (stop the instances) | `scripts/demo-pause.sh pause <gateway id> <node1> <node2> <node3>` |
| Resume | `scripts/demo-pause.sh resume <gateway id> <node1> <node2> <node3>` |
| Tear down | `scripts/demo-down.sh --state-bucket <B> --artifacts-bucket <B2> --domain <D>` prints the steps; you run them |

`aws_instance.node` has `ignore_changes = [ami, user_data]`, so `terraform apply` never updates a node
binary: only `rolling-update.sh` does. It never replaces an instance (a replacement would wipe the WAL
and the vote, RUNBOOK 7.3); a restart reuses the WAL on the data volume.

### Rolling update, in short

Read-only preflight (all 3 nodes answer, ids match their slots, one leader, nobody more than `--max-lag`
entries behind, default 5). Then stage and verify the sha256 on every node that needs it (the hash comes
from your local `dist/<version>/MANIFEST`; no service is touched). Then one node at a time, followers
first and the leader last (order recomputed from live `/health` before each node): copy the live binary to
`raftkvd.prev`, swap, restart, wait up to `--timeout` seconds (default 180) until the node is healthy and
caught up. A node already on the target is skipped, so a rerun is safe.

Exit codes: 0 done; 1 refused or preflight failed (nothing written); 2 staging failed (nothing
restarted); 3 a node failed its health gate and was rolled back (only that node), later nodes untouched;
4 the rollback failed too (MANUAL: look at the node, `journalctl -u raftkvd`). Nodes before the failing
one stay on the new binary: a mixed state is normal for a rolling update.

Known limits, stated plainly:

- **Wire compatibility between adjacent releases during the mixed-version window is an UNVERIFIED
  assumption.** Check the release's proto and WAL changes before you update across a format change.
- The public chaos button can overlap an update: the gateway allows one faulty node and the update takes
  one down, so two can be down for a few seconds. That costs availability only (the WAL is fsynced).
  `--gateway-url https://<host>` refuses to start while the gateway reports chaos `recovering`; the
  residual window is accepted (no gateway maintenance switch).
- Leader and lag come from `curl http://127.0.0.1:2112/health` on each node over EC2 Instance Connect
  Endpoint. `curl` on AL2023 is unverified; the script stops with a clear message if it is missing.
  The EC2 Instance Connect remote-command and stdin behaviour is unverified too (same caveat as
  `infra/scripts/push-node-tokens.sh`). It needs `ssh_debug = true` (the default).
- Binary only: unit files and flags are not pushed. To change them, edit the unit on one node, restart
  it, wait for it to be healthy, then do the next node, followers first.

### Updating the gateway

The gateway has no ssh and no SSM session, so no script reaches it. Bump `release_version` and
`release_sha256` in your tfvars, run `terraform plan`, read it, run `infra/policy/check.sh` on the plan JSON,
then `terraform apply` yourself. The user-data change replaces the gateway instance (volume and Elastic IP
are kept). Expect a few minutes of outage; the dashboard falls back to replay. Boot refetches both
tokens. The same bump also stops a later node replacement from installing an older binary.

## Node down

Reach the node: `aws ec2-instance-connect ssh --instance-id <id> --os-user ec2-user --connection-type eice`,
then `systemctl status raftkvd` and `journalctl -u raftkvd -n 100`.

- `Restart=always` with `RestartSec=8`: a crashed (or chaos-crashed) node is down for about 8 s plus the
  restart. That is the designed downtime, not an incident.
- A node with no token files is skipped by systemd (`ConditionPathExists`), not failed: it shows
  `inactive (dead)` with a skipped condition. Push the tokens again with `infra/scripts/push-node-tokens.sh`.
  This happens only after a node replacement; a stop and start keeps the files.
- Two nodes down means no quorum: reads and writes fail until one returns. Do not restart a third.
- Never replace an instance to "fix" it: that wipes its WAL and vote. Reboot or restart the unit.

## Leader flapping

Symptoms: `term` keeps rising on `/v1/cluster`, writes time out intermittently. Elections take 1.0 to 1.9 s
(tested defaults), so a flap is visible.

- The instances use `standard` CPU credits (not unlimited). A drained credit balance throttles the CPU
  and heartbeats arrive late. Check `CPUCreditBalance` for each instance in CloudWatch (AWS/EC2 namespace,
  `aws cloudwatch get-metric-statistics`). If it is near zero, pause and let it recover, or move up a size.
- Check `journalctl -u raftkvd` for repeated elections and fsync latency. Check no chaos fault is active
  (`/v1/cluster` `chaos.state`).
- Whether t4g.micro holds a steady leader under the demo load is unverified until D8 measures it.

## Gateway unreachable

The gateway has no shell. From your machine:

1. `aws ec2 describe-instance-status --instance-ids <gateway id> --include-all-instances`
2. `aws ec2 get-console-output --instance-id <gateway id> --latest` (boot log and unit failures)
3. `curl -sS https://api.<domain>/healthz` (expect `{"status":"ok"}`; `-k` with the staging certificate)
4. DNS: the A record for `api.<domain>` must still be the Elastic IP (`terraform output gateway_public_ip`).

The fix is the tfvars bump and apply described above (replaces the instance, keeps volume and IP), or a
reboot (`aws ec2 reboot-instances --instance-ids <gateway id>`), which safe-starts the gateway. While the
gateway is down the dashboard plays its recording and says REPLAY, so the demo link still works.

## Certificate renewal failure

The gateway gets its certificate from Let's Encrypt with autocert on port 443 (TLS-ALPN) and redirects
from 80; the cache is `/var/lib/raftkv-gateway/autocert` on the gateway volume. Likely causes:

- DNS for `api.<domain>` no longer points at the Elastic IP, or 443 is blocked (security group).
- `--tls-staging` was left on (`tls_staging = true`): the certificate is not trusted by browsers.
- The cache directory was refused for permissions (it must not be accessible to group or others).
- Let's Encrypt rate limits after a lost cache (the cache is on the persistent volume for this reason).

Renewal starts about 30 days before expiry (assumption, unverified): the window to act is wide. Fix the
cause, then reboot the gateway to retry (`aws ec2 reboot-instances --instance-ids <gateway id>`) and check
`curl -sS https://api.<domain>/healthz`.

## Token rotation

There are three SSM SecureStrings: `/raftkv/admin-token`, `/raftkv/node-admin-token`,
`/raftkv/cluster-token`. They must differ. Rotate one by overwriting it from a 0600 temp file, as in
`infra/README.md` step 5 (add `--overwrite` to `aws ssm put-parameter`); never `--value <token>`.

`infra/scripts/push-node-tokens.sh` only runs `systemctl start`, which does nothing on a running unit, so
a push alone does not rotate anything on a running node. To rotate:

1. Put the new value(s) in SSM.
2. Push the node-side tokens: `infra/scripts/push-node-tokens.sh <node1> <node2> <node3>`.
3. Restart the nodes one at a time: `scripts/rolling-update.sh --restart-only <node1> <node2> <node3>`.
4. Reboot the gateway (it refetches both of its tokens on every start):
   `aws ec2 reboot-instances --instance-ids <gateway id>`.

Whether a node re-reads a changed token file without a restart is unverified; assume it does not.
Rotating `/raftkv/admin-token` only needs step 1 and step 4.

## Cost alarms

Budget alerts fire at $20, $30 and $40 actual spend (credits excluded, so you see gross burn) and at 100
percent of forecast.

- **$20**: open Cost Explorer, group by usage type, and compare with the expected 31 to 35 dollars a month.
- **$30**: `scripts/demo-pause.sh pause <gateway id> <node1> <node2> <node3>`, then investigate: a NAT
  gateway (there should be none), an extra Elastic IP, `unlimited` CPU credits, S3 object versions.
- **$40** (the ceiling): pause now. If the cause is not explained, run `scripts/demo-down.sh` (it prints the
  teardown steps; you run them) and leave the dashboard in replay.

Paused still bills: the Elastic IP (about $3.65 a month), the gp3 EBS volumes (46 GiB, price unverified)
and S3 pennies. On the first resume, confirm `raftkvd` is active on each node (data volume remount is
unverified).

## Stuck chaos slot (recovering)

`/v1/cluster` reports `chaos.state: "recovering"` while a node is still down after a heal; no new fault
is accepted until all 3 nodes answer `Status`. Heal is public and never throttled: use the dashboard's
heal button. A crash is not undone by heal: wait out the node's restart (about 8 s), or restart the unit
(see Node down). If it stays stuck, reboot the gateway: its safe start heals every node and holds the slot
until all 3 answer. The idle spelling of `chaos.state` is unverified; `recovering` is documented.

## Backups

There are no backups. No snapshot or backup resource exists under `infra/`, EBS volumes are not
snapshotted, and the three WALs are the only copies of the data. S3 versioning covers release artifacts
only. The keyspace is a demo that resets; do not store anything here you cannot lose.
