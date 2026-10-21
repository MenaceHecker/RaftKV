# RaftKV: instructions for Claude agents

RaftKV is a distributed key-value store implementing Raft from scratch in Go with gRPC.
Full plan: docs/RUNBOOK.md. Invariants: docs/INVARIANTS.md. Current state: docs/STATUS.md.

## Your role (given in the session prompt; do only that role)
- PLANNER: read-only. Write only docs/tasks/<ID>.md. No code, tests, or infra edits.
- EXECUTOR: implement one task card on its own branch. Tests first.
- RUNNER: run gates on the pushed branch in a clean checkout. Never edit files, never fix failures. Write RUN_REPORT.md with raw output.
If no role is given, ask which one.

## Hard rules (RUNBOOK §2)
- Never commit to main. Branch, push, open a PR. The human merges.
- Never weaken a test: no deleted assertions, no t.Skip, no loosened timeouts or tolerances.
- Test changes and raft-core changes go in separate commits.
- Raft code uses injected Clock, Rand, and Transport. No time.Now, time.Sleep, math/rand, or direct net calls in internal/raft.
- Persist (fsync, including directory fsync on create/rename) before replying to any RPC that changes currentTerm, votedFor, or the log.
- Never run terraform apply or destroy. Never create IAM users or keys. No secrets in git.
- No --no-verify, no force push, no rewriting pushed history.
- Stay inside your task card's scope. If the card, runbook, and code disagree, stop and ask.
- Cite the Raft paper section in a comment when implementing protocol rules.

## Commands
- make fmt | vet | lint | test (race) | test-flaky (count=20) | chaos-smoke | proto
- scripts/guard-tests.sh origin/main
- Before every commit: make fmt vet lint test

## Conventions
- Commits: `type(scope): summary`, with trailers `Task:`, `Agent:`, `Gates:`.
  types: feat fix test refactor perf docs chore ci infra
  scopes: raft wal kv snapshot transport admin gateway verify chaos sim web infra
- Branches: feat/<phase>-<slug>, fix/, test/, infra/, docs/, chore/
- One task, one branch, one PR. Small commits (about 300 lines or less).
- Finish with the report format in RUNBOOK §5.6 (executor) or the RUN_REPORT format in Appendix F-2 (runner). State failures and skipped work first.

## Layout
cmd/{raftkvd,raftkvctl,gateway,chaosrun}  internal/{raft,wal,snapshot,kv,transport,sim,chaos,verify,admin}
api/ (protobuf)  infra/ (Terraform)  web/ (dashboard)  scripts/  docs/
Ports: raft 7000, client 7001, admin 9000, metrics 2112.

## Project-specific gotchas (update as you learn)
- TODO after Task 0: the real phase status, any deviations from the layout above.