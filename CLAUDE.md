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
- After adding tests or merging main: `make readme-counts` (fixes the README test counts; `make readme-check` only reports). `make sync-main` merges origin/main and runs it; it never pushes.
- Before every commit: make fmt vet lint test

## Conventions
- Commits: `type(scope): summary`, with trailers `Task:`, `Agent:`, `Gates:`.
  types: feat fix test refactor perf docs chore ci infra
  scopes: raft wal kv snapshot transport admin gateway verify chaos sim web infra
- Branches: feat/<phase>-<slug>, fix/, test/, infra/, docs/, chore/
- One task, one branch, one PR. Small commits (about 300 lines or less).
- Finish with the report format in RUNBOOK §5.6 (executor) or the RUN_REPORT format in Appendix F-2 (runner). State failures and skipped work first.

## Layout (real tree; see docs/STATUS.md)
cmd/{raftkv-server,raftkv-bench}  chaos/  internal/{raft,storage,statemachine,node,transport,metrics,determinism}
deploy/ (kubernetes, compose, grafana, prometheus)  docs/  Dockerfile  docker-compose.yml  buf.yaml  buf.gen.yaml
Protobuf source and generated code: internal/transport/raftkv/v1. The server takes one `--listen` gRPC address plus `--metrics-listen`.
The Makefile, scripts/, .golangci.yml and .githooks/ exist (T0.1). Also in the tree: cmd/{gateway,chaosrun,raftkvctl,readmecounts,flakeselect,cpuhog},
internal/{client,gateway,faultinject,buildinfo,readmecounts,infradrift}, web/ (dashboard), infra/ (Terraform, human-applied), docs/tasks/.

## Project-specific gotchas (update as you learn)
- Run the Go tests in all three CI modes before declaring a change done: `go test ./...`, `GOMAXPROCS=1 go test -count=1 ./...` and `go test -race ./...`. The pre-commit hook runs only `-race -short`, and most recent CI failures were timing races visible only in the other modes on the 2-core runner.
- The hook cannot take red commits, so tests-first means: write the tests, run them red against a stub, save the output, then commit code first and tests in a separate commit.
- Never use a nested git worktree inside the repo directory (the README test walks every .go file); create worktrees outside it.
- Real phase status: docs/STATUS.md. Deployment steps and what D8 must verify: infra/README.md. Operations: docs/OPS.md.