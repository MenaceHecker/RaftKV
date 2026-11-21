# CI notes: flake-guard (T6.4)

## Why

About ten consecutive CI failures in November 2026 were test races seen only on the 2-core runner, never on a laptop and not
caught by the pre-commit hook (`go test -race -short ./...`). By PR title they were #32, #43, #47, #48, #51 and #52. The
fake-clock driver pacing fix is thought to be PR #41, but #41 is titled "chaos controller (D5b)", so that attribution is
UNVERIFIED. RUNBOOK 6.4 already calls one failure in `-count=20` a P0 bug; the guard enforces it on the PR that introduces
the test.

## What runs

`scripts/flake-guard.sh` (job `flake-guard` in `.github/workflows/ci.yml`, pull requests only):

1. Selection (`cmd/flakeselect`, go/parser, not a regex over diff text). `git diff -U0 --no-renames BASE...HEAD -- '*.go'`
   gives post-image hunks. A hunk inside a `TestX`/`FuzzX` selects it. A hunk inside any other top-level declaration of a
   test file (spy, driver, fake clock, shared var) selects every Test in the package that references it, directly or through
   another declaration. A hunk in an import block selects every test of that file; a changed `TestMain` selects every test of
   the package. Renames and moves are seen because selection is by post-image content. Subtests ride with their parent.
   `-run` is `^(A|B)$` built from the exact names, per package.
2. Per selected package, in three modes: `plain`, `procs1` (`GOMAXPROCS=1`), `race` (`-race`); each `go test -count=20
   -timeout 10m -json`. Always `-count`, so never cached. Above 40 selected tests COUNT drops to 10 and the summary says so.
3. Whole-package pass for `./internal/gateway` and `./internal/transport` when the PR touches a test or source file there:
   `-count=3`, plain and `GOMAXPROCS=1` only (race is already covered by `go test -race ./...`).
4. Report: a table `mode | package | test | failures/runs` to the log and `$GITHUB_STEP_SUMMARY`, an `::error` annotation per
   failing test, the first failure's last 30 lines, and the exact local command. Exit 1 on any failure.
5. No test changes: prints `flake-guard: no test changes, nothing to run` and exits 0 in seconds.

`LOAD=1` (matrix leg `load=1`) runs only `plain` and `race`, beside `cmd/cpuhog` (2 busy goroutines), and only for gateway and
transport selections, to approximate packages running in parallel on the runner. UNVERIFIED that 2 goroutines reproduces
the CI contention; expect to tune it. It can false-positive on a test with a genuinely small deadline.

## The guard never retries

A failure is reported once. There is no retry-to-green, no label override and no `continue-on-error` inside the script. The
human merging decides on a red run.

## Status: advisory for two weeks, then required

At launch the job has `continue-on-error: true`: a failure shows as a red step, an `::error` annotation and the step summary,
but does not block merging. After two weeks the human decides whether to make it required in branch protection. That is a
repository setting only the human can change; nothing in this repo changes it. When making it required, also remove
`continue-on-error` from the job. The job is skipped (not absent) on pushes.

No auto-label and no `pull-requests: write`: annotations and the step summary only.

## Run it locally

    make flake-guard BASE=origin/main            # env: COUNT PKG_COUNT MODES LOAD
    go test -race -count=20 -run '^TestX$' ./internal/pkg    # one test, as the failure message prints it

The self-test is `scripts/flake-guard_test.sh` (about 20 s; builds throwaway git repos and runs the real guard). It is not
part of `go test ./...`. Its fixtures are `scripts/testdata/flake/*.go.txt`: deliberately NOT `.go` files (a departure from
the T6.4 card), because the README test count walks every `.go` file regardless of build tags and would count tests that
never run. The self-test copies them into a temp repo under their real names.

## Nightly

`flaky-nightly` runs only on the 04:17 schedule: `make test-flaky-nightly` is `go test -race -count=20` on raft, storage,
statemachine, internal/gateway and internal/transport (`make test-flaky` keeps the first three). Its duration is NOT
measured; estimate from the whole-repo race pass of about 96 s per run on the runner (a sample of one) times 20, roughly 30
minutes or more. Job timeout 120 minutes.

## Measured runtimes (one laptop, Apple silicon, warm caches; the runner is slower, 2 cores)

| part | measured |
|---|---|
| no test changes | under 1 s |
| this PR's own diff (16 tests, 3 modes, count 20) | 7 s wall |
| 1 racy test in transport, 3 modes, count 20 | 3 s |
| whole-package pass transport, plain count 3 / GOMAXPROCS=1 count 3 | 29 s / 34 s |
| whole-package pass gateway, plain count 3 / GOMAXPROCS=1 count 3 | 33 s / 33 s |
| one new transport test end to end (selection + both whole passes) | 66 s wall |
| gateway source-only change end to end | 66 s wall |
| `LOAD=1`, one transport test, plain+hog and race+hog, count 20 | 2.8 s wall |

Whole-package passes dominate; 79 gateway and 110 transport tests. The card's estimate for a typical PR (3 to 10 minutes) is
not confirmed or refuted by these: they are laptop numbers for tiny tests. The three-real-PR wall times on the GitHub runner
are not available until the job has run there.

## Not tested locally

The GitHub Actions run itself (job `if`, matrix, `continue-on-error` display, `fetch-depth: 0`, annotations rendering),
`actionlint` and `shellcheck` (neither installed). The workflow YAML was only parsed.
