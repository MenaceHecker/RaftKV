# Gates from RUNBOOK 6.1, mapped to the real packages (see docs/STATUS.md).
.PHONY: fmt vet lint test test-flaky cover proto chaos-smoke readme-counts readme-check sync-main release release-upload

# Generated protobuf code is excluded, as in .github/workflows/ci.yml.
GOFILES = $(shell find . -name "*.go" -not -name "*.pb.go" -not -path "./.git/*")

fmt:
	gofmt -l -w $(GOFILES)

vet:
	go vet ./...

lint:
	@command -v golangci-lint >/dev/null || { echo "golangci-lint is not installed: https://golangci-lint.run/welcome/install/"; exit 1; }
	golangci-lint run

test:
	go test -race -count=1 -timeout 10m ./...

test-flaky:
	go test -race -count=20 -timeout 30m ./internal/raft/... ./internal/storage/... ./internal/statemachine/...

cover:
	go test -race -coverprofile=cover.out ./... && go tool cover -func=cover.out

proto:
	@command -v buf >/dev/null || { echo "buf is not installed: https://buf.build/docs/installation"; exit 1; }
	buf generate

# Every chaos scenario over seeds 1-20 on virtual time (RUNBOOK 6.1, T5.6). There is no -duration:
# the simulator advances by ticks, not wall time. This no longer refreshes docs/chaos-report.md;
# `go test ./chaos -run TestChaosReport` still does.
chaos-smoke:
	go run ./cmd/chaosrun -sim -seeds 1-20

# The README states the exact test count, and internal/determinism fails if it is stale.
# `readme-counts` rewrites the numbers; `readme-check` only reports, failing exactly when
# internal/determinism/readme_test.go would (used by CI and pre-commit).
readme-counts:
	go run ./cmd/readmecounts

readme-check:
	go run ./cmd/readmecounts -check

# Merge origin/main and refresh the README counts in one step. Never pushes; completes the
# merge commit when README.md is the only conflict.
sync-main:
	scripts/sync-main.sh

# Reproducible linux/arm64 release build into dist/<version>/ (RUNBOOK 10.3 D3). BUILD ONLY: no AWS,
# never uploads. Needs a clean tree (ALLOW_DIRTY=1 gives a -dirty build that can never be uploaded).
release:
	scripts/release.sh

# Upload a verified dist/<version>/ to S3. Run by a human with an SSO profile, never in CI:
#   RELEASE_AWS_PROFILE=<sso profile> RELEASE_BUCKET=<bucket> make release-upload RELEASE_DIR=dist/<version>
# RELEASE_DIR has no default on purpose.
release-upload:
	@test -n "$(RELEASE_DIR)" || { echo "release-upload: RELEASE_DIR=dist/<version> is required"; exit 2; }
	scripts/release-upload.sh "$(RELEASE_DIR)"
