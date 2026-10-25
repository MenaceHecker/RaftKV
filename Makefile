# Gates from RUNBOOK 6.1, mapped to the real packages (see docs/STATUS.md).
.PHONY: fmt vet lint test test-flaky cover proto chaos-smoke

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

# No cmd/chaosrun exists yet (RUNBOOK T5.6); run the full chaos report test, which -short skips.
chaos-smoke:
	go test ./chaos -run TestChaosReport -count=1 -timeout 10m
