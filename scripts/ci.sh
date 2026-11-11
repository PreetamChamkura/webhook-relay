#!/usr/bin/env bash
# Local CI: runs the same checks the GitHub workflow would.
# GitHub Actions is disabled on this repo, so run this before pushing.
set -euo pipefail
export GOFLAGS=-buildvcs=false GOTOOLCHAIN=auto
export PATH="$(go env GOPATH)/bin:$PATH"
step() { printf '\n==> %s\n' "$*"; }

step "gofmt";        test -z "$(gofmt -l cmd internal test migrations)"
step "go vet";       go vet ./... && go vet -tags integration ./...
step "golangci-lint"; golangci-lint run ./...
step "unit tests";   go test -race -count=1 ./...
step "govulncheck";  govulncheck ./...

if docker info >/dev/null 2>&1; then
  step "integration + chaos tests (testcontainers)"
  go test -tags integration -race -count=1 -timeout 15m -v ./test/integration/...
  step "docker build"; docker build -q -t webhook-relay .
else
  step "SKIPPED integration tests and docker build: Docker not available"
fi
if command -v terraform >/dev/null; then
  step "terraform"; (cd terraform && terraform fmt -check -recursive && terraform init -backend=false -input=false >/dev/null && terraform validate)
else
  step "SKIPPED terraform validate: terraform not installed"
fi
printf '\nAll available checks passed.\n'
