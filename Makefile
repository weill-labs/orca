.PHONY: setup install vet test test-race test-flakes bench coverage release-dry-run verify

BUILD_COMMIT ?= $(shell git rev-parse --short=7 HEAD 2>/dev/null || echo dev)
GO_LDFLAGS := -ldflags "-X main.BuildCommit=$(BUILD_COMMIT)"

setup: ## Configure git hooks and install tools
	git config core.hooksPath .githooks
	@echo "Hooks activated from .githooks/"

install: ## Install orca
	go build -trimpath $(GO_LDFLAGS) -o ~/.local/bin/orca .

vet: ## Run go vet
	go vet ./...

test: ## Run all tests
	./.claude/hooks/block-autonomous-backlog.test.sh
	go test ./... -timeout 120s

test-race: ## Run the test suite with the race detector
	go test -race ./... -timeout 120s

test-flakes: ## Run the full test suite repeatedly to catch flakes
	scripts/flake-check.sh

bench: ## Run microbenchmarks
	go test -bench=. -benchmem -count=3 -run='^$$' ./... -timeout 120s

coverage: ## Collect test coverage
	scripts/coverage.sh

release-dry-run: ## Test release build locally (no publish)
	goreleaser release --snapshot --clean

verify: ## Run the full local verification suite
	go vet ./...
	go test -race ./... -timeout 120s
	$(MAKE) test
	$(MAKE) coverage
	$(MAKE) test-flakes
	$(MAKE) release-dry-run
