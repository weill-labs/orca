.PHONY: setup install test bench coverage release-dry-run

setup: ## Configure git hooks and install tools
	git config core.hooksPath .githooks
	@echo "Hooks activated from .githooks/"

install: ## Install orca
	go build -trimpath -o ~/.local/bin/orca .

test: ## Run all tests
	go test ./... -timeout 120s

bench: ## Run microbenchmarks
	go test -bench=. -benchmem -count=3 -run='^$$' ./... -timeout 120s

coverage: ## Collect test coverage
	scripts/coverage.sh

release-dry-run: ## Test release build locally (no publish)
	goreleaser release --snapshot --clean
