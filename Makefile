# gosbee developer tasks.
#
# Every target carrying a "## " comment is listed by `make help`.
# Keep those descriptions to a single, concise line.

GO          ?= go
REPL_PKG    ?= ./cmd/repl
BIN_DIR     ?= bin
COVER_DIR   ?= coverage
COVER_FILE  ?= $(COVER_DIR)/coverage.out
LINT_TIMEOUT ?= 5m

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this list of make targets
	@echo "Usage: make <target>"
	@echo ""
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| LC_ALL=C sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: deps
deps: ## Download the module dependencies
	$(GO) mod download

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum
	$(GO) mod tidy

.PHONY: fmt
fmt: ## Format all Go source with gofmt -s -w
	gofmt -s -w .

.PHONY: fmt-check
fmt-check: ## Fail if any Go source needs gofmt -s formatting
	@unformatted=$$(gofmt -s -l . | grep -v '^\.git' || true); \
	if [ -n "$$unformatted" ]; then \
		echo "These files need formatting (run 'make fmt'):"; \
		echo "$$unformatted" | sed 's/^/  /'; \
		exit 1; \
	fi
	@echo "All files are gofmt -s clean"

.PHONY: vet
vet: ## Run go vet across all packages
	$(GO) vet ./...

.PHONY: test
test: ## Run the full test suite
	$(GO) test ./...

.PHONY: test-race
test-race: ## Run the test suite with the race detector, as CI does
	$(GO) test -race -v ./...

.PHONY: coverage
coverage: ## Run tests with coverage and write coverage/coverage.out
	@mkdir -p $(COVER_DIR)
	@COVERPKG=$$($(GO) list ./... | grep -v testutil | tr '\n' ',' | sed 's/,$$//'); \
	$(GO) test -race -coverprofile=$(COVER_FILE) -covermode=atomic -coverpkg="$$COVERPKG" ./...
	@$(GO) tool cover -func=$(COVER_FILE) | tail -1

.PHONY: coverage-html
coverage-html: coverage ## Open the coverage report in a browser
	$(GO) tool cover -html=$(COVER_FILE)

.PHONY: lint
lint: ## Run golangci-lint with the CI timeout
	golangci-lint run --timeout=$(LINT_TIMEOUT)

.PHONY: vuln
vuln: ## Scan dependencies for known vulnerabilities
	govulncheck ./...

.PHONY: build
build: ## Build the REPL binary into bin/
	$(GO) build -o $(BIN_DIR)/repl $(REPL_PKG)

.PHONY: repl
repl: ## Run the interactive gosbee REPL
	$(GO) run $(REPL_PKG)

.PHONY: pre-ci
pre-ci: ## Run the full pre-CI validation script
	./scripts/pre-ci-check.sh

.PHONY: pre-ci-fix
pre-ci-fix: ## Run pre-CI validation, auto-fixing formatting first
	./scripts/pre-ci-check.sh --fix gofmt

.PHONY: check
check: fmt-check vet lint test-race ## Run formatting, vet, lint and race tests

.PHONY: clean
clean: ## Remove build and coverage artefacts
	rm -rf $(BIN_DIR)
	rm -f $(COVER_DIR)/pre-ci-* $(COVER_FILE)
