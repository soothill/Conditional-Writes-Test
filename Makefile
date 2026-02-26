.PHONY: test test-pretty test-json test-matrix test-matrix-json test-verbose test-unit test-put test-multipart test-copy test-get test-head test-edge test-lancedb lint fmt vet tidy clean build check help

GOLANGCI_LINT := golangci-lint

# Integration tests require -tags integration and S3_BUCKET to be set.
INTEGRATION_FLAGS := -tags integration -v -count=1

# Regex passed to s3conditionaltest run --filter: only the conditional S3 tests
# are shown by default. Tests that do NOT match this pattern are still run and
# still counted in the summary, but their output is suppressed when they pass.
# If any of them fail they always appear regardless of the filter.
TESTFMT_FILTER := Conditional|EdgeCases|LanceDB

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run all integration tests (requires S3_BUCKET)
	go test $(INTEGRATION_FLAGS) -timeout 10m ./s3test/

test-pretty: ## Run all integration tests with clean, readable output (requires S3_BUCKET)
	go test $(INTEGRATION_FLAGS) -timeout 10m -json ./s3test/ | go run ./cmd/s3conditionaltest run --filter '$(TESTFMT_FILTER)'

test-json: ## Run all integration tests and emit results as JSON (requires S3_BUCKET)
	go test $(INTEGRATION_FLAGS) -timeout 10m -json ./s3test/ | go run ./cmd/s3conditionaltest run --format=json --filter '$(TESTFMT_FILTER)'

test-matrix: ## Run tests against all providers in testmatrix.json and print comparison table
	go run ./cmd/s3conditionaltest matrix --filter '$(TESTFMT_FILTER)'

test-matrix-json: ## Run tests against all providers and emit comparison as JSON
	go run ./cmd/s3conditionaltest matrix --format=json --filter '$(TESTFMT_FILTER)'

test-verbose: ## Run all integration tests with extra verbose output
	go test $(INTEGRATION_FLAGS) -timeout 10m -run . ./s3test/

test-unit: ## Run unit tests only (no S3 credentials required)
	go test -v -count=1 ./s3test/

test-put: ## Run only PutObject conditional write tests
	go test $(INTEGRATION_FLAGS) -timeout 5m -run TestPutObjectConditionalWrites ./s3test/

test-multipart: ## Run only CompleteMultipartUpload conditional write tests
	go test $(INTEGRATION_FLAGS) -timeout 5m -run TestMultipartConditionalWrites ./s3test/

test-copy: ## Run only CopyObject conditional tests
	go test $(INTEGRATION_FLAGS) -timeout 5m -run TestCopyObjectConditionalWrites ./s3test/

test-get: ## Run only GetObject conditional read tests
	go test $(INTEGRATION_FLAGS) -timeout 5m -run TestGetObjectConditionalReads ./s3test/

test-head: ## Run only HeadObject conditional read tests
	go test $(INTEGRATION_FLAGS) -timeout 5m -run TestHeadObjectConditionalReads ./s3test/

test-edge: ## Run only edge case tests
	go test $(INTEGRATION_FLAGS) -timeout 10m -run TestEdgeCases ./s3test/

test-lancedb: ## Run only LanceDB compatibility tests
	go test $(INTEGRATION_FLAGS) -timeout 5m -run TestLanceDBCompatibility ./s3test/

build: ## Build the s3conditionaltest CLI tool to the repo root
	go build -o s3conditionaltest ./cmd/s3conditionaltest/

lint: ## Run golangci-lint
	$(GOLANGCI_LINT) run ./...

fmt: ## Format code
	gofmt -s -w .
	goimports -w .

vet: ## Run go vet
	go vet -tags integration ./...

tidy: ## Tidy go modules
	go mod tidy

clean: ## Clean test cache
	go clean -testcache

check: fmt lint vet build test ## Run fmt, lint, vet, build, and all integration tests
