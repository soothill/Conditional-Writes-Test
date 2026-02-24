.PHONY: test test-verbose test-unit test-put test-multipart test-copy test-get test-head test-edge lint fmt vet tidy clean check help

GOLANGCI_LINT := golangci-lint

# Integration tests require -tags integration and S3_BUCKET to be set.
INTEGRATION_FLAGS := -tags integration -v -count=1

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run all integration tests (requires S3_BUCKET)
	go test $(INTEGRATION_FLAGS) -timeout 10m ./s3test/

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

check: fmt lint vet test ## Run fmt, lint, vet, and all integration tests
