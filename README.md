# Conditional-Writes-Test

Go integration test suite for validating AWS S3 conditional write and conditional read operations. Tests can run against AWS S3 or any S3-compatible endpoint (MinIO, LocalStack, Ceph).

## Prerequisites

- Go 1.24+
- An S3 bucket with write access
- [golangci-lint](https://golangci-lint.run/) v2 (for linting only)

## Configuration

Parameters can be supplied via a `.env` config file, environment variables, or both. **Environment variables always take precedence over the config file.**

### Config file (recommended)

Copy the sample file and fill in your values:

```bash
cp .env.example .env
# edit .env with your bucket name, credentials, etc.
```

The `.env` file is listed in `.gitignore` and will never be committed. `.env.example` is the committed template.

The config file is found automatically (first match wins):

1. Path in the `S3_CONFIG_FILE` environment variable
2. `.env` in the current directory
3. `.env` one directory up (project root when running `go test ./s3test/`)

### Parameters

| Variable | Required | Default | Description |
|---|---|---|---|
| `S3_BUCKET` | **Yes** | - | Bucket name for tests |
| `S3_ENDPOINT` | No | AWS default | Custom endpoint (MinIO, LocalStack) |
| `AWS_REGION` | No | `us-east-1` | AWS region |
| `AWS_ACCESS_KEY_ID` | No | SDK default chain | Access key |
| `AWS_SECRET_ACCESS_KEY` | No | SDK default chain | Secret key |
| `AWS_SESSION_TOKEN` | No | - | Session token |
| `S3_PATH_STYLE` | No | `true` if endpoint set | Force path-style addressing |
| `S3_CONFIG_FILE` | No | `.env` (auto-found) | Path to a custom config file |

## Running Tests

### All tests

```bash
# Using a .env file (recommended)
cp .env.example .env   # fill in your values once
make test

# Or pass values inline
S3_BUCKET=my-test-bucket make test
```

### Individual test groups

```bash
make test-put        # PutObject conditional writes
make test-multipart  # CompleteMultipartUpload conditional writes
make test-copy       # CopyObject conditional writes/reads
make test-get        # GetObject conditional reads
make test-head       # HeadObject conditional reads
make test-edge       # Edge cases (concurrency, large objects, etc.)
```

### Pretty output (conditional tests highlighted)

```bash
make test-pretty     # Clean, readable output showing S3 response codes
make test-json       # Same, emitted as JSON for machine consumption
```

### Single subtest

```bash
go test -tags integration -v -count=1 -run TestPutObjectConditionalWrites/IfMatch/CorrectETag ./s3test/
```

### With a custom endpoint (MinIO)

Add these lines to your `.env` file:

```
S3_BUCKET=test
S3_ENDPOINT=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
```

Or pass them inline:

```bash
S3_ENDPOINT=http://localhost:9000 \
S3_BUCKET=test \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
make test
```

## Multi-Provider Testing

`s3conditionaltest matrix` runs the full test suite against every provider
listed in `testmatrix.json` in parallel and prints a side-by-side comparison
table showing which tests pass or fail on each provider.

### Setup

```bash
cp testmatrix.json.example testmatrix.json
# edit testmatrix.json — add a [[providers]] entry for each endpoint
# create one .env.* file per provider with its credentials
```

`testmatrix.json` is gitignored (it contains local paths and credentials).
`testmatrix.json.example` is the committed template.

### Run

```bash
make test-matrix          # comparison table (text)
make test-matrix-json     # same, emitted as JSON
```

Or directly:

```bash
s3conditionaltest matrix
s3conditionaltest matrix --config=my-matrix.json --timeout=5m --format=json
```

## `s3conditionaltest` CLI

The `s3conditionaltest` binary provides two subcommands for working with test output.

### Build

```bash
make build               # outputs ./s3conditionaltest
# or
go build -o s3conditionaltest ./cmd/s3conditionaltest/
```

### `run` — format `go test -json` output

Reads `go test -json` from stdin and formats it as a clean, readable summary
with pass/fail icons, elapsed times, and the last S3 HTTP response for each
sub-test.

```bash
go test -tags integration -v -count=1 -json ./s3test/ | s3conditionaltest run
go test -tags integration -v -count=1 -json ./s3test/ | s3conditionaltest run --filter Conditional
go test -tags integration -v -count=1 -json ./s3test/ | s3conditionaltest run --format=json
```

| Flag | Default | Description |
|---|---|---|
| `--format` | `text` | Output format: `text` or `json` |
| `--filter` | _(none)_ | Regex: only show matching test groups; failures always shown |

### `matrix` — multi-provider comparison table

Runs the integration tests against every provider in `testmatrix.json` in
parallel and prints a comparison table.

```bash
s3conditionaltest matrix
s3conditionaltest matrix --config=my-matrix.json --timeout=5m
s3conditionaltest matrix --format=json
```

| Flag | Default | Description |
|---|---|---|
| `--config` | `testmatrix.json` | Path to matrix config file |
| `--format` | `text` | Output format: `text` or `json` |
| `--filter` | _(from config)_ | Override filter regex from config file |
| `--timeout` | `10m` | Timeout per provider run (passed to `go test -timeout`) |

## Test Coverage

**47 subtests** across 6 test files:

| File | Test Function | Subtests | Description |
|---|---|---|---|
| `putobject_test.go` | `TestPutObjectConditionalWrites` | 8 | If-None-Match and If-Match on PutObject |
| `multipart_test.go` | `TestMultipartConditionalWrites` | 6 | If-None-Match and If-Match on CompleteMultipartUpload |
| `copyobject_test.go` | `TestCopyObjectConditionalWrites` | 10 | Destination and source conditionals on CopyObject |
| `getobject_test.go` | `TestGetObjectConditionalReads` | 8 | If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since |
| `headobject_test.go` | `TestHeadObjectConditionalReads` | 8 | Same conditionals as GetObject |
| `edge_test.go` | `TestEdgeCases` | 7 | Concurrency, empty body, large objects, special chars, ETag round-trip |

## Makefile Targets

```
make test              Run all integration tests
make test-pretty       Run all tests with clean, readable output
make test-json         Run all tests and emit results as JSON
make test-matrix       Run tests against all providers in testmatrix.json
make test-matrix-json  Run matrix tests and emit comparison as JSON
make test-verbose      Run all tests with verbose output
make test-unit         Run unit tests only (no S3 credentials required)
make test-put          Run only PutObject tests
make test-multipart    Run only multipart upload tests
make test-copy         Run only CopyObject tests
make test-get          Run only GetObject conditional read tests
make test-head         Run only HeadObject conditional read tests
make test-edge         Run only edge case tests
make build             Build s3conditionaltest CLI to repo root
make lint              Run golangci-lint
make fmt               Format code (gofmt + goimports)
make vet               Run go vet
make tidy              Tidy go modules
make clean             Clean test cache
make check             Run fmt, lint, vet, build, and all tests
```

## Project Structure

```
s3test/
  config.go             Configuration loading from .env file and environment variables
  config_test.go        Unit tests for configuration loading
  client.go             S3 client construction
  logmiddleware.go      AWS SDK middleware that logs S3 HTTP responses per test
  helpers.go            Shared test utilities (assertions, cleanup, key generation)
  helpers_test.go       Unit tests for helper functions
  testmain_test.go      TestMain — shared client/config setup and preflight check
  putobject_test.go     PutObject conditional write tests
  multipart_test.go     CompleteMultipartUpload conditional write tests
  copyobject_test.go    CopyObject conditional write/read tests
  getobject_test.go     GetObject conditional read tests
  headobject_test.go    HeadObject conditional read tests
  edge_test.go          Edge case and concurrency tests

cmd/s3conditionaltest/
  main.go               Unified CLI: `run` (format test output) and `matrix` (multi-provider)

.env.example            Template config file (committed)
testmatrix.json.example Template matrix config (committed; copy to testmatrix.json)
```
