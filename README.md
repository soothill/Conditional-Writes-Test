# Conditional-Writes-Test

Go integration test suite for validating AWS S3 conditional write and conditional read operations. Tests can run against AWS S3 or any S3-compatible endpoint (MinIO, LocalStack, Ceph).

## Prerequisites

- Go 1.23+
- An S3 bucket with write access
- [golangci-lint](https://golangci-lint.run/) v2 (for linting)

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
make test           Run all tests
make test-verbose   Run all tests with verbose output
make test-put       Run only PutObject tests
make test-multipart Run only multipart upload tests
make test-copy      Run only CopyObject tests
make test-get       Run only GetObject conditional read tests
make test-head      Run only HeadObject conditional read tests
make test-edge      Run only edge case tests
make lint           Run golangci-lint
make fmt            Format code (gofmt + goimports)
make vet            Run go vet
make tidy           Tidy go modules
make clean          Clean test cache
make check          Run fmt, lint, vet, and all tests
```

## Project Structure

```
s3test/
  config.go           Configuration loading from .env file and environment variables
  client.go           S3 client construction
  helpers.go          Shared test utilities (assertions, cleanup, key generation)
  helpers_test.go     Unit tests for helper functions
  testmain_test.go    TestMain - shared client/config setup
  putobject_test.go   PutObject conditional write tests
  multipart_test.go   CompleteMultipartUpload conditional write tests
  copyobject_test.go  CopyObject conditional write/read tests
  getobject_test.go   GetObject conditional read tests
  headobject_test.go  HeadObject conditional read tests
  edge_test.go        Edge case and concurrency tests
```
