# Conditional-Writes-Test

Go integration test suite for validating AWS S3 conditional write and conditional read operations. Tests can run against AWS S3 or any S3-compatible endpoint (MinIO, LocalStack, Ceph).

## Prerequisites

- Go 1.23+
- An S3 bucket with write access
- [golangci-lint](https://golangci-lint.run/) v2 (for linting)

## Configuration

Configuration is provided via environment variables:

| Variable | Required | Default | Description |
|---|---|---|---|
| `S3_BUCKET` | **Yes** | - | Bucket name for tests |
| `S3_ENDPOINT` | No | AWS default | Custom endpoint (MinIO, LocalStack) |
| `AWS_REGION` | No | `us-east-1` | AWS region |
| `AWS_ACCESS_KEY_ID` | No | SDK default chain | Access key |
| `AWS_SECRET_ACCESS_KEY` | No | SDK default chain | Secret key |
| `AWS_SESSION_TOKEN` | No | - | Session token |
| `S3_PATH_STYLE` | No | `true` if endpoint set | Force path-style addressing |

## Running Tests

### All tests

```bash
S3_BUCKET=my-test-bucket make test
```

### Individual test groups

```bash
S3_BUCKET=my-bucket make test-put        # PutObject conditional writes
S3_BUCKET=my-bucket make test-multipart  # CompleteMultipartUpload conditional writes
S3_BUCKET=my-bucket make test-copy       # CopyObject conditional writes/reads
S3_BUCKET=my-bucket make test-get        # GetObject conditional reads
S3_BUCKET=my-bucket make test-head       # HeadObject conditional reads
S3_BUCKET=my-bucket make test-edge       # Edge cases (concurrency, large objects, etc.)
```

### Single subtest

```bash
S3_BUCKET=my-bucket go test ./s3test/ -v -count=1 -run TestPutObjectConditionalWrites/IfMatch/CorrectETag
```

### With a custom endpoint (MinIO)

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
  config.go           Configuration loading from environment variables
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
