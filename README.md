# Conditional-Writes-Test

Go integration test suite for validating AWS S3 conditional write and conditional read operations across multiple S3-compatible storage providers. Tests run against AWS S3 or any S3-compatible endpoint (Wasabi, Backblaze B2, Impossible Cloud, MinIO, LocalStack, Ceph, etc.).

Conditional writes — using `If-None-Match` and `If-Match` on `PutObject`, `CompleteMultipartUpload`, and `CopyObject` — were added to AWS S3 in August 2024. This suite verifies whether an endpoint implements them correctly, and whether existing conditional read operations behave as specified.

## Prerequisites

- Go 1.24.5+
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
| `S3_BUCKET` | **Yes** | — | Bucket name for tests |
| `S3_ENDPOINT` | No | AWS default | Custom endpoint URL (leave empty for real AWS S3) |
| `AWS_REGION` | No | `us-east-1` | AWS region |
| `AWS_ACCESS_KEY_ID` | No | SDK default chain | Access key |
| `AWS_SECRET_ACCESS_KEY` | No | SDK default chain | Secret key |
| `AWS_SESSION_TOKEN` | No | — | Session token (STS / assume-role / SSO) |
| `S3_PATH_STYLE` | No | `true` if endpoint set | Force path-style addressing |
| `S3_CONFIG_FILE` | No | `.env` (auto-found) | Path to a custom config file |

> **AWS S3 note:** Leave `S3_ENDPOINT` empty and set `S3_PATH_STYLE=false`. The SDK routes requests to the correct regional endpoint automatically.

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

`s3conditionaltest matrix` runs the full test suite against every provider listed in `testmatrix.json` in parallel and prints a side-by-side comparison table showing which tests pass or fail on each provider.

### Setup

```bash
cp testmatrix.json.example testmatrix.json
# edit testmatrix.json — add a "providers" entry for each endpoint
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

## Expected Results

Results from a full matrix run on **26 February 2026** (40 tests, 4 providers).
"✓" means the full sub-group passes; "✗" means at least one sub-test in the group fails;
the status code in parentheses is the HTTP response that caused the failure (or that was
returned unexpectedly).

| Test group / Sub-group | AWS S3 | Backblaze B2 | Impossible Cloud | Wasabi |
|---|:---:|:---:|:---:|:---:|
| **PutObject — IfNoneMatch** | ✓ | ✗ | ✗ | ✗ |
| **PutObject — IfMatch** | ✓ | ✗ | ✗ | ✗ |
| **Multipart — IfNoneMatch** | ✓ | ✗ | ✗ | ✗ |
| **Multipart — IfMatch** | ✓ | ✗ | ✗ | ✗ |
| **CopyObject dest — IfNoneMatch** | ✓ | ✗ | ✗ | ✗ |
| **CopyObject dest — IfMatch** | ✓ | ✗ | ✗ | ✗ |
| **CopyObject src — CopySourceIfMatch** | ✓ | ✓ | ✓ | ✗ |
| **CopyObject src — CopySourceIfNoneMatch** | ✓ | ✓ | ✓ | ✗ |
| **CopyObject src — CopySourceIfModifiedSince** | ✓ | ✓ | ✓ | ✗ |
| **CopyObject src — CopySourceIfUnmodifiedSince** | ✓ | ✓ | ✓ | ✗ |
| **GetObject — IfMatch** | ✓ | ✓ | ✓ | ✓ |
| **GetObject — IfNoneMatch** | ✓ | ✗ | ✗ | ✗ |
| **GetObject — IfModifiedSince** | ✓ | ✓ | ✓ | ✓ |
| **GetObject — IfUnmodifiedSince** | ✓ | ✓ | ✓ | ✓ |
| **HeadObject — IfMatch** | ✓ | ✓ | ✓ | ✓ |
| **HeadObject — IfNoneMatch** | ✓ | ✗ | ✗ | ✗ |
| **HeadObject — IfModifiedSince** | ✓ | ✓ | ✓ | ✓ |
| **HeadObject — IfUnmodifiedSince** | ✓ | ✓ | ✓ | ✓ |
| **Edge — concurrent IfNoneMatch** | ✓ | ✗ (501) | ✗ (501) | ✗ (200) |
| **Edge — concurrent IfMatch** | ✓ | ✗ (501) | ✗ (501) | ✗ (200) |
| **Edge — empty body + IfNoneMatch** | ✓ | ✗ (501) | ✗ (501) | ✓ |
| **Edge — large object (10 MB)** | ✓ | ✗ | ✗ | ✓ |
| **Edge — special chars in key** | ✓ | ✗ | ✗ | ✓ |
| **Edge — ETag round-trip** | ✓ | ✗ (501) | ✗ (501) | ✓ |
| **Edge — in-progress multipart invisible to IfNoneMatch** | ✓ | ✗ (501) | ✗ (501) | ✓ |
| **Edge — IfNoneMatch + IfMatch mutual exclusion** | ✓ (501) | ✓ (501) | ✓ (501) | ✗ (200) |
| **LanceDB — range reads (GetObject Range)** | ✓ | ✓ | ✓ | ✓ |
| **LanceDB — HeadObject metadata** | ✓ | ✓ | ✓ | ✓ |
| **LanceDB — unconditional PutObject** | ✓ | ✓ | ✓ | ✓ |
| **LanceDB — AtomicCreate succeeds on new key** | ✓ | ✗ (501) | ✗ (501) | ✓ |
| **LanceDB — AtomicCreate rejects existing key** | ✓ (412) | ✗ (501) | ✗ (501) | ✗ (200) |
| **LanceDB — ConditionalUpdate (If-Match)** | ✓ | ✗ (501) | ✗ (501) | ✓ |
| **LanceDB — multipart upload** | ✓ | ✓ | ✓ | ✓ |
| **LanceDB — multipart atomic create** | ✓ (412) | ✗ (200) | ✗ (200) | ✗ (200) |
| **LanceDB — AbortMultipartUpload** | ✓ | ✓ | ✗ (403) | ✓ |
| **LanceDB — ListObjectsV2** | ✓ | ✓ | ✓ | ✓ |
| **LanceDB — DeleteObject** | ✓ | ✓ | ✓ | ✓ |
| **LanceDB — BulkDeleteObjects** | ✓ | ✓ | ✓ | ✓ |
| **LanceDB — CopyObject** | ✓ | ✓ | ✓ | ✓ |
| **LanceDB — manifest commit workflow** | ✓ | ✗ (501) | ✗ (501) | ✗ (200) |
| **Total (40 tests)** | **40 / 40** | **20 / 40** | **19 / 40** | **22 / 40** |

### Provider notes

**AWS S3** — Full support for all 40 tested operations. Returns `412 Precondition Failed`
when a write condition fails, `404 NoSuchKey` when `If-Match` is used against a key that
does not exist, and `501 Not Implemented` when both `If-None-Match` and `If-Match` are
sent simultaneously (a logically contradictory combination it has never implemented).
The only S3-compatible provider in this test run to pass the full LanceDB compatibility
suite.

**Backblaze B2** — Conditional writes (`If-None-Match` and `If-Match` on `PutObject`,
`CompleteMultipartUpload`, and `CopyObject` destination) are not supported — headers are
rejected with `501 Not Implemented`. `If-None-Match` on `GetObject` and `HeadObject` is
also broken (304 Not Modified is not returned correctly). `CopySourceIfMatch`,
`CopySourceIfNoneMatch`, `CopySourceIfModifiedSince`, and `CopySourceIfUnmodifiedSince`
all work. Basic LanceDB operations (range reads, unconditional puts, multipart, listing,
deletes, copies) pass; atomic manifest commits fail because they rely on `If-None-Match`
on `PutObject` or `CompleteMultipartUpload`.

**Impossible Cloud** — Same pattern as Backblaze B2 (20/40 failing), with one additional
failure: `AbortMultipartUpload` returns `403 Forbidden` instead of `204 No Content`,
which would prevent LanceDB from cleaning up failed uploads. All other results match B2.

**Wasabi** — Conditional writes on `PutObject` and `CompleteMultipartUpload` are not
enforced (headers accepted but operations proceed regardless, returning `200` instead of
`412`). All six `CopyObject` conditionals — both destination (`If-None-Match`,
`If-Match`) and source (`CopySourceIfMatch`, `CopySourceIfNoneMatch`,
`CopySourceIfModifiedSince`, `CopySourceIfUnmodifiedSince`) — fail. `If-None-Match` on
`GetObject` and `HeadObject` is also broken. Sending both `If-None-Match=*` and
`If-Match` simultaneously returns `200` instead of rejecting the contradictory
combination. LanceDB's atomic `If-None-Match=*` `PutObject` succeeds on a new key but
fails to reject an overwrite (returns `200` instead of `412`), and
`CompleteMultipartUpload` with `If-None-Match=*` is not enforced either, making Wasabi
unsafe for LanceDB's multi-writer commit protocol.

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

**68 subtests** across 7 test files:

| File | Test Function | Subtests | Description |
|---|---|---|---|
| `putobject_test.go` | `TestPutObjectConditionalWrites` | 8 | `If-None-Match` and `If-Match` on `PutObject` |
| `multipart_test.go` | `TestMultipartConditionalWrites` | 6 | `If-None-Match` and `If-Match` on `CompleteMultipartUpload` |
| `copyobject_test.go` | `TestCopyObjectConditionalWrites` | 14 | Destination and source conditionals on `CopyObject` |
| `getobject_test.go` | `TestGetObjectConditionalReads` | 9 | `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since` |
| `headobject_test.go` | `TestHeadObjectConditionalReads` | 9 | Same conditionals as `GetObject` |
| `edge_test.go` | `TestEdgeCases` | 8 | Concurrency, empty body, large objects, special chars, ETag round-trip, in-progress multipart |
| `lancedb_test.go` | `TestLanceDBCompatibility` | 14 | LanceDB-required S3 operations (range reads, atomic creates, multipart, bulk delete, etc.) |

### Sub-test breakdown

<details>
<summary>PutObject conditional writes (8 sub-tests)</summary>

| Sub-test | What it verifies |
|---|---|
| `IfNoneMatch/NewKey` | `IfNoneMatch=*` succeeds when the key does not exist |
| `IfNoneMatch/ExistingKey` | `IfNoneMatch=*` returns 412 when the key already exists |
| `IfNoneMatch/AfterDelete` | `IfNoneMatch=*` succeeds after the key has been deleted |
| `IfMatch/CorrectETag` | `IfMatch=<etag>` succeeds when the ETag matches |
| `IfMatch/WrongETag` | `IfMatch=<wrong-etag>` returns 412 |
| `IfMatch/NonExistentKey` | `IfMatch` on a missing key returns 404 (AWS) or 412 (others) |
| `IfMatch/StaleETag` | `IfMatch` with a superseded ETag returns 412 |
| `IfMatch/ChainedUpdates` | Three sequential IfMatch updates each succeed and produce distinct ETags |

</details>

<details>
<summary>Multipart conditional writes (6 sub-tests)</summary>

| Sub-test | What it verifies |
|---|---|
| `IfNoneMatch/NewKey` | `CompleteMultipartUpload` with `IfNoneMatch=*` on a new key succeeds |
| `IfNoneMatch/ExistingKey` | Returns 412 when the destination key already exists |
| `IfNoneMatch/AfterDelete` | Succeeds after the key has been deleted |
| `IfMatch/CorrectETag` | `CompleteMultipartUpload` with correct `IfMatch` ETag succeeds |
| `IfMatch/WrongETag` | Returns 412 with a wrong ETag |
| `IfMatch/NonExistentKey` | Returns 404 or 412 on a missing key |

</details>

<details>
<summary>CopyObject conditional writes (14 sub-tests)</summary>

| Sub-test | What it verifies |
|---|---|
| `IfNoneMatch/NewDestination` | `IfNoneMatch=*` on destination succeeds when destination does not exist |
| `IfNoneMatch/ExistingDestination` | Returns 412 when destination already exists |
| `IfNoneMatch/AfterDelete` | Succeeds after destination has been deleted |
| `IfMatch/CorrectETag` | `IfMatch` on destination succeeds with correct ETag |
| `IfMatch/WrongETag` | Returns 412 on destination with wrong ETag |
| `IfMatch/NonExistentDestination` | Returns 404 or 412 when destination does not exist |
| `CopySourceIfMatch/CorrectETag` | Copy proceeds when source ETag matches |
| `CopySourceIfMatch/WrongETag` | Returns 412 when source ETag does not match |
| `CopySourceIfNoneMatch/MatchingETag` | Returns 412 when source ETag matches (condition inverted) |
| `CopySourceIfNoneMatch/DifferentETag` | Copy proceeds when source ETag differs |
| `CopySourceIfModifiedSince/Modified` | Copy proceeds when source was modified after the given date |
| `CopySourceIfModifiedSince/NotModified` | Returns 412 when source has not been modified since the given date |
| `CopySourceIfUnmodifiedSince/Unmodified` | Copy proceeds when source has not been modified since the given date |
| `CopySourceIfUnmodifiedSince/Modified` | Returns 412 when source was modified after the given date |

</details>

<details>
<summary>GetObject / HeadObject conditional reads (9 sub-tests each)</summary>

| Sub-test | What it verifies |
|---|---|
| `IfMatch/CorrectETag` | Returns 200 with body when ETag matches |
| `IfMatch/WrongETag` | Returns 412 when ETag does not match |
| `IfNoneMatch/MatchingETag` | Returns 304 Not Modified when ETag matches |
| `IfNoneMatch/DifferentETag` | Returns 200 with body when ETag differs |
| `IfNoneMatch/Wildcard` | Returns 304 Not Modified when `If-None-Match: *` and any object exists (RFC 7232 wildcard) |
| `IfModifiedSince/Modified` | Returns 200 when object was modified after the given date |
| `IfModifiedSince/NotModified` | Returns 304 when object has not changed since the given date |
| `IfUnmodifiedSince/Unmodified` | Returns 200 when object has not changed since the given date |
| `IfUnmodifiedSince/Modified` | Returns 412 when object was modified after the given date |

</details>

<details>
<summary>Edge cases (8 sub-tests)</summary>

| Sub-test | What it verifies |
|---|---|
| `ConcurrentIfNoneMatch` | Exactly one of N concurrent `IfNoneMatch=*` writes succeeds; the rest return 412 or 409; winner's data is durably stored |
| `ConcurrentIfMatch` | Exactly one of N concurrent `IfMatch` writes succeeds; the rest return 412 or 409; winner's data is durably stored |
| `EmptyBody` | `IfNoneMatch=*` works correctly with a zero-byte body |
| `LargeObject` | `IfNoneMatch=*` then `IfMatch` work correctly with a 10 MB body |
| `SpecialCharsInKey` | `IfNoneMatch=*` works with spaces, Unicode, deep paths, `+`, and `&` in keys |
| `ETagRoundTrip` | ETag from `PutObject` can be used with `HeadObject` and then `PutObject IfMatch` and `GetObject IfMatch` |
| `InProgressMultipartInvisibleToIfNoneMatch` | `PutObject If-None-Match=*` succeeds while a multipart upload for the same key is in progress (in-progress uploads are invisible to conditional write evaluation) |
| `IfNoneMatchAndIfMatchMutualExclusion` | Sending both `If-None-Match=*` and `If-Match` simultaneously is rejected (one condition must always fail) |

</details>

<details>
<summary>LanceDB compatibility (14 sub-tests)</summary>

Verifies the S3 operations required by [LanceDB](https://lancedb.github.io/lancedb/), which uses Apache Arrow's `object_store` crate and relies on `If-None-Match: *` for atomic multi-writer manifest commits.

| Sub-test | What it verifies |
|---|---|
| `RangeRead` | Byte-range `GetObject` (Range header) returns the correct slice of object data |
| `HeadObject` | `HeadObject` returns correct `ContentLength` and `ETag` metadata |
| `PutObject` | Unconditional `PutObject` stores an object and returns an ETag |
| `AtomicCreate_SucceedsOnNewKey` | `PutObject If-None-Match=*` succeeds when the key does not exist |
| `AtomicCreate_FailsOnExistingKey` | `PutObject If-None-Match=*` returns 412 or 409 when the key already exists |
| `ConditionalUpdate` | `PutObject If-Match=<etag>` updates the object when the ETag matches and returns a new ETag |
| `MultipartWrite` | Full multipart upload (create → upload part → complete) succeeds and the body round-trips |
| `MultipartAtomicCreate` | `CompleteMultipartUpload If-None-Match=*` succeeds on a new key |
| `AbortMultipartUpload` | `AbortMultipartUpload` removes an in-progress upload without error |
| `ListObjectsV2` | Prefix listing with pagination (`MaxKeys=1` + `ContinuationToken`) returns all objects |
| `DeleteObject` | `DeleteObject` removes an object; subsequent `HeadObject` returns 404 |
| `BulkDeleteObjects` | `DeleteObjects` removes multiple objects in a single API call |
| `CopyObject` | `CopyObject` copies an object to a new key and returns a matching ETag |
| `ManifestCommitWorkflow` | End-to-end simulation of LanceDB's write workflow: PutObject data, atomic manifest commit, conditional update, and verify final state |

</details>

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
  lancedb_test.go       LanceDB S3 compatibility tests

cmd/s3conditionaltest/
  main.go               Package doc, shared types/helpers, ANSI helpers, main()
  run.go                `run` subcommand — formats go test -json output
  matrix.go             `matrix` subcommand — multi-provider comparison table

.env.example            Template config file (committed)
.env.aws                AWS S3 provider config example
.env.wasabi             Wasabi provider config example
.env.backblaze          Backblaze B2 provider config example
.env.impossible         Impossible Cloud provider config example
testmatrix.json.example Template matrix config (committed; copy to testmatrix.json)
```
