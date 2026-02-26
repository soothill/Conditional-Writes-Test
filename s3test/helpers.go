package s3test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

const (
	// wrongETag is an ETag that will never match any real object, used to
	// exercise conditional-write/read failure paths.
	wrongETag = `"0000000000000000000000000000dead"`

	// emptyObjectETag is the MD5 ETag of a zero-byte object. Used when tests
	// need an IfMatch value that is syntactically valid but will not match a
	// non-empty or non-existent key.
	emptyObjectETag = `"d41d8cd98f00b204e9800998ecf8427e"`

	// concurrentWorkers is the number of goroutines launched by concurrent
	// edge-case tests.
	concurrentWorkers = 10
)

// testContext returns a context that inherits from t.Context() (cancelled when
// the test ends), adds a 30-second per-operation deadline, and injects t into
// the context so the s3responseLogger middleware can log S3 responses against
// the correct test.
func testContext(t testing.TB) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx := injectTesting(t.Context(), t)
	return context.WithTimeout(ctx, 30*time.Second)
}

// uniqueKey generates a unique S3 key using the test name, a nanosecond
// timestamp, and a random suffix to avoid collisions across parallel or
// repeated runs (two goroutines on the same fast CPU can share a nanosecond).
func uniqueKey(t testing.TB, prefix string) string {
	t.Helper()
	return fmt.Sprintf("test/%s/%s/%d-%x", prefix, t.Name(), time.Now().UnixNano(), rand.Int64())
}

// wellPastTime returns a fixed timestamp well in the past (2020-01-01 UTC)
// used by conditional-read tests that need an IfModifiedSince or
// CopySourceIfModifiedSince value the object was certainly modified after.
func wellPastTime() time.Time {
	return time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
}

// wellFutureTime returns a timestamp 24 hours in the future, used by
// conditional-read tests that need an IfUnmodifiedSince or
// CopySourceIfUnmodifiedSince value the object has certainly not been modified
// after (i.e. it is still "unmodified" relative to that future date).
func wellFutureTime() time.Time {
	return time.Now().Add(24 * time.Hour)
}

// copySource formats the CopySource field required by CopyObject as "bucket/key".
func copySource(bucket, key string) string {
	return bucket + "/" + key
}

// putObject is a convenience wrapper that puts a small string body and returns
// the raw ETag (with surrounding quotes, as returned by S3).
func putObject(t testing.TB, client *s3.Client, bucket, key, body string) string {
	t.Helper()
	ctx, cancel := testContext(t)
	defer cancel()

	out, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	})
	require.NoError(t, err, "putObject failed for key %s", key)
	require.NotNil(t, out.ETag, "putObject returned nil ETag for key %s", key)
	return *out.ETag
}

// deleteObject deletes an object. It does not fail on NoSuchKey.
func deleteObject(t testing.TB, client *s3.Client, bucket, key string) {
	t.Helper()
	ctx, cancel := testContext(t)
	defer cancel()

	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Ignore NoSuchKey errors during cleanup.
		var nsk *types.NoSuchKey
		if !errors.As(err, &nsk) {
			t.Logf("warning: deleteObject for key %s returned error: %v", key, err)
		}
	}
}

// cleanupKey registers a t.Cleanup function to delete the given key after the
// test. It uses context.Background() — NOT testContext(t) — so the cleanup
// DeleteObject call is invisible to the s3responseLogger middleware and never
// overwrites the last [S3] log line that belongs to the test itself.
func cleanupKey(t testing.TB, client *s3.Client, bucket, key string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			var nsk *types.NoSuchKey
			if !errors.As(err, &nsk) {
				t.Logf("cleanup: delete %s: %v", key, err)
			}
		}
	})
}

// requireHTTPStatus asserts that err is an AWS HTTP response error with the expected status code.
func requireHTTPStatus(t testing.TB, err error, expectedStatus int) {
	t.Helper()
	require.Error(t, err, "expected an error with HTTP status %d but got nil", expectedStatus)

	var respErr *awshttp.ResponseError
	require.True(t, errors.As(err, &respErr),
		"expected *awshttp.ResponseError, got %T: %v", err, err)
	require.Equal(t, expectedStatus, respErr.HTTPStatusCode(),
		"expected HTTP %d, got HTTP %d: %v", expectedStatus, respErr.HTTPStatusCode(), err)
}

// requirePreconditionFailed asserts that err is an HTTP 412 Precondition Failed response.
func requirePreconditionFailed(t testing.TB, err error) {
	t.Helper()
	requireHTTPStatus(t, err, 412)
}

// requireNotModified asserts that err is an HTTP 304 Not Modified response.
// Use this for conditional GET/HEAD failures; for CopyObject and conditional
// write failures use requirePreconditionFailed — S3 returns 412, not 304, for
// all CopyObject conditional header failures.
func requireNotModified(t testing.TB, err error) {
	t.Helper()
	requireHTTPStatus(t, err, 304)
}

// requireIfMatchKeyMissing asserts that err is either an HTTP 404 Not Found or
// an HTTP 412 Precondition Failed response. When IfMatch is specified for a key
// that does not exist, AWS S3 returns 404 (the key genuinely does not exist and
// there is no ETag to compare), while some other S3-compatible implementations
// return 412 (the ETag condition cannot be satisfied). Both responses indicate
// that the conditional write was correctly rejected.
func requireIfMatchKeyMissing(t testing.TB, err error) {
	t.Helper()
	require.Error(t, err, "expected an error for IfMatch on non-existent key but got nil")

	var respErr *awshttp.ResponseError
	require.True(t, errors.As(err, &respErr),
		"expected *awshttp.ResponseError (404 or 412), got %T: %v", err, err)

	status := respErr.HTTPStatusCode()
	require.True(t, status == 404 || status == 412,
		"expected HTTP 404 (NotFound) or 412 (PreconditionFailed) for IfMatch on missing key, got HTTP %d: %v",
		status, err)
}

// requireConditionalWriteFailure asserts that err is either an HTTP 412
// Precondition Failed or an HTTP 409 ConditionalRequestConflict response.
// S3 returns 412 when a conditional header fails at request-evaluation time.
// It returns 409 when a competing write races and completes between the moment
// S3 evaluates the condition and the moment the write is committed. Under high
// concurrency both are valid failure outcomes for conditional writes.
func requireConditionalWriteFailure(t testing.TB, err error) {
	t.Helper()
	require.Error(t, err)

	var respErr *awshttp.ResponseError
	require.True(t, errors.As(err, &respErr),
		"expected *awshttp.ResponseError (412 or 409), got %T: %v", err, err)

	status := respErr.HTTPStatusCode()
	require.True(t, status == 412 || status == 409,
		"expected HTTP 412 (PreconditionFailed) or 409 (ConditionalRequestConflict), got HTTP %d: %v",
		status, err)
}

// stripQuotes removes surrounding double quotes from an ETag string returned
// by S3. Use this when you need the raw hex digest rather than the quoted form.
// Most S3 conditional headers expect the quoted form (e.g. `"abc123"`), so only
// strip quotes when working with the digest directly.
func stripQuotes(etag string) string {
	return strings.Trim(etag, "\"")
}

// runConcurrent launches n goroutines simultaneously via a barrier channel.
// Each goroutine receives its zero-based index, calls fn, and sends the
// resulting error to a buffered channel. The slice of errors is returned in
// completion order (which may differ from launch order).
func runConcurrent(n int, fn func(id int) error) []error {
	results := make(chan error, n)
	start := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			results <- fn(id)
		}(i)
	}

	close(start) // Release all goroutines simultaneously.
	wg.Wait()
	close(results)

	errs := make([]error, 0, n)
	for err := range results {
		errs = append(errs, err)
	}
	return errs
}

// doMultipartUpload performs a full multipart upload (create, upload one part,
// complete) with optional conditional headers on CompleteMultipartUpload.
// A cleanup is registered to abort the upload in case completion fails.
// Returns the CompleteMultipartUpload output and any error from the Complete call.
func doMultipartUpload(
	t testing.TB,
	client *s3.Client,
	bucket, key, body string,
	ifNoneMatch, ifMatch *string,
) (*s3.CompleteMultipartUploadOutput, error) {
	t.Helper()
	ctx, cancel := testContext(t)
	defer cancel()

	// Step 1: Create multipart upload.
	createOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "CreateMultipartUpload failed")
	require.NotNil(t, createOut.UploadId, "CreateMultipartUpload returned nil UploadId")

	uploadID := createOut.UploadId

	// Register cleanup to abort the upload. Use context.Background() here —
	// NOT t.Context() — because t.Context() is already cancelled by the time
	// t.Cleanup functions run.
	t.Cleanup(func() {
		abortCtx, abortCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer abortCancel()
		_, abortErr := client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: uploadID,
		})
		if abortErr != nil {
			// 404 means the upload was already completed or aborted — benign.
			var respErr *awshttp.ResponseError
			if errors.As(abortErr, &respErr) && respErr.HTTPStatusCode() == 404 {
				return
			}
			t.Logf("cleanup: abort multipart upload for key %s: %v", key, abortErr)
		}
	})

	// Step 2: Upload a single part.
	partOut, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   uploadID,
		PartNumber: aws.Int32(1),
		Body:       strings.NewReader(body),
	})
	require.NoError(t, err, "UploadPart failed")
	require.NotNil(t, partOut.ETag, "UploadPart returned nil ETag")

	// Step 3: Complete multipart upload with conditional headers.
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{
					PartNumber: aws.Int32(1),
					ETag:       partOut.ETag,
				},
			},
		},
	}
	if ifNoneMatch != nil {
		completeInput.IfNoneMatch = ifNoneMatch
	}
	if ifMatch != nil {
		completeInput.IfMatch = ifMatch
	}

	return client.CompleteMultipartUpload(ctx, completeInput)
}
