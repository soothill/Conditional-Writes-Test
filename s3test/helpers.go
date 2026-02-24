package s3test

import (
	"context"
	"errors"
	"fmt"
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
// the test ends) and adds a 30-second per-operation deadline.
func testContext(t testing.TB) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(t.Context(), 30*time.Second)
}

// uniqueKey generates a unique S3 key using the test name and a nanosecond
// timestamp to avoid collisions across parallel or repeated runs.
func uniqueKey(t testing.TB, prefix string) string {
	t.Helper()
	return fmt.Sprintf("test/%s/%s/%d", prefix, t.Name(), time.Now().UnixNano())
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

// cleanupKey registers a t.Cleanup function to delete the given key after the test.
func cleanupKey(t testing.TB, client *s3.Client, bucket, key string) {
	t.Helper()
	t.Cleanup(func() {
		deleteObject(t, client, bucket, key)
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
func requireNotModified(t testing.TB, err error) {
	t.Helper()
	requireHTTPStatus(t, err, 304)
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
		//nolint:errcheck // Best-effort abort; may fail if the upload already completed.
		client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: uploadID,
		})
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
