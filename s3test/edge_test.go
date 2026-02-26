//go:build integration

package s3test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEdgeCases(t *testing.T) {
	t.Run("ConcurrentIfNoneMatch", func(t *testing.T) {
		key := uniqueKey(t, "concurrent-ifnonematch")
		cleanupKey(t, testClient, testBucket, key)

		errs := runConcurrent(concurrentWorkers, func(id int) error {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err := testClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      aws.String(testBucket),
				Key:         aws.String(key),
				Body:        strings.NewReader(fmt.Sprintf("writer-%d", id)),
				IfNoneMatch: aws.String("*"),
			})
			return err
		})

		successCount, failCount := 0, 0
		for _, err := range errs {
			if err == nil {
				successCount++
			} else {
				// S3 returns 412 when the condition fails at evaluation time, or
				// 409 ConditionalRequestConflict when a competing write races
				// and completes between condition evaluation and the commit.
				// Both are valid outcomes under concurrent load.
				requireConditionalWriteFailure(t, err)
				failCount++
			}
		}
		assert.Equal(t, 1, successCount, "exactly one concurrent write should succeed")
		assert.Equal(t, concurrentWorkers-1, failCount, "all other writes should fail (412 or 409)")

		// Verify the winner's content was durably written. A compliant backend
		// must report success to exactly one writer AND persist that writer's data.
		getCtx, getCancel := testContext(t)
		defer getCancel()
		getOut, getErr := testClient.GetObject(getCtx, &s3.GetObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
		})
		require.NoError(t, getErr, "GetObject should succeed after concurrent writes")
		defer getOut.Body.Close()
		bodyBytes, readErr := io.ReadAll(getOut.Body)
		require.NoError(t, readErr)
		assert.Contains(t, string(bodyBytes), "writer-",
			"body should be one of the concurrent writers' payloads, got %q", string(bodyBytes))
	})

	t.Run("ConcurrentIfMatch", func(t *testing.T) {
		key := uniqueKey(t, "concurrent-ifmatch")
		cleanupKey(t, testClient, testBucket, key)

		// Create initial object and capture its ETag.
		etag := putObject(t, testClient, testBucket, key, "initial")

		errs := runConcurrent(concurrentWorkers, func(id int) error {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err := testClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket:  aws.String(testBucket),
				Key:     aws.String(key),
				Body:    strings.NewReader(fmt.Sprintf("writer-%d", id)),
				IfMatch: aws.String(etag),
			})
			return err
		})

		successCount, failCount := 0, 0
		for _, err := range errs {
			if err == nil {
				successCount++
			} else {
				// S3 returns 412 when the condition fails at evaluation time, or
				// 409 ConditionalRequestConflict when a competing write races
				// and completes between condition evaluation and the commit.
				// Both are valid outcomes under concurrent load.
				requireConditionalWriteFailure(t, err)
				failCount++
			}
		}
		assert.Equal(t, 1, successCount, "exactly one concurrent IfMatch write should succeed")
		assert.Equal(t, concurrentWorkers-1, failCount, "all other writes should fail (412 or 409)")

		// Verify the winner's content was durably written.
		getCtx, getCancel := testContext(t)
		defer getCancel()
		getOut, getErr := testClient.GetObject(getCtx, &s3.GetObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
		})
		require.NoError(t, getErr, "GetObject should succeed after concurrent IfMatch writes")
		defer getOut.Body.Close()
		bodyBytes, readErr := io.ReadAll(getOut.Body)
		require.NoError(t, readErr)
		assert.Contains(t, string(bodyBytes), "writer-",
			"body should be one of the concurrent writers' payloads, got %q", string(bodyBytes))
	})

	t.Run("EmptyBody", func(t *testing.T) {
		key := uniqueKey(t, "empty-body")
		cleanupKey(t, testClient, testBucket, key)

		ctx, cancel := testContext(t)
		defer cancel()

		out, err := testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(testBucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(""),
			IfNoneMatch: aws.String("*"),
		})
		require.NoError(t, err, "PutObject with empty body and IfNoneMatch=* should succeed")
		require.NotNil(t, out.ETag)
		assert.NotEmpty(t, *out.ETag)
	})

	t.Run("LargeObject", func(t *testing.T) {
		key := uniqueKey(t, "large-object")
		cleanupKey(t, testClient, testBucket, key)

		// Create a 10 MB body.
		largeBody := strings.Repeat("A", 10*1024*1024)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		out, err := testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(testBucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(largeBody),
			IfNoneMatch: aws.String("*"),
		})
		require.NoError(t, err, "PutObject with 10MB body and IfNoneMatch=* should succeed")
		require.NotNil(t, out.ETag)
		etag := *out.ETag

		// Verify IfMatch works with the large object's ETag.
		out2, err := testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:  aws.String(testBucket),
			Key:     aws.String(key),
			Body:    strings.NewReader("replaced"),
			IfMatch: aws.String(etag),
		})
		require.NoError(t, err, "PutObject with IfMatch on large object should succeed")
		require.NotNil(t, out2.ETag)
	})

	t.Run("SpecialCharsInKey", func(t *testing.T) {
		keys := []struct {
			name string
			key  string
		}{
			{"spaces", "test/special chars/file (1).txt"},
			{"unicode", "test/unicode/caf\u00e9.txt"},
			{"slashes", "test/deep/nested/path/to/file.txt"},
			{"plus_sign", "test/plus+sign/file.txt"},
			{"ampersand", "test/ampersand&file.txt"},
		}

		for _, tc := range keys {
			t.Run(tc.name, func(t *testing.T) {
				fullKey := uniqueKey(t, tc.key)
				cleanupKey(t, testClient, testBucket, fullKey)

				ctx, cancel := testContext(t)
				defer cancel()

				out, err := testClient.PutObject(ctx, &s3.PutObjectInput{
					Bucket:      aws.String(testBucket),
					Key:         aws.String(fullKey),
					Body:        strings.NewReader("special-char-content"),
					IfNoneMatch: aws.String("*"),
				})
				require.NoError(t, err, "PutObject with IfNoneMatch=* and special chars in key %q should succeed", tc.key)
				require.NotNil(t, out.ETag)
			})
		}
	})

	t.Run("ETagRoundTrip", func(t *testing.T) {
		key := uniqueKey(t, "etag-roundtrip")
		cleanupKey(t, testClient, testBucket, key)

		ctx, cancel := testContext(t)
		defer cancel()

		// Step 1: PutObject.
		putOut, err := testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("version1"),
		})
		require.NoError(t, err)
		etag1 := *putOut.ETag

		// Step 2: HeadObject with IfMatch using etag1.
		headOut, err := testClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket:  aws.String(testBucket),
			Key:     aws.String(key),
			IfMatch: aws.String(etag1),
		})
		require.NoError(t, err, "HeadObject with IfMatch=etag1 should succeed")
		assert.Equal(t, etag1, *headOut.ETag, "HeadObject should return the same ETag")

		// Step 3: PutObject with IfMatch using etag1 to update.
		putOut2, err := testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:  aws.String(testBucket),
			Key:     aws.String(key),
			Body:    strings.NewReader("version2"),
			IfMatch: aws.String(etag1),
		})
		require.NoError(t, err, "PutObject with IfMatch=etag1 should succeed")
		etag2 := *putOut2.ETag
		assert.NotEqual(t, etag1, etag2)

		// Step 4: GetObject with IfMatch using etag2.
		getOut, err := testClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket:  aws.String(testBucket),
			Key:     aws.String(key),
			IfMatch: aws.String(etag2),
		})
		require.NoError(t, err, "GetObject with IfMatch=etag2 should succeed")
		defer getOut.Body.Close()

		body, err := io.ReadAll(getOut.Body)
		require.NoError(t, err)
		assert.Equal(t, "version2", string(body))
	})

	t.Run("InProgressMultipartInvisibleToIfNoneMatch", func(t *testing.T) {
		key := uniqueKey(t, "mpu-invisible")
		cleanupKey(t, testClient, testBucket, key)

		ctx, cancel := testContext(t)
		defer cancel()

		// Start a multipart upload but leave it incomplete (never call Complete).
		createOut, err := testClient.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "CreateMultipartUpload should succeed")
		uploadID := createOut.UploadId

		// Abort the incomplete upload at test teardown to avoid orphaned parts.
		t.Cleanup(func() {
			abortCtx, abortCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer abortCancel()
			//nolint:errcheck // Best-effort abort; the object may have been replaced.
			testClient.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(testBucket),
				Key:      aws.String(key),
				UploadId: uploadID,
			})
		})

		// While the multipart upload is in progress, no completed object exists
		// at the key. A PutObject with If-None-Match=* should therefore succeed
		// because in-progress multipart uploads are invisible to conditional
		// write evaluation (documented AWS S3 behaviour).
		putOut, err := testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(testBucket),
			Key:         aws.String(key),
			Body:        strings.NewReader("parallel-put"),
			IfNoneMatch: aws.String("*"),
		})
		require.NoError(t, err,
			"PutObject with If-None-Match=* should succeed while a multipart upload is in progress "+
				"(in-progress uploads are invisible to conditional write checks)")
		require.NotNil(t, putOut.ETag)
	})

	t.Run("IfNoneMatchAndIfMatchMutualExclusion", func(t *testing.T) {
		key := uniqueKey(t, "mutual-exclusion")
		cleanupKey(t, testClient, testBucket, key)

		etag := putObject(t, testClient, testBucket, key, "original")

		ctx, cancel := testContext(t)
		defer cancel()

		// Setting both IfNoneMatch=* and IfMatch simultaneously is logically
		// contradictory: IfNoneMatch requires the key to NOT exist while IfMatch
		// requires the key to exist with a specific ETag. At least one condition
		// must always fail. AWS S3 returns 501 Not Implemented for this
		// combination because it never implemented handling for both headers
		// together — other implementations may return 400 Bad Request or 412.
		_, err := testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(testBucket),
			Key:         aws.String(key),
			Body:        strings.NewReader("both-headers"),
			IfNoneMatch: aws.String("*"),
			IfMatch:     aws.String(etag),
		})
		require.Error(t, err, "PutObject with both IfNoneMatch=* and IfMatch should be rejected")
		t.Logf("mutual exclusion error (expected): %v", err)
	})
}
