//go:build integration

package s3test

import (
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetObjectConditionalReads(t *testing.T) {
	t.Run("IfMatch", func(t *testing.T) {
		t.Run("CorrectETag", func(t *testing.T) {
			key := uniqueKey(t, "get-ifmatch-correct")
			cleanupKey(t, testClient, testBucket, key)

			etag := putObject(t, testClient, testBucket, key, "get-me")

			ctx, cancel := testContext(t)
			defer cancel()

			out, err := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket:  aws.String(testBucket),
				Key:     aws.String(key),
				IfMatch: aws.String(etag),
			})
			require.NoError(t, err, "GetObject with correct IfMatch should succeed")
			defer out.Body.Close()

			body, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, "get-me", string(body))
		})

		t.Run("WrongETag", func(t *testing.T) {
			key := uniqueKey(t, "get-ifmatch-wrong")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "get-me")

			ctx, cancel := testContext(t)
			defer cancel()

			_, err := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket:  aws.String(testBucket),
				Key:     aws.String(key),
				IfMatch: aws.String(wrongETag),
			})
			requirePreconditionFailed(t, err)
		})
	})

	t.Run("IfNoneMatch", func(t *testing.T) {
		t.Run("MatchingETag", func(t *testing.T) {
			key := uniqueKey(t, "get-ifnonematch-match")
			cleanupKey(t, testClient, testBucket, key)

			etag := putObject(t, testClient, testBucket, key, "get-me")

			ctx, cancel := testContext(t)
			defer cancel()

			_, err := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket:      aws.String(testBucket),
				Key:         aws.String(key),
				IfNoneMatch: aws.String(etag),
			})
			requireNotModified(t, err)
		})

		t.Run("DifferentETag", func(t *testing.T) {
			key := uniqueKey(t, "get-ifnonematch-diff")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "get-me")

			ctx, cancel := testContext(t)
			defer cancel()

			out, err := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket:      aws.String(testBucket),
				Key:         aws.String(key),
				IfNoneMatch: aws.String(wrongETag),
			})
			require.NoError(t, err, "GetObject with non-matching IfNoneMatch should succeed")
			defer out.Body.Close()

			body, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, "get-me", string(body))
		})
	})

	t.Run("IfModifiedSince", func(t *testing.T) {
		t.Run("Modified", func(t *testing.T) {
			key := uniqueKey(t, "get-ifmodified-yes")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "get-me")

			ctx, cancel := testContext(t)
			defer cancel()

			// Use a timestamp well in the past; object was modified after this time.
			pastTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			out, err := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket:          aws.String(testBucket),
				Key:             aws.String(key),
				IfModifiedSince: aws.Time(pastTime),
			})
			require.NoError(t, err, "GetObject with IfModifiedSince in the past should succeed")
			defer out.Body.Close()

			body, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, "get-me", string(body))
		})

		t.Run("NotModified", func(t *testing.T) {
			key := uniqueKey(t, "get-ifmodified-no")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "get-me")

			// AWS S3 evaluates If-Modified-Since with a strict greater-than
			// comparison at 1-second HTTP date granularity. If-Modified-Since
			// must be in a strictly later second than the object's LastModified
			// to produce a 304. Using a future timestamp (e.g. +24h) causes
			// AWS to silently ignore the header and return 200. We therefore
			// advance past the current second boundary before capturing the
			// timestamp used as IfModifiedSince.
			nextSecond := time.Now().Truncate(time.Second).Add(time.Second)
			time.Sleep(time.Until(nextSecond) + 50*time.Millisecond)
			afterCreate := time.Now()

			ctx, cancel := testContext(t)
			defer cancel()

			_, err := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket:          aws.String(testBucket),
				Key:             aws.String(key),
				IfModifiedSince: aws.Time(afterCreate),
			})
			requireNotModified(t, err)
		})
	})

	t.Run("IfUnmodifiedSince", func(t *testing.T) {
		t.Run("Unmodified", func(t *testing.T) {
			key := uniqueKey(t, "get-ifunmodified-yes")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "get-me")

			ctx, cancel := testContext(t)
			defer cancel()

			// Use a timestamp in the future; object has not been modified since then.
			futureTime := time.Now().Add(24 * time.Hour)
			out, err := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket:            aws.String(testBucket),
				Key:               aws.String(key),
				IfUnmodifiedSince: aws.Time(futureTime),
			})
			require.NoError(t, err, "GetObject with IfUnmodifiedSince in the future should succeed")
			defer out.Body.Close()

			body, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, "get-me", string(body))
		})

		t.Run("Modified", func(t *testing.T) {
			key := uniqueKey(t, "get-ifunmodified-no")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "get-me")

			ctx, cancel := testContext(t)
			defer cancel()

			// Use a timestamp well in the past; object was modified after this.
			pastTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			_, err := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket:            aws.String(testBucket),
				Key:               aws.String(key),
				IfUnmodifiedSince: aws.Time(pastTime),
			})
			requirePreconditionFailed(t, err)
		})
	})
}
