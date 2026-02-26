//go:build integration

package s3test

import (
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetObjectConditionalReads(t *testing.T) {
	t.Run("IfMatch", func(t *testing.T) {
		t.Run("CorrectETag", func(t *testing.T) {
			key, etag := putKeyForTest(t, "get-ifmatch-correct", "get-me")
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
			key, _ := putKeyForTest(t, "get-ifmatch-wrong", "get-me")
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
			key, etag := putKeyForTest(t, "get-ifnonematch-match", "get-me")
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
			key, _ := putKeyForTest(t, "get-ifnonematch-diff", "get-me")
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

		t.Run("Wildcard", func(t *testing.T) {
			key, _ := putKeyForTest(t, "get-ifnonematch-wildcard", "get-me")
			ctx, cancel := testContext(t)
			defer cancel()

			// RFC 7232: If-None-Match: * means "the condition is false if any
			// representation of the resource currently exists". Since the object
			// exists, S3 should return 304 Not Modified.
			_, err := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket:      aws.String(testBucket),
				Key:         aws.String(key),
				IfNoneMatch: aws.String("*"),
			})
			requireNotModified(t, err)
		})
	})

	t.Run("IfModifiedSince", func(t *testing.T) {
		t.Run("Modified", func(t *testing.T) {
			key, _ := putKeyForTest(t, "get-ifmodified-yes", "get-me")
			ctx, cancel := testContext(t)
			defer cancel()

			// Object was modified well after wellPastTime(), so the condition passes.
			out, err := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket:          aws.String(testBucket),
				Key:             aws.String(key),
				IfModifiedSince: aws.Time(wellPastTime()),
			})
			require.NoError(t, err, "GetObject with IfModifiedSince in the past should succeed")
			defer out.Body.Close()

			body, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, "get-me", string(body))
		})

		t.Run("NotModified", func(t *testing.T) {
			key, _ := putKeyForTest(t, "get-ifmodified-no", "get-me")

			// AWS S3 evaluates If-Modified-Since with a strict greater-than
			// comparison at 1-second HTTP date granularity. If-Modified-Since
			// must be in a strictly later second than the object's LastModified
			// to produce a 304. Using a future timestamp (e.g. +24h) causes
			// AWS to silently ignore the header and return 200. We therefore
			// advance past the current second boundary before capturing the
			// timestamp used as IfModifiedSince.
			afterCreate := waitForNextSecond()

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
			key, _ := putKeyForTest(t, "get-ifunmodified-yes", "get-me")
			ctx, cancel := testContext(t)
			defer cancel()

			// Object has not been modified since wellFutureTime(), so the condition passes.
			out, err := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket:            aws.String(testBucket),
				Key:               aws.String(key),
				IfUnmodifiedSince: aws.Time(wellFutureTime()),
			})
			require.NoError(t, err, "GetObject with IfUnmodifiedSince in the future should succeed")
			defer out.Body.Close()

			body, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, "get-me", string(body))
		})

		t.Run("Modified", func(t *testing.T) {
			key, _ := putKeyForTest(t, "get-ifunmodified-no", "get-me")
			ctx, cancel := testContext(t)
			defer cancel()

			// Object was modified after wellPastTime(), so the unmodified condition fails.
			_, err := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket:            aws.String(testBucket),
				Key:               aws.String(key),
				IfUnmodifiedSince: aws.Time(wellPastTime()),
			})
			requirePreconditionFailed(t, err)
		})
	})
}
