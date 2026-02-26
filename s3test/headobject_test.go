//go:build integration

package s3test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeadObjectConditionalReads(t *testing.T) {
	t.Run("IfMatch", func(t *testing.T) {
		t.Run("CorrectETag", func(t *testing.T) {
			key := uniqueKey(t, "head-ifmatch-correct")
			cleanupKey(t, testClient, testBucket, key)

			etag := putObject(t, testClient, testBucket, key, "head-me")

			ctx, cancel := testContext(t)
			defer cancel()

			out, err := testClient.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket:  aws.String(testBucket),
				Key:     aws.String(key),
				IfMatch: aws.String(etag),
			})
			require.NoError(t, err, "HeadObject with correct IfMatch should succeed")
			require.NotNil(t, out.ETag)
			assert.Equal(t, etag, *out.ETag)
		})

		t.Run("WrongETag", func(t *testing.T) {
			key := uniqueKey(t, "head-ifmatch-wrong")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "head-me")

			ctx, cancel := testContext(t)
			defer cancel()

			_, err := testClient.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket:  aws.String(testBucket),
				Key:     aws.String(key),
				IfMatch: aws.String(wrongETag),
			})
			requirePreconditionFailed(t, err)
		})
	})

	t.Run("IfNoneMatch", func(t *testing.T) {
		t.Run("MatchingETag", func(t *testing.T) {
			key := uniqueKey(t, "head-ifnonematch-match")
			cleanupKey(t, testClient, testBucket, key)

			etag := putObject(t, testClient, testBucket, key, "head-me")

			ctx, cancel := testContext(t)
			defer cancel()

			_, err := testClient.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket:      aws.String(testBucket),
				Key:         aws.String(key),
				IfNoneMatch: aws.String(etag),
			})
			requireNotModified(t, err)
		})

		t.Run("DifferentETag", func(t *testing.T) {
			key := uniqueKey(t, "head-ifnonematch-diff")
			cleanupKey(t, testClient, testBucket, key)

			etag := putObject(t, testClient, testBucket, key, "head-me")

			ctx, cancel := testContext(t)
			defer cancel()

			out, err := testClient.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket:      aws.String(testBucket),
				Key:         aws.String(key),
				IfNoneMatch: aws.String(wrongETag),
			})
			require.NoError(t, err, "HeadObject with non-matching IfNoneMatch should succeed")
			require.NotNil(t, out.ETag)
			assert.Equal(t, etag, *out.ETag)
		})

		t.Run("Wildcard", func(t *testing.T) {
			key := uniqueKey(t, "head-ifnonematch-wildcard")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "head-me")

			ctx, cancel := testContext(t)
			defer cancel()

			// RFC 7232: If-None-Match: * means "the condition is false if any
			// representation of the resource currently exists". Since the object
			// exists, S3 should return 304 Not Modified.
			_, err := testClient.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket:      aws.String(testBucket),
				Key:         aws.String(key),
				IfNoneMatch: aws.String("*"),
			})
			requireNotModified(t, err)
		})
	})

	t.Run("IfModifiedSince", func(t *testing.T) {
		t.Run("Modified", func(t *testing.T) {
			key := uniqueKey(t, "head-ifmodified-yes")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "head-me")

			ctx, cancel := testContext(t)
			defer cancel()

			pastTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			out, err := testClient.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket:          aws.String(testBucket),
				Key:             aws.String(key),
				IfModifiedSince: aws.Time(pastTime),
			})
			require.NoError(t, err, "HeadObject with IfModifiedSince in the past should succeed")
			require.NotNil(t, out.ETag)
			require.NotNil(t, out.LastModified)
		})

		t.Run("NotModified", func(t *testing.T) {
			key := uniqueKey(t, "head-ifmodified-no")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "head-me")

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

			_, err := testClient.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket:          aws.String(testBucket),
				Key:             aws.String(key),
				IfModifiedSince: aws.Time(afterCreate),
			})
			requireNotModified(t, err)
		})
	})

	t.Run("IfUnmodifiedSince", func(t *testing.T) {
		t.Run("Unmodified", func(t *testing.T) {
			key := uniqueKey(t, "head-ifunmodified-yes")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "head-me")

			ctx, cancel := testContext(t)
			defer cancel()

			futureTime := time.Now().Add(24 * time.Hour)
			out, err := testClient.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket:            aws.String(testBucket),
				Key:               aws.String(key),
				IfUnmodifiedSince: aws.Time(futureTime),
			})
			require.NoError(t, err, "HeadObject with IfUnmodifiedSince in the future should succeed")
			require.NotNil(t, out.ETag)
		})

		t.Run("Modified", func(t *testing.T) {
			key := uniqueKey(t, "head-ifunmodified-no")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "head-me")

			ctx, cancel := testContext(t)
			defer cancel()

			pastTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			_, err := testClient.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket:            aws.String(testBucket),
				Key:               aws.String(key),
				IfUnmodifiedSince: aws.Time(pastTime),
			})
			requirePreconditionFailed(t, err)
		})
	})
}
