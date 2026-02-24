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

			ctx, cancel := testContext(t)
			defer cancel()

			futureTime := time.Now().Add(24 * time.Hour)
			_, err := testClient.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket:          aws.String(testBucket),
				Key:             aws.String(key),
				IfModifiedSince: aws.Time(futureTime),
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
