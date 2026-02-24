//go:build integration

package s3test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyObjectConditionalWrites(t *testing.T) {
	t.Run("IfNoneMatch", func(t *testing.T) {
		t.Run("NewDestination", func(t *testing.T) {
			srcKey := uniqueKey(t, "copy-ifnonematch-src")
			dstKey := uniqueKey(t, "copy-ifnonematch-dst-new")
			cleanupKey(t, testClient, testBucket, srcKey)
			cleanupKey(t, testClient, testBucket, dstKey)

			putObject(t, testClient, testBucket, srcKey, "source-content")

			ctx, cancel := testContext(t)
			defer cancel()

			out, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:      aws.String(testBucket),
				Key:         aws.String(dstKey),
				CopySource:  aws.String(copySource(testBucket, srcKey)),
				IfNoneMatch: aws.String("*"),
			})
			require.NoError(t, err, "CopyObject with IfNoneMatch=* to new destination should succeed")
			require.NotNil(t, out.CopyObjectResult)
			assert.NotEmpty(t, *out.CopyObjectResult.ETag)
		})

		t.Run("ExistingDestination", func(t *testing.T) {
			srcKey := uniqueKey(t, "copy-ifnonematch-src")
			dstKey := uniqueKey(t, "copy-ifnonematch-dst-existing")
			cleanupKey(t, testClient, testBucket, srcKey)
			cleanupKey(t, testClient, testBucket, dstKey)

			putObject(t, testClient, testBucket, srcKey, "source-content")
			putObject(t, testClient, testBucket, dstKey, "existing-dest")

			ctx, cancel := testContext(t)
			defer cancel()

			_, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:      aws.String(testBucket),
				Key:         aws.String(dstKey),
				CopySource:  aws.String(copySource(testBucket, srcKey)),
				IfNoneMatch: aws.String("*"),
			})
			requirePreconditionFailed(t, err)
		})

		t.Run("AfterDelete", func(t *testing.T) {
			srcKey := uniqueKey(t, "copy-ifnonematch-src")
			dstKey := uniqueKey(t, "copy-ifnonematch-dst-afterdel")
			cleanupKey(t, testClient, testBucket, srcKey)
			cleanupKey(t, testClient, testBucket, dstKey)

			putObject(t, testClient, testBucket, srcKey, "source-content")
			putObject(t, testClient, testBucket, dstKey, "to-be-deleted")
			deleteObject(t, testClient, testBucket, dstKey)

			ctx, cancel := testContext(t)
			defer cancel()

			out, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:      aws.String(testBucket),
				Key:         aws.String(dstKey),
				CopySource:  aws.String(copySource(testBucket, srcKey)),
				IfNoneMatch: aws.String("*"),
			})
			require.NoError(t, err, "CopyObject with IfNoneMatch=* after delete should succeed")
			require.NotNil(t, out.CopyObjectResult)
		})
	})

	t.Run("IfMatch", func(t *testing.T) {
		t.Run("CorrectETag", func(t *testing.T) {
			srcKey := uniqueKey(t, "copy-ifmatch-src")
			dstKey := uniqueKey(t, "copy-ifmatch-dst-correct")
			cleanupKey(t, testClient, testBucket, srcKey)
			cleanupKey(t, testClient, testBucket, dstKey)

			putObject(t, testClient, testBucket, srcKey, "source-content")
			dstETag := putObject(t, testClient, testBucket, dstKey, "dest-original")

			ctx, cancel := testContext(t)
			defer cancel()

			out, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:     aws.String(testBucket),
				Key:        aws.String(dstKey),
				CopySource: aws.String(copySource(testBucket, srcKey)),
				IfMatch:    aws.String(dstETag),
			})
			require.NoError(t, err, "CopyObject with correct IfMatch ETag should succeed")
			require.NotNil(t, out.CopyObjectResult)
			assert.NotEqual(t, dstETag, *out.CopyObjectResult.ETag)
		})

		t.Run("WrongETag", func(t *testing.T) {
			srcKey := uniqueKey(t, "copy-ifmatch-src")
			dstKey := uniqueKey(t, "copy-ifmatch-dst-wrong")
			cleanupKey(t, testClient, testBucket, srcKey)
			cleanupKey(t, testClient, testBucket, dstKey)

			putObject(t, testClient, testBucket, srcKey, "source-content")
			putObject(t, testClient, testBucket, dstKey, "dest-original")

			ctx, cancel := testContext(t)
			defer cancel()

			_, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:     aws.String(testBucket),
				Key:        aws.String(dstKey),
				CopySource: aws.String(copySource(testBucket, srcKey)),
				IfMatch:    aws.String(wrongETag),
			})
			requirePreconditionFailed(t, err)
		})

		t.Run("NonExistentDestination", func(t *testing.T) {
			srcKey := uniqueKey(t, "copy-ifmatch-src")
			dstKey := uniqueKey(t, "copy-ifmatch-dst-nonexistent")
			cleanupKey(t, testClient, testBucket, srcKey)

			putObject(t, testClient, testBucket, srcKey, "source-content")

			ctx, cancel := testContext(t)
			defer cancel()

			_, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:     aws.String(testBucket),
				Key:        aws.String(dstKey),
				CopySource: aws.String(copySource(testBucket, srcKey)),
				IfMatch:    aws.String(emptyObjectETag),
			})
			requirePreconditionFailed(t, err)
		})
	})

	t.Run("CopySourceIfMatch", func(t *testing.T) {
		t.Run("CorrectETag", func(t *testing.T) {
			srcKey := uniqueKey(t, "copysrc-ifmatch-src")
			dstKey := uniqueKey(t, "copysrc-ifmatch-dst")
			cleanupKey(t, testClient, testBucket, srcKey)
			cleanupKey(t, testClient, testBucket, dstKey)

			srcETag := putObject(t, testClient, testBucket, srcKey, "source-content")

			ctx, cancel := testContext(t)
			defer cancel()

			out, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:            aws.String(testBucket),
				Key:               aws.String(dstKey),
				CopySource:        aws.String(copySource(testBucket, srcKey)),
				CopySourceIfMatch: aws.String(srcETag),
			})
			require.NoError(t, err, "CopyObject with correct CopySourceIfMatch should succeed")
			require.NotNil(t, out.CopyObjectResult)
		})

		t.Run("WrongETag", func(t *testing.T) {
			srcKey := uniqueKey(t, "copysrc-ifmatch-src-wrong")
			dstKey := uniqueKey(t, "copysrc-ifmatch-dst-wrong")
			cleanupKey(t, testClient, testBucket, srcKey)
			cleanupKey(t, testClient, testBucket, dstKey)

			putObject(t, testClient, testBucket, srcKey, "source-content")

			ctx, cancel := testContext(t)
			defer cancel()

			_, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:            aws.String(testBucket),
				Key:               aws.String(dstKey),
				CopySource:        aws.String(copySource(testBucket, srcKey)),
				CopySourceIfMatch: aws.String(wrongETag),
			})
			requirePreconditionFailed(t, err)
		})
	})

	t.Run("CopySourceIfNoneMatch", func(t *testing.T) {
		t.Run("MatchingETag", func(t *testing.T) {
			srcKey := uniqueKey(t, "copysrc-ifnonematch-src-match")
			dstKey := uniqueKey(t, "copysrc-ifnonematch-dst-match")
			cleanupKey(t, testClient, testBucket, srcKey)
			cleanupKey(t, testClient, testBucket, dstKey)

			srcETag := putObject(t, testClient, testBucket, srcKey, "source-content")

			ctx, cancel := testContext(t)
			defer cancel()

			// When CopySourceIfNoneMatch matches the source ETag, the copy should fail.
			_, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:                aws.String(testBucket),
				Key:                   aws.String(dstKey),
				CopySource:            aws.String(copySource(testBucket, srcKey)),
				CopySourceIfNoneMatch: aws.String(srcETag),
			})
			// S3 returns 304 Not Modified for matching CopySourceIfNoneMatch.
			requireNotModified(t, err)
		})

		t.Run("DifferentETag", func(t *testing.T) {
			srcKey := uniqueKey(t, "copysrc-ifnonematch-src-diff")
			dstKey := uniqueKey(t, "copysrc-ifnonematch-dst-diff")
			cleanupKey(t, testClient, testBucket, srcKey)
			cleanupKey(t, testClient, testBucket, dstKey)

			putObject(t, testClient, testBucket, srcKey, "source-content")

			ctx, cancel := testContext(t)
			defer cancel()

			out, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:                aws.String(testBucket),
				Key:                   aws.String(dstKey),
				CopySource:            aws.String(copySource(testBucket, srcKey)),
				CopySourceIfNoneMatch: aws.String(wrongETag),
			})
			require.NoError(t, err, "CopyObject with non-matching CopySourceIfNoneMatch should succeed")
			require.NotNil(t, out.CopyObjectResult)
		})
	})
}
