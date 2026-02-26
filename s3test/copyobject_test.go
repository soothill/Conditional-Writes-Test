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
			// AWS S3 returns 404 NoSuchKey when IfMatch is used against a
			// destination key that does not exist (no ETag to compare). Other
			// implementations may return 412 Precondition Failed instead.
			requireIfMatchKeyMissing(t, err)
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

			// When CopySourceIfNoneMatch matches the source ETag the copy should
			// fail. Unlike If-None-Match on GET/HEAD (which returns 304), S3
			// returns 412 Precondition Failed for all CopyObject conditional
			// failures regardless of which header triggered them.
			_, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:                aws.String(testBucket),
				Key:                   aws.String(dstKey),
				CopySource:            aws.String(copySource(testBucket, srcKey)),
				CopySourceIfNoneMatch: aws.String(srcETag),
			})
			requirePreconditionFailed(t, err)
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

	t.Run("CopySourceIfModifiedSince", func(t *testing.T) {
		t.Run("Modified", func(t *testing.T) {
			srcKey := uniqueKey(t, "copysrc-ifmodified-src-yes")
			dstKey := uniqueKey(t, "copysrc-ifmodified-dst-yes")
			cleanupKey(t, testClient, testBucket, srcKey)
			cleanupKey(t, testClient, testBucket, dstKey)

			putObject(t, testClient, testBucket, srcKey, "source-content")

			ctx, cancel := testContext(t)
			defer cancel()

			// Source was created (modified) well after 2020-01-01, so the copy
			// should proceed because the source has been modified since then.
			pastTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			out, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:                   aws.String(testBucket),
				Key:                      aws.String(dstKey),
				CopySource:               aws.String(copySource(testBucket, srcKey)),
				CopySourceIfModifiedSince: aws.Time(pastTime),
			})
			require.NoError(t, err, "CopyObject with CopySourceIfModifiedSince in the past should succeed")
			require.NotNil(t, out.CopyObjectResult)
		})

		t.Run("NotModified", func(t *testing.T) {
			srcKey := uniqueKey(t, "copysrc-ifmodified-src-no")
			dstKey := uniqueKey(t, "copysrc-ifmodified-dst-no")
			cleanupKey(t, testClient, testBucket, srcKey)
			cleanupKey(t, testClient, testBucket, dstKey)

			putObject(t, testClient, testBucket, srcKey, "source-content")

			// AWS S3 evaluates CopySourceIfModifiedSince at 1-second HTTP date
			// granularity. We must wait until the current second rolls over so
			// that the timestamp we pass is strictly after the source object's
			// LastModified, ensuring the "not modified since" condition is true.
			nextSecond := time.Now().Truncate(time.Second).Add(time.Second)
			time.Sleep(time.Until(nextSecond) + 50*time.Millisecond)
			afterCreate := time.Now()

			ctx, cancel := testContext(t)
			defer cancel()

			// Source has not been modified since afterCreate → copy should fail.
			_, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:                   aws.String(testBucket),
				Key:                      aws.String(dstKey),
				CopySource:               aws.String(copySource(testBucket, srcKey)),
				CopySourceIfModifiedSince: aws.Time(afterCreate),
			})
			requirePreconditionFailed(t, err)
		})
	})

	t.Run("CopySourceIfUnmodifiedSince", func(t *testing.T) {
		t.Run("Unmodified", func(t *testing.T) {
			srcKey := uniqueKey(t, "copysrc-ifunmodified-src-yes")
			dstKey := uniqueKey(t, "copysrc-ifunmodified-dst-yes")
			cleanupKey(t, testClient, testBucket, srcKey)
			cleanupKey(t, testClient, testBucket, dstKey)

			putObject(t, testClient, testBucket, srcKey, "source-content")

			ctx, cancel := testContext(t)
			defer cancel()

			// Use a timestamp in the future: the source has not been modified
			// since then, so the copy condition is satisfied.
			futureTime := time.Now().Add(24 * time.Hour)
			out, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:                     aws.String(testBucket),
				Key:                        aws.String(dstKey),
				CopySource:                 aws.String(copySource(testBucket, srcKey)),
				CopySourceIfUnmodifiedSince: aws.Time(futureTime),
			})
			require.NoError(t, err, "CopyObject with CopySourceIfUnmodifiedSince in the future should succeed")
			require.NotNil(t, out.CopyObjectResult)
		})

		t.Run("Modified", func(t *testing.T) {
			srcKey := uniqueKey(t, "copysrc-ifunmodified-src-no")
			dstKey := uniqueKey(t, "copysrc-ifunmodified-dst-no")
			cleanupKey(t, testClient, testBucket, srcKey)
			cleanupKey(t, testClient, testBucket, dstKey)

			putObject(t, testClient, testBucket, srcKey, "source-content")

			ctx, cancel := testContext(t)
			defer cancel()

			// Source was created (modified) well after 2020-01-01, so it has
			// been modified since that time → copy condition is not satisfied.
			pastTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			_, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:                     aws.String(testBucket),
				Key:                        aws.String(dstKey),
				CopySource:                 aws.String(copySource(testBucket, srcKey)),
				CopySourceIfUnmodifiedSince: aws.Time(pastTime),
			})
			requirePreconditionFailed(t, err)
		})
	})
}
