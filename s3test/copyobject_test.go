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
			srcKey, _ := putKeyForTest(t, "copy-ifnonematch-src", "source-content")
			dstKey := setupKey(t, "copy-ifnonematch-dst-new")

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
			srcKey, _ := putKeyForTest(t, "copy-ifnonematch-src", "source-content")
			dstKey, _ := putKeyForTest(t, "copy-ifnonematch-dst-existing", "existing-dest")

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
			srcKey, _ := putKeyForTest(t, "copy-ifnonematch-src", "source-content")
			dstKey, _ := putKeyForTest(t, "copy-ifnonematch-dst-afterdel", "to-be-deleted")
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
			srcKey, _ := putKeyForTest(t, "copy-ifmatch-src", "source-content")
			dstKey, dstETag := putKeyForTest(t, "copy-ifmatch-dst-correct", "dest-original")

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
			srcKey, _ := putKeyForTest(t, "copy-ifmatch-src", "source-content")
			dstKey, _ := putKeyForTest(t, "copy-ifmatch-dst-wrong", "dest-original")

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
			srcKey, _ := putKeyForTest(t, "copy-ifmatch-src", "source-content")
			// setupKey registers cleanup; deleteObject is a no-op if the
			// object is never created, so cleanup is always safe.
			dstKey := setupKey(t, "copy-ifmatch-dst-nonexistent")

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
			srcKey, srcETag := putKeyForTest(t, "copysrc-ifmatch-src", "source-content")
			dstKey := setupKey(t, "copysrc-ifmatch-dst")

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
			srcKey, _ := putKeyForTest(t, "copysrc-ifmatch-src-wrong", "source-content")
			dstKey := setupKey(t, "copysrc-ifmatch-dst-wrong")

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
			srcKey, srcETag := putKeyForTest(t, "copysrc-ifnonematch-src-match", "source-content")
			dstKey := setupKey(t, "copysrc-ifnonematch-dst-match")

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
			srcKey, _ := putKeyForTest(t, "copysrc-ifnonematch-src-diff", "source-content")
			dstKey := setupKey(t, "copysrc-ifnonematch-dst-diff")

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
			srcKey, _ := putKeyForTest(t, "copysrc-ifmodified-src-yes", "source-content")
			dstKey := setupKey(t, "copysrc-ifmodified-dst-yes")

			ctx, cancel := testContext(t)
			defer cancel()

			// Source was modified well after wellPastTime(), so the copy proceeds.
			out, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:                    aws.String(testBucket),
				Key:                       aws.String(dstKey),
				CopySource:                aws.String(copySource(testBucket, srcKey)),
				CopySourceIfModifiedSince: aws.Time(wellPastTime()),
			})
			require.NoError(t, err, "CopyObject with CopySourceIfModifiedSince in the past should succeed")
			require.NotNil(t, out.CopyObjectResult)
		})

		t.Run("NotModified", func(t *testing.T) {
			srcKey, _ := putKeyForTest(t, "copysrc-ifmodified-src-no", "source-content")
			dstKey := setupKey(t, "copysrc-ifmodified-dst-no")

			// AWS S3 evaluates CopySourceIfModifiedSince at 1-second HTTP date
			// granularity. We must wait until the current second rolls over so
			// that the timestamp we pass is strictly after the source object's
			// LastModified, ensuring the "not modified since" condition is true.
			afterCreate := waitForNextSecond()

			ctx, cancel := testContext(t)
			defer cancel()

			// Source has not been modified since afterCreate → copy should fail.
			_, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:                    aws.String(testBucket),
				Key:                       aws.String(dstKey),
				CopySource:                aws.String(copySource(testBucket, srcKey)),
				CopySourceIfModifiedSince: aws.Time(afterCreate),
			})
			requirePreconditionFailed(t, err)
		})
	})

	t.Run("CopySourceIfUnmodifiedSince", func(t *testing.T) {
		t.Run("Unmodified", func(t *testing.T) {
			srcKey, _ := putKeyForTest(t, "copysrc-ifunmodified-src-yes", "source-content")
			dstKey := setupKey(t, "copysrc-ifunmodified-dst-yes")

			ctx, cancel := testContext(t)
			defer cancel()

			// Source has not been modified since wellFutureTime(), so the copy proceeds.
			out, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:                      aws.String(testBucket),
				Key:                         aws.String(dstKey),
				CopySource:                  aws.String(copySource(testBucket, srcKey)),
				CopySourceIfUnmodifiedSince: aws.Time(wellFutureTime()),
			})
			require.NoError(t, err, "CopyObject with CopySourceIfUnmodifiedSince in the future should succeed")
			require.NotNil(t, out.CopyObjectResult)
		})

		t.Run("Modified", func(t *testing.T) {
			srcKey, _ := putKeyForTest(t, "copysrc-ifunmodified-src-no", "source-content")
			dstKey := setupKey(t, "copysrc-ifunmodified-dst-no")

			ctx, cancel := testContext(t)
			defer cancel()

			// Source was modified after wellPastTime(), so the unmodified condition fails.
			_, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:                      aws.String(testBucket),
				Key:                         aws.String(dstKey),
				CopySource:                  aws.String(copySource(testBucket, srcKey)),
				CopySourceIfUnmodifiedSince: aws.Time(wellPastTime()),
			})
			requirePreconditionFailed(t, err)
		})
	})
}
