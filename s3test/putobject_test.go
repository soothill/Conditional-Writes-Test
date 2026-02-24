//go:build integration

package s3test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutObjectConditionalWrites(t *testing.T) {
	t.Run("IfNoneMatch", func(t *testing.T) {
		t.Run("NewKey", func(t *testing.T) {
			key := uniqueKey(t, "put-ifnonematch-new")
			cleanupKey(t, testClient, testBucket, key)

			ctx, cancel := testContext(t)
			defer cancel()

			out, err := testClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      aws.String(testBucket),
				Key:         aws.String(key),
				Body:        strings.NewReader("hello"),
				IfNoneMatch: aws.String("*"),
			})
			require.NoError(t, err, "PutObject with IfNoneMatch=* to new key should succeed")
			require.NotNil(t, out.ETag, "response should include ETag")
			assert.NotEmpty(t, *out.ETag, "ETag should not be empty")
		})

		t.Run("ExistingKey", func(t *testing.T) {
			key := uniqueKey(t, "put-ifnonematch-existing")
			cleanupKey(t, testClient, testBucket, key)

			// Create the object first.
			putObject(t, testClient, testBucket, key, "original")

			ctx, cancel := testContext(t)
			defer cancel()

			_, err := testClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      aws.String(testBucket),
				Key:         aws.String(key),
				Body:        strings.NewReader("overwrite-attempt"),
				IfNoneMatch: aws.String("*"),
			})
			requirePreconditionFailed(t, err)
		})

		t.Run("AfterDelete", func(t *testing.T) {
			key := uniqueKey(t, "put-ifnonematch-afterdelete")
			cleanupKey(t, testClient, testBucket, key)

			// Create then delete the object.
			putObject(t, testClient, testBucket, key, "temporary")
			deleteObject(t, testClient, testBucket, key)

			ctx, cancel := testContext(t)
			defer cancel()

			out, err := testClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      aws.String(testBucket),
				Key:         aws.String(key),
				Body:        strings.NewReader("after-delete"),
				IfNoneMatch: aws.String("*"),
			})
			require.NoError(t, err, "PutObject with IfNoneMatch=* after delete should succeed")
			require.NotNil(t, out.ETag)
		})
	})

	t.Run("IfMatch", func(t *testing.T) {
		t.Run("CorrectETag", func(t *testing.T) {
			key := uniqueKey(t, "put-ifmatch-correct")
			cleanupKey(t, testClient, testBucket, key)

			// Create object and capture ETag.
			etag := putObject(t, testClient, testBucket, key, "original")

			ctx, cancel := testContext(t)
			defer cancel()

			out, err := testClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket:  aws.String(testBucket),
				Key:     aws.String(key),
				Body:    strings.NewReader("updated"),
				IfMatch: aws.String(etag),
			})
			require.NoError(t, err, "PutObject with correct IfMatch ETag should succeed")
			require.NotNil(t, out.ETag)
			assert.NotEqual(t, etag, *out.ETag, "new ETag should differ from original")
		})

		t.Run("WrongETag", func(t *testing.T) {
			key := uniqueKey(t, "put-ifmatch-wrong")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "original")

			ctx, cancel := testContext(t)
			defer cancel()

			_, err := testClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket:  aws.String(testBucket),
				Key:     aws.String(key),
				Body:    strings.NewReader("overwrite-attempt"),
				IfMatch: aws.String(wrongETag),
			})
			requirePreconditionFailed(t, err)
		})

		t.Run("NonExistentKey", func(t *testing.T) {
			key := uniqueKey(t, "put-ifmatch-nonexistent")
			// No cleanup needed; object should not be created.

			ctx, cancel := testContext(t)
			defer cancel()

			_, err := testClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket:  aws.String(testBucket),
				Key:     aws.String(key),
				Body:    strings.NewReader("should-not-exist"),
				IfMatch: aws.String(emptyObjectETag),
			})
			requirePreconditionFailed(t, err)
		})

		t.Run("StaleETag", func(t *testing.T) {
			key := uniqueKey(t, "put-ifmatch-stale")
			cleanupKey(t, testClient, testBucket, key)

			// Create object, capture original ETag.
			originalETag := putObject(t, testClient, testBucket, key, "version1")

			// Overwrite to get a new ETag; original is now stale.
			putObject(t, testClient, testBucket, key, "version2")

			ctx, cancel := testContext(t)
			defer cancel()

			_, err := testClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket:  aws.String(testBucket),
				Key:     aws.String(key),
				Body:    strings.NewReader("version3-attempt"),
				IfMatch: aws.String(originalETag),
			})
			requirePreconditionFailed(t, err)
		})

		t.Run("ChainedUpdates", func(t *testing.T) {
			key := uniqueKey(t, "put-ifmatch-chained")
			cleanupKey(t, testClient, testBucket, key)

			// Put version 1.
			etag1 := putObject(t, testClient, testBucket, key, "version1")

			ctx, cancel := testContext(t)
			defer cancel()

			// Update to version 2 using etag1.
			out2, err := testClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket:  aws.String(testBucket),
				Key:     aws.String(key),
				Body:    strings.NewReader("version2"),
				IfMatch: aws.String(etag1),
			})
			require.NoError(t, err, "chained update 1->2 should succeed")
			require.NotNil(t, out2.ETag)
			etag2 := *out2.ETag

			// Update to version 3 using etag2.
			out3, err := testClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket:  aws.String(testBucket),
				Key:     aws.String(key),
				Body:    strings.NewReader("version3"),
				IfMatch: aws.String(etag2),
			})
			require.NoError(t, err, "chained update 2->3 should succeed")
			require.NotNil(t, out3.ETag)
			etag3 := *out3.ETag

			// All three ETags should be different.
			assert.NotEqual(t, etag1, etag2, "etag1 and etag2 should differ")
			assert.NotEqual(t, etag2, etag3, "etag2 and etag3 should differ")
			assert.NotEqual(t, etag1, etag3, "etag1 and etag3 should differ")
		})
	})
}
