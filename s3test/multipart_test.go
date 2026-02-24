//go:build integration

package s3test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultipartConditionalWrites(t *testing.T) {
	t.Run("IfNoneMatch", func(t *testing.T) {
		t.Run("NewKey", func(t *testing.T) {
			key := uniqueKey(t, "mpu-ifnonematch-new")
			cleanupKey(t, testClient, testBucket, key)

			out, err := doMultipartUpload(t, testClient, testBucket, key, "multipart-body",
				aws.String("*"), nil)
			require.NoError(t, err, "CompleteMultipartUpload with IfNoneMatch=* to new key should succeed")
			require.NotNil(t, out.ETag, "response should include ETag")
			assert.NotEmpty(t, *out.ETag)
		})

		t.Run("ExistingKey", func(t *testing.T) {
			key := uniqueKey(t, "mpu-ifnonematch-existing")
			cleanupKey(t, testClient, testBucket, key)

			// Create the object first.
			putObject(t, testClient, testBucket, key, "existing-object")

			_, err := doMultipartUpload(t, testClient, testBucket, key, "multipart-overwrite",
				aws.String("*"), nil)
			requirePreconditionFailed(t, err)
		})

		t.Run("AfterDelete", func(t *testing.T) {
			key := uniqueKey(t, "mpu-ifnonematch-afterdelete")
			cleanupKey(t, testClient, testBucket, key)

			// Create then delete the object.
			putObject(t, testClient, testBucket, key, "temporary")
			deleteObject(t, testClient, testBucket, key)

			out, err := doMultipartUpload(t, testClient, testBucket, key, "after-delete",
				aws.String("*"), nil)
			require.NoError(t, err, "CompleteMultipartUpload with IfNoneMatch=* after delete should succeed")
			require.NotNil(t, out.ETag)
		})
	})

	t.Run("IfMatch", func(t *testing.T) {
		t.Run("CorrectETag", func(t *testing.T) {
			key := uniqueKey(t, "mpu-ifmatch-correct")
			cleanupKey(t, testClient, testBucket, key)

			// Create object and capture ETag.
			etag := putObject(t, testClient, testBucket, key, "original")

			out, err := doMultipartUpload(t, testClient, testBucket, key, "updated-via-mpu",
				nil, aws.String(etag))
			require.NoError(t, err, "CompleteMultipartUpload with correct IfMatch ETag should succeed")
			require.NotNil(t, out.ETag)
			assert.NotEqual(t, etag, *out.ETag, "new ETag should differ from original")
		})

		t.Run("WrongETag", func(t *testing.T) {
			key := uniqueKey(t, "mpu-ifmatch-wrong")
			cleanupKey(t, testClient, testBucket, key)

			putObject(t, testClient, testBucket, key, "original")

			_, err := doMultipartUpload(t, testClient, testBucket, key, "overwrite-attempt",
				nil, aws.String(wrongETag))
			requirePreconditionFailed(t, err)
		})

		t.Run("NonExistentKey", func(t *testing.T) {
			key := uniqueKey(t, "mpu-ifmatch-nonexistent")

			_, err := doMultipartUpload(t, testClient, testBucket, key, "should-not-exist",
				nil, aws.String(emptyObjectETag))
			requirePreconditionFailed(t, err)
		})
	})
}
