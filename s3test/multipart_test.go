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

func TestMultipartConditionalWrites(t *testing.T) {
	t.Run("IfNoneMatch", func(t *testing.T) {
		t.Run("NewKey", func(t *testing.T) {
			key := setupKey(t, "mpu-ifnonematch-new")

			out, err := doMultipartUpload(t, testClient, testBucket, key, "multipart-body",
				aws.String("*"), nil)
			require.NoError(t, err, "CompleteMultipartUpload with IfNoneMatch=* to new key should succeed")
			require.NotNil(t, out.ETag, "response should include ETag")
			assert.NotEmpty(t, *out.ETag)

			// Verify the uploaded body round-trips correctly.
			ctx, cancel := testContext(t)
			defer cancel()
			getOut, getErr := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(testBucket),
				Key:    aws.String(key),
			})
			require.NoError(t, getErr, "GetObject after multipart upload should succeed")
			defer getOut.Body.Close()
			gotBody, readErr := io.ReadAll(getOut.Body)
			require.NoError(t, readErr)
			assert.Equal(t, "multipart-body", string(gotBody), "body should round-trip correctly")
		})

		t.Run("ExistingKey", func(t *testing.T) {
			key, _ := putKeyForTest(t, "mpu-ifnonematch-existing", "existing-object")

			_, err := doMultipartUpload(t, testClient, testBucket, key, "multipart-overwrite",
				aws.String("*"), nil)
			requirePreconditionFailed(t, err)
		})

		t.Run("AfterDelete", func(t *testing.T) {
			key, _ := putKeyForTest(t, "mpu-ifnonematch-afterdelete", "temporary")
			deleteObject(t, testClient, testBucket, key)

			out, err := doMultipartUpload(t, testClient, testBucket, key, "after-delete",
				aws.String("*"), nil)
			require.NoError(t, err, "CompleteMultipartUpload with IfNoneMatch=* after delete should succeed")
			require.NotNil(t, out.ETag)
		})
	})

	t.Run("IfMatch", func(t *testing.T) {
		t.Run("CorrectETag", func(t *testing.T) {
			key, etag := putKeyForTest(t, "mpu-ifmatch-correct", "original")

			out, err := doMultipartUpload(t, testClient, testBucket, key, "updated-via-mpu",
				nil, aws.String(etag))
			require.NoError(t, err, "CompleteMultipartUpload with correct IfMatch ETag should succeed")
			require.NotNil(t, out.ETag)
			assert.NotEqual(t, etag, *out.ETag, "new ETag should differ from original")

			// Verify the updated body is durably stored.
			ctx, cancel := testContext(t)
			defer cancel()
			getOut, getErr := testClient.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(testBucket),
				Key:    aws.String(key),
			})
			require.NoError(t, getErr, "GetObject after multipart upload should succeed")
			defer getOut.Body.Close()
			gotBody, readErr := io.ReadAll(getOut.Body)
			require.NoError(t, readErr)
			assert.Equal(t, "updated-via-mpu", string(gotBody), "body should reflect the multipart upload content")
		})

		t.Run("WrongETag", func(t *testing.T) {
			key, _ := putKeyForTest(t, "mpu-ifmatch-wrong", "original")

			_, err := doMultipartUpload(t, testClient, testBucket, key, "overwrite-attempt",
				nil, aws.String(wrongETag))
			requirePreconditionFailed(t, err)
		})

		t.Run("NonExistentKey", func(t *testing.T) {
			key := setupKey(t, "mpu-ifmatch-nonexistent")

			_, err := doMultipartUpload(t, testClient, testBucket, key, "should-not-exist",
				nil, aws.String(emptyObjectETag))
			// AWS S3 returns 404 NoSuchKey when IfMatch is used against a key
			// that does not exist (no ETag to compare). Other implementations
			// may return 412 Precondition Failed instead.
			requireIfMatchKeyMissing(t, err)
		})
	})
}
