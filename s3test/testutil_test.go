//go:build integration

package s3test

import "testing"

// setupKey generates a unique S3 key with prefix, registers t.Cleanup to
// delete it from testBucket after the test, and returns the key. It is the
// standard one-line replacement for the two-line
//
//	key := uniqueKey(t, prefix)
//	cleanupKey(t, testClient, testBucket, key)
//
// pattern that appears throughout the integration tests.
func setupKey(t testing.TB, prefix string) string {
	t.Helper()
	key := uniqueKey(t, prefix)
	cleanupKey(t, testClient, testBucket, key)
	return key
}

// putKeyForTest creates a fresh key via setupKey, uploads body to testBucket,
// and returns the key and its ETag. It condenses the standard three-line
//
//	key  := uniqueKey(t, prefix)
//	cleanupKey(t, testClient, testBucket, key)
//	etag := putObject(t, testClient, testBucket, key, body)
//
// setup pattern used in nearly every subtest into a single call.
func putKeyForTest(t testing.TB, prefix, body string) (key, etag string) {
	t.Helper()
	key = setupKey(t, prefix)
	etag = putObject(t, testClient, testBucket, key, body)
	return
}
