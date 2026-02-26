//go:build integration

package s3test

// TestLanceDBCompatibility verifies that this S3-compatible storage provider
// supports every S3 API operation required by LanceDB.
//
// LanceDB (https://github.com/lancedb/lancedb) is built on the lance Rust
// library (https://github.com/lancedb/lance) which accesses object storage
// exclusively through the Apache Arrow object_store crate (v0.12.x). Lance
// never calls the AWS SDK directly — all S3 calls go through object_store's
// AmazonS3 implementation.
//
// The operations tested here were determined by auditing:
//   lance/rust/lance/src/dataset/commit.rs          — ConditionalPutCommitHandler
//   lance/rust/lance-io/src/object_writer.rs         — file writes
//   lance/rust/lance-io/src/object_reader.rs         — range reads
//   lance/rust/lance/src/dataset/external_manifest.rs — s3+ddb:// mode
//   apache/arrow-rs/object_store/src/aws/client.rs   — S3 HTTP layer
//
// Minimum required operations for the default s3:// commit mode:
//   GetObject (with Range header)  — fragment and manifest reads
//   HeadObject                     — size and ETag retrieval
//   PutObject (unconditional)      — data file writes
//   PutObject with If-None-Match:* — atomic manifest commit   ← CRITICAL
//   PutObject with If-Match:<etag> — conditional manifest update
//   CreateMultipartUpload          — large file setup
//   UploadPart                     — large file data transfer
//   CompleteMultipartUpload        — large file commit
//   CompleteMultipartUpload If-None-Match:* — atomic large file commit
//   AbortMultipartUpload           — large file cleanup on error
//   ListObjectsV2                  — manifest/fragment enumeration
//   DeleteObjects (bulk)           — compaction cleanup
//
// Additional operations for the s3+ddb:// commit mode:
//   CopyObject                     — staging-to-final manifest copy
//   DeleteObject (single)          — staging file cleanup

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLanceDBCompatibility(t *testing.T) {
	// ─────────────────────────────────────────────────────────────────
	// 1. Range reads (GetObject with Range header)
	//
	// Lance reads data from fragment files using arbitrary byte ranges
	// and reads only the final 64 KB of manifest files on dataset open
	// (to locate the manifest footer without fetching the whole file).
	//
	// Source: lance-io/src/object_reader.rs CloudObjectReader::get_range()
	// ─────────────────────────────────────────────────────────────────
	t.Run("RangeRead", func(t *testing.T) {
		key := uniqueKey(t, "lancedb-range")
		cleanupKey(t, testClient, testBucket, key)

		content := "ABCDEFGHIJKLMNOPQRSTUVWXYZ" // 26 bytes, easy to reason about
		putObject(t, testClient, testBucket, key, content)

		ctx, cancel := testContext(t)
		defer cancel()

		// Read the first 5 bytes: expect "ABCDE".
		out, err := testClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
			Range:  aws.String("bytes=0-4"),
		})
		require.NoError(t, err, "GetObject with Range header should succeed")
		defer out.Body.Close()
		body, err := io.ReadAll(out.Body)
		require.NoError(t, err)
		assert.Equal(t, "ABCDE", string(body), "range read should return only the requested bytes")

		// Read the last 5 bytes using an explicit absolute range: expect "VWXYZ".
		out2, err := testClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", len(content)-5, len(content)-1)),
		})
		require.NoError(t, err, "GetObject with suffix range should succeed")
		defer out2.Body.Close()
		body2, err := io.ReadAll(out2.Body)
		require.NoError(t, err)
		assert.Equal(t, "VWXYZ", string(body2), "suffix range read should return the last 5 bytes")
	})

	// ─────────────────────────────────────────────────────────────────
	// 2. HeadObject — file size and ETag retrieval
	//
	// Lance calls HeadObject to determine file size before issuing
	// range reads on manifest files, and to retrieve the final ETag
	// after a CopyObject in the s3+ddb:// commit path.
	//
	// Source: lance/src/dataset/manifest.rs, external_manifest.rs
	// ─────────────────────────────────────────────────────────────────
	t.Run("HeadObject", func(t *testing.T) {
		const body = "lance-head-me"
		key := uniqueKey(t, "lancedb-head")
		cleanupKey(t, testClient, testBucket, key)

		etag := putObject(t, testClient, testBucket, key, body)

		ctx, cancel := testContext(t)
		defer cancel()

		out, err := testClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "HeadObject should succeed for an existing key")
		require.NotNil(t, out.ETag, "HeadObject must return an ETag")
		assert.Equal(t, etag, *out.ETag, "ETag must match the value returned by PutObject")
		require.NotNil(t, out.ContentLength, "HeadObject must return ContentLength")
		assert.Equal(t, int64(len(body)), *out.ContentLength, "ContentLength must equal the body length")
	})

	// ─────────────────────────────────────────────────────────────────
	// 3. PutObject — unconditional write
	//
	// LanceDB writes data fragment files, index files, and other
	// non-manifest objects with a plain unconditional PutObject. These
	// files are written once and never overwritten; no conditional
	// header is needed.
	//
	// Source: lance-io/src/object_writer.rs ObjectWriter::flush()
	// ─────────────────────────────────────────────────────────────────
	t.Run("PutObject", func(t *testing.T) {
		key := uniqueKey(t, "lancedb-put")
		cleanupKey(t, testClient, testBucket, key)

		ctx, cancel := testContext(t)
		defer cancel()

		out, err := testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("lance-arrow-ipc-fragment-data"),
		})
		require.NoError(t, err, "unconditional PutObject should succeed")
		require.NotNil(t, out.ETag)
		assert.NotEmpty(t, *out.ETag)
	})

	// ─────────────────────────────────────────────────────────────────
	// 4. Atomic object creation — PutObject with If-None-Match: *
	//
	// THIS IS THE CRITICAL LANCEDB OPERATION.
	//
	// LanceDB's default commit strategy (ConditionalPutCommitHandler,
	// used for all s3:// URIs) commits each new manifest version as:
	//
	//   PUT /{bucket}/_versions/{N}.manifest
	//   If-None-Match: *
	//
	// On success (HTTP 200): the writer owns manifest version N.
	// On conflict (HTTP 412): another writer committed first; lance
	//   increments N and retries — this is how multi-writer safety is
	//   achieved without a coordinator.
	//
	// Without this header, two concurrent writers can silently
	// overwrite each other's manifests, corrupting the dataset.
	//
	// Source: lance/src/dataset/commit.rs ConditionalPutCommitHandler
	// ─────────────────────────────────────────────────────────────────
	t.Run("AtomicCreate_SucceedsOnNewKey", func(t *testing.T) {
		key := uniqueKey(t, "lancedb-atomic-new")
		cleanupKey(t, testClient, testBucket, key)

		ctx, cancel := testContext(t)
		defer cancel()

		out, err := testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(testBucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(`{"version":1,"fragments":[]}`),
			IfNoneMatch: aws.String("*"),
		})
		require.NoError(t, err,
			"PutObject If-None-Match=* MUST succeed for a non-existent key — "+
				"this is LanceDB's primary atomic commit mechanism")
		require.NotNil(t, out.ETag, "response must include an ETag for the new object")
	})

	t.Run("AtomicCreate_FailsOnExistingKey", func(t *testing.T) {
		key := uniqueKey(t, "lancedb-atomic-conflict")
		cleanupKey(t, testClient, testBucket, key)

		putObject(t, testClient, testBucket, key, `{"version":1,"fragments":[]}`)

		ctx, cancel := testContext(t)
		defer cancel()

		_, err := testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(testBucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(`{"version":1,"conflict":true}`),
			IfNoneMatch: aws.String("*"),
		})
		// MUST return 412 Precondition Failed (or 409 ConditionalRequestConflict
		// under high concurrency). LanceDB maps 412/409 to CommitConflict and
		// retries with the next version number.
		requireConditionalWriteFailure(t, err)
	})

	// ─────────────────────────────────────────────────────────────────
	// 5. Conditional update — PutObject with If-Match: <etag>
	//
	// The object_store crate's PutMode::Update translates to:
	//
	//   PUT /{bucket}/{key}
	//   If-Match: <current-etag>
	//
	// Lance uses this for in-place updates of existing objects when
	// the caller holds a known current ETag (e.g. updating metadata).
	//
	// Source: apache/arrow-rs object_store PutMode::Update
	// ─────────────────────────────────────────────────────────────────
	t.Run("ConditionalUpdate", func(t *testing.T) {
		key := uniqueKey(t, "lancedb-conditional-update")
		cleanupKey(t, testClient, testBucket, key)

		etag := putObject(t, testClient, testBucket, key, "v1")

		ctx, cancel := testContext(t)
		defer cancel()

		out, err := testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:  aws.String(testBucket),
			Key:     aws.String(key),
			Body:    strings.NewReader("v2"),
			IfMatch: aws.String(etag),
		})
		require.NoError(t, err, "PutObject If-Match=<etag> should succeed when ETag is current")
		require.NotNil(t, out.ETag)
		assert.NotEqual(t, etag, *out.ETag, "ETag must change after a successful update")
	})

	// ─────────────────────────────────────────────────────────────────
	// 6. Multipart upload — large file write
	//
	// LanceDB uses multipart upload for files exceeding the
	// LANCE_INITIAL_UPLOAD_SIZE threshold (~5 MB). Real Lance datasets
	// routinely contain fragment files of tens of MB or more.
	//
	// Required sequence:
	//   POST /{bucket}/{key}?uploads           → CreateMultipartUpload
	//   PUT  /{bucket}/{key}?partNumber=N&...  → UploadPart (×N parts)
	//   POST /{bucket}/{key}?uploadId=...      → CompleteMultipartUpload
	//
	// Source: lance-io/src/object_writer.rs ObjectWriter::write_part()
	// ─────────────────────────────────────────────────────────────────
	t.Run("MultipartWrite", func(t *testing.T) {
		key := uniqueKey(t, "lancedb-mpu")
		cleanupKey(t, testClient, testBucket, key)

		out, err := doMultipartUpload(t, testClient, testBucket, key,
			"lance-large-fragment-body", nil, nil)
		require.NoError(t, err, "multipart upload (Create+Upload+Complete) should succeed")
		require.NotNil(t, out.ETag)
		assert.NotEmpty(t, *out.ETag)
	})

	// ─────────────────────────────────────────────────────────────────
	// 7. Atomic multipart create — CompleteMultipartUpload If-None-Match: *
	//
	// When a manifest file exceeds the single-part threshold, LanceDB
	// commits it via CompleteMultipartUpload with If-None-Match: *,
	// providing the same atomic-create semantics as PutObject for large
	// objects (CompleteMultipartMode::Create in object_store).
	//
	// Source: lance/src/dataset/commit.rs + object_store CompleteMultipartMode
	// ─────────────────────────────────────────────────────────────────
	t.Run("MultipartAtomicCreate", func(t *testing.T) {
		key := uniqueKey(t, "lancedb-mpu-atomic")
		cleanupKey(t, testClient, testBucket, key)

		// First upload must succeed (key does not yet exist).
		out, err := doMultipartUpload(t, testClient, testBucket, key,
			"lance-large-manifest-body", aws.String("*"), nil)
		require.NoError(t, err,
			"CompleteMultipartUpload If-None-Match=* MUST succeed for a new key")
		require.NotNil(t, out.ETag)

		// Second upload to the same key must be rejected.
		_, err = doMultipartUpload(t, testClient, testBucket, key,
			"conflict-upload", aws.String("*"), nil)
		requirePreconditionFailed(t, err)
	})

	// ─────────────────────────────────────────────────────────────────
	// 8. AbortMultipartUpload — cleanup on write failure
	//
	// Lance aborts any in-progress multipart upload when it encounters
	// an error, to avoid leaving orphaned parts that incur storage
	// charges. The provider must accept AbortMultipartUpload calls and
	// remove all uploaded parts so the key no longer appears to exist.
	//
	// Source: lance-io/src/object_writer.rs ObjectWriter::abort()
	// ─────────────────────────────────────────────────────────────────
	t.Run("AbortMultipartUpload", func(t *testing.T) {
		key := uniqueKey(t, "lancedb-abort")
		cleanupKey(t, testClient, testBucket, key)

		ctx, cancel := testContext(t)
		defer cancel()

		createOut, err := testClient.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "CreateMultipartUpload should succeed")

		_, err = testClient.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(testBucket),
			Key:      aws.String(key),
			UploadId: createOut.UploadId,
		})
		require.NoError(t, err, "AbortMultipartUpload should succeed")

		// After abort, no completed object should exist at the key.
		_, err = testClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
		})
		requireHTTPStatus(t, err, 404)
	})

	// ─────────────────────────────────────────────────────────────────
	// 9. ListObjectsV2 — prefix enumeration with pagination
	//
	// Lance lists manifest files, fragment files, and index files by
	// prefix when opening a dataset or running compaction. The
	// object_store crate always uses ListObjectsV2 (list-type=2) and
	// handles pagination via NextContinuationToken.
	//
	// Source: apache/arrow-rs object_store/src/aws/client.rs list_request()
	// ─────────────────────────────────────────────────────────────────
	t.Run("ListObjectsV2", func(t *testing.T) {
		prefix := uniqueKey(t, "lancedb-list") + "/"
		for _, name := range []string{"frag-0.lance", "frag-1.lance", "frag-2.lance"} {
			k := prefix + name
			putObject(t, testClient, testBucket, k, "fragment")
			cleanupKey(t, testClient, testBucket, k)
		}

		ctx, cancel := testContext(t)
		defer cancel()

		// Full listing — all three objects in one page.
		listOut, err := testClient.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(testBucket),
			Prefix: aws.String(prefix),
		})
		require.NoError(t, err, "ListObjectsV2 should succeed")
		assert.Len(t, listOut.Contents, 3, "should list all three objects")
		assert.False(t, aws.ToBool(listOut.IsTruncated), "single-page result must not be truncated")

		// Paginated listing — force MaxKeys=1 to exercise continuation tokens.
		page1, err := testClient.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(testBucket),
			Prefix:  aws.String(prefix),
			MaxKeys: aws.Int32(1),
		})
		require.NoError(t, err, "ListObjectsV2 page 1 should succeed")
		assert.Len(t, page1.Contents, 1, "page 1 with MaxKeys=1 should contain exactly 1 object")
		assert.True(t, aws.ToBool(page1.IsTruncated),
			"response must be marked truncated when MaxKeys=1 and 3 objects exist")
		require.NotNil(t, page1.NextContinuationToken,
			"truncated response must include a NextContinuationToken")

		// Fetch page 2 using the continuation token.
		page2, err := testClient.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(testBucket),
			Prefix:            aws.String(prefix),
			MaxKeys:           aws.Int32(1),
			ContinuationToken: page1.NextContinuationToken,
		})
		require.NoError(t, err, "ListObjectsV2 page 2 should succeed")
		assert.Len(t, page2.Contents, 1)
		assert.NotEqual(t, page1.Contents[0].Key, page2.Contents[0].Key,
			"pages must return different objects")
	})

	// ─────────────────────────────────────────────────────────────────
	// 10. DeleteObject — single object removal
	//
	// The DynamoDB-backed commit handler (s3+ddb:// mode) deletes the
	// staging manifest file after copying it to its final versioned
	// path. Single-object delete is also used for ad-hoc cleanup.
	//
	// Source: lance/src/dataset/external_manifest.rs
	// ─────────────────────────────────────────────────────────────────
	t.Run("DeleteObject", func(t *testing.T) {
		key := uniqueKey(t, "lancedb-delete-single")
		// No cleanupKey registration: the test itself removes the object.
		putObject(t, testClient, testBucket, key, "to-be-deleted")

		ctx, cancel := testContext(t)
		defer cancel()

		_, err := testClient.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "DeleteObject should succeed")

		// The key must no longer be accessible after deletion.
		_, err = testClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(key),
		})
		requireHTTPStatus(t, err, 404)
	})

	// ─────────────────────────────────────────────────────────────────
	// 11. DeleteObjects (bulk) — multi-object removal
	//
	// LanceDB batches up to 1,000 object deletions per request when
	// performing compaction (removing superseded fragment and index
	// files). The object_store crate uses the S3 batch-delete endpoint
	// (POST /{bucket}?delete) with an XML body listing all keys.
	//
	// Source: apache/arrow-rs object_store/src/aws/client.rs bulk_delete_request()
	// ─────────────────────────────────────────────────────────────────
	t.Run("BulkDeleteObjects", func(t *testing.T) {
		prefix := uniqueKey(t, "lancedb-bulk-delete") + "/"
		keys := []string{
			prefix + "old-frag-0.lance",
			prefix + "old-frag-1.lance",
			prefix + "old-frag-2.lance",
		}
		for _, k := range keys {
			putObject(t, testClient, testBucket, k, "superseded-fragment")
			cleanupKey(t, testClient, testBucket, k) // safety net if bulk delete fails
		}

		ids := make([]types.ObjectIdentifier, len(keys))
		for i, k := range keys {
			ids[i] = types.ObjectIdentifier{Key: aws.String(k)}
		}

		ctx, cancel := testContext(t)
		defer cancel()

		delOut, err := testClient.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(testBucket),
			Delete: &types.Delete{Objects: ids},
		})
		require.NoError(t, err, "DeleteObjects should succeed")
		assert.Empty(t, delOut.Errors, "bulk delete must report no per-object errors")
		assert.Len(t, delOut.Deleted, len(keys), "all objects must be reported as deleted")
	})

	// ─────────────────────────────────────────────────────────────────
	// 12. CopyObject — staging-to-final manifest copy (s3+ddb:// mode)
	//
	// When using the DynamoDB-backed commit handler, lance writes the
	// new manifest to a temporary staging key and then copies it to
	// the final versioned path once the DynamoDB lock is acquired.
	// This avoids needing If-None-Match on PutObject for providers
	// that support DynamoDB but not conditional writes.
	//
	// Source: lance/src/dataset/external_manifest.rs ExternalManifestCommitHandler
	// ─────────────────────────────────────────────────────────────────
	t.Run("CopyObject", func(t *testing.T) {
		srcKey := uniqueKey(t, "lancedb-copy-src")
		dstKey := uniqueKey(t, "lancedb-copy-dst")
		cleanupKey(t, testClient, testBucket, srcKey)
		cleanupKey(t, testClient, testBucket, dstKey)

		const manifestJSON = `{"version":1,"fragments":["frag-0.lance"]}`
		putObject(t, testClient, testBucket, srcKey, manifestJSON)

		ctx, cancel := testContext(t)
		defer cancel()

		copyOut, err := testClient.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(testBucket),
			Key:        aws.String(dstKey),
			CopySource: aws.String(copySource(testBucket, srcKey)),
		})
		require.NoError(t, err, "CopyObject from staging to final path should succeed")
		require.NotNil(t, copyOut.CopyObjectResult)
		require.NotNil(t, copyOut.CopyObjectResult.ETag)

		// Verify the copy is readable and its content is intact.
		getOut, err := testClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(dstKey),
		})
		require.NoError(t, err)
		defer getOut.Body.Close()
		body, err := io.ReadAll(getOut.Body)
		require.NoError(t, err)
		assert.Equal(t, manifestJSON, string(body), "copied object content must be identical to source")
	})

	// ─────────────────────────────────────────────────────────────────
	// 13. End-to-end manifest commit workflow
	//
	// Simulates the full write-then-commit cycle that LanceDB performs
	// for every dataset mutation using the ConditionalPutCommitHandler
	// (the default for all s3:// URIs):
	//
	//   1. Write data file         — unconditional PutObject
	//   2. Commit manifest v1      — PutObject If-None-Match:* → 200
	//   3. Concurrent writer fails — PutObject If-None-Match:* → 412
	//   4. Verify v1 is unchanged  — HeadObject If-Match + GetObject
	//   5. Commit manifest v2      — PutObject If-None-Match:* → 200
	//   6. List _versions/         — ListObjectsV2 → 2 entries
	//   7. Dataset compaction      — DeleteObjects (bulk) → 3 deleted
	//
	// This test ensures the entire multi-writer safety contract works
	// end-to-end, not just each primitive in isolation.
	// ─────────────────────────────────────────────────────────────────
	t.Run("ManifestCommitWorkflow", func(t *testing.T) {
		base := uniqueKey(t, "lancedb-workflow")
		dataKey := base + "/data/frag-0.lance"
		manifest1Key := base + "/_versions/1.manifest"
		manifest2Key := base + "/_versions/2.manifest"
		for _, k := range []string{dataKey, manifest1Key, manifest2Key} {
			cleanupKey(t, testClient, testBucket, k)
		}

		ctx, cancel := testContext(t)
		defer cancel()

		// Step 1: Write a data fragment file — no conditional header.
		_, err := testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(dataKey),
			Body:   strings.NewReader("lance-arrow-ipc-encoded-data"),
		})
		require.NoError(t, err, "data file write should succeed")

		// Step 2: Writer A commits manifest v1 atomically.
		const v1Content = `{"version":1,"fragments":["frag-0.lance"]}`
		commitOut, err := testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(testBucket),
			Key:         aws.String(manifest1Key),
			Body:        strings.NewReader(v1Content),
			IfNoneMatch: aws.String("*"),
		})
		require.NoError(t, err, "initial manifest commit with If-None-Match=* must succeed")
		require.NotNil(t, commitOut.ETag)
		v1ETag := *commitOut.ETag

		// Step 3: Writer B races and attempts the same manifest version — must lose.
		_, err = testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(testBucket),
			Key:         aws.String(manifest1Key),
			Body:        strings.NewReader(`{"version":1,"conflict":true}`),
			IfNoneMatch: aws.String("*"),
		})
		requirePreconditionFailed(t, err)

		// Step 4: Verify manifest v1 is intact and its ETag is stable.
		headOut, err := testClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket:  aws.String(testBucket),
			Key:     aws.String(manifest1Key),
			IfMatch: aws.String(v1ETag),
		})
		require.NoError(t, err, "HeadObject If-Match=v1ETag must succeed")
		assert.Equal(t, v1ETag, *headOut.ETag, "ETag must not change after a rejected concurrent write")

		getOut, err := testClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(manifest1Key),
		})
		require.NoError(t, err)
		defer getOut.Body.Close()
		readBody, err := io.ReadAll(getOut.Body)
		require.NoError(t, err)
		assert.Equal(t, v1Content, string(readBody),
			"manifest v1 content must not be corrupted by the rejected concurrent write")

		// Step 5: Writer A now commits manifest v2 (a new, distinct key).
		_, err = testClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(testBucket),
			Key:         aws.String(manifest2Key),
			Body:        strings.NewReader(`{"version":2,"fragments":["frag-0.lance","frag-1.lance"]}`),
			IfNoneMatch: aws.String("*"),
		})
		require.NoError(t, err, "manifest v2 commit must succeed")

		// Step 6: List the _versions/ directory — must see both v1 and v2.
		listOut, err := testClient.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(testBucket),
			Prefix: aws.String(base + "/_versions/"),
		})
		require.NoError(t, err, "ListObjectsV2 should succeed")
		assert.Len(t, listOut.Contents, 2, "both manifest versions must be visible in the listing")

		// Step 7: Bulk-delete all dataset files (compaction / dataset removal).
		ids := []types.ObjectIdentifier{
			{Key: aws.String(dataKey)},
			{Key: aws.String(manifest1Key)},
			{Key: aws.String(manifest2Key)},
		}
		delOut, err := testClient.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(testBucket),
			Delete: &types.Delete{Objects: ids},
		})
		require.NoError(t, err, "bulk delete of all dataset files should succeed")
		assert.Empty(t, delOut.Errors, "bulk delete must report no per-object errors")
		assert.Len(t, delOut.Deleted, 3, "all three dataset files must be deleted")
	})
}
