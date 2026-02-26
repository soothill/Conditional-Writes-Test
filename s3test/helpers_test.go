package s3test

import (
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeAWSHTTPError builds a minimal *awshttp.ResponseError with the given HTTP
// status code. This lets unit tests exercise the require* assertion helpers
// without making real S3 calls.
func makeAWSHTTPError(status int) error {
	return &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{
				Response: &http.Response{StatusCode: status},
			},
		},
	}
}

// ── stripQuotes ───────────────────────────────────────────────────────────────

func TestStripQuotes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "quoted etag",
			input:    `"d41d8cd98f00b204e9800998ecf8427e"`,
			expected: "d41d8cd98f00b204e9800998ecf8427e",
		},
		{
			name:     "unquoted etag",
			input:    "d41d8cd98f00b204e9800998ecf8427e",
			expected: "d41d8cd98f00b204e9800998ecf8427e",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "just quotes",
			input:    `""`,
			expected: "",
		},
		{
			name:     "leading quote only",
			input:    `"abc`,
			expected: "abc",
		},
		{
			name:     "trailing quote only",
			input:    `abc"`,
			expected: "abc",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, stripQuotes(tc.input))
		})
	}
}

// ── copySource ────────────────────────────────────────────────────────────────

func TestCopySource(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		key    string
		want   string
	}{
		{name: "simple", bucket: "my-bucket", key: "my-key", want: "my-bucket/my-key"},
		{name: "key with slashes", bucket: "b", key: "dir/sub/file.txt", want: "b/dir/sub/file.txt"},
		{name: "empty key", bucket: "b", key: "", want: "b/"},
		{name: "empty bucket", bucket: "", key: "k", want: "/k"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, copySource(tc.bucket, tc.key))
		})
	}
}

// ── runConcurrent ─────────────────────────────────────────────────────────────

func TestRunConcurrent(t *testing.T) {
	t.Run("returns one error per goroutine", func(t *testing.T) {
		errs := runConcurrent(5, func(id int) error { return nil })
		require.Len(t, errs, 5)
		for _, e := range errs {
			assert.NoError(t, e)
		}
	})

	t.Run("each goroutine receives its unique id", func(t *testing.T) {
		seen := make([]bool, 10)
		var mu = make(chan struct{}, 1)
		mu <- struct{}{}
		errs := runConcurrent(10, func(id int) error {
			<-mu
			seen[id] = true
			mu <- struct{}{}
			return nil
		})
		require.Len(t, errs, 10)
		for i, v := range seen {
			assert.True(t, v, "id %d was never seen", i)
		}
	})

	t.Run("errors are collected from all goroutines", func(t *testing.T) {
		sentinel := errors.New("expected error")
		errs := runConcurrent(3, func(id int) error { return sentinel })
		require.Len(t, errs, 3)
		for _, e := range errs {
			assert.Equal(t, sentinel, e)
		}
	})

	t.Run("zero workers returns empty slice", func(t *testing.T) {
		errs := runConcurrent(0, func(id int) error { return nil })
		assert.Empty(t, errs)
	})
}

// ── requireHTTPStatus ─────────────────────────────────────────────────────────

// mockTB is a minimal testing.TB implementation that captures failure state
// without calling os.Exit or runtime.Goexit, so we can unit-test the require*
// helpers. Only the methods used by testify/require are implemented.
type mockTB struct {
	testing.TB           // embed for zero-value of unused methods
	failed  bool
	helper  bool
	logMsgs []string
}

func (m *mockTB) Helper()                    { m.helper = true }
func (m *mockTB) Name() string               { return "mockTB" }
func (m *mockTB) Errorf(f string, a ...any) { m.logMsgs = append(m.logMsgs, fmt.Sprintf(f, a...)); m.failed = true }
func (m *mockTB) Fatalf(f string, a ...any) { m.logMsgs = append(m.logMsgs, fmt.Sprintf(f, a...)); m.FailNow() }
// FailNow marks the mock failed and terminates the current goroutine via
// runtime.Goexit so that any remaining assertions in the helper under test
// do not run — mirroring how the real testing.T.FailNow() behaves.
// Callers should wrap the helper invocation in runInSubGoroutine so the
// goexit exits only the sub-goroutine and not the test goroutine.
func (m *mockTB) FailNow()                  { m.failed = true; runtime.Goexit() }
func (m *mockTB) Fail()                     { m.failed = true }
func (m *mockTB) Failed() bool              { return m.failed }
func (m *mockTB) Log(args ...any)           {}
func (m *mockTB) Logf(f string, a ...any)  {}
func (m *mockTB) Cleanup(f func())         { f() }

// runInSubGoroutine executes fn in a separate goroutine and waits for it to
// finish. When a require.* assertion inside fn calls FailNow → runtime.Goexit,
// only the sub-goroutine terminates; the calling test goroutine is unaffected.
func runInSubGoroutine(fn func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	<-done
}

func TestRequireHTTPStatus_Pass(t *testing.T) {
	err := makeAWSHTTPError(412)
	tb := &mockTB{}
	// Success path: no goroutine wrapping needed — FailNow is never called.
	requireHTTPStatus(tb, err, 412)
	assert.False(t, tb.failed, "requireHTTPStatus should not fail when status matches")
}

func TestRequireHTTPStatus_WrongStatus(t *testing.T) {
	err := makeAWSHTTPError(200)
	tb := &mockTB{}
	runInSubGoroutine(func() { requireHTTPStatus(tb, err, 412) })
	assert.True(t, tb.failed, "requireHTTPStatus should fail when status does not match")
}

func TestRequireHTTPStatus_NilError(t *testing.T) {
	tb := &mockTB{}
	runInSubGoroutine(func() { requireHTTPStatus(tb, nil, 412) })
	assert.True(t, tb.failed, "requireHTTPStatus should fail when err is nil")
}

func TestRequireHTTPStatus_NonAWSError(t *testing.T) {
	tb := &mockTB{}
	runInSubGoroutine(func() { requireHTTPStatus(tb, errors.New("plain error"), 412) })
	assert.True(t, tb.failed, "requireHTTPStatus should fail for non-AWS error")
}

// ── requirePreconditionFailed ─────────────────────────────────────────────────

func TestRequirePreconditionFailed_Pass(t *testing.T) {
	tb := &mockTB{}
	requirePreconditionFailed(tb, makeAWSHTTPError(412))
	assert.False(t, tb.failed)
}

func TestRequirePreconditionFailed_WrongStatus(t *testing.T) {
	tb := &mockTB{}
	runInSubGoroutine(func() { requirePreconditionFailed(tb, makeAWSHTTPError(404)) })
	assert.True(t, tb.failed)
}

// ── requireNotModified ────────────────────────────────────────────────────────

func TestRequireNotModified_Pass(t *testing.T) {
	tb := &mockTB{}
	requireNotModified(tb, makeAWSHTTPError(304))
	assert.False(t, tb.failed)
}

func TestRequireNotModified_WrongStatus(t *testing.T) {
	tb := &mockTB{}
	runInSubGoroutine(func() { requireNotModified(tb, makeAWSHTTPError(412)) })
	assert.True(t, tb.failed)
}

// ── requireIfMatchKeyMissing ──────────────────────────────────────────────────

func TestRequireIfMatchKeyMissing_404(t *testing.T) {
	tb := &mockTB{}
	requireIfMatchKeyMissing(tb, makeAWSHTTPError(404))
	assert.False(t, tb.failed, "404 should be accepted by requireIfMatchKeyMissing")
}

func TestRequireIfMatchKeyMissing_412(t *testing.T) {
	tb := &mockTB{}
	requireIfMatchKeyMissing(tb, makeAWSHTTPError(412))
	assert.False(t, tb.failed, "412 should be accepted by requireIfMatchKeyMissing")
}

func TestRequireIfMatchKeyMissing_200(t *testing.T) {
	tb := &mockTB{}
	runInSubGoroutine(func() { requireIfMatchKeyMissing(tb, makeAWSHTTPError(200)) })
	assert.True(t, tb.failed, "200 should not be accepted by requireIfMatchKeyMissing")
}

func TestRequireIfMatchKeyMissing_Nil(t *testing.T) {
	tb := &mockTB{}
	runInSubGoroutine(func() { requireIfMatchKeyMissing(tb, nil) })
	assert.True(t, tb.failed, "nil error should not be accepted")
}

// ── requireConditionalWriteFailure ────────────────────────────────────────────

func TestRequireConditionalWriteFailure_412(t *testing.T) {
	tb := &mockTB{}
	requireConditionalWriteFailure(tb, makeAWSHTTPError(412))
	assert.False(t, tb.failed, "412 should be accepted")
}

func TestRequireConditionalWriteFailure_409(t *testing.T) {
	tb := &mockTB{}
	requireConditionalWriteFailure(tb, makeAWSHTTPError(409))
	assert.False(t, tb.failed, "409 should be accepted")
}

func TestRequireConditionalWriteFailure_200(t *testing.T) {
	tb := &mockTB{}
	runInSubGoroutine(func() { requireConditionalWriteFailure(tb, makeAWSHTTPError(200)) })
	assert.True(t, tb.failed, "200 should not be accepted")
}

func TestRequireConditionalWriteFailure_Nil(t *testing.T) {
	tb := &mockTB{}
	runInSubGoroutine(func() { requireConditionalWriteFailure(tb, nil) })
	assert.True(t, tb.failed, "nil error should not be accepted")
}
