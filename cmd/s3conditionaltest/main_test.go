package main

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureStdout redirects os.Stdout to a pipe for the duration of fn, then
// returns everything that was written. Restores os.Stdout before returning.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	old := os.Stdout
	os.Stdout = w
	t.Cleanup(func() { os.Stdout = old })

	fn()

	w.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	r.Close()
	os.Stdout = old // restore early so subsequent t.Log calls work
	return buf.String()
}

// ── isBoilerplate ──────────────────────────────────────────────────────────────

func TestIsBoilerplate(t *testing.T) {
	tests := []struct {
		name  string
		line  string
		want  bool
	}{
		{name: "error trace", line: "    Error Trace:  foo_test.go:42", want: true},
		{name: "test label", line: "    Test:         TestFoo", want: true},
		{name: "messages label", line: "    Messages:     expected true", want: true},
		{name: "dash pass", line: "--- PASS: TestFoo (0.01s)", want: true},
		{name: "dash fail", line: "--- FAIL: TestBar (0.02s)", want: true},
		{name: "equals run", line: "=== RUN   TestFoo", want: true},
		{name: "equals pause", line: "=== PAUSE TestFoo", want: true},
		{name: "equals cont", line: "=== CONT  TestFoo", want: true},
		{name: "PASS alone", line: "PASS", want: true},
		{name: "FAIL alone", line: "FAIL", want: true},
		{name: "ok tab", line: "ok  \tgithub.com/foo/bar  1.23s", want: true},
		{name: "question tab", line: "?   \tgithub.com/foo/bar  [no test files]", want: true},

		// leading spaces should be stripped
		{name: "with leading spaces", line: "    --- PASS: TestFoo (0.01s)", want: true},

		// non-boilerplate content
		{name: "error message", line: "    Error:      expected 1 but got 2", want: false},
		{name: "assertion detail", line: "    Received unexpected error:", want: false},
		{name: "normal log line", line: "    some debug output", want: false},
		{name: "empty string", line: "", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isBoilerplate(tc.line))
		})
	}
}

// ── extractFailureMsg ──────────────────────────────────────────────────────────

func TestExtractFailureMsg(t *testing.T) {
	tests := []struct {
		name  string
		lines []string
		want  string
	}{
		{
			name: "testify Error: line is preferred",
			lines: []string{
				"    Error Trace:  foo_test.go:12",
				"    Error:      \tReceived unexpected error:",
				"    Test:       TestFoo",
			},
			want: "Received unexpected error:",
		},
		{
			name: "Error Trace is ignored when looking for Error:",
			lines: []string{
				"    Error Trace:  foo_test.go:12",
				"    Test:       TestFoo",
				"    some other line",
			},
			want: "some other line",
		},
		{
			name: "falls back to first non-boilerplate non-empty line",
			lines: []string{
				"    --- FAIL: TestFoo (0.01s)",
				"",
				"    not equal: blah blah",
			},
			want: "not equal: blah blah",
		},
		{
			name:  "all boilerplate returns empty string",
			lines: []string{"--- PASS: TestFoo (0.00s)", "PASS"},
			want:  "",
		},
		{
			name:  "empty slice returns empty string",
			lines: nil,
			want:  "",
		},
		{
			// When "Error:" has only whitespace after it the first-pass returns
			// nothing, but the fallback loop finds "Error:" itself as the first
			// non-boilerplate non-empty line.
			name: "Error: with empty value — fallback returns Error: line",
			lines: []string{
				"    Error:    \t",
				"    fallback message",
			},
			want: "Error:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, extractFailureMsg(tc.lines))
		})
	}
}

// ── humanSubName ──────────────────────────────────────────────────────────────

func TestHumanSubName(t *testing.T) {
	tests := []struct {
		name     string
		fullName string
		want     string
	}{
		{
			name:     "nested sub-test",
			fullName: "TestPutObjectConditionalWrites/IfNoneMatch/NewKey",
			want:     "NewKey",
		},
		{
			name:     "underscores become spaces",
			fullName: "TestPutObjectConditionalWrites/IfNoneMatch/After_Delete",
			want:     "After Delete",
		},
		{
			name:     "top-level name (no slash)",
			fullName: "TestPutObjectConditionalWrites",
			want:     "TestPutObjectConditionalWrites",
		},
		{
			name:     "two-level path",
			fullName: "TestFoo/Sub",
			want:     "Sub",
		},
		{
			name:     "underscores in middle component are retained",
			fullName: "TestFoo_Bar/Sub",
			want:     "Sub",
		},
		{
			name:     "empty string",
			fullName: "",
			want:     "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, humanSubName(tc.fullName))
		})
	}
}

// ── humanTopName ──────────────────────────────────────────────────────────────

func TestHumanTopName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "strips Test prefix", input: "TestPutObjectConditionalWrites", want: "PutObjectConditionalWrites"},
		{name: "no Test prefix", input: "PutObject", want: "PutObject"},
		{name: "only Test word", input: "Test", want: ""},
		{name: "empty string", input: "", want: ""},
		{name: "Test at end not stripped", input: "FooTest", want: "FooTest"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, humanTopName(tc.input))
		})
	}
}

// ── padRight ──────────────────────────────────────────────────────────────────

func TestPadRight(t *testing.T) {
	tests := []struct {
		name  string
		s     string
		n     int
		want  string
	}{
		{name: "pads short string", s: "hello", n: 10, want: "hello     "},
		// padRight uses count >= n, so exact-length strings are truncated with
		// an ellipsis.  Use n = count+1 to test "just fits without truncation".
		{name: "one char less than input — no truncation", s: "hello", n: 6, want: "hello "},
		{name: "exact length gets ellipsis", s: "hello", n: 5, want: "hell…"},
		{name: "truncates with ellipsis", s: "hello world", n: 7, want: "hello …"},
		{name: "empty string padded", s: "", n: 4, want: "    "},
		{name: "unicode chars counted correctly", s: "café", n: 8, want: "café    "},
		{name: "unicode truncated correctly", s: "héllo", n: 4, want: "hél…"},
		{name: "n=1 truncates to just ellipsis", s: "hello", n: 1, want: "…"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := padRight(tc.s, tc.n)
			assert.Equal(t, tc.want, got)
		})
	}
}

// ── colored ───────────────────────────────────────────────────────────────────

func TestColored(t *testing.T) {
	t.Run("color disabled returns plain string", func(t *testing.T) {
		got := colored(false, ansiGreen, "hello")
		assert.Equal(t, "hello", got)
	})

	t.Run("color enabled wraps with code and reset", func(t *testing.T) {
		got := colored(true, ansiGreen, "hello")
		assert.Equal(t, ansiGreen+"hello"+ansiReset, got)
	})

	t.Run("color enabled with bold", func(t *testing.T) {
		got := colored(true, ansiBold, "text")
		assert.Equal(t, ansiBold+"text"+ansiReset, got)
	})

	t.Run("empty string", func(t *testing.T) {
		got := colored(true, ansiRed, "")
		assert.Equal(t, ansiRed+ansiReset, got)
	})
}
