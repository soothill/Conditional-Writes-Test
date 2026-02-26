package main

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── buildCell ──────────────────────────────────────────────────────────────────

func TestBuildCell(t *testing.T) {
	t.Run("nil node returns empty cell", func(t *testing.T) {
		c := buildCell(nil)
		assert.Equal(t, cell{}, c)
	})

	t.Run("passing node with HTTP code", func(t *testing.T) {
		n := &testNode{
			result:  "pass",
			s3Lines: []string{"PutObject → HTTP 200 OK (None)"},
		}
		c := buildCell(n)
		assert.Equal(t, "pass", c.result)
		assert.Equal(t, "200", c.s3Code)
		assert.Empty(t, c.failure)
	})

	t.Run("failing node extracts HTTP code and failure msg", func(t *testing.T) {
		n := &testNode{
			result:  "fail",
			s3Lines: []string{"PutObject → HTTP 412 Precondition Failed (PreconditionFailed)"},
			rawLines: []string{
				"    Error Trace:  foo_test.go:42",
				"    Error:      \tReceived unexpected error:",
			},
		}
		c := buildCell(n)
		assert.Equal(t, "fail", c.result)
		assert.Equal(t, "412", c.s3Code)
		assert.Equal(t, "Received unexpected error:", c.failure)
	})

	t.Run("node with no s3Lines has empty s3Code", func(t *testing.T) {
		n := &testNode{result: "pass"}
		c := buildCell(n)
		assert.Equal(t, "pass", c.result)
		assert.Empty(t, c.s3Code)
	})

	t.Run("skip result", func(t *testing.T) {
		n := &testNode{result: "skip"}
		c := buildCell(n)
		assert.Equal(t, "skip", c.result)
	})
}

// ── cellText ──────────────────────────────────────────────────────────────────

func TestCellText(t *testing.T) {
	tests := []struct {
		name string
		c    cell
		want string
	}{
		{name: "pass with code", c: cell{result: "pass", s3Code: "200"}, want: "✓  200"},
		{name: "pass without code", c: cell{result: "pass"}, want: "✓"},
		{name: "fail with code", c: cell{result: "fail", s3Code: "412"}, want: "✗  412"},
		{name: "fail without code", c: cell{result: "fail"}, want: "✗  ERR"},
		{name: "skip", c: cell{result: "skip"}, want: "~"},
		{name: "empty (not run)", c: cell{}, want: "-"},
		{name: "unknown result", c: cell{result: "other"}, want: "-"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, cellText(tc.c))
		})
	}
}

// ── cellDisplay ───────────────────────────────────────────────────────────────

func TestCellDisplay(t *testing.T) {
	t.Run("no color pads to width", func(t *testing.T) {
		c := cell{result: "pass", s3Code: "200"}
		got := cellDisplay(false, c, 12)
		// "✓  200" is 6 visible chars; should be padded to 12
		assert.Equal(t, 12, len([]rune(got)))
		assert.True(t, strings.HasPrefix(got, "✓  200"))
	})

	t.Run("with color contains ANSI codes", func(t *testing.T) {
		c := cell{result: "pass", s3Code: "200"}
		got := cellDisplay(true, c, 10)
		assert.Contains(t, got, "\033[")
		assert.Contains(t, got, "200")
	})

	t.Run("fail cell colored red", func(t *testing.T) {
		c := cell{result: "fail", s3Code: "412"}
		got := cellDisplay(true, c, 10)
		assert.Contains(t, got, ansiRed)
	})

	t.Run("skip cell colored yellow", func(t *testing.T) {
		c := cell{result: "skip"}
		got := cellDisplay(true, c, 6)
		assert.Contains(t, got, ansiYellow)
	})

	t.Run("no padding when text fills width", func(t *testing.T) {
		c := cell{result: "pass"} // "✓" — 1 rune
		got := cellDisplay(false, c, 1)
		// visLen(1) == width(1), so no padding appended
		assert.Equal(t, "✓", got)
	})
}

// ── buildSubprocessEnv ────────────────────────────────────────────────────────

func TestBuildSubprocessEnv(t *testing.T) {
	result := buildSubprocessEnv("/path/to/config.env")

	// The last entry must always set S3_CONFIG_FILE.
	last := result[len(result)-1]
	assert.Equal(t, "S3_CONFIG_FILE=/path/to/config.env", last)

	// None of the cleared vars should appear in the output.
	for _, entry := range result {
		for _, cleared := range s3ConfigVars {
			if strings.HasPrefix(entry, cleared+"=") && entry != last {
				t.Errorf("cleared variable %q should not appear in env: %s", cleared, entry)
			}
		}
	}
}

// ── cleanPreflightError ───────────────────────────────────────────────────────

func TestCleanPreflightError(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string // substring or exact match
	}{
		{
			name: "strips FAIL lines",
			raw:  "FAIL\nFAIL\tsome/pkg 1.23s\nFATAL bucket not found\n",
			want: "FATAL bucket not found",
		},
		{
			name: "preserves diagnostic lines",
			raw:  "--- FAIL: TestMain (0.01s)\nbucket \"mybucket\" not found: NoSuchBucket\n",
			want: "bucket \"mybucket\" not found: NoSuchBucket",
		},
		{
			name: "multiple diagnostic lines joined with indent",
			raw:  "line one\nline two\n",
			want: "line one\n        line two",
		},
		{
			name: "empty input returns trimmed empty",
			raw:  "   \n  \n",
			want: "",
		},
		{
			// When every line is boilerplate, cleanPreflightError falls back to
			// strings.TrimSpace(raw) rather than returning empty string.
			name: "all boilerplate returns trimmed original",
			raw:  "FAIL\n",
			want: "FAIL",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := cleanPreflightError(tc.raw)
			assert.Equal(t, tc.want, got)
		})
	}
}

// ── parseTestOutput ───────────────────────────────────────────────────────────

// makeTestJSON constructs a minimal `go test -json` event line.
func makeTestJSON(action, testName, output string) string {
	ev := testEvent{Action: action, Test: testName, Output: output}
	b, _ := json.Marshal(ev)
	return string(b)
}

func TestParseTestOutput(t *testing.T) {
	lines := []string{
		makeTestJSON("run", "TestFoo", ""),
		makeTestJSON("run", "TestFoo/SubA", ""),
		makeTestJSON("output", "TestFoo/SubA", "    [S3] PutObject → HTTP 200 OK\n"),
		makeTestJSON("pass", "TestFoo/SubA", ""),
		makeTestJSON("output", "TestFoo", "    some raw line\n"),
		makeTestJSON("pass", "TestFoo", ""),
		// Non-JSON line should be silently skipped.
		"NOT JSON",
	}

	res := &matrixRunResult{
		nodes: make(map[string]*testNode),
	}
	parseTestOutput(bytes.NewReader([]byte(strings.Join(lines, "\n"))), res)

	require.Contains(t, res.nodes, "TestFoo")
	require.Contains(t, res.nodes, "TestFoo/SubA")

	topFoo := res.nodes["TestFoo"]
	assert.Equal(t, "pass", topFoo.result)
	assert.Equal(t, []string{"TestFoo/SubA"}, topFoo.children)

	subA := res.nodes["TestFoo/SubA"]
	assert.Equal(t, "pass", subA.result)
	assert.Equal(t, []string{"PutObject → HTTP 200 OK"}, subA.s3Lines)

	require.Len(t, res.topOrder, 1)
	assert.Equal(t, "TestFoo", res.topOrder[0])
}

func TestParseTestOutput_PackageLevelEventsSkipped(t *testing.T) {
	// Package-level events (Test == "") should be ignored.
	pkgEvent := testEvent{Action: "pass", Package: "github.com/foo/bar"}
	b, _ := json.Marshal(pkgEvent)

	res := &matrixRunResult{nodes: make(map[string]*testNode)}
	parseTestOutput(bytes.NewReader(b), res)

	assert.Empty(t, res.topOrder)
	assert.Empty(t, res.nodes)
}

// ── canonicalTopOrder ─────────────────────────────────────────────────────────

func TestCanonicalTopOrder(t *testing.T) {
	t.Run("single run returns that run's order", func(t *testing.T) {
		runs := []*matrixRunResult{
			{topOrder: []string{"TestA", "TestB", "TestC"}},
		}
		assert.Equal(t, []string{"TestA", "TestB", "TestC"}, canonicalTopOrder(runs))
	})

	t.Run("merges unique names from multiple runs", func(t *testing.T) {
		runs := []*matrixRunResult{
			{topOrder: []string{"TestA", "TestB"}},
			{topOrder: []string{"TestB", "TestC"}},
		}
		got := canonicalTopOrder(runs)
		assert.Equal(t, []string{"TestA", "TestB", "TestC"}, got)
	})

	t.Run("deduplicates within a single run", func(t *testing.T) {
		runs := []*matrixRunResult{
			{topOrder: []string{"TestA", "TestA", "TestB"}},
		}
		got := canonicalTopOrder(runs)
		assert.Equal(t, []string{"TestA", "TestB"}, got)
	})

	t.Run("empty runs returns nil", func(t *testing.T) {
		assert.Nil(t, canonicalTopOrder([]*matrixRunResult{}))
	})
}

// ── canonicalChildren ─────────────────────────────────────────────────────────

func TestCanonicalChildren(t *testing.T) {
	t.Run("children from single run", func(t *testing.T) {
		runs := []*matrixRunResult{
			{
				nodes: map[string]*testNode{
					"TestFoo": {children: []string{"TestFoo/A", "TestFoo/B"}},
				},
			},
		}
		got := canonicalChildren("TestFoo", runs)
		assert.Equal(t, []string{"TestFoo/A", "TestFoo/B"}, got)
	})

	t.Run("merges unique children across runs", func(t *testing.T) {
		runs := []*matrixRunResult{
			{nodes: map[string]*testNode{"TestFoo": {children: []string{"TestFoo/A"}}}},
			{nodes: map[string]*testNode{"TestFoo": {children: []string{"TestFoo/A", "TestFoo/B"}}}},
		}
		got := canonicalChildren("TestFoo", runs)
		assert.Equal(t, []string{"TestFoo/A", "TestFoo/B"}, got)
	})

	t.Run("missing node returns empty", func(t *testing.T) {
		runs := []*matrixRunResult{
			{nodes: map[string]*testNode{}},
		}
		got := canonicalChildren("TestFoo", runs)
		assert.Empty(t, got)
	})
}

// ── matrixShouldDisplay ───────────────────────────────────────────────────────

func TestMatrixShouldDisplay(t *testing.T) {
	re := regexp.MustCompile("PutObject")

	passingRuns := []*matrixRunResult{
		{nodes: map[string]*testNode{"TestPutObjectConditionalWrites": {result: "pass"}}},
	}
	failingRuns := []*matrixRunResult{
		{nodes: map[string]*testNode{"TestEdgeCases": {result: "fail"}}},
	}

	t.Run("no filter always shows", func(t *testing.T) {
		assert.True(t, matrixShouldDisplay("TestAnything", passingRuns, nil))
	})
	t.Run("filter matches — shown", func(t *testing.T) {
		assert.True(t, matrixShouldDisplay("TestPutObjectConditionalWrites", passingRuns, re))
	})
	t.Run("filter no match, all pass — hidden", func(t *testing.T) {
		assert.False(t, matrixShouldDisplay("TestEdgeCases", passingRuns, re))
	})
	t.Run("filter no match but any run fails — shown", func(t *testing.T) {
		assert.True(t, matrixShouldDisplay("TestEdgeCases", failingRuns, re))
	})
}

// ── matrixRenderText ──────────────────────────────────────────────────────────

func buildMatrixPassingRuns() []*matrixRunResult {
	nodesProv1 := map[string]*testNode{
		"TestFoo": {
			name:     "TestFoo",
			result:   "pass",
			children: []string{"TestFoo/Sub"},
		},
		"TestFoo/Sub": {
			name:    "TestFoo/Sub",
			result:  "pass",
			s3Lines: []string{"PutObject → HTTP 200 OK"},
		},
	}
	nodesProv2 := map[string]*testNode{
		"TestFoo": {
			name:     "TestFoo",
			result:   "pass",
			children: []string{"TestFoo/Sub"},
		},
		"TestFoo/Sub": {
			name:    "TestFoo/Sub",
			result:  "pass",
			s3Lines: []string{"PutObject → HTTP 200 OK"},
		},
	}
	return []*matrixRunResult{
		{providerName: "AWS S3", nodes: nodesProv1, topOrder: []string{"TestFoo"}},
		{providerName: "Wasabi", nodes: nodesProv2, topOrder: []string{"TestFoo"}},
	}
}

func TestMatrixRenderText_AllPassing(t *testing.T) {
	runs := buildMatrixPassingRuns()
	topOrder := canonicalTopOrder(runs)

	out := captureStdout(t, func() {
		matrixRenderText(runs, topOrder, nil, false)
	})

	assert.Contains(t, out, "AWS S3")
	assert.Contains(t, out, "Wasabi")
	assert.Contains(t, out, "Foo") // humanTopName("TestFoo")
	assert.Contains(t, out, "Sub")
	assert.Contains(t, out, "TOTAL")
}

func TestMatrixRenderText_HeaderAlignment(t *testing.T) {
	// Render with useColor=false so there are no ANSI codes to strip.
	runs := buildMatrixPassingRuns()
	topOrder := canonicalTopOrder(runs)

	out := captureStdout(t, func() {
		matrixRenderText(runs, topOrder, nil, false)
	})

	// The separator line should be present and consistent.
	assert.Contains(t, out, "─")
	assert.Contains(t, out, "│")

	// Find the first separator line and any group-header row (starts with "  ",
	// contains "│"). Compare their visible rune counts — this catches the ANSI
	// padding bug where invisible escape bytes were counted as visible chars.
	lines := strings.Split(out, "\n")
	var sepRunes int
	for _, line := range lines {
		if strings.HasPrefix(line, "─") {
			sepRunes = utf8.RuneCountInString(line)
			break
		}
	}
	if sepRunes == 0 {
		t.Fatal("no separator line found in output")
	}
	for _, line := range lines {
		if strings.HasPrefix(line, "  ") && strings.Contains(line, "│") {
			headerRunes := utf8.RuneCountInString(line)
			assert.Equal(t, sepRunes, headerRunes,
				"group header rune width (%d) should match separator rune width (%d)\n  sep:    %q\n  header: %q",
				headerRunes, sepRunes, lines[1], line)
			break
		}
	}
}

// stripANSI removes ANSI escape sequences from s.
func stripANSI(s string) string {
	var out strings.Builder
	i := 0
	for i < len(s) {
		if s[i] == '\033' && i+1 < len(s) && s[i+1] == '[' {
			// Skip until 'm'
			for i < len(s) && s[i] != 'm' {
				i++
			}
			i++ // skip 'm'
			continue
		}
		out.WriteByte(s[i])
		i++
	}
	return out.String()
}

// ── matrixRenderJSON ──────────────────────────────────────────────────────────

func TestMatrixRenderJSON_AllPassing(t *testing.T) {
	runs := buildMatrixPassingRuns()
	topOrder := canonicalTopOrder(runs)

	out := captureStdout(t, func() {
		matrixRenderJSON(runs, topOrder, nil)
	})

	var m jsonMatrix
	require.NoError(t, json.Unmarshal([]byte(out), &m))

	assert.Equal(t, []string{"AWS S3", "Wasabi"}, m.Providers)
	require.Len(t, m.Groups, 1)
	assert.Equal(t, "TestFoo", m.Groups[0].Name)
	assert.Equal(t, "Foo", m.Groups[0].Display)

	require.Len(t, m.Groups[0].Tests, 1)
	test := m.Groups[0].Tests[0]
	assert.Equal(t, "TestFoo/Sub", test.Name)
	assert.Equal(t, "Sub", test.Display)
	assert.Equal(t, "pass", test.Results["AWS S3"].Result)
	assert.Equal(t, "pass", test.Results["Wasabi"].Result)
}

func TestMatrixRenderJSON_WithFilter(t *testing.T) {
	runs := buildMatrixPassingRuns()
	// Add a second group that won't match the filter.
	for _, r := range runs {
		r.nodes["TestOther"] = &testNode{name: "TestOther", result: "pass"}
		r.topOrder = append(r.topOrder, "TestOther")
	}
	topOrder := canonicalTopOrder(runs)

	filterRe := regexp.MustCompile("Foo")

	out := captureStdout(t, func() {
		matrixRenderJSON(runs, topOrder, filterRe)
	})

	var m jsonMatrix
	require.NoError(t, json.Unmarshal([]byte(out), &m))

	for _, g := range m.Groups {
		assert.NotEqual(t, "TestOther", g.Name,
			"TestOther should be hidden by filter")
	}
}
