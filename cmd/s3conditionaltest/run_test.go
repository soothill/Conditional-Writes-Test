package main

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── runShouldDisplay ──────────────────────────────────────────────────────────

func TestRunShouldDisplay(t *testing.T) {
	re := regexp.MustCompile("Conditional")

	tests := []struct {
		name     string
		topName  string
		result   string
		filterRe *regexp.Regexp
		want     bool
	}{
		{name: "fail always shown", topName: "TestFoo", result: "fail", filterRe: re, want: true},
		{name: "pass with no filter shown", topName: "TestFoo", result: "pass", filterRe: nil, want: true},
		{name: "pass matching filter shown", topName: "TestConditionalWrites", result: "pass", filterRe: re, want: true},
		{name: "pass not matching filter hidden", topName: "TestEdgeCases", result: "pass", filterRe: re, want: false},
		{name: "skip with no filter shown", topName: "TestFoo", result: "skip", filterRe: nil, want: true},
		{name: "skip matching filter shown", topName: "TestConditional", result: "skip", filterRe: re, want: true},
		{name: "skip not matching filter hidden", topName: "TestOther", result: "skip", filterRe: re, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := runShouldDisplay(tc.topName, tc.result, tc.filterRe)
			assert.Equal(t, tc.want, got)
		})
	}
}

// ── runResultIcon ─────────────────────────────────────────────────────────────

func TestRunResultIcon(t *testing.T) {
	tests := []struct {
		result    string
		wantPlain string // expected when useColor=false
	}{
		{result: "pass", wantPlain: "✓"},
		{result: "fail", wantPlain: "✗"},
		{result: "skip", wantPlain: "~"},
		{result: "", wantPlain: "?"},
		{result: "unknown", wantPlain: "?"},
	}

	for _, tc := range tests {
		t.Run(tc.result+"_noColor", func(t *testing.T) {
			got := runResultIcon(tc.result, false)
			assert.Equal(t, tc.wantPlain, got)
		})
		t.Run(tc.result+"_withColor", func(t *testing.T) {
			got := runResultIcon(tc.result, true)
			assert.Contains(t, got, tc.wantPlain, "color output should contain the icon character")
			assert.Contains(t, got, "\033[", "color output should contain an ANSI escape sequence")
		})
	}
}

// ── runResultLabel ────────────────────────────────────────────────────────────

func TestRunResultLabel(t *testing.T) {
	tests := []struct {
		result    string
		wantPlain string
	}{
		{result: "pass", wantPlain: "PASS"},
		{result: "fail", wantPlain: "FAIL"},
		{result: "skip", wantPlain: "SKIP"},
		{result: "", wantPlain: "????"},
		{result: "unknown", wantPlain: "????"},
	}

	for _, tc := range tests {
		t.Run(tc.result+"_noColor", func(t *testing.T) {
			got := runResultLabel(tc.result, false)
			assert.Equal(t, tc.wantPlain, got)
		})
		t.Run(tc.result+"_withColor", func(t *testing.T) {
			got := runResultLabel(tc.result, true)
			if tc.wantPlain == "????" {
				// unknown result: no ANSI wrapping
				assert.Equal(t, "????", got)
			} else {
				assert.Contains(t, got, tc.wantPlain)
				assert.Contains(t, got, "\033[")
			}
		})
	}
}

// ── nullSlice ─────────────────────────────────────────────────────────────────

func TestNullSlice(t *testing.T) {
	t.Run("nil input returns nil", func(t *testing.T) {
		assert.Nil(t, nullSlice(nil))
	})
	t.Run("empty slice returns nil", func(t *testing.T) {
		assert.Nil(t, nullSlice([]string{}))
	})
	t.Run("non-empty slice returns same slice", func(t *testing.T) {
		s := []string{"a", "b"}
		got := nullSlice(s)
		assert.Equal(t, s, got)
	})
}

// ── runRenderText ─────────────────────────────────────────────────────────────

// buildPassingNodes is a test helper that constructs a minimal passing node
// set for use by render functions.
func buildPassingNodes() (map[string]*testNode, []string) {
	top := &testNode{
		name:    "TestPutObjectConditionalWrites",
		result:  "pass",
		elapsed: 0.12,
		children: []string{
			"TestPutObjectConditionalWrites/IfNoneMatch/NewKey",
			"TestPutObjectConditionalWrites/IfNoneMatch/ExistingKey",
		},
	}
	child1 := &testNode{
		name:    "TestPutObjectConditionalWrites/IfNoneMatch/NewKey",
		result:  "pass",
		elapsed: 0.05,
		s3Lines: []string{"PutObject → HTTP 200 OK"},
	}
	child2 := &testNode{
		name:    "TestPutObjectConditionalWrites/IfNoneMatch/ExistingKey",
		result:  "pass",
		elapsed: 0.07,
		s3Lines: []string{"PutObject → HTTP 412 Precondition Failed"},
	}
	nodes := map[string]*testNode{
		top.name:    top,
		child1.name: child1,
		child2.name: child2,
	}
	topOrder := []string{top.name}
	return nodes, topOrder
}

func TestRunRenderText_AllPassing(t *testing.T) {
	nodes, topOrder := buildPassingNodes()

	out := captureStdout(t, func() {
		runRenderText(nodes, topOrder, 0.12, false, nil, false)
	})

	assert.Contains(t, out, "TestPutObjectConditionalWrites")
	assert.Contains(t, out, "PASS")
	assert.Contains(t, out, "NewKey")
	assert.Contains(t, out, "ExistingKey")
	assert.Contains(t, out, "1 passed")
	assert.Contains(t, out, "0 failed")
}

func TestRunRenderText_WithFilter(t *testing.T) {
	nodes, topOrder := buildPassingNodes()

	// Add a second top-level test that does NOT match the filter.
	nodes["TestEdgeCases"] = &testNode{name: "TestEdgeCases", result: "pass", elapsed: 0.01}
	topOrder = append(topOrder, "TestEdgeCases")

	filterRe := regexp.MustCompile("PutObject")
	out := captureStdout(t, func() {
		runRenderText(nodes, topOrder, 0.13, false, filterRe, false)
	})

	assert.Contains(t, out, "TestPutObjectConditionalWrites")
	assert.NotContains(t, out, "TestEdgeCases")
	// Both tests are counted in the summary even though one is filtered.
	assert.Contains(t, out, "2 passed")
}

func TestRunRenderText_PKGElapsedInSummary(t *testing.T) {
	nodes, topOrder := buildPassingNodes()

	out := captureStdout(t, func() {
		runRenderText(nodes, topOrder, 2.50, false, nil, false)
	})

	assert.Contains(t, out, "total: 2.50s")
}

func TestRunRenderText_SkippedInSummary(t *testing.T) {
	nodes := map[string]*testNode{
		"TestSkipped": {name: "TestSkipped", result: "skip", elapsed: 0.01},
	}
	topOrder := []string{"TestSkipped"}

	out := captureStdout(t, func() {
		runRenderText(nodes, topOrder, 0.01, false, nil, false)
	})

	assert.Contains(t, out, "1 skipped")
}

// ── runRenderJSON ─────────────────────────────────────────────────────────────

func TestRunRenderJSON_AllPassing(t *testing.T) {
	nodes, topOrder := buildPassingNodes()

	out := captureStdout(t, func() {
		runRenderJSON(nodes, topOrder, nil)
	})

	var results []runJSONResult
	require.NoError(t, json.Unmarshal([]byte(out), &results))

	// The top node has children, so only children appear in the output.
	require.Len(t, results, 2)

	names := []string{results[0].Test, results[1].Test}
	assert.Contains(t, names, "TestPutObjectConditionalWrites/IfNoneMatch/NewKey")
	assert.Contains(t, names, "TestPutObjectConditionalWrites/IfNoneMatch/ExistingKey")

	for _, r := range results {
		assert.Equal(t, "pass", r.Result)
		assert.NotNil(t, r.S3Responses)
	}
}

func TestRunRenderJSON_WithFilter(t *testing.T) {
	nodes, topOrder := buildPassingNodes()
	nodes["TestEdgeCases"] = &testNode{name: "TestEdgeCases", result: "pass", elapsed: 0.01}
	topOrder = append(topOrder, "TestEdgeCases")

	filterRe := regexp.MustCompile("PutObject")

	out := captureStdout(t, func() {
		runRenderJSON(nodes, topOrder, filterRe)
	})

	var results []runJSONResult
	require.NoError(t, json.Unmarshal([]byte(out), &results))

	for _, r := range results {
		assert.True(t, strings.Contains(r.Test, "PutObject"),
			"expected only PutObject tests in filtered output, got %q", r.Test)
	}
}

func TestRunRenderJSON_NoChildrenTopLevel(t *testing.T) {
	// A top-level test with no children should appear directly in JSON output.
	nodes := map[string]*testNode{
		"TestSomething": {
			name:    "TestSomething",
			result:  "pass",
			elapsed: 0.03,
			s3Lines: []string{"GetObject → HTTP 200 OK"},
		},
	}
	topOrder := []string{"TestSomething"}

	out := captureStdout(t, func() {
		runRenderJSON(nodes, topOrder, nil)
	})

	var results []runJSONResult
	require.NoError(t, json.Unmarshal([]byte(out), &results))
	require.Len(t, results, 1)
	assert.Equal(t, "TestSomething", results[0].Test)
	assert.Equal(t, "pass", results[0].Result)
}
