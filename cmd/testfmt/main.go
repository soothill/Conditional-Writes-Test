// cmd/testfmt formats the output of `go test -json` into a readable summary
// that shows each test alongside its S3 HTTP response. It can also emit the
// same data as a JSON array for machine consumption.
//
// Usage:
//
//	go test -tags integration -v -json ./s3test/ | go run ./cmd/testfmt
//	go test -tags integration -v -json ./s3test/ | go run ./cmd/testfmt --format=json
//
// Flags:
//
//	--format=text|json   Output format (default: text)
//	--filter=<regex>     Only display test groups whose name matches this regex.
//	                     Failing groups are always displayed regardless of the
//	                     filter. The summary counts all tests. Useful to suppress
//	                     unit-test noise when running integration tests.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// testEvent mirrors one line of `go test -json` output.
type testEvent struct {
	Time    time.Time `json:"Time"`
	Action  string    `json:"Action"`
	Package string    `json:"Package"`
	Test    string    `json:"Test"`
	Elapsed float64   `json:"Elapsed"`
	Output  string    `json:"Output"`
}

// testNode holds the collected state for one test or sub-test.
type testNode struct {
	name     string   // full name, e.g. "TestFoo/Bar"
	result   string   // "pass", "fail", "skip", or "" if still running
	elapsed  float64  // seconds
	s3Lines  []string // log lines after "[S3] " prefix, in order
	rawLines []string // other output lines (used to extract failure messages)
	children []string // full names of direct sub-tests, in insertion order
}

// boilerplatePrefixes are prefixes that, when found on a trimmed output line,
// mark it as testing/testify framework noise we want to suppress.
var boilerplatePrefixes = []string{
	"Error Trace:", "Test:", "Messages:",
	"--- PASS", "--- FAIL",
	"=== RUN", "=== PAUSE", "=== CONT",
	"PASS", "FAIL",
	"ok  \t", "?   \t",
}

func isBoilerplate(line string) bool {
	t := strings.TrimSpace(line)
	for _, p := range boilerplatePrefixes {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}

// extractFailureMsg returns a short, clean failure description from raw output
// lines collected during a test run. It tries to find the testify "Error:"
// label first, falling back to the first non-boilerplate non-empty line.
func extractFailureMsg(lines []string) string {
	for _, line := range lines {
		t := strings.TrimSpace(line)
		// testify formats this as: "Error:      \tReceived unexpected error:"
		if strings.HasPrefix(t, "Error:") && !strings.HasPrefix(t, "Error Trace:") {
			msg := strings.TrimSpace(strings.TrimPrefix(t, "Error:"))
			if msg != "" {
				return msg
			}
		}
	}
	// Fallback: first non-boilerplate non-empty line
	for _, line := range lines {
		t := strings.TrimSpace(line)
		if t != "" && !isBoilerplate(t) {
			return t
		}
	}
	return ""
}

// humanName converts the last component of a test name to a human-readable
// form by replacing underscores with spaces (Go replaces spaces with
// underscores when building sub-test names).
func humanName(fullName string) string {
	s := fullName
	if i := strings.LastIndex(s, "/"); i >= 0 {
		s = s[i+1:]
	}
	return strings.ReplaceAll(s, "_", " ")
}

// ── ANSI colour helpers ────────────────────────────────────────────────────

const (
	ansiReset  = "\033[0m"
	ansiGreen  = "\033[32m"
	ansiRed    = "\033[31m"
	ansiYellow = "\033[33m"
	ansiBold   = "\033[1m"
	ansiDim    = "\033[2m"
	ansiCyan   = "\033[36m"
)

// isColorEnabled returns true when ANSI colour output is appropriate for
// stdout (terminal attached, NO_COLOR unset, TERM≠dumb).
func isColorEnabled() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if os.Getenv("TERM") == "dumb" {
		return false
	}
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

func colored(useColor bool, code, s string) string {
	if !useColor {
		return s
	}
	return code + s + ansiReset
}

// padRight pads (or truncates with an ellipsis) s to exactly n visible runes.
func padRight(s string, n int) string {
	count := utf8.RuneCountInString(s)
	if count >= n {
		runes := []rune(s)
		return string(runes[:n-1]) + "…"
	}
	return s + strings.Repeat(" ", n-count)
}

// ── JSON output schema ─────────────────────────────────────────────────────

type jsonResult struct {
	Test        string   `json:"test"`
	Result      string   `json:"result"`
	ElapsedMS   int64    `json:"elapsed_ms"`
	S3Responses []string `json:"s3_responses"`
	Failure     string   `json:"failure,omitempty"`
}

// ── main ───────────────────────────────────────────────────────────────────

func main() {
	format := flag.String("format", "text", "Output format: text or json")
	filterStr := flag.String("filter", "",
		"Only display test groups matching this regex; failures always shown regardless")
	flag.Parse()

	// Compile the filter regex, if one was provided.
	var filterRe *regexp.Regexp
	if *filterStr != "" {
		var err error
		filterRe, err = regexp.Compile(*filterStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid --filter regex: %v\n", err)
			os.Exit(1)
		}
	}

	useColor := isColorEnabled() && *format == "text"

	nodes := make(map[string]*testNode)
	var topOrder []string // ordered top-level test names
	var pkgLines []string // package-level output (e.g. compile errors)
	pkgFailed := false
	var pkgElapsed float64

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1<<20), 1<<20) // 1 MB — handles long output lines

	for scanner.Scan() {
		var ev testEvent
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			continue
		}

		if ev.Test == "" {
			// Package-level event.
			switch ev.Action {
			case "fail":
				pkgFailed = true
				pkgElapsed = ev.Elapsed
			case "pass":
				pkgElapsed = ev.Elapsed
			case "output":
				line := strings.TrimRight(ev.Output, "\n")
				if line != "" {
					pkgLines = append(pkgLines, line)
				}
			}
			continue
		}

		n, exists := nodes[ev.Test]
		if !exists {
			n = &testNode{name: ev.Test}
			nodes[ev.Test] = n
			if !strings.Contains(ev.Test, "/") {
				topOrder = append(topOrder, ev.Test)
			} else {
				parentName := ev.Test[:strings.LastIndex(ev.Test, "/")]
				if parent, ok := nodes[parentName]; ok {
					parent.children = append(parent.children, ev.Test)
				}
				// If parent hasn't appeared yet (shouldn't happen with go test
				// -json — parent "run" always precedes child "run"), we skip
				// adding the child to any parent, but it still gets recorded.
			}
		}

		switch ev.Action {
		case "pass", "fail", "skip":
			n.result = ev.Action
			n.elapsed = ev.Elapsed
		case "output":
			line := strings.TrimRight(ev.Output, "\n")
			if idx := strings.Index(line, "[S3] "); idx >= 0 {
				n.s3Lines = append(n.s3Lines, line[idx+5:])
			} else {
				n.rawLines = append(n.rawLines, line)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "read error: %v\n", err)
		os.Exit(1)
	}

	// If no test events were collected, the package likely failed to compile.
	// Print any package-level output and exit.
	if len(topOrder) == 0 {
		for _, line := range pkgLines {
			if !isBoilerplate(line) {
				fmt.Fprintln(os.Stderr, line)
			}
		}
		if pkgFailed {
			os.Exit(1)
		}
		return
	}

	if *format == "json" {
		renderJSON(nodes, topOrder, filterRe)
	} else {
		renderText(nodes, topOrder, pkgElapsed, pkgFailed, filterRe, useColor)
	}
}

// shouldDisplay returns true when a top-level test group should be rendered.
// A group is always shown when it failed (so failures are never hidden), or
// when no filter is active, or when its name matches the filter regex.
func shouldDisplay(topName string, result string, filterRe *regexp.Regexp) bool {
	if result == "fail" {
		return true // failures are always shown, filter or not
	}
	if filterRe == nil {
		return true // no filter — show everything
	}
	return filterRe.MatchString(topName)
}

// ── text renderer ──────────────────────────────────────────────────────────

func resultIcon(result string, useColor bool) string {
	switch result {
	case "pass":
		return colored(useColor, ansiGreen, "✓")
	case "fail":
		return colored(useColor, ansiRed, "✗")
	case "skip":
		return colored(useColor, ansiYellow, "~")
	default:
		return colored(useColor, ansiYellow, "?")
	}
}

func resultLabel(result string, useColor bool) string {
	switch result {
	case "pass":
		return colored(useColor, ansiGreen, "PASS")
	case "fail":
		return colored(useColor, ansiRed, "FAIL")
	case "skip":
		return colored(useColor, ansiYellow, "SKIP")
	default:
		return "????"
	}
}

func renderText(
	nodes map[string]*testNode,
	topOrder []string,
	pkgElapsed float64,
	pkgFailed bool,
	filterRe *regexp.Regexp,
	useColor bool,
) {
	const (
		nameColWidth = 48 // visible characters for the sub-test name column
		timeColWidth = 7  // "99.99s" + space
	)

	var passed, failed, skipped int

	for _, topName := range topOrder {
		top := nodes[topName]
		if top == nil {
			continue
		}

		// Always tally — the summary counts every test even if not displayed.
		switch top.result {
		case "pass":
			passed++
		case "fail":
			failed++
		case "skip":
			skipped++
		}

		// Skip display if the filter is active and this group passes/skips.
		if !shouldDisplay(topName, top.result, filterRe) {
			continue
		}

		// ── Group header ─────────────────────────────────────────────────
		label := resultLabel(top.result, useColor)
		elapsed := fmt.Sprintf("%.2fs", top.elapsed)
		fmt.Printf("%s  %s  %s\n",
			colored(useColor, ansiBold, padRight(topName, 54)),
			label,
			colored(useColor, ansiDim, elapsed),
		)

		// ── Sub-tests ─────────────────────────────────────────────────────
		for _, childName := range top.children {
			child := nodes[childName]
			if child == nil {
				continue
			}

			icon := resultIcon(child.result, useColor)
			short := humanName(childName)
			childElapsed := fmt.Sprintf("%.2fs", child.elapsed)

			// Use the last [S3] log line as the "response" column.
			var s3Info string
			if len(child.s3Lines) > 0 {
				s3Info = child.s3Lines[len(child.s3Lines)-1]
			}

			if s3Info != "" {
				fmt.Printf("  %s  %s  %s  %s\n",
					icon,
					padRight(short, nameColWidth),
					colored(useColor, ansiDim, fmt.Sprintf("%*s", timeColWidth, childElapsed)),
					colored(useColor, ansiCyan, s3Info),
				)
			} else {
				fmt.Printf("  %s  %s  %s\n",
					icon,
					padRight(short, nameColWidth),
					colored(useColor, ansiDim, fmt.Sprintf("%*s", timeColWidth, childElapsed)),
				)
			}

			// Show a single-line failure reason for failing sub-tests.
			if child.result == "fail" {
				msg := extractFailureMsg(child.rawLines)
				if msg != "" {
					const maxMsg = 90
					if utf8.RuneCountInString(msg) > maxMsg {
						runes := []rune(msg)
						msg = string(runes[:maxMsg-1]) + "…"
					}
					fmt.Printf("     %s\n", colored(useColor, ansiRed, msg))
				}
			}
		}

		// ── Top-level test with no sub-tests ──────────────────────────────
		if len(top.children) == 0 {
			var s3Info string
			if len(top.s3Lines) > 0 {
				s3Info = top.s3Lines[len(top.s3Lines)-1]
			}
			if s3Info != "" {
				fmt.Printf("       %s\n", colored(useColor, ansiCyan, s3Info))
			}
			if top.result == "fail" {
				msg := extractFailureMsg(top.rawLines)
				if msg != "" {
					const maxMsg = 90
					if utf8.RuneCountInString(msg) > maxMsg {
						runes := []rune(msg)
						msg = string(runes[:maxMsg-1]) + "…"
					}
					fmt.Printf("     %s\n", colored(useColor, ansiRed, msg))
				}
			}
		}

		fmt.Println()
	}

	// ── Summary ───────────────────────────────────────────────────────────
	hr := strings.Repeat("─", 70)
	fmt.Println(colored(useColor, ansiDim, hr))

	summary := fmt.Sprintf("  %d passed  %d failed", passed, failed)
	if skipped > 0 {
		summary += fmt.Sprintf("  %d skipped", skipped)
	}
	if pkgElapsed > 0 {
		summary += fmt.Sprintf("  total: %.2fs", pkgElapsed)
	}

	if failed > 0 || pkgFailed {
		fmt.Println(colored(useColor, ansiRed, summary))
		os.Exit(1)
	}
	fmt.Println(colored(useColor, ansiGreen, summary))
}

// ── JSON renderer ──────────────────────────────────────────────────────────

func renderJSON(nodes map[string]*testNode, topOrder []string, filterRe *regexp.Regexp) {
	var results []jsonResult

	for _, topName := range topOrder {
		top := nodes[topName]
		if top == nil {
			continue
		}

		// Apply the same filter logic as the text renderer.
		if !shouldDisplay(topName, top.result, filterRe) {
			continue
		}

		if len(top.children) == 0 {
			// Top-level test with no sub-tests — emit as a single entry.
			results = append(results, jsonResult{
				Test:        topName,
				Result:      top.result,
				ElapsedMS:   int64(top.elapsed * 1000),
				S3Responses: nullSlice(top.s3Lines),
				Failure:     extractFailureMsg(top.rawLines),
			})
		} else {
			// Emit one entry per sub-test.
			for _, childName := range top.children {
				child := nodes[childName]
				if child == nil {
					continue
				}
				results = append(results, jsonResult{
					Test:        childName,
					Result:      child.result,
					ElapsedMS:   int64(child.elapsed * 1000),
					S3Responses: nullSlice(child.s3Lines),
					Failure:     extractFailureMsg(child.rawLines),
				})
			}
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		fmt.Fprintf(os.Stderr, "json encode: %v\n", err)
		os.Exit(1)
	}
}

// nullSlice returns nil when s is empty so that JSON encodes the field as
// null rather than []. Keeps the output clean when there are no S3 responses.
func nullSlice(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}
