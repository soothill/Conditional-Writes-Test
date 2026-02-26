// cmd/s3conditionaltest is a unified CLI tool for the Conditional-Writes-Test
// suite. It provides two subcommands:
//
//	run     – Format the output of `go test -json` into a readable summary.
//	          Reads from stdin; pipe `go test ... -json ./s3test/` into it.
//
//	matrix  – Run the integration tests against every provider defined in a
//	          matrix config file and print a side-by-side comparison table.
//
// Usage:
//
//	go test -tags integration -v -count=1 -json ./s3test/ | s3conditionaltest run [--format=text|json] [--filter=<regex>]
//	s3conditionaltest matrix [--config=testmatrix.json] [--format=text|json] [--filter=<regex>] [--timeout=10m]
package main

import (
	"fmt"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

// ── Shared types ───────────────────────────────────────────────────────────────

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
	name     string
	result   string   // "pass", "fail", "skip", or "" if still running
	elapsed  float64  // seconds
	s3Lines  []string // log lines after "[S3] " prefix, in order
	rawLines []string // other output lines (used to extract failure messages)
	children []string // full names of direct sub-tests, in insertion order
}

// ── Shared helpers ─────────────────────────────────────────────────────────────

// boilerplatePrefixes marks testing/testify framework noise to suppress.
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

// extractFailureMsg returns a short failure description from raw output lines.
// It looks for testify's "Error:" label first, then falls back to the first
// non-boilerplate non-empty line.
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
	for _, line := range lines {
		t := strings.TrimSpace(line)
		if t != "" && !isBoilerplate(t) {
			return t
		}
	}
	return ""
}

// humanSubName takes the last component of a sub-test name and converts
// underscores to spaces (Go replaces spaces with underscores in sub-test names).
func humanSubName(fullName string) string {
	s := fullName
	if i := strings.LastIndex(s, "/"); i >= 0 {
		s = s[i+1:]
	}
	return strings.ReplaceAll(s, "_", " ")
}

// humanTopName strips the "Test" prefix from a top-level test name for display.
func humanTopName(name string) string {
	return strings.TrimPrefix(name, "Test")
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

// ── ANSI colour helpers ────────────────────────────────────────────────────────

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

// ── main ───────────────────────────────────────────────────────────────────────

func printUsage() {
	fmt.Fprint(os.Stderr, `s3conditionaltest — S3 conditional-write test suite CLI

Usage:
  s3conditionaltest <subcommand> [flags]

Subcommands:
  run     Format the output of 'go test -json' into a readable summary.
          Reads from stdin; pipe 'go test ... -json ./s3test/' into it.

  matrix  Run integration tests against multiple S3 providers and print
          a side-by-side comparison table.

  help    Show this help message.

Run 's3conditionaltest <subcommand> --help' for subcommand-specific flags.
`)
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "run":
		runMain(os.Args[2:])
	case "matrix":
		matrixMain(os.Args[2:])
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %q\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}
