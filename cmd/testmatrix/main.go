// cmd/testmatrix runs the S3 integration test suite against every provider
// listed in a matrix config file and prints a side-by-side comparison table
// showing which tests pass or fail on each provider, together with the HTTP
// response code returned by the endpoint.
//
// Usage:
//
//	go run ./cmd/testmatrix                         # uses testmatrix.json
//	go run ./cmd/testmatrix --config=my-matrix.json
//	go run ./cmd/testmatrix --format=json
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// ── Config ────────────────────────────────────────────────────────────────────

// matrixConfig is the structure of testmatrix.json.
type matrixConfig struct {
	// Filter is an optional regex applied to top-level test names. Groups that
	// don't match are still run and counted but are hidden from the table when
	// they all pass (same semantics as cmd/testfmt --filter). Override at the
	// command line with --filter.
	Filter    string        `json:"filter"`
	Providers []providerDef `json:"providers"`
}

type providerDef struct {
	Name   string `json:"name"`
	Config string `json:"config"` // path to .env file for this provider
}

// ── Test output types ─────────────────────────────────────────────────────────

type testEvent struct {
	Time    time.Time `json:"Time"`
	Action  string    `json:"Action"`
	Package string    `json:"Package"`
	Test    string    `json:"Test"`
	Elapsed float64   `json:"Elapsed"`
	Output  string    `json:"Output"`
}

type testNode struct {
	name     string
	result   string   // "pass", "fail", "skip", or "" if still running
	elapsed  float64
	s3Lines  []string // [S3] log lines (after the prefix)
	rawLines []string // other output lines
	children []string // full names of direct sub-tests, in order
}

// ── Run result ────────────────────────────────────────────────────────────────

type runResult struct {
	providerName string
	nodes        map[string]*testNode
	topOrder     []string // top-level test names in run order
	runErr       string   // non-empty if the entire run failed
}

// ── Cell ──────────────────────────────────────────────────────────────────────

// cell represents the content of one table cell (one test × one provider).
type cell struct {
	result  string // "pass", "fail", "skip", or ""
	s3Code  string // HTTP status code, e.g. "200", "412"
	failure string // short failure message (for failed cells)
}

func buildCell(n *testNode) cell {
	if n == nil {
		return cell{}
	}
	c := cell{result: n.result}
	if len(n.s3Lines) > 0 {
		last := n.s3Lines[len(n.s3Lines)-1]
		// Format: "PutObject → HTTP 412 Precondition Failed (PreconditionFailed)"
		if i := strings.Index(last, "HTTP "); i >= 0 {
			if fields := strings.Fields(last[i+5:]); len(fields) > 0 {
				c.s3Code = fields[0]
			}
		}
	}
	if n.result == "fail" {
		c.failure = extractFailureMsg(n.rawLines)
	}
	return c
}

// cellText returns the display text for a cell (without ANSI codes).
//
//	pass → "✓  200"  or  "✓"
//	fail → "✗  412"  or  "✗  ERR"
//	skip → "~"
//	    → "-"         (not run / no result)
func cellText(c cell) string {
	switch c.result {
	case "pass":
		if c.s3Code != "" {
			return "✓  " + c.s3Code
		}
		return "✓"
	case "fail":
		if c.s3Code != "" {
			return "✗  " + c.s3Code
		}
		return "✗  ERR"
	case "skip":
		return "~"
	default:
		return "-"
	}
}

// cellDisplay returns the cell text padded to `width` visible characters.
// ANSI colour codes are applied after padding so they don't throw off alignment.
func cellDisplay(useColor bool, c cell, width int) string {
	txt := cellText(c)
	visLen := utf8.RuneCountInString(txt)

	var out string
	switch c.result {
	case "pass":
		out = colored(useColor, ansiGreen, txt)
	case "fail":
		out = colored(useColor, ansiRed, txt)
	case "skip":
		out = colored(useColor, ansiYellow, txt)
	default:
		out = colored(useColor, ansiDim, txt)
	}

	if visLen < width {
		out += strings.Repeat(" ", width-visLen)
	}
	return out
}

// ── Helpers ───────────────────────────────────────────────────────────────────

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

func extractFailureMsg(lines []string) string {
	for _, line := range lines {
		t := strings.TrimSpace(line)
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

// humanTopName strips the "Test" prefix from a top-level test name.
func humanTopName(name string) string {
	return strings.TrimPrefix(name, "Test")
}

// humanSubName takes the last component of a sub-test name and converts
// underscores to spaces.
func humanSubName(fullName string) string {
	s := fullName
	if i := strings.LastIndex(s, "/"); i >= 0 {
		s = s[i+1:]
	}
	return strings.ReplaceAll(s, "_", " ")
}

func padRight(s string, n int) string {
	count := utf8.RuneCountInString(s)
	if count >= n {
		runes := []rune(s)
		return string(runes[:n-1]) + "…"
	}
	return s + strings.Repeat(" ", n-count)
}

// ── ANSI colour ───────────────────────────────────────────────────────────────

const (
	ansiReset  = "\033[0m"
	ansiGreen  = "\033[32m"
	ansiRed    = "\033[31m"
	ansiYellow = "\033[33m"
	ansiBold   = "\033[1m"
	ansiDim    = "\033[2m"
)

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

// ── Subprocess runner ─────────────────────────────────────────────────────────

// s3ConfigVars are cleared from the subprocess environment before setting
// S3_CONFIG_FILE, so the provider's config file is the sole source of
// S3 configuration regardless of what is set in the host environment.
var s3ConfigVars = []string{
	"S3_BUCKET", "S3_ENDPOINT", "S3_PATH_STYLE", "S3_CONFIG_FILE",
	"AWS_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
}

func buildSubprocessEnv(configFile string) []string {
	clear := make(map[string]bool, len(s3ConfigVars))
	for _, v := range s3ConfigVars {
		clear[v] = true
	}
	var env []string
	for _, e := range os.Environ() {
		if idx := strings.Index(e, "="); idx >= 0 && !clear[e[:idx]] {
			env = append(env, e)
		}
	}
	return append(env, "S3_CONFIG_FILE="+configFile)
}

func runProvider(p providerDef) *runResult {
	res := &runResult{
		providerName: p.Name,
		nodes:        make(map[string]*testNode),
	}

	if _, err := os.Stat(p.Config); err != nil {
		res.runErr = fmt.Sprintf("config file not found: %s", p.Config)
		return res
	}

	absConfig, err := filepath.Abs(p.Config)
	if err != nil {
		absConfig = p.Config
	}

	cmd := exec.Command("go", "test",
		"-tags", "integration",
		"-v", "-count=1",
		"-json",
		"-timeout", "10m",
		"./s3test/",
	)
	cmd.Env = buildSubprocessEnv(absConfig)

	out, execErr := cmd.Output()
	if execErr != nil {
		if exitErr, ok := execErr.(*exec.ExitError); ok {
			if len(out) == 0 {
				// No JSON output: preflight failure, compile error, etc.
				msg := strings.TrimSpace(string(exitErr.Stderr))
				if msg == "" {
					msg = execErr.Error()
				}
				res.runErr = msg
				return res
			}
			// Non-zero exit with JSON output is normal (test failures). Parse it.
		} else {
			res.runErr = execErr.Error()
			return res
		}
	}

	parseTestOutput(bytes.NewReader(out), res)
	return res
}

func parseTestOutput(r io.Reader, res *runResult) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		var ev testEvent
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			continue
		}
		if ev.Test == "" {
			continue
		}

		n, exists := res.nodes[ev.Test]
		if !exists {
			n = &testNode{name: ev.Test}
			res.nodes[ev.Test] = n
			if !strings.Contains(ev.Test, "/") {
				res.topOrder = append(res.topOrder, ev.Test)
			} else {
				parentName := ev.Test[:strings.LastIndex(ev.Test, "/")]
				if parent, ok := res.nodes[parentName]; ok {
					parent.children = append(parent.children, ev.Test)
				}
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
}

// ── Test ordering ─────────────────────────────────────────────────────────────

// canonicalTopOrder returns a stable, deduplicated list of top-level test names
// across all runs, using the first provider's order as the primary order and
// appending any names only seen in later runs.
func canonicalTopOrder(runs []*runResult) []string {
	seen := make(map[string]bool)
	var order []string
	for _, r := range runs {
		for _, name := range r.topOrder {
			if !seen[name] {
				seen[name] = true
				order = append(order, name)
			}
		}
	}
	return order
}

// canonicalChildren returns the deduplicated, stable list of children for a
// top-level test, merging across all runs.
func canonicalChildren(topName string, runs []*runResult) []string {
	seen := make(map[string]bool)
	var order []string
	for _, r := range runs {
		if n, ok := r.nodes[topName]; ok {
			for _, child := range n.children {
				if !seen[child] {
					seen[child] = true
					order = append(order, child)
				}
			}
		}
	}
	return order
}

// ── Visibility filter ─────────────────────────────────────────────────────────

// shouldDisplay returns true when a top-level test group should be rendered.
// A failing group is always shown; passing groups are hidden when a filter is
// active and their name doesn't match.
func shouldDisplay(topName string, runs []*runResult, filterRe *regexp.Regexp) bool {
	if filterRe == nil {
		return true
	}
	if filterRe.MatchString(topName) {
		return true
	}
	// Always surface failures even when the group doesn't match the filter.
	for _, r := range runs {
		if n, ok := r.nodes[topName]; ok && n.result == "fail" {
			return true
		}
	}
	return false
}

// ── Text renderer ─────────────────────────────────────────────────────────────

const testColWidth = 52 // visible chars for the test-name column

func renderText(runs []*runResult, topOrder []string, filterRe *regexp.Regexp, useColor bool) {
	// Column width per provider: wide enough for the name plus 2 spaces of
	// padding, with a minimum of 10.
	colWidths := make([]int, len(runs))
	for i, r := range runs {
		w := utf8.RuneCountInString(r.providerName) + 2
		if w < 10 {
			w = 10
		}
		colWidths[i] = w
	}

	// Build the separator line (reused between groups).
	buildSep := func() string {
		s := strings.Repeat("─", testColWidth)
		for _, w := range colWidths {
			s += "─┼─" + strings.Repeat("─", w)
		}
		return s
	}
	sep := buildSep()

	// ── Header row ──────────────────────────────────────────────────────────
	header := padRight("", testColWidth)
	for i, r := range runs {
		name := r.providerName
		// Center the provider name in its column.
		vis := utf8.RuneCountInString(name)
		pad := colWidths[i] - vis
		left := pad / 2
		right := pad - left
		header += " │ " + strings.Repeat(" ", left) + colored(useColor, ansiBold, name) + strings.Repeat(" ", right)
	}
	fmt.Println(header)
	fmt.Println(colored(useColor, ansiDim, sep))

	passed := make([]int, len(runs))
	failed := make([]int, len(runs))
	skipped := make([]int, len(runs))

	for _, topName := range topOrder {
		children := canonicalChildren(topName, runs)

		// Count every test regardless of whether we display it.
		countTargets := children
		if len(children) == 0 {
			countTargets = []string{topName}
		}
		for _, testName := range countTargets {
			for i, r := range runs {
				if n, ok := r.nodes[testName]; ok {
					switch n.result {
					case "pass":
						passed[i]++
					case "fail":
						failed[i]++
					case "skip":
						skipped[i]++
					}
				}
			}
		}

		if !shouldDisplay(topName, runs, filterRe) {
			continue
		}

		// ── Group header row (no cells) ──────────────────────────────────
		groupLabel := "  " + colored(useColor, ansiBold, humanTopName(topName))
		fmt.Printf("%s", padRight(groupLabel, testColWidth))
		for i, r := range runs {
			if r.runErr != "" {
				fmt.Printf(" │ %s", cellDisplay(useColor, cell{result: "fail"}, colWidths[i]))
			} else {
				fmt.Printf(" │ %s", strings.Repeat(" ", colWidths[i]))
			}
		}
		fmt.Println()

		// ── Sub-test rows ────────────────────────────────────────────────
		for _, childName := range children {
			short := humanSubName(childName)
			fmt.Printf("%s", padRight("    "+short, testColWidth))
			for i, r := range runs {
				c := buildCell(r.nodes[childName])
				fmt.Printf(" │ %s", cellDisplay(useColor, c, colWidths[i]))
			}
			fmt.Println()
		}

		// ── Leaf test (no sub-tests): inline result ───────────────────────
		if len(children) == 0 {
			// Re-print the row with cells (the group header above had no cells).
			fmt.Printf("%s", padRight("  "+colored(useColor, ansiBold, humanTopName(topName)), testColWidth))
			for i, r := range runs {
				c := buildCell(r.nodes[topName])
				fmt.Printf(" │ %s", cellDisplay(useColor, c, colWidths[i]))
			}
			fmt.Println()
		}

		fmt.Println(colored(useColor, ansiDim, sep))
	}

	// ── Summary row ──────────────────────────────────────────────────────────
	fmt.Printf("%s", colored(useColor, ansiBold, padRight("  TOTAL", testColWidth)))
	allPassed := true
	for i, r := range runs {
		var cell string
		if r.runErr != "" {
			cell = colored(useColor, ansiRed, padRight("RUN ERROR", colWidths[i]))
			allPassed = false
		} else {
			p := fmt.Sprintf("%d✓", passed[i])
			f := fmt.Sprintf("%d✗", failed[i])
			raw := p + "  " + f
			if skipped[i] > 0 {
				raw += fmt.Sprintf("  %d~", skipped[i])
			}
			if failed[i] > 0 {
				cell = colored(useColor, ansiRed, padRight(raw, colWidths[i]))
				allPassed = false
			} else {
				cell = colored(useColor, ansiGreen, padRight(raw, colWidths[i]))
			}
		}
		fmt.Printf(" │ %s", cell)
	}
	fmt.Println()
	fmt.Println(colored(useColor, ansiDim, buildSep()))

	// Print run errors below the table.
	for _, r := range runs {
		if r.runErr != "" {
			fmt.Fprintf(os.Stderr, "\n%s error:\n  %s\n",
				colored(useColor, ansiRed, r.providerName), r.runErr)
		}
	}

	if !allPassed {
		os.Exit(1)
	}
}

// ── JSON renderer ─────────────────────────────────────────────────────────────

type jsonMatrix struct {
	Providers []string     `json:"providers"`
	Groups    []jsonGroup  `json:"groups"`
}

type jsonGroup struct {
	Name    string      `json:"name"`
	Display string      `json:"display"`
	Tests   []jsonTest  `json:"tests"`
}

type jsonTest struct {
	Name    string                 `json:"name"`
	Display string                 `json:"display"`
	Results map[string]jsonCellResult `json:"results"`
}

type jsonCellResult struct {
	Result  string `json:"result"`
	S3Code  string `json:"s3_code,omitempty"`
	Failure string `json:"failure,omitempty"`
}

func renderJSON(runs []*runResult, topOrder []string, filterRe *regexp.Regexp) {
	providerNames := make([]string, len(runs))
	for i, r := range runs {
		providerNames[i] = r.providerName
	}

	var groups []jsonGroup
	for _, topName := range topOrder {
		if !shouldDisplay(topName, runs, filterRe) {
			continue
		}
		children := canonicalChildren(topName, runs)

		grp := jsonGroup{
			Name:    topName,
			Display: humanTopName(topName),
		}

		targets := children
		if len(children) == 0 {
			targets = []string{topName}
		}

		for _, testName := range targets {
			jt := jsonTest{
				Name:    testName,
				Display: humanSubName(testName),
				Results: make(map[string]jsonCellResult, len(runs)),
			}
			for _, r := range runs {
				c := buildCell(r.nodes[testName])
				jt.Results[r.providerName] = jsonCellResult{
					Result:  c.result,
					S3Code:  c.s3Code,
					Failure: c.failure,
				}
			}
			grp.Tests = append(grp.Tests, jt)
		}

		groups = append(groups, grp)
	}

	out := jsonMatrix{
		Providers: providerNames,
		Groups:    groups,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintf(os.Stderr, "json encode: %v\n", err)
		os.Exit(1)
	}
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	configFile := flag.String("config", "testmatrix.json", "Path to matrix config file")
	format := flag.String("format", "text", "Output format: text or json")
	filterOverride := flag.String("filter", "", "Override the filter regex from the config file")
	flag.Parse()

	// Load matrix config.
	cfgData, err := os.ReadFile(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot read config %s: %v\n", *configFile, err)
		os.Exit(1)
	}
	var cfg matrixConfig
	if err := json.Unmarshal(cfgData, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "cannot parse config %s: %v\n", *configFile, err)
		os.Exit(1)
	}
	if len(cfg.Providers) == 0 {
		fmt.Fprintf(os.Stderr, "no providers defined in %s\n", *configFile)
		os.Exit(1)
	}

	// Resolve filter regex (CLI flag overrides config file).
	filterStr := cfg.Filter
	if *filterOverride != "" {
		filterStr = *filterOverride
	}
	var filterRe *regexp.Regexp
	if filterStr != "" {
		filterRe, err = regexp.Compile(filterStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid filter regex %q: %v\n", filterStr, err)
			os.Exit(1)
		}
	}

	useColor := isColorEnabled() && *format == "text"

	// Run each provider in parallel.
	fmt.Fprintf(os.Stderr, "Running tests against %d provider(s) in parallel…\n",
		len(cfg.Providers))

	results := make([]*runResult, len(cfg.Providers))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i, p := range cfg.Providers {
		wg.Add(1)
		go func(idx int, prov providerDef) {
			defer wg.Done()
			start := time.Now()
			r := runProvider(prov)
			elapsed := time.Since(start).Round(time.Millisecond)
			results[idx] = r
			mu.Lock()
			if r.runErr != "" {
				fmt.Fprintf(os.Stderr, "  ✗ %-20s  %s  (%v)\n",
					prov.Name, colored(useColor, ansiRed, "failed"), elapsed)
			} else {
				fmt.Fprintf(os.Stderr, "  ✓ %-20s  %s\n",
					prov.Name, colored(useColor, ansiDim, elapsed.String()))
			}
			mu.Unlock()
		}(i, p)
	}
	wg.Wait()
	fmt.Fprintln(os.Stderr)

	topOrder := canonicalTopOrder(results)
	if len(topOrder) == 0 {
		fmt.Fprintln(os.Stderr, "no test results collected — check provider configs and errors above")
		os.Exit(1)
	}

	if *format == "json" {
		renderJSON(results, topOrder, filterRe)
	} else {
		renderText(results, topOrder, filterRe, useColor)
	}
}
