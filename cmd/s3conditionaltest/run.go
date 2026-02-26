// run.go implements the `run` subcommand, which reads `go test -json` output
// from stdin and formats it into a clean, human-readable summary with icons,
// S3 response codes, and an overall pass/fail count. Supports --format=json
// for machine-readable output.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"
)

// runJSONResult is the per-test schema for --format=json output.
type runJSONResult struct {
	Test        string   `json:"test"`
	Result      string   `json:"result"`
	ElapsedMS   int64    `json:"elapsed_ms"`
	S3Responses []string `json:"s3_responses"`
	Failure     string   `json:"failure,omitempty"`
}

// runShouldDisplay returns true when a top-level test group should be rendered.
// A group is always shown when it failed (so failures are never hidden), or
// when no filter is active, or when its name matches the filter regex.
func runShouldDisplay(topName string, result string, filterRe *regexp.Regexp) bool {
	if result == "fail" {
		return true
	}
	if filterRe == nil {
		return true
	}
	return filterRe.MatchString(topName)
}

func runResultIcon(result string, useColor bool) string {
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

func runResultLabel(result string, useColor bool) string {
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

func runRenderText(
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

		if !runShouldDisplay(topName, top.result, filterRe) {
			continue
		}

		// ── Group header ──────────────────────────────────────────────────
		label := runResultLabel(top.result, useColor)
		elapsed := fmt.Sprintf("%.2fs", top.elapsed)
		fmt.Printf("%s  %s  %s\n",
			colored(useColor, ansiBold, padRight(topName, 54)),
			label,
			colored(useColor, ansiDim, elapsed),
		)

		// ── Sub-tests ──────────────────────────────────────────────────────
		for _, childName := range top.children {
			child := nodes[childName]
			if child == nil {
				continue
			}

			icon := runResultIcon(child.result, useColor)
			short := humanSubName(childName)
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

func runRenderJSON(nodes map[string]*testNode, topOrder []string, filterRe *regexp.Regexp) {
	var results []runJSONResult

	for _, topName := range topOrder {
		top := nodes[topName]
		if top == nil {
			continue
		}

		if !runShouldDisplay(topName, top.result, filterRe) {
			continue
		}

		if len(top.children) == 0 {
			results = append(results, runJSONResult{
				Test:        topName,
				Result:      top.result,
				ElapsedMS:   int64(top.elapsed * 1000),
				S3Responses: nullSlice(top.s3Lines),
				Failure:     extractFailureMsg(top.rawLines),
			})
		} else {
			for _, childName := range top.children {
				child := nodes[childName]
				if child == nil {
					continue
				}
				results = append(results, runJSONResult{
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

// runMain implements the `run` subcommand.
func runMain(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	format := fs.String("format", "text", "Output format: text or json")
	filterStr := fs.String("filter", "",
		"Only display test groups matching this regex; failures always shown regardless")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: s3conditionaltest run [flags]\n\n")
		fmt.Fprintf(os.Stderr, "Reads `go test -json` from stdin and formats it as a readable summary.\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  go test -tags integration -v -count=1 -json ./s3test/ | s3conditionaltest run\n")
		fmt.Fprintf(os.Stderr, "  go test -tags integration -v -count=1 -json ./s3test/ | s3conditionaltest run --filter Conditional\n")
	}
	_ = fs.Parse(args)

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
	var topOrder []string
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
		runRenderJSON(nodes, topOrder, filterRe)
	} else {
		runRenderText(nodes, topOrder, pkgElapsed, pkgFailed, filterRe, useColor)
	}
}
