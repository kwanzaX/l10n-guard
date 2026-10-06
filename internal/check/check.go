// Package check holds the automated quality checks run on every translation before it
// can ship. Checks are pure functions of (source, translation, options) so they can run
// in the worker, in CI, or in an editor plugin with the same result.
package check

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Severity of a finding. Any error rejects the translation; warnings let it ship
// but are surfaced to reviewers.
type Severity string

const (
	Error   Severity = "error"
	Warning Severity = "warning"
)

// Finding is one problem found in a translation.
type Finding struct {
	Rule     string   `json:"rule"`
	Severity Severity `json:"severity"`
	Message  string   `json:"message"`
}

// Status summarises a set of findings.
type Status string

const (
	OK       Status = "ok"
	Warned   Status = "warned"
	Rejected Status = "rejected"
)

// Options tune the checks per key. Zero values mean defaults.
type Options struct {
	// MaxLength caps the translation length in characters (runes). 0 disables the hard cap.
	MaxLength int
	// MaxGrowth is the allowed length ratio translation/source before a warning. Default 1.6.
	MaxGrowth float64
}

var (
	// {name}, {0}, {{name}}, %s, %d, %1$s, %.2f — the placeholder shapes used across web, mobile and CMS.
	placeholderRe = regexp.MustCompile(`\{\{\s*[\w.]+\s*\}\}|\{[\w.]+\}|%(?:\d+\$)?[-+ #0]*\d*(?:\.\d+)?[sdifuxXeEgGc@]`)
	tagRe         = regexp.MustCompile(`</?([a-zA-Z][a-zA-Z0-9]*)\b[^>]*>`)
	icuPluralRe   = regexp.MustCompile(`\{\s*\w+\s*,\s*(plural|selectordinal)\s*,`)
)

// Run executes every check and returns the findings and the resulting status.
func Run(source, translation string, opt Options) ([]Finding, Status) {
	var fs []Finding
	add := func(rule string, sev Severity, format string, args ...any) {
		fs = append(fs, Finding{Rule: rule, Severity: sev, Message: fmt.Sprintf(format, args...)})
	}

	if strings.TrimSpace(translation) == "" {
		add("empty", Error, "translation is empty")
		return fs, Rejected
	}

	// Placeholders must survive translation exactly: a missing one drops data from the UI,
	// an extra one renders a raw token or crashes the formatter.
	src, dst := placeholders(source), placeholders(translation)
	if missing := minus(src, dst); len(missing) > 0 {
		add("placeholders", Error, "missing placeholders: %s", strings.Join(missing, ", "))
	}
	if extra := minus(dst, src); len(extra) > 0 {
		add("placeholders", Error, "unexpected placeholders: %s", strings.Join(extra, ", "))
	}

	// ICU messages: braces must balance, and a plural/select needs its `other` branch.
	if depth, ok := braceBalance(translation); !ok {
		add("icu_braces", Error, "unbalanced braces (depth %d at end)", depth)
	}
	if icuPluralRe.MatchString(source) && !strings.Contains(translation, "other") {
		add("icu_plural", Error, "plural message is missing its `other` branch")
	}

	// Markup must match tag for tag, or the page renders broken HTML.
	if a, b := tags(source), tags(translation); !equal(a, b) {
		add("markup", Error, "HTML tags differ: source %v, translation %v", a, b)
	}

	srcLen, dstLen := utf8.RuneCountInString(source), utf8.RuneCountInString(translation)
	if opt.MaxLength > 0 && dstLen > opt.MaxLength {
		add("length", Error, "%d characters, limit is %d", dstLen, opt.MaxLength)
	}
	growth := opt.MaxGrowth
	if growth == 0 {
		growth = 1.6
	}
	if srcLen >= 10 && float64(dstLen) > float64(srcLen)*growth {
		add("length_growth", Warning, "translation is %.1fx the source length", float64(dstLen)/float64(srcLen))
	}

	if hasLetters(source) && strings.TrimSpace(source) == strings.TrimSpace(translation) {
		add("untranslated", Warning, "translation is identical to the source")
	}
	if edge(source) != edge(translation) {
		add("whitespace", Warning, "leading/trailing whitespace differs from the source")
	}

	return fs, statusOf(fs)
}

func statusOf(fs []Finding) Status {
	st := OK
	for _, f := range fs {
		if f.Severity == Error {
			return Rejected
		}
		st = Warned
	}
	return st
}

func placeholders(s string) []string {
	out := placeholderRe.FindAllString(s, -1)
	for i, p := range out {
		out[i] = strings.Join(strings.Fields(p), "") // "{{ name }}" == "{{name}}"
	}
	sort.Strings(out)
	return out
}

func tags(s string) []string {
	var out []string
	for _, m := range tagRe.FindAllStringSubmatch(s, -1) {
		name := strings.ToLower(m[1])
		if strings.HasPrefix(m[0], "</") {
			name = "/" + name
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// minus returns the elements of a not matched one-for-one in b (multiset difference).
func minus(a, b []string) []string {
	count := map[string]int{}
	for _, x := range b {
		count[x]++
	}
	var out []string
	for _, x := range a {
		if count[x] > 0 {
			count[x]--
			continue
		}
		out = append(out, x)
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// braceBalance ignores ICU-quoted text ('{' is a literal brace in ICU).
func braceBalance(s string) (int, bool) {
	depth, quoted := 0, false
	for _, r := range s {
		switch {
		case r == '\'':
			quoted = !quoted
		case quoted:
		case r == '{':
			depth++
		case r == '}':
			depth--
			if depth < 0 {
				return depth, false
			}
		}
	}
	return depth, depth == 0
}

func hasLetters(s string) bool {
	n := 0
	for _, r := range s {
		if unicode.IsLetter(r) {
			n++
		}
	}
	return n >= 4 // short tokens like "OK" or "PDF" are often left as is on purpose
}

func edge(s string) string {
	lead := len(s) - len(strings.TrimLeftFunc(s, unicode.IsSpace))
	trail := len(s) - len(strings.TrimRightFunc(s, unicode.IsSpace))
	return fmt.Sprintf("%t/%t", lead > 0, trail > 0)
}
