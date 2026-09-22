package determinism

import (
	"go/parser"
	"go/token"
	"regexp"
	"strings"
	"testing"
)

// The phases were a plan, and the plan is finished. A comment in shipped code
// that still describes one as future work is not a stylistic complaint: it is
// a sentence that was true when it was written, is false now, and reads as
// though the behaviour it describes has not been built.
//
// This is not hypothetical. The gRPC transport dropped every message addressed
// to a node it had no address for, under a comment reading "Phase 4's
// membership changes make this reachable; for now it means a stray message".
// Phase 4 had long since landed. Adding a member at runtime committed into the
// configuration and was then never heard from, and the sentence explaining why
// that was fine sat directly above the line doing it. Two more comments in the
// core said the same kind of thing, including one stating that the core did
// not act on configuration changes at all.
//
// Tests are exempt. A test comment naming the phase whose exit criterion it
// checks is a record of why the test exists, not a claim about unwritten code.

// deferralPhrases mark a comment as describing something still to come.
var deferralPhrases = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\bfor now\b`),
	regexp.MustCompile(`(?i)\byet\b`),
	regexp.MustCompile(`(?i)\breserved for\b`),
	regexp.MustCompile(`(?i)\bonce\b`),
	regexp.MustCompile(`(?i)\blands\b`),
	regexp.MustCompile(`(?i)\bbefore phase\b`),
	// A phase as the subject of a verb, which is how a comment says a phase
	// is going to do something rather than that it already did.
	regexp.MustCompile(`(?i)\bphase \d+ (adds|ships|exposes|introduces|makes|brings|will)\b`),
}

var phaseMention = regexp.MustCompile(`(?i)\bphase \d`)

func TestShippedCodeDoesNotDeferToACompletedPhase(t *testing.T) {
	fset := token.NewFileSet()

	walkGo(t, func(path string, isTest bool) {
		if isTest || strings.HasSuffix(path, ".pb.go") {
			return
		}

		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}

		for _, group := range f.Comments {
			text := group.Text()
			if !phaseMention.MatchString(text) {
				continue
			}
			for _, phrase := range deferralPhrases {
				if !phrase.MatchString(text) {
					continue
				}
				t.Errorf("%s:%d: a comment mentions a phase and defers to it (%q). "+
					"Every phase is finished, so say what the code does now",
					path, fset.Position(group.Pos()).Line, phrase.FindString(text))
			}
		}
	})
}
