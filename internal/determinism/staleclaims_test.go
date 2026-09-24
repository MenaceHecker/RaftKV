package determinism

import (
	"go/parser"
	"go/token"
	"regexp"
	"strings"
	"testing"
)

var deferralPhrases = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\bfor now\b`),
	regexp.MustCompile(`(?i)\byet\b`),
	regexp.MustCompile(`(?i)\breserved for\b`),
	regexp.MustCompile(`(?i)\bonce\b`),
	regexp.MustCompile(`(?i)\blands\b`),
	regexp.MustCompile(`(?i)\bbefore phase\b`),
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
