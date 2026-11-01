package main

import (
	"reflect"
	"strings"
	"testing"
)

// quoteValue writes s as a REPL string literal.
func quoteValue(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// stripBlanks drops the bytes tokenize treats as separators outside a quote.
func stripBlanks(s string) string {
	return strings.NewReplacer(" ", "", "\t", "").Replace(s)
}

// FuzzTokenize: tokenize never yields an empty token and never loses or
// invents a byte (its tokens joined, blanks removed, equal the input with
// blanks removed). A quoted value s after "x =" comes back as exactly the
// three tokens x, = and the quoted literal, however s is made.
func FuzzTokenize(f *testing.F) {
	for _, in := range []string{
		"name = 'O''Brien'", "a>=1 AND b<>2", "'unterminated", "x != 'a b'",
		"\t", "", "f(a, b) || c", "'it''s' '' ''''",
	} {
		f.Add(in, in)
	}
	f.Fuzz(func(t *testing.T, input, s string) {
		toks := tokenize(input)
		for i, tok := range toks {
			if tok == "" {
				t.Fatalf("tokenize(%q) token %d is empty: %q", input, i, toks)
			}
		}
		if got, want := stripBlanks(strings.Join(toks, "")), stripBlanks(input); got != want {
			t.Fatalf("tokenize(%q) = %q joins to %q, want %q", input, toks, got, want)
		}

		q := quoteValue(s)
		if got, want := tokenize("x = "+q), []string{"x", "=", q}; !reflect.DeepEqual(got, want) {
			t.Fatalf("tokenize(%q) = %q, want %q", "x = "+q, got, want)
		}
	})
}

// FuzzParseValue: any string, written as a quoted literal, parses back to
// itself.
func FuzzParseValue(f *testing.F) {
	for _, s := range []string{"", "O'Brien", "''", "a b", `\`, "\xff\x00", "true", "12"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		got, err := parseValue(quoteValue(s))
		if err != nil {
			t.Fatalf("parseValue(%q): %v", quoteValue(s), err)
		}
		if got != s {
			t.Fatalf("parseValue(%q) = %#v, want %q", quoteValue(s), got, s)
		}
	})
}
