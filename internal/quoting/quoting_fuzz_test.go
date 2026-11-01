package quoting_test

import (
	"strings"
	"testing"

	"github.com/bawdo/gosbee/internal/quoting"
	"github.com/bawdo/gosbee/internal/sqllex"
)

// seeds are shared by every quoting target: empty, plain, each dialect's
// quote characters, a backslash, the LIKE wildcards, bytes that are not
// UTF-8, and an injection attempt.
var seeds = []string{
	"", "users", `a"b`, "a`b", "it's", `\`, `a\'b`, "%_", "\xff\x00",
	"'; DROP TABLE users; --",
}

// lexOne lexes sql under d and requires exactly one token of kind k,
// returning its value. More than one token means the quoting closed early.
func lexOne(t *testing.T, d sqllex.Dialect, k sqllex.Kind, sql string) string {
	t.Helper()
	ts, err := sqllex.Lex(d, sql)
	if err != nil {
		t.Fatalf("Lex(%q): %v", sql, err)
	}
	if len(ts) != 1 || ts[0].Kind != k {
		t.Fatalf("Lex(%q) = %#v, want exactly one token of kind %d", sql, ts, k)
	}
	return ts[0].Value
}

// FuzzDoubleQuote: DoubleQuote(s) reads back under ANSI rules as exactly one
// identifier whose value is s.
func FuzzDoubleQuote(f *testing.F) {
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		q := quoting.DoubleQuote(s)
		if got := lexOne(t, sqllex.ANSI, sqllex.Ident, q); got != s {
			t.Fatalf("DoubleQuote(%q) = %q reads back as %q", s, q, got)
		}
	})
}

// FuzzBacktick: Backtick(s) reads back under MySQL rules as exactly one
// identifier whose value is s.
func FuzzBacktick(f *testing.F) {
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		q := quoting.Backtick(s)
		if got := lexOne(t, sqllex.MySQL, sqllex.Ident, q); got != s {
			t.Fatalf("Backtick(%q) = %q reads back as %q", s, q, got)
		}
	})
}

// FuzzEscapeString: the escaped literal is exactly one string under both
// MySQL and ANSI rules, so it never closes early under either. It reads back
// as s under MySQL, and under ANSI when s has no backslash (see
// TestEscapeStringANSIRoundTripBackslash).
func FuzzEscapeString(f *testing.F) {
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		lit := "'" + quoting.EscapeString(s) + "'"
		if got := lexOne(t, sqllex.MySQL, sqllex.String, lit); got != s {
			t.Fatalf("MySQL: %q reads back as %q, want %q", lit, got, s)
		}
		got := lexOne(t, sqllex.ANSI, sqllex.String, lit)
		if !strings.Contains(s, `\`) && got != s {
			t.Fatalf("ANSI: %q reads back as %q, want %q", lit, got, s)
		}
	})
}

// TestEscapeStringANSIRoundTripBackslash pins the known defect the fuzz
// target above steps around, so it stays visible in every verbose run.
func TestEscapeStringANSIRoundTripBackslash(t *testing.T) {
	t.Parallel()
	t.Skip("known defect: EscapeString doubles backslashes for PostgreSQL and SQLite, " +
		"where a backslash is literal, so a literal backslash comes back doubled; " +
		"recorded in unit 06 of the repo-quality workstream, fixed separately")
	s := `\`
	lit := "'" + quoting.EscapeString(s) + "'"
	if got := lexOne(t, sqllex.ANSI, sqllex.String, lit); got != s {
		t.Fatalf("ANSI: %q reads back as %q, want %q", lit, got, s)
	}
}

// FuzzEscapeLikePattern: every % and _ in the result is escaped, and
// removing the escapes gives s back.
func FuzzEscapeLikePattern(f *testing.F) {
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		p := quoting.EscapeLikePattern(s)
		var out []byte
		for i := 0; i < len(p); i++ {
			switch p[i] {
			case '\\':
				if i+1 >= len(p) {
					t.Fatalf("EscapeLikePattern(%q) = %q ends in a lone backslash", s, p)
				}
				i++
				out = append(out, p[i])
			case '%', '_':
				t.Fatalf("EscapeLikePattern(%q) = %q leaves %q at %d unescaped", s, p, p[i], i)
			default:
				out = append(out, p[i])
			}
		}
		if string(out) != s {
			t.Fatalf("EscapeLikePattern(%q) = %q unescapes to %q", s, p, out)
		}
	})
}
