package sqllex

import (
	"reflect"
	"testing"
)

func TestLex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		d    Dialect
		sql  string
		want []Token
	}{
		{"ansi ident doubled quote", ANSI, `"a""b"`, []Token{{Ident, `a"b`}}},
		{"ansi string doubled quote", ANSI, `'it''s'`, []Token{{String, "it's"}}},
		{"ansi backslash is literal", ANSI, `'a\'`, []Token{{String, `a\`}}},
		{"mysql backslash escape", MySQL, `'a\'b'`, []Token{{String, "a'b"}}},
		{"mysql doubled backslash", MySQL, `'a\\b'`, []Token{{String, `a\b`}}},
		{"mysql doubled quote", MySQL, `'it''s'`, []Token{{String, "it's"}}},
		{"mysql backtick doubled", MySQL, "`x``y`", []Token{{Ident, "x`y"}}},
		{"mysql double quote is a string", MySQL, `"a"`, []Token{{String, "a"}}},
		{"mysql double quoted string escapes", MySQL, `"a""b\"c'd"`, []Token{{String, `a"b"c'd`}}},
		{"mysql single quote inside double", MySQL, `"a' 'b"`, []Token{{String, "a' 'b"}}},
		{"empty ident", ANSI, `""`, []Token{{Ident, ""}}},
		{"empty string", ANSI, `''`, []Token{{String, ""}}},
		{"nul and invalid utf8", ANSI, "'\xff\x00'", []Token{{String, "\xff\x00"}}},
		{"spaces kept inside quotes", ANSI, `'a b'`, []Token{{String, "a b"}}},
		{"statement", ANSI, `SELECT "t".* FROM "t"`, []Token{
			{Other, "SELECT"}, {Ident, "t"}, {Other, ".*"}, {Other, "FROM"}, {Ident, "t"},
		}},
		{"qualified column", MySQL, "`t`.`c` = ?", []Token{
			{Ident, "t"}, {Other, "."}, {Ident, "c"}, {Other, "="}, {Other, "?"},
		}},
		{"empty input", ANSI, "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Lex(tt.d, tt.sql)
			if err != nil {
				t.Fatalf("Lex(%q) error: %v", tt.sql, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Lex(%q) = %#v, want %#v", tt.sql, got, tt.want)
			}
		})
	}
}

func TestLexErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		d    Dialect
		sql  string
	}{
		{"unterminated ansi ident", ANSI, `"a`},
		{"unterminated ansi string", ANSI, `'a`},
		{"unterminated backtick", MySQL, "`a"},
		{"unterminated mysql double quoted string", MySQL, `"a`},
		{"mysql escaped closing quote", MySQL, `'a\'`},
		{"mysql dangling backslash", MySQL, `'a\`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got, err := Lex(tt.d, tt.sql); err == nil {
				t.Errorf("Lex(%q) = %#v, want an error", tt.sql, got)
			}
		})
	}
}

func TestOf(t *testing.T) {
	t.Parallel()
	ts := []Token{{Ident, "a"}, {Other, "="}, {String, "x"}, {Ident, "b"}}
	if got := Of(ts, Ident); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("Of(Ident) = %q", got)
	}
	if got := Of(ts, String); !reflect.DeepEqual(got, []string{"x"}) {
		t.Errorf("Of(String) = %q", got)
	}
	if got := Of(nil, String); got != nil {
		t.Errorf("Of(nil) = %q, want nil", got)
	}
}
