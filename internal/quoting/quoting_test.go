package quoting

import "testing"

func TestEscapeString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", ""},
		{"no quotes", "hello", "hello"},
		{"single quote", "it's", "it''s"},
		{"double single quote", "it''s", "it''''s"},
		{"multiple quotes", "a'b'c", "a''b''c"},
		{"only quote", "'", "''"},
		{"leading quote", "'hello", "''hello"},
		{"trailing quote", "hello'", "hello''"},
		{"backslash", `hello\world`, `hello\\world`},
		{"null byte", "hello\x00world", "hello\x00world"},
		{"unicode", "caf\u00e9", "caf\u00e9"},
		{"unicode with quote", "caf\u00e9's", "caf\u00e9''s"},
		{"injection attempt", "'; DROP TABLE users; --", "''; DROP TABLE users; --"},
		{"long string", string(make([]byte, 10000)), string(make([]byte, 10000))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EscapeString(tt.input)
			if got != tt.want {
				t.Errorf("EscapeString(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestDoubleQuote(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple", "users", `"users"`},
		{"empty", "", `""`},
		{"with double quote", `us"ers`, `"us""ers"`},
		{"multiple double quotes", `a"b"c`, `"a""b""c"`},
		{"only double quote", `"`, `""""`},
		{"with space", "my table", `"my table"`},
		{"injection attempt", `users"."passwords`, `"users"".""passwords"`},
		{"backslash", `us\ers`, `"us\ers"`},
		{"unicode", "caf\u00e9", "\"caf\u00e9\""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DoubleQuote(tt.input)
			if got != tt.want {
				t.Errorf("DoubleQuote(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestBacktick(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple", "users", "`users`"},
		{"empty", "", "``"},
		{"with backtick", "us`ers", "`us``ers`"},
		{"multiple backticks", "a`b`c", "`a``b``c`"},
		{"only backtick", "`", "````"},
		{"with space", "my table", "`my table`"},
		{"injection attempt", "users`.`passwords", "`users``.``passwords`"},
		{"backslash", `us\ers`, "`us\\ers`"},
		{"unicode", "caf\u00e9", "`caf\u00e9`"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Backtick(tt.input)
			if got != tt.want {
				t.Errorf("Backtick(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
