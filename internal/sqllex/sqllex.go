// Package sqllex reads rendered SQL back into tokens under one dialect's
// quoting rules. It exists for tests: a fuzz property checked against an
// independent reading of the output catches what a check written against the
// code that produced it would share.
//
// It knows quoting and nothing else. Quoted identifiers and string literals
// are decoded; every other run of bytes between spaces and quotes is one
// Other token, kept verbatim.
package sqllex

import "fmt"

// Dialect selects the quoting rules Lex applies.
type Dialect int

const (
	// ANSI is PostgreSQL and SQLite: "identifiers" with "" doubling, and
	// 'strings' with '' doubling, where a backslash is an ordinary byte.
	ANSI Dialect = iota
	// MySQL has `identifiers` with `` doubling, and 'strings' or "strings"
	// (the default sql_mode, without ANSI_QUOTES) with the quote doubled or
	// backslash escaped. \x decodes to x for every x. That is exact for the
	// escaping gosbee emits (\\ and ''), but not for MySQL in general, which
	// keeps the backslash in \% and \_ and reads \n, \0 and \Z as control bytes.
	// Lex reads bytes, so it cannot model a multibyte client charset such as
	// GBK, where a lead byte can swallow an escaping backslash.
	MySQL
)

// Kind is a token's kind.
type Kind int

const (
	// Ident is a quoted identifier; its Value is decoded.
	Ident Kind = iota
	// String is a string literal; its Value is decoded.
	String
	// Other is any other run of bytes, verbatim.
	Other
)

// Token is one lexed token.
type Token struct {
	Kind  Kind
	Value string
}

// Lex splits sql into tokens under d's rules. It returns an error for an
// unterminated quoted identifier or string.
func Lex(d Dialect, sql string) ([]Token, error) {
	identQuote := byte('"')
	if d == MySQL {
		identQuote = '`'
	}

	var tokens []Token
	i := 0
	for i < len(sql) {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n':
			i++
		case c == identQuote:
			val, next, err := quoted(sql, i, identQuote, false)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, Token{Ident, val})
			i = next
		case c == '\'' || (d == MySQL && c == '"'):
			val, next, err := quoted(sql, i, c, d == MySQL)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, Token{String, val})
			i = next
		default:
			start := i
			for i < len(sql) {
				c := sql[i]
				if c == ' ' || c == '\t' || c == '\n' || c == identQuote || c == '\'' || (d == MySQL && c == '"') {
					break
				}
				i++
			}
			tokens = append(tokens, Token{Other, sql[start:i]})
		}
	}
	return tokens, nil
}

// quoted decodes the quoted run opening at sql[start], which is q. A doubled
// q is one q. With backslash set, \x is x. It returns the decoded value and
// the index after the closing quote.
func quoted(sql string, start int, q byte, backslash bool) (string, int, error) {
	var val []byte
	i := start + 1
	for i < len(sql) {
		c := sql[i]
		switch {
		case backslash && c == '\\':
			if i+1 >= len(sql) {
				return "", 0, fmt.Errorf("sqllex: dangling backslash at %d", i)
			}
			val = append(val, sql[i+1])
			i += 2
		case c == q && i+1 < len(sql) && sql[i+1] == q:
			val = append(val, q)
			i += 2
		case c == q:
			return string(val), i + 1, nil
		default:
			val = append(val, c)
			i++
		}
	}
	return "", 0, fmt.Errorf("sqllex: unterminated %c quote opened at %d", q, start)
}

// Of returns the values of every token of kind k, in order, or nil if none.
func Of(ts []Token, k Kind) []string {
	var out []string
	for _, t := range ts {
		if t.Kind == k {
			out = append(out, t.Value)
		}
	}
	return out
}
