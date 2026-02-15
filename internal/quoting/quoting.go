// Package quoting provides shared identifier quoting utilities.
package quoting

import "strings"

// DoubleQuote quotes a SQL identifier using double quotes (PostgreSQL, SQLite, ANSI SQL).
// Internal double quotes are escaped by doubling them.
func DoubleQuote(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// Backtick quotes a SQL identifier using backticks (MySQL).
// Internal backticks are escaped by doubling them.
func Backtick(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// EscapeString escapes a string literal for SQL by doubling single quotes
// and escaping backslashes (for MySQL compatibility).
//
// SECURITY: This escaping is intended for non-parameterized mode only.
// Production code should use parameterized queries (visitors.WithParams())
// for all user-provided values. In particular, MySQL with non-default
// character sets (GBK, SJIS) may have multi-byte sequences where a trailing
// byte coincides with backslash or quote; parameterized queries avoid this
// class of attack entirely.
func EscapeString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, "'", "''")
}

// EscapeLikePattern escapes LIKE wildcard characters (%, _) in a string
// so they are matched literally. The backslash is used as the escape character.
func EscapeLikePattern(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "%", `\%`)
	s = strings.ReplaceAll(s, "_", `\_`)
	return s
}
