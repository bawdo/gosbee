package visitors

import (
	"fmt"

	"github.com/bawdo/gosbee/internal/quoting"
)

// PostgresVisitor generates PostgreSQL-dialect SQL.
// Identifiers are quoted with double quotes: "table"."column".
type PostgresVisitor struct {
	*baseVisitor
}

// NewPostgresVisitor creates a PostgresVisitor ready for use.
// Parameterized mode is enabled by default for SQL injection protection.
// Pass WithoutParams() to disable (not recommended for production).
func NewPostgresVisitor(opts ...Option) *PostgresVisitor {
	v := &PostgresVisitor{}
	v.baseVisitor = &baseVisitor{
		outer:        v,
		quoteIdent:   quoting.DoubleQuote,
		placeholder:  func(i int) string { return fmt.Sprintf("$%d", i) },
		parameterize: true, // Enable by default
	}
	v.applyOptions(opts)
	return v
}
