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
// Pass WithParams() for production queries to enable parameterized mode.
func NewPostgresVisitor(opts ...Option) *PostgresVisitor {
	v := &PostgresVisitor{}
	v.baseVisitor = &baseVisitor{
		outer:       v,
		quoteIdent:  quoting.DoubleQuote,
		placeholder: func(i int) string { return fmt.Sprintf("$%d", i) },
	}
	v.applyOptions(opts)
	return v
}
