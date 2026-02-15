package visitors

import (
	"github.com/bawdo/gosbee/internal/quoting"
	"github.com/bawdo/gosbee/nodes"
)

// SQLiteVisitor generates SQLite-dialect SQL.
// Identifiers are quoted with double quotes: "table"."column" (ANSI SQL).
type SQLiteVisitor struct {
	*baseVisitor
}

// NewSQLiteVisitor creates a SQLiteVisitor ready for use.
// Pass WithParams() for production queries to enable parameterized mode.
func NewSQLiteVisitor(opts ...Option) *SQLiteVisitor {
	v := &SQLiteVisitor{}
	v.baseVisitor = &baseVisitor{
		outer:       v,
		quoteIdent:  quoting.DoubleQuote,
		placeholder: func(_ int) string { return "?" },
	}
	v.applyOptions(opts)
	return v
}

func (v *SQLiteVisitor) VisitComparison(n *nodes.ComparisonNode) string {
	switch n.Op {
	case nodes.OpRegexp:
		return n.Left.Accept(v) + " REGEXP " + n.Right.Accept(v)
	case nodes.OpNotRegexp:
		return n.Left.Accept(v) + " NOT REGEXP " + n.Right.Accept(v)
	case nodes.OpCaseSensitiveEq:
		return n.Left.Accept(v) + " = " + n.Right.Accept(v) + " COLLATE BINARY"
	case nodes.OpCaseInsensitiveEq:
		return n.Left.Accept(v) + " = " + n.Right.Accept(v) + " COLLATE NOCASE"
	default:
		return v.baseVisitor.VisitComparison(n)
	}
}
