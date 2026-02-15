package visitors

import (
	"github.com/bawdo/gosbee/internal/quoting"
	"github.com/bawdo/gosbee/nodes"
)

// MySQLVisitor generates MySQL-dialect SQL.
// Identifiers are quoted with backticks: `table`.`column`.
type MySQLVisitor struct {
	*baseVisitor
}

// NewMySQLVisitor creates a MySQLVisitor ready for use.
// Parameterized mode is enabled by default for SQL injection protection.
// Pass WithoutParams() to disable (not recommended for production).
func NewMySQLVisitor(opts ...Option) *MySQLVisitor {
	v := &MySQLVisitor{}
	v.baseVisitor = &baseVisitor{
		outer:        v,
		quoteIdent:   quoting.Backtick,
		placeholder:  func(_ int) string { return "?" },
		parameterize: true, // Enable by default
	}
	v.applyOptions(opts)
	return v
}

func (v *MySQLVisitor) VisitComparison(n *nodes.ComparisonNode) string {
	switch n.Op {
	case nodes.OpRegexp:
		return n.Left.Accept(v) + " REGEXP " + n.Right.Accept(v)
	case nodes.OpNotRegexp:
		return n.Left.Accept(v) + " NOT REGEXP " + n.Right.Accept(v)
	case nodes.OpCaseSensitiveEq:
		return n.Left.Accept(v) + " = BINARY " + n.Right.Accept(v)
	case nodes.OpCaseInsensitiveEq:
		return n.Left.Accept(v) + " = " + n.Right.Accept(v)
	default:
		return v.baseVisitor.VisitComparison(n)
	}
}
