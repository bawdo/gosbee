package nodes

// LiteralNode wraps a raw Go value (string, int, float, bool, etc.) as an AST node.
type LiteralNode struct {
	Predications
	Combinable
	Value any
}

func (n *LiteralNode) Accept(v Visitor) string { return v.VisitLiteral(n) }

// StarNode represents a SQL star (*) or qualified star (table.*).
type StarNode struct {
	Table *Table // nil for unqualified *
}

func (n *StarNode) Accept(v Visitor) string { return v.VisitStar(n) }

// SqlLiteral represents a raw SQL fragment injected verbatim into the query.
//
// SECURITY: The Raw field is rendered directly into SQL output without escaping
// or parameterization. Never pass user-controlled input to NewSqlLiteral or
// NewBoundSqlLiteral's raw parameter. Use parameterized queries (BindParam)
// for user-provided values.
type SqlLiteral struct {
	Predications
	Combinable
	Raw   string
	Binds []any // optional bind parameters for parameterized mode
}

func NewSqlLiteral(raw string) *SqlLiteral {
	n := &SqlLiteral{Raw: raw}
	n.Predications.self = n
	n.Combinable.self = n
	return n
}

func (n *SqlLiteral) Accept(v Visitor) string { return v.VisitSqlLiteral(n) }

// NewBoundSqlLiteral creates a SqlLiteral with bind parameters.
// In parameterized mode, the binds are added to the parameter list.
//
// SECURITY: Only the binds are parameterized. The raw string is injected
// verbatim into SQL output and must not contain user-controlled input.
func NewBoundSqlLiteral(raw string, binds ...any) *SqlLiteral {
	n := NewSqlLiteral(raw)
	n.Binds = binds
	return n
}

// Star returns an unqualified StarNode representing SQL *.
func Star() *StarNode {
	return &StarNode{}
}
