package nodes

// UnaryOp represents a unary postfix operator.
type UnaryOp int

const (
	OpIsNull UnaryOp = iota
	OpIsNotNull
)

// UnaryNode represents a unary predicate: Expr IS NULL / IS NOT NULL.
type UnaryNode struct {
	Combinable
	Expr Node
	Op   UnaryOp
}

func (n *UnaryNode) Accept(v Visitor) string { return v.VisitUnary(n) }
