package nodes

// AndNode represents a logical AND between two expressions.
type AndNode struct {
	Combinable
	Left  Node
	Right Node
}

func (n *AndNode) Accept(v Visitor) string { return v.VisitAnd(n) }

// OrNode represents a logical OR between two expressions.
type OrNode struct {
	Combinable
	Left  Node
	Right Node
}

func (n *OrNode) Accept(v Visitor) string { return v.VisitOr(n) }

// NotNode represents a logical NOT of an expression.
type NotNode struct {
	Combinable
	Expr Node
}

func (n *NotNode) Accept(v Visitor) string { return v.VisitNot(n) }
