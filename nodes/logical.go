package nodes

// AndNode represents a logical AND between two expressions.
type AndNode struct {
	Combinable
	Left  Node
	Right Node
}

func (n *AndNode) Accept(v Visitor) string { return v.VisitAnd(n) }

// NewAndNode creates an AndNode with properly initialised embedded structs.
func NewAndNode(left, right Node) *AndNode {
	n := &AndNode{Left: left, Right: right}
	n.self = n
	return n
}

// OrNode represents a logical OR between two expressions.
type OrNode struct {
	Combinable
	Left  Node
	Right Node
}

func (n *OrNode) Accept(v Visitor) string { return v.VisitOr(n) }

// NewOrNode creates an OrNode with properly initialised embedded structs.
// Unlike Combinable.Or it does not wrap the result in a GroupingNode.
func NewOrNode(left, right Node) *OrNode {
	n := &OrNode{Left: left, Right: right}
	n.self = n
	return n
}

// NotNode represents a logical NOT of an expression.
type NotNode struct {
	Combinable
	Expr Node
}

func (n *NotNode) Accept(v Visitor) string { return v.VisitNot(n) }

// NewNotNode creates a NotNode with properly initialised embedded structs.
func NewNotNode(expr Node) *NotNode {
	n := &NotNode{Expr: expr}
	n.self = n
	return n
}
