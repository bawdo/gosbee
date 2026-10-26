package nodes

// InNode represents an IN or NOT IN set predicate.
type InNode struct {
	Combinable
	Expr   Node
	Vals   []Node
	Negate bool
}

func (n *InNode) Accept(v Visitor) string { return v.VisitIn(n) }

// NewInNode creates an InNode with properly initialised embedded structs.
// Vals are used as given, without Literal wrapping.
func NewInNode(expr Node, vals []Node, negate bool) *InNode {
	n := &InNode{Expr: expr, Vals: vals, Negate: negate}
	n.self = n
	return n
}

// BetweenNode represents a BETWEEN or NOT BETWEEN range predicate.
type BetweenNode struct {
	Combinable
	Expr   Node
	Low    Node
	High   Node
	Negate bool
}

func (n *BetweenNode) Accept(v Visitor) string { return v.VisitBetween(n) }

// NewBetweenNode creates a BetweenNode with properly initialised embedded structs.
func NewBetweenNode(expr, low, high Node, negate bool) *BetweenNode {
	n := &BetweenNode{Expr: expr, Low: low, High: high, Negate: negate}
	n.self = n
	return n
}
