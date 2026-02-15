package nodes

// InNode represents an IN or NOT IN set predicate.
type InNode struct {
	Combinable
	Expr   Node
	Vals   []Node
	Negate bool
}

func (n *InNode) Accept(v Visitor) string { return v.VisitIn(n) }

// BetweenNode represents a BETWEEN or NOT BETWEEN range predicate.
type BetweenNode struct {
	Combinable
	Expr   Node
	Low    Node
	High   Node
	Negate bool
}

func (n *BetweenNode) Accept(v Visitor) string { return v.VisitBetween(n) }
