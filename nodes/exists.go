package nodes

// ExistsNode represents an EXISTS or NOT EXISTS subquery expression.
type ExistsNode struct {
	Subquery Node
	Negated  bool
	Combinable
}

func (n *ExistsNode) Accept(v Visitor) string { return v.VisitExists(n) }

// Exists creates an EXISTS(subquery) node.
func Exists(subquery Node) *ExistsNode {
	n := &ExistsNode{Subquery: subquery}
	n.self = n
	return n
}

// NotExists creates a NOT EXISTS(subquery) node.
func NotExists(subquery Node) *ExistsNode {
	n := &ExistsNode{Subquery: subquery, Negated: true}
	n.self = n
	return n
}
