package nodes

// GroupingNode wraps an expression in parentheses for precedence control.
type GroupingNode struct {
	Combinable
	Expr Node
}

func (n *GroupingNode) Accept(v Visitor) string { return v.VisitGrouping(n) }

// NewGroupingNode creates a GroupingNode with properly initialised embedded structs.
func NewGroupingNode(expr Node) *GroupingNode {
	n := &GroupingNode{Expr: expr}
	n.self = n
	return n
}
