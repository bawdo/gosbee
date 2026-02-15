package nodes

// GroupingNode wraps an expression in parentheses for precedence control.
type GroupingNode struct {
	Combinable
	Expr Node
}

func (n *GroupingNode) Accept(v Visitor) string { return v.VisitGrouping(n) }
