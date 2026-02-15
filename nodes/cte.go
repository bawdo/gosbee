package nodes

// CTENode represents a Common Table Expression (WITH clause).
type CTENode struct {
	Name      string
	Query     Node
	Recursive bool
	Columns   []string // optional column list
}

func (n *CTENode) Accept(v Visitor) string { return v.VisitCTE(n) }
