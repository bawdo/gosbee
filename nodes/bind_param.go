package nodes

// BindParamNode represents an explicit bind parameter placeholder.
// Its Value is always emitted as a bind parameter in parameterized mode,
// or rendered as a literal value in non-parameterized mode.
type BindParamNode struct {
	Value any
}

func (n *BindParamNode) Accept(v Visitor) string { return v.VisitBindParam(n) }

// NewBindParam creates a BindParamNode.
func NewBindParam(value any) *BindParamNode {
	return &BindParamNode{Value: value}
}
