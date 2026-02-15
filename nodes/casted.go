package nodes

// CastedNode represents a typed value that knows its SQL type.
// Used for type-aware rendering (e.g., ensuring correct casting).
type CastedNode struct {
	Predications
	Arithmetics
	Combinable
	Value    any
	TypeName string
}

func (n *CastedNode) Accept(v Visitor) string { return v.VisitCasted(n) }

// NewCasted creates a CastedNode with properly initialised embedded structs.
func NewCasted(value any, typeName string) *CastedNode {
	n := &CastedNode{Value: value, TypeName: typeName}
	n.Predications.self = n
	n.Arithmetics.self = n
	n.Combinable.self = n
	return n
}
