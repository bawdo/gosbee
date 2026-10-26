package nodes

// OrderDirection represents ASC or DESC ordering.
type OrderDirection int

const (
	Asc OrderDirection = iota
	Desc
)

// NullsDirection controls NULLS FIRST/LAST positioning.
type NullsDirection int

const (
	NullsDefault NullsDirection = iota
	NullsFirst
	NullsLast
)

// OrderingNode represents an ORDER BY expression with a direction.
type OrderingNode struct {
	Expr      Node
	Direction OrderDirection
	Nulls     NullsDirection
	Combinable
}

func (n *OrderingNode) Accept(v Visitor) string { return v.VisitOrdering(n) }

// NewOrderingNode creates an OrderingNode with properly initialised embedded structs.
func NewOrderingNode(expr Node, dir OrderDirection, nulls NullsDirection) *OrderingNode {
	n := &OrderingNode{Expr: expr, Direction: dir, Nulls: nulls}
	n.self = n
	return n
}
