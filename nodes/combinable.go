package nodes

// Combinable provides logical chaining methods to types that embed it.
// The self field must be set to the embedding node.
type Combinable struct {
	self Node
}

// And creates an AndNode combining self with other.
func (c Combinable) And(other Node) *AndNode {
	n := &AndNode{Left: c.self, Right: other}
	n.self = n
	return n
}

// Or creates an OrNode wrapped in a GroupingNode for correct precedence.
func (c Combinable) Or(other Node) *GroupingNode {
	or := &OrNode{Left: c.self, Right: other}
	or.self = or
	g := &GroupingNode{Expr: or}
	g.self = g
	return g
}

// Not creates a NotNode negating self.
func (c Combinable) Not() *NotNode {
	n := &NotNode{Expr: c.self}
	n.self = n
	return n
}
