package nodes

// AliasNode represents a column or expression alias: expr AS "name".
type AliasNode struct {
	Predications
	Arithmetics
	Combinable
	Expr Node
	Name string
}

func (n *AliasNode) Accept(v Visitor) string { return v.VisitAlias(n) }

// NewAliasNode creates an AliasNode with properly initialised embedded structs.
func NewAliasNode(expr Node, name string) *AliasNode {
	n := &AliasNode{Expr: expr, Name: name}
	n.Predications.self = n
	n.Arithmetics.self = n
	n.Combinable.self = n
	return n
}
