package nodes

// ComparisonOp represents a binary comparison operator.
type ComparisonOp int

const (
	OpEq ComparisonOp = iota
	OpNotEq
	OpGt
	OpGtEq
	OpLt
	OpLtEq
	OpLike
	OpNotLike
	OpRegexp
	OpNotRegexp
	OpDistinctFrom
	OpNotDistinctFrom
	OpCaseSensitiveEq
	OpCaseInsensitiveEq
	OpContains
	OpOverlaps
)

// ComparisonNode represents a binary comparison: Left Op Right.
type ComparisonNode struct {
	Combinable
	Left  Node
	Right Node
	Op    ComparisonOp
}

func (n *ComparisonNode) Accept(v Visitor) string { return v.VisitComparison(n) }

// NewComparisonNode creates a ComparisonNode with properly initialised embedded structs.
func NewComparisonNode(left, right Node, op ComparisonOp) *ComparisonNode {
	n := &ComparisonNode{Left: left, Right: right, Op: op}
	n.self = n
	return n
}
