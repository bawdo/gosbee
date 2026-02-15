package nodes

// GroupingSetType identifies the type of advanced grouping.
type GroupingSetType int

const (
	Cube GroupingSetType = iota
	Rollup
	GroupingSets
)

// GroupingSetNode represents CUBE(...), ROLLUP(...), or GROUPING SETS((...), ...).
type GroupingSetNode struct {
	Type    GroupingSetType
	Columns []Node   // used by CUBE/ROLLUP (flat column list)
	Sets    [][]Node // used by GROUPING SETS (list of column groups)
}

func (n *GroupingSetNode) Accept(v Visitor) string { return v.VisitGroupingSet(n) }

// NewCube creates a CUBE(cols...) grouping set.
func NewCube(cols ...Node) *GroupingSetNode {
	return &GroupingSetNode{Type: Cube, Columns: cols}
}

// NewRollup creates a ROLLUP(cols...) grouping set.
func NewRollup(cols ...Node) *GroupingSetNode {
	return &GroupingSetNode{Type: Rollup, Columns: cols}
}

// NewGroupingSets creates a GROUPING SETS(sets...) grouping set.
func NewGroupingSets(sets ...[]Node) *GroupingSetNode {
	return &GroupingSetNode{Type: GroupingSets, Sets: sets}
}
