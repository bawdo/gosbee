package nodes

// LockMode represents row-level locking for SELECT queries.
type LockMode int

const (
	NoLock         LockMode = iota
	ForUpdate               // FOR UPDATE
	ForShare                // FOR SHARE
	ForNoKeyUpdate          // FOR NO KEY UPDATE
	ForKeyShare             // FOR KEY SHARE
)

// String returns the SQL keyword for this lock mode.
func (m LockMode) String() string {
	switch m {
	case ForUpdate:
		return "FOR UPDATE"
	case ForShare:
		return "FOR SHARE"
	case ForNoKeyUpdate:
		return "FOR NO KEY UPDATE"
	case ForKeyShare:
		return "FOR KEY SHARE"
	default:
		return ""
	}
}

// SelectCore represents the data container for a SELECT clause.
// The fluent API for building queries lives in the managers package.
type SelectCore struct {
	From        Node
	Projections []Node
	Wheres      []Node
	Joins       []*JoinNode
	Groups      []Node              // GROUP BY expressions
	Havings     []Node              // HAVING conditions
	Windows     []*WindowDefinition // WINDOW definitions
	Orders      []Node              // OrderingNode values
	Limit       Node                // nil or LiteralNode
	Offset      Node                // nil or LiteralNode
	Distinct    bool
	DistinctOn  []Node     // DISTINCT ON columns (PostgreSQL)
	Lock        LockMode   // FOR UPDATE/SHARE
	SkipLocked  bool       // SKIP LOCKED
	Comment     string     // query comment /* ... */
	Hints       []string   // optimizer hints /*+ ... */
	CTEs        []*CTENode // WITH clause
}

func (n *SelectCore) Accept(v Visitor) string { return v.VisitSelectCore(n) }
