package nodes

// JoinType represents the type of SQL JOIN.
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftOuterJoin
	RightOuterJoin
	FullOuterJoin
	CrossJoin
	StringJoin // raw SQL join fragment
)

// String returns the display name for this join type.
func (t JoinType) String() string {
	switch t {
	case InnerJoin:
		return "INNER JOIN"
	case LeftOuterJoin:
		return "LEFT OUTER JOIN"
	case RightOuterJoin:
		return "RIGHT OUTER JOIN"
	case FullOuterJoin:
		return "FULL OUTER JOIN"
	case CrossJoin:
		return "CROSS JOIN"
	case StringJoin:
		return "STRING JOIN"
	default:
		return "JOIN"
	}
}

// JoinNode represents a SQL JOIN clause.
type JoinNode struct {
	Left    Node     // source table
	Right   Node     // target table or subquery
	Type    JoinType // join type
	On      Node     // join condition (nil for CROSS JOIN)
	Lateral bool     // LATERAL modifier (PostgreSQL)
}

func (n *JoinNode) Accept(v Visitor) string { return v.VisitJoin(n) }
