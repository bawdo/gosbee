package nodes

// SetOpType represents the type of set operation.
type SetOpType int

const (
	Union SetOpType = iota
	UnionAll
	Intersect
	IntersectAll
	Except
	ExceptAll
)

// String returns the SQL keyword for this set operation type.
func (t SetOpType) String() string {
	switch t {
	case Union:
		return "UNION"
	case UnionAll:
		return "UNION ALL"
	case Intersect:
		return "INTERSECT"
	case IntersectAll:
		return "INTERSECT ALL"
	case Except:
		return "EXCEPT"
	case ExceptAll:
		return "EXCEPT ALL"
	default:
		return "UNION"
	}
}

// SetOperationNode represents a set operation between two queries.
type SetOperationNode struct {
	Left  Node
	Right Node
	Type  SetOpType
}

func (n *SetOperationNode) Accept(v Visitor) string { return v.VisitSetOperation(n) }
