package nodes

// CaseWhen represents a single WHEN ... THEN ... pair in a CASE expression.
type CaseWhen struct {
	Condition Node
	Result    Node
}

// CaseNode represents a SQL CASE expression:
//
//	CASE [operand] WHEN cond THEN result ... [ELSE val] END
//
// If Operand is nil, it is a "searched CASE" (CASE WHEN cond THEN ...).
type CaseNode struct {
	Predications
	Arithmetics
	Combinable
	Operand Node       // nil for searched CASE
	Whens   []CaseWhen // WHEN ... THEN ... pairs
	ElseVal Node       // ELSE value (nil if omitted)
}

func (n *CaseNode) Accept(v Visitor) string { return v.VisitCase(n) }

// NewCase creates a CaseNode. Pass an operand for simple CASE, or nil/no args for searched CASE.
func NewCase(operand ...Node) *CaseNode {
	n := &CaseNode{}
	if len(operand) > 0 {
		n.Operand = operand[0]
	}
	n.Predications.self = n
	n.Arithmetics.self = n
	n.Combinable.self = n
	return n
}

// When adds a WHEN ... THEN ... pair and returns the CaseNode for chaining.
func (n *CaseNode) When(cond, result Node) *CaseNode {
	n.Whens = append(n.Whens, CaseWhen{Condition: cond, Result: result})
	return n
}

// Else sets the ELSE value and returns the CaseNode for chaining.
func (n *CaseNode) Else(result Node) *CaseNode {
	n.ElseVal = result
	return n
}
