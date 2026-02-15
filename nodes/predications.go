package nodes

// Predications provides comparison methods to types that embed it.
// The self field must be set to the embedding node so that comparisons
// reference the correct left-hand side.
type Predications struct {
	self Node
}

// Eq creates an equality comparison: self = val.
func (p Predications) Eq(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpEq}
	n.self = n
	return n
}

// NotEq creates an inequality comparison: self != val.
func (p Predications) NotEq(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpNotEq}
	n.self = n
	return n
}

// Gt creates a greater-than comparison: self > val.
func (p Predications) Gt(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpGt}
	n.self = n
	return n
}

// GtEq creates a greater-than-or-equal comparison: self >= val.
func (p Predications) GtEq(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpGtEq}
	n.self = n
	return n
}

// Lt creates a less-than comparison: self < val.
func (p Predications) Lt(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpLt}
	n.self = n
	return n
}

// LtEq creates a less-than-or-equal comparison: self <= val.
func (p Predications) LtEq(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpLtEq}
	n.self = n
	return n
}

// Like creates a LIKE comparison: self LIKE val.
func (p Predications) Like(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpLike}
	n.self = n
	return n
}

// NotLike creates a NOT LIKE comparison: self NOT LIKE val.
func (p Predications) NotLike(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpNotLike}
	n.self = n
	return n
}

// In creates an IN predicate: self IN (vals...).
func (p Predications) In(vals ...any) *InNode {
	wrapped := make([]Node, len(vals))
	for i, v := range vals {
		wrapped[i] = Literal(v)
	}
	n := &InNode{Expr: p.self, Vals: wrapped}
	n.self = n
	return n
}

// NotIn creates a NOT IN predicate: self NOT IN (vals...).
func (p Predications) NotIn(vals ...any) *InNode {
	wrapped := make([]Node, len(vals))
	for i, v := range vals {
		wrapped[i] = Literal(v)
	}
	n := &InNode{Expr: p.self, Vals: wrapped, Negate: true}
	n.self = n
	return n
}

// Between creates a BETWEEN predicate: self BETWEEN low AND high.
func (p Predications) Between(low, high any) *BetweenNode {
	n := &BetweenNode{Expr: p.self, Low: Literal(low), High: Literal(high)}
	n.self = n
	return n
}

// NotBetween creates a NOT BETWEEN predicate: self NOT BETWEEN low AND high.
func (p Predications) NotBetween(low, high any) *BetweenNode {
	n := &BetweenNode{Expr: p.self, Low: Literal(low), High: Literal(high), Negate: true}
	n.self = n
	return n
}

// MatchesRegexp creates a regexp match: self ~ val.
func (p Predications) MatchesRegexp(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpRegexp}
	n.self = n
	return n
}

// DoesNotMatchRegexp creates a negated regexp match: self !~ val.
func (p Predications) DoesNotMatchRegexp(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpNotRegexp}
	n.self = n
	return n
}

// IsDistinctFrom creates an IS DISTINCT FROM comparison.
func (p Predications) IsDistinctFrom(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpDistinctFrom}
	n.self = n
	return n
}

// IsNotDistinctFrom creates an IS NOT DISTINCT FROM comparison.
func (p Predications) IsNotDistinctFrom(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpNotDistinctFrom}
	n.self = n
	return n
}

// CaseSensitiveEq creates a case-sensitive equality comparison.
func (p Predications) CaseSensitiveEq(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpCaseSensitiveEq}
	n.self = n
	return n
}

// CaseInsensitiveEq creates a case-insensitive equality comparison.
func (p Predications) CaseInsensitiveEq(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpCaseInsensitiveEq}
	n.self = n
	return n
}

// Contains creates an array/JSONB containment operator: self @> val.
func (p Predications) Contains(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpContains}
	n.self = n
	return n
}

// Overlaps creates an array overlap operator: self && val.
func (p Predications) Overlaps(val any) *ComparisonNode {
	n := &ComparisonNode{Left: p.self, Right: Literal(val), Op: OpOverlaps}
	n.self = n
	return n
}

// IsNull creates an IS NULL predicate.
func (p Predications) IsNull() *UnaryNode {
	n := &UnaryNode{Expr: p.self, Op: OpIsNull}
	n.self = n
	return n
}

// IsNotNull creates an IS NOT NULL predicate.
func (p Predications) IsNotNull() *UnaryNode {
	n := &UnaryNode{Expr: p.self, Op: OpIsNotNull}
	n.self = n
	return n
}

// EqAny returns col = v1 OR col = v2 OR ... wrapped in a GroupingNode.
func (p Predications) EqAny(vals ...any) *GroupingNode {
	return p.anyComparison(OpEq, vals)
}

// EqAll returns col = v1 AND col = v2 AND ...
func (p Predications) EqAll(vals ...any) Node {
	return p.allComparison(OpEq, vals)
}

// MatchesAny returns col LIKE p1 OR col LIKE p2 OR ... wrapped in a GroupingNode.
func (p Predications) MatchesAny(vals ...any) *GroupingNode {
	return p.anyComparison(OpLike, vals)
}

// MatchesAll returns col LIKE p1 AND col LIKE p2 AND ...
func (p Predications) MatchesAll(vals ...any) Node {
	return p.allComparison(OpLike, vals)
}

// InAny returns col IN (set1) OR col IN (set2) OR ... wrapped in a GroupingNode.
// Each argument is a []any slice representing one IN set.
func (p Predications) InAny(sets ...[]any) *GroupingNode {
	nodes := make([]Node, len(sets))
	for i, set := range sets {
		nodes[i] = p.In(set...)
	}
	return groupOr(nodes)
}

// InAll returns col IN (set1) AND col IN (set2) AND ...
// Each argument is a []any slice representing one IN set.
func (p Predications) InAll(sets ...[]any) Node {
	nds := make([]Node, len(sets))
	for i, set := range sets {
		nds[i] = p.In(set...)
	}
	return chainAnd(nds)
}

func (p Predications) anyComparison(op ComparisonOp, vals []any) *GroupingNode {
	nodes := make([]Node, len(vals))
	for i, v := range vals {
		nodes[i] = NewComparisonNode(p.self, Literal(v), op)
	}
	return groupOr(nodes)
}

func (p Predications) allComparison(op ComparisonOp, vals []any) Node {
	nodes := make([]Node, len(vals))
	for i, v := range vals {
		nodes[i] = NewComparisonNode(p.self, Literal(v), op)
	}
	return chainAnd(nodes)
}

// groupOr chains nodes with OR and wraps in a GroupingNode.
// Returns nil if nds is empty.
func groupOr(nds []Node) *GroupingNode {
	if len(nds) == 0 {
		return nil
	}
	if len(nds) == 1 {
		g := &GroupingNode{Expr: nds[0]}
		g.self = g
		return g
	}
	var result = nds[0]
	for i := 1; i < len(nds); i++ {
		or := &OrNode{Left: result, Right: nds[i]}
		or.self = or
		result = or
	}
	g := &GroupingNode{Expr: result}
	g.self = g
	return g
}

// chainAnd chains nodes with AND.
// Returns nil if nds is empty.
func chainAnd(nds []Node) Node {
	if len(nds) == 0 {
		return nil
	}
	if len(nds) == 1 {
		return nds[0]
	}
	var result = nds[0]
	for i := 1; i < len(nds); i++ {
		and := &AndNode{Left: result, Right: nds[i]}
		and.self = and
		result = and
	}
	return result
}

// As creates an AliasNode wrapping self with the given alias name.
func (p Predications) As(name string) *AliasNode {
	return NewAliasNode(p.self, name)
}

// Asc creates an ascending ordering node.
func (p Predications) Asc() *OrderingNode {
	n := &OrderingNode{Expr: p.self, Direction: Asc}
	n.self = n
	return n
}

// Desc creates a descending ordering node.
func (p Predications) Desc() *OrderingNode {
	n := &OrderingNode{Expr: p.self, Direction: Desc}
	n.self = n
	return n
}
