package nodes

// AggregateFunc identifies the aggregate function.
type AggregateFunc int

const (
	AggCount AggregateFunc = iota
	AggSum
	AggAvg
	AggMin
	AggMax
)

// AggregateNode represents an aggregate function call (COUNT, SUM, AVG, MIN, MAX).
type AggregateNode struct {
	Predications
	Arithmetics
	Combinable
	Func     AggregateFunc
	Expr     Node // argument (nil for COUNT(*))
	Distinct bool // COUNT(DISTINCT ...)
	Filter   Node // FILTER (WHERE ...) clause, nil if not used
}

func (n *AggregateNode) Accept(v Visitor) string { return v.VisitAggregate(n) }

// NewAggregateNode creates an AggregateNode with properly initialised embedded structs.
func NewAggregateNode(fn AggregateFunc, expr Node) *AggregateNode {
	n := &AggregateNode{Func: fn, Expr: expr}
	n.Predications.self = n
	n.Arithmetics.self = n
	n.Combinable.self = n
	return n
}

// Count creates a COUNT aggregate. Pass nil for COUNT(*).
func Count(expr Node) *AggregateNode {
	return NewAggregateNode(AggCount, expr)
}

// Sum creates a SUM aggregate.
func Sum(expr Node) *AggregateNode {
	return NewAggregateNode(AggSum, expr)
}

// Avg creates an AVG aggregate.
func Avg(expr Node) *AggregateNode {
	return NewAggregateNode(AggAvg, expr)
}

// Min creates a MIN aggregate.
func Min(expr Node) *AggregateNode {
	return NewAggregateNode(AggMin, expr)
}

// Max creates a MAX aggregate.
func Max(expr Node) *AggregateNode {
	return NewAggregateNode(AggMax, expr)
}

// CountDistinct creates a COUNT(DISTINCT expr) aggregate.
func CountDistinct(expr Node) *AggregateNode {
	n := NewAggregateNode(AggCount, expr)
	n.Distinct = true
	return n
}

// Over wraps the aggregate with an inline window definition.
func (n *AggregateNode) Over(def *WindowDefinition) *OverNode {
	o := NewOverNode(n)
	o.Window = def
	return o
}

// OverName wraps the aggregate with a named window reference.
func (n *AggregateNode) OverName(name string) *OverNode {
	o := NewOverNode(n)
	o.WindowName = name
	return o
}

// WithFilter returns a copy of the aggregate with a FILTER (WHERE ...) clause.
func (n *AggregateNode) WithFilter(condition Node) *AggregateNode {
	out := &AggregateNode{
		Func:     n.Func,
		Expr:     n.Expr,
		Distinct: n.Distinct,
		Filter:   condition,
	}
	out.Predications.self = out
	out.Arithmetics.self = out
	out.Combinable.self = out
	return out
}

// ExtractField identifies the date/time field for EXTRACT.
type ExtractField int

const (
	ExtractYear ExtractField = iota
	ExtractMonth
	ExtractDay
	ExtractHour
	ExtractMinute
	ExtractSecond
	ExtractDow // day of week
	ExtractDoy // day of year
	ExtractEpoch
	ExtractQuarter
	ExtractWeek
)

// ExtractNode represents EXTRACT(field FROM expr).
type ExtractNode struct {
	Predications
	Arithmetics
	Combinable
	Field ExtractField
	Expr  Node
}

func (n *ExtractNode) Accept(v Visitor) string { return v.VisitExtract(n) }

// NewExtractNode creates an ExtractNode with properly initialised embedded structs.
func NewExtractNode(field ExtractField, expr Node) *ExtractNode {
	n := &ExtractNode{Field: field, Expr: expr}
	n.Predications.self = n
	n.Arithmetics.self = n
	n.Combinable.self = n
	return n
}

// Extract creates an EXTRACT(field FROM expr) node.
func Extract(field ExtractField, expr Node) *ExtractNode {
	return NewExtractNode(field, expr)
}
