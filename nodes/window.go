package nodes

// WindowFunc identifies the window function.
type WindowFunc int

const (
	WinRowNumber WindowFunc = iota
	WinRank
	WinDenseRank
	WinNtile
	WinLag
	WinLead
	WinFirstValue
	WinLastValue
	WinNthValue
	WinCumeDist
	WinPercentRank
)

// FrameType specifies ROWS or RANGE for a window frame.
type FrameType int

const (
	FrameRows FrameType = iota
	FrameRange
)

// BoundType specifies a window frame boundary.
type BoundType int

const (
	BoundUnboundedPreceding BoundType = iota
	BoundPreceding
	BoundCurrentRow
	BoundFollowing
	BoundUnboundedFollowing
)

// WindowFuncNode represents a window function call (e.g. ROW_NUMBER(), RANK()).
// It is always wrapped by OverNode for the OVER clause.
type WindowFuncNode struct {
	Func WindowFunc
	Args []Node
}

func (n *WindowFuncNode) Accept(v Visitor) string { return v.VisitWindowFunction(n) }

// OverNode wraps an expression (window function or aggregate) with an OVER clause.
type OverNode struct {
	Predications
	Arithmetics
	Combinable
	Expr       Node              // WindowFuncNode or AggregateNode
	Window     *WindowDefinition // inline window definition (nil if using WindowName)
	WindowName string            // named window reference (empty if using Window)
}

func (n *OverNode) Accept(v Visitor) string { return v.VisitOver(n) }

// WindowDefinition describes a window specification: name, partitioning, ordering, and frame.
type WindowDefinition struct {
	Name        string
	PartitionBy []Node
	OrderBy     []Node
	Frame       *WindowFrame
}

// WindowFrame describes the frame clause (ROWS/RANGE BETWEEN ... AND ...).
type WindowFrame struct {
	Type  FrameType
	Start FrameBound
	End   *FrameBound // nil means no BETWEEN (just the Start bound)
}

// FrameBound describes a single frame boundary.
type FrameBound struct {
	Type   BoundType
	Offset Node // only for BoundPreceding / BoundFollowing
}

// --- WindowDefinition builder ---

// NewWindowDef creates a new WindowDefinition with an optional name.
func NewWindowDef(name ...string) *WindowDefinition {
	w := &WindowDefinition{}
	if len(name) > 0 {
		w.Name = name[0]
	}
	return w
}

// Partition sets the PARTITION BY columns.
func (w *WindowDefinition) Partition(cols ...Node) *WindowDefinition {
	w.PartitionBy = cols
	return w
}

// Order sets the ORDER BY expressions.
func (w *WindowDefinition) Order(orderings ...Node) *WindowDefinition {
	w.OrderBy = orderings
	return w
}

// Rows sets a ROWS frame with start and optional end bound.
func (w *WindowDefinition) Rows(start FrameBound, end ...FrameBound) *WindowDefinition {
	f := &WindowFrame{Type: FrameRows, Start: start}
	if len(end) > 0 {
		e := end[0]
		f.End = &e
	}
	w.Frame = f
	return w
}

// Range sets a RANGE frame with start and optional end bound.
func (w *WindowDefinition) Range(start FrameBound, end ...FrameBound) *WindowDefinition {
	f := &WindowFrame{Type: FrameRange, Start: start}
	if len(end) > 0 {
		e := end[0]
		f.End = &e
	}
	w.Frame = f
	return w
}

// --- Frame bound helpers ---

// UnboundedPreceding returns an UNBOUNDED PRECEDING frame bound.
func UnboundedPreceding() FrameBound {
	return FrameBound{Type: BoundUnboundedPreceding}
}

// Preceding returns a N PRECEDING frame bound.
func Preceding(n Node) FrameBound {
	return FrameBound{Type: BoundPreceding, Offset: n}
}

// CurrentRow returns a CURRENT ROW frame bound.
func CurrentRow() FrameBound {
	return FrameBound{Type: BoundCurrentRow}
}

// Following returns a N FOLLOWING frame bound.
func Following(n Node) FrameBound {
	return FrameBound{Type: BoundFollowing, Offset: n}
}

// UnboundedFollowing returns an UNBOUNDED FOLLOWING frame bound.
func UnboundedFollowing() FrameBound {
	return FrameBound{Type: BoundUnboundedFollowing}
}

// --- Window function constructors ---

// RowNumber creates a ROW_NUMBER() window function node.
func RowNumber() *WindowFuncNode {
	return &WindowFuncNode{Func: WinRowNumber}
}

// Rank creates a RANK() window function node.
func Rank() *WindowFuncNode {
	return &WindowFuncNode{Func: WinRank}
}

// DenseRank creates a DENSE_RANK() window function node.
func DenseRank() *WindowFuncNode {
	return &WindowFuncNode{Func: WinDenseRank}
}

// CumeDist creates a CUME_DIST() window function node.
func CumeDist() *WindowFuncNode {
	return &WindowFuncNode{Func: WinCumeDist}
}

// PercentRank creates a PERCENT_RANK() window function node.
func PercentRank() *WindowFuncNode {
	return &WindowFuncNode{Func: WinPercentRank}
}

// Ntile creates an NTILE(n) window function node.
func Ntile(n Node) *WindowFuncNode {
	return &WindowFuncNode{Func: WinNtile, Args: []Node{n}}
}

// FirstValue creates a FIRST_VALUE(expr) window function node.
func FirstValue(expr Node) *WindowFuncNode {
	return &WindowFuncNode{Func: WinFirstValue, Args: []Node{expr}}
}

// LastValue creates a LAST_VALUE(expr) window function node.
func LastValue(expr Node) *WindowFuncNode {
	return &WindowFuncNode{Func: WinLastValue, Args: []Node{expr}}
}

// Lag creates a LAG(expr [, offset [, default]]) window function node.
func Lag(args ...Node) *WindowFuncNode {
	return &WindowFuncNode{Func: WinLag, Args: args}
}

// Lead creates a LEAD(expr [, offset [, default]]) window function node.
func Lead(args ...Node) *WindowFuncNode {
	return &WindowFuncNode{Func: WinLead, Args: args}
}

// NthValue creates an NTH_VALUE(expr, n) window function node.
func NthValue(args ...Node) *WindowFuncNode {
	return &WindowFuncNode{Func: WinNthValue, Args: args}
}

// NewOverNode creates an OverNode with properly initialised embedded structs.
func NewOverNode(expr Node) *OverNode {
	o := &OverNode{Expr: expr}
	o.Predications.self = o
	o.Arithmetics.self = o
	o.Combinable.self = o
	return o
}

// --- Over methods on WindowFuncNode ---

// Over wraps the window function with an inline window definition.
func (n *WindowFuncNode) Over(def *WindowDefinition) *OverNode {
	o := NewOverNode(n)
	o.Window = def
	return o
}

// OverName wraps the window function with a named window reference.
func (n *WindowFuncNode) OverName(name string) *OverNode {
	o := NewOverNode(n)
	o.WindowName = name
	return o
}
