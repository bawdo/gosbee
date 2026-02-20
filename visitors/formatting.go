package visitors

import "github.com/bawdo/gosbee/nodes"

// FormattingVisitor wraps any nodes.Visitor (dialect visitor) and will
// eventually produce human-readable multi-line SQL. For now, it delegates
// all methods to the inner visitor. The structural override methods
// (VisitSelectCore, VisitSetOperation, VisitInsertStatement,
// VisitUpdateStatement, VisitDeleteStatement) are temporary stubs that
// also delegate — these will be replaced in future tasks.
type FormattingVisitor struct {
	inner nodes.Visitor
}

var _ nodes.Visitor = (*FormattingVisitor)(nil)
var _ nodes.Parameterizer = (*FormattingVisitor)(nil)

// NewFormattingVisitor constructs a FormattingVisitor wrapping the given
// dialect visitor.
func NewFormattingVisitor(inner nodes.Visitor) *FormattingVisitor {
	if inner == nil {
		panic("gosbee: FormattingVisitor requires a non-nil inner visitor")
	}
	return &FormattingVisitor{inner: inner}
}

// Params delegates to the inner visitor if it implements nodes.Parameterizer,
// otherwise returns nil.
func (f *FormattingVisitor) Params() []any {
	if p, ok := f.inner.(nodes.Parameterizer); ok {
		return p.Params()
	}
	return nil
}

// Reset delegates to the inner visitor if it implements nodes.Parameterizer.
func (f *FormattingVisitor) Reset() {
	if p, ok := f.inner.(nodes.Parameterizer); ok {
		p.Reset()
	}
}

// --- Delegation methods for all nodes.Visitor methods ---

func (f *FormattingVisitor) VisitTable(node *nodes.Table) string {
	return f.inner.VisitTable(node)
}

func (f *FormattingVisitor) VisitTableAlias(node *nodes.TableAlias) string {
	return f.inner.VisitTableAlias(node)
}

func (f *FormattingVisitor) VisitAttribute(node *nodes.Attribute) string {
	return f.inner.VisitAttribute(node)
}

func (f *FormattingVisitor) VisitLiteral(node *nodes.LiteralNode) string {
	return f.inner.VisitLiteral(node)
}

func (f *FormattingVisitor) VisitStar(node *nodes.StarNode) string {
	return f.inner.VisitStar(node)
}

func (f *FormattingVisitor) VisitSqlLiteral(node *nodes.SqlLiteral) string {
	return f.inner.VisitSqlLiteral(node)
}

func (f *FormattingVisitor) VisitComparison(node *nodes.ComparisonNode) string {
	return f.inner.VisitComparison(node)
}

func (f *FormattingVisitor) VisitUnary(node *nodes.UnaryNode) string {
	return f.inner.VisitUnary(node)
}

func (f *FormattingVisitor) VisitAnd(node *nodes.AndNode) string {
	return f.inner.VisitAnd(node)
}

func (f *FormattingVisitor) VisitOr(node *nodes.OrNode) string {
	return f.inner.VisitOr(node)
}

func (f *FormattingVisitor) VisitNot(node *nodes.NotNode) string {
	return f.inner.VisitNot(node)
}

func (f *FormattingVisitor) VisitIn(node *nodes.InNode) string {
	return f.inner.VisitIn(node)
}

func (f *FormattingVisitor) VisitBetween(node *nodes.BetweenNode) string {
	return f.inner.VisitBetween(node)
}

func (f *FormattingVisitor) VisitGrouping(node *nodes.GroupingNode) string {
	return f.inner.VisitGrouping(node)
}

func (f *FormattingVisitor) VisitJoin(node *nodes.JoinNode) string {
	return f.inner.VisitJoin(node)
}

func (f *FormattingVisitor) VisitOrdering(node *nodes.OrderingNode) string {
	return f.inner.VisitOrdering(node)
}

func (f *FormattingVisitor) VisitAssignment(node *nodes.AssignmentNode) string {
	return f.inner.VisitAssignment(node)
}

func (f *FormattingVisitor) VisitOnConflict(node *nodes.OnConflictNode) string {
	return f.inner.VisitOnConflict(node)
}

func (f *FormattingVisitor) VisitInfix(node *nodes.InfixNode) string {
	return f.inner.VisitInfix(node)
}

func (f *FormattingVisitor) VisitUnaryMath(node *nodes.UnaryMathNode) string {
	return f.inner.VisitUnaryMath(node)
}

func (f *FormattingVisitor) VisitAggregate(node *nodes.AggregateNode) string {
	return f.inner.VisitAggregate(node)
}

func (f *FormattingVisitor) VisitExtract(node *nodes.ExtractNode) string {
	return f.inner.VisitExtract(node)
}

func (f *FormattingVisitor) VisitWindowFunction(node *nodes.WindowFuncNode) string {
	return f.inner.VisitWindowFunction(node)
}

func (f *FormattingVisitor) VisitOver(node *nodes.OverNode) string {
	return f.inner.VisitOver(node)
}

func (f *FormattingVisitor) VisitExists(node *nodes.ExistsNode) string {
	return f.inner.VisitExists(node)
}

func (f *FormattingVisitor) VisitCTE(node *nodes.CTENode) string {
	return f.inner.VisitCTE(node)
}

func (f *FormattingVisitor) VisitNamedFunction(node *nodes.NamedFunctionNode) string {
	return f.inner.VisitNamedFunction(node)
}

func (f *FormattingVisitor) VisitCase(node *nodes.CaseNode) string {
	return f.inner.VisitCase(node)
}

func (f *FormattingVisitor) VisitGroupingSet(node *nodes.GroupingSetNode) string {
	return f.inner.VisitGroupingSet(node)
}

func (f *FormattingVisitor) VisitAlias(node *nodes.AliasNode) string {
	return f.inner.VisitAlias(node)
}

func (f *FormattingVisitor) VisitBindParam(node *nodes.BindParamNode) string {
	return f.inner.VisitBindParam(node)
}

func (f *FormattingVisitor) VisitCasted(node *nodes.CastedNode) string {
	return f.inner.VisitCasted(node)
}

// --- Structural override stubs (temporary — will be replaced in later tasks) ---

// VisitSelectCore — stub, replaced in Task 3.
// When implementing: call child nodes via node.Accept(f), not node.Accept(f.inner).
func (f *FormattingVisitor) VisitSelectCore(node *nodes.SelectCore) string {
	return f.inner.VisitSelectCore(node) // temporary — replaced in Task 3
}

// VisitSetOperation — stub, replaced in Task 3.
// When implementing: call child nodes via node.Accept(f), not node.Accept(f.inner).
func (f *FormattingVisitor) VisitSetOperation(node *nodes.SetOperationNode) string {
	return f.inner.VisitSetOperation(node) // temporary — replaced in Task 3
}

// VisitInsertStatement — stub, replaced in Task 3.
// When implementing: call child nodes via node.Accept(f), not node.Accept(f.inner).
func (f *FormattingVisitor) VisitInsertStatement(node *nodes.InsertStatement) string {
	return f.inner.VisitInsertStatement(node) // temporary — replaced in Task 3
}

// VisitUpdateStatement — stub, replaced in Task 3.
// When implementing: call child nodes via node.Accept(f), not node.Accept(f.inner).
func (f *FormattingVisitor) VisitUpdateStatement(node *nodes.UpdateStatement) string {
	return f.inner.VisitUpdateStatement(node) // temporary — replaced in Task 3
}

// VisitDeleteStatement — stub, replaced in Task 3.
// When implementing: call child nodes via node.Accept(f), not node.Accept(f.inner).
func (f *FormattingVisitor) VisitDeleteStatement(node *nodes.DeleteStatement) string {
	return f.inner.VisitDeleteStatement(node) // temporary — replaced in Task 3
}
