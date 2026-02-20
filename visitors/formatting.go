package visitors

import (
	"strings"

	"github.com/bawdo/gosbee/nodes"
)

// FormattingVisitor wraps any nodes.Visitor (dialect visitor) and produces
// human-readable multi-line SQL. VisitSelectCore is a real implementation;
// VisitSetOperation, VisitInsertStatement, VisitUpdateStatement, and
// VisitDeleteStatement are still temporary stubs that delegate to the inner
// visitor and will be replaced in future tasks.
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

// VisitSelectCore renders a SELECT statement in multi-line formatted style.
// Projections use leading-comma continuation; all major clauses begin on a
// new line. Child expressions are rendered via f.inner (dialect-specific).
func (f *FormattingVisitor) VisitSelectCore(node *nodes.SelectCore) string {
	var sb strings.Builder

	// WITH / WITH RECURSIVE
	if len(node.CTEs) > 0 {
		hasRecursive := false
		for _, cte := range node.CTEs {
			if cte.Recursive {
				hasRecursive = true
				break
			}
		}
		if hasRecursive {
			sb.WriteString("WITH RECURSIVE ")
		} else {
			sb.WriteString("WITH ")
		}
		for i, cte := range node.CTEs {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(cte.Accept(f))
		}
		sb.WriteString("\n")
	}

	// Comment / Hints
	if node.Comment != "" {
		sb.WriteString("/* ")
		sb.WriteString(strings.ReplaceAll(node.Comment, "*/", "* /"))
		sb.WriteString(" */\n")
	}

	// SELECT keyword
	sb.WriteString("SELECT")

	// Hints (optimizer hints after SELECT keyword)
	if len(node.Hints) > 0 {
		sb.WriteString(" /*+ ")
		for i, h := range node.Hints {
			if i > 0 {
				sb.WriteString(" ")
			}
			sb.WriteString(strings.ReplaceAll(h, "*/", "* /"))
		}
		sb.WriteString(" */")
	}

	// DISTINCT / DISTINCT ON
	if len(node.DistinctOn) > 0 {
		sb.WriteString(" DISTINCT ON (")
		for i, c := range node.DistinctOn {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(c.Accept(f.inner))
		}
		sb.WriteString(")")
	} else if node.Distinct {
		sb.WriteString(" DISTINCT")
	}

	// Projections — leading-comma style
	if len(node.Projections) == 0 {
		sb.WriteString(" *")
	} else {
		sb.WriteString(" ")
		sb.WriteString(node.Projections[0].Accept(f.inner))
		for _, p := range node.Projections[1:] {
			sb.WriteString("\n\t,")
			sb.WriteString(p.Accept(f.inner))
		}
	}

	// FROM
	if node.From != nil {
		sb.WriteString("\nFROM ")
		sb.WriteString(node.From.Accept(f.inner))
	}

	// JOINs
	for _, j := range node.Joins {
		sb.WriteString("\n")
		sb.WriteString(j.Accept(f.inner))
	}

	// WHERE
	if len(node.Wheres) > 0 {
		sb.WriteString("\nWHERE ")
		sb.WriteString(node.Wheres[0].Accept(f.inner))
		for _, w := range node.Wheres[1:] {
			sb.WriteString("\n\tAND ")
			sb.WriteString(w.Accept(f.inner))
		}
	}

	// GROUP BY — leading-comma style
	if len(node.Groups) > 0 {
		sb.WriteString("\nGROUP BY ")
		sb.WriteString(node.Groups[0].Accept(f.inner))
		for _, g := range node.Groups[1:] {
			sb.WriteString("\n\t,")
			sb.WriteString(g.Accept(f.inner))
		}
	}

	// HAVING
	if len(node.Havings) > 0 {
		sb.WriteString("\nHAVING ")
		sb.WriteString(node.Havings[0].Accept(f.inner))
		for _, h := range node.Havings[1:] {
			sb.WriteString("\n\tAND ")
			sb.WriteString(h.Accept(f.inner))
		}
	}

	// WINDOW
	if len(node.Windows) > 0 {
		sb.WriteString("\nWINDOW ")
		for i, w := range node.Windows {
			if i > 0 {
				sb.WriteString(", ")
			}
			// Render the window name using the inner visitor for correct quoting,
			// then the parenthesised definition via RenderWindowDef.
			sb.WriteString(nodes.NewTable(w.Name).Accept(f.inner))
			sb.WriteString(" AS ")
			sb.WriteString(RenderWindowDef(f.inner, &nodes.WindowDefinition{
				PartitionBy: w.PartitionBy,
				OrderBy:     w.OrderBy,
				Frame:       w.Frame,
			}))
		}
	}

	// ORDER BY — leading-comma style
	if len(node.Orders) > 0 {
		sb.WriteString("\nORDER BY ")
		sb.WriteString(node.Orders[0].Accept(f.inner))
		for _, o := range node.Orders[1:] {
			sb.WriteString("\n\t,")
			sb.WriteString(o.Accept(f.inner))
		}
	}

	// LIMIT
	if node.Limit != nil {
		sb.WriteString("\nLIMIT ")
		sb.WriteString(node.Limit.Accept(f.inner))
	}

	// OFFSET
	if node.Offset != nil {
		sb.WriteString("\nOFFSET ")
		sb.WriteString(node.Offset.Accept(f.inner))
	}

	// Locking
	if node.Lock != nodes.NoLock {
		sb.WriteString("\n")
		sb.WriteString(lockModeSQL[node.Lock])
		if node.SkipLocked {
			sb.WriteString(" SKIP LOCKED")
		}
	}

	return sb.String()
}

// VisitSetOperation — stub, will be replaced in Task 8.
// When implementing: call child nodes via node.Accept(f), not node.Accept(f.inner).
func (f *FormattingVisitor) VisitSetOperation(node *nodes.SetOperationNode) string {
	return f.inner.VisitSetOperation(node) // temporary — replaced in Task 8
}

// VisitInsertStatement — stub, will be replaced in Task 9.
// When implementing: call child nodes via node.Accept(f), not node.Accept(f.inner).
func (f *FormattingVisitor) VisitInsertStatement(node *nodes.InsertStatement) string {
	return f.inner.VisitInsertStatement(node) // temporary — replaced in Task 9
}

// VisitUpdateStatement — stub, will be replaced in Task 9.
// When implementing: call child nodes via node.Accept(f), not node.Accept(f.inner).
func (f *FormattingVisitor) VisitUpdateStatement(node *nodes.UpdateStatement) string {
	return f.inner.VisitUpdateStatement(node) // temporary — replaced in Task 9
}

// VisitDeleteStatement — stub, will be replaced in Task 9.
// When implementing: call child nodes via node.Accept(f), not node.Accept(f.inner).
func (f *FormattingVisitor) VisitDeleteStatement(node *nodes.DeleteStatement) string {
	return f.inner.VisitDeleteStatement(node) // temporary — replaced in Task 9
}
