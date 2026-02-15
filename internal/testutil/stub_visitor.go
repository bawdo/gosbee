// Package testutil provides shared test helpers for the gosbee project.
package testutil

import "github.com/bawdo/gosbee/nodes"

// StubVisitor implements nodes.Visitor with minimal return values for testing.
// Methods return meaningful short strings to aid in test assertions.
type StubVisitor struct{}

var _ nodes.Visitor = StubVisitor{}

func (sv StubVisitor) VisitTable(n *nodes.Table) string             { return n.Name }
func (sv StubVisitor) VisitTableAlias(n *nodes.TableAlias) string   { return n.AliasName }
func (sv StubVisitor) VisitAttribute(n *nodes.Attribute) string     { return "attr" }
func (sv StubVisitor) VisitLiteral(n *nodes.LiteralNode) string     { return "lit" }
func (sv StubVisitor) VisitStar(n *nodes.StarNode) string           { return "*" }
func (sv StubVisitor) VisitSqlLiteral(n *nodes.SqlLiteral) string   { return n.Raw }
func (sv StubVisitor) VisitComparison(n *nodes.ComparisonNode) string {
	return n.Left.Accept(sv) + "=?" + n.Right.Accept(sv)
}
func (sv StubVisitor) VisitUnary(n *nodes.UnaryNode) string                    { return "unary" }
func (sv StubVisitor) VisitAnd(n *nodes.AndNode) string                        { return "and" }
func (sv StubVisitor) VisitOr(n *nodes.OrNode) string                          { return "or" }
func (sv StubVisitor) VisitNot(n *nodes.NotNode) string                        { return "not" }
func (sv StubVisitor) VisitIn(n *nodes.InNode) string                          { return "in" }
func (sv StubVisitor) VisitBetween(n *nodes.BetweenNode) string                { return "between" }
func (sv StubVisitor) VisitGrouping(n *nodes.GroupingNode) string              { return "grouping" }
func (sv StubVisitor) VisitJoin(n *nodes.JoinNode) string                      { return "join" }
func (sv StubVisitor) VisitOrdering(n *nodes.OrderingNode) string              { return "ordering" }
func (sv StubVisitor) VisitSelectCore(n *nodes.SelectCore) string              { return "select_core" }
func (sv StubVisitor) VisitInsertStatement(n *nodes.InsertStatement) string    { return "insert" }
func (sv StubVisitor) VisitUpdateStatement(n *nodes.UpdateStatement) string    { return "update" }
func (sv StubVisitor) VisitDeleteStatement(n *nodes.DeleteStatement) string    { return "delete" }
func (sv StubVisitor) VisitAssignment(n *nodes.AssignmentNode) string          { return "assign" }
func (sv StubVisitor) VisitOnConflict(n *nodes.OnConflictNode) string          { return "conflict" }
func (sv StubVisitor) VisitInfix(n *nodes.InfixNode) string                    { return "infix" }
func (sv StubVisitor) VisitUnaryMath(n *nodes.UnaryMathNode) string            { return "unary_math" }
func (sv StubVisitor) VisitAggregate(n *nodes.AggregateNode) string            { return "aggregate" }
func (sv StubVisitor) VisitExtract(n *nodes.ExtractNode) string                { return "extract" }
func (sv StubVisitor) VisitWindowFunction(n *nodes.WindowFuncNode) string      { return "window_func" }
func (sv StubVisitor) VisitOver(n *nodes.OverNode) string                      { return "over" }
func (sv StubVisitor) VisitExists(n *nodes.ExistsNode) string                  { return "exists" }
func (sv StubVisitor) VisitSetOperation(n *nodes.SetOperationNode) string      { return "set_op" }
func (sv StubVisitor) VisitCTE(n *nodes.CTENode) string                        { return "cte" }
func (sv StubVisitor) VisitNamedFunction(n *nodes.NamedFunctionNode) string    { return "named_func" }
func (sv StubVisitor) VisitCase(n *nodes.CaseNode) string                      { return "case" }
func (sv StubVisitor) VisitGroupingSet(n *nodes.GroupingSetNode) string        { return "grouping_set" }
func (sv StubVisitor) VisitAlias(n *nodes.AliasNode) string                    { return "alias" }
func (sv StubVisitor) VisitBindParam(n *nodes.BindParamNode) string            { return "bind_param" }
func (sv StubVisitor) VisitCasted(n *nodes.CastedNode) string                  { return "casted" }

// StubParamVisitor implements nodes.Visitor and nodes.Parameterizer for testing.
type StubParamVisitor struct {
	StubVisitor
	params []any
}

var _ nodes.Visitor = (*StubParamVisitor)(nil)
var _ nodes.Parameterizer = (*StubParamVisitor)(nil)

func (sv *StubParamVisitor) Params() []any { return sv.params }
func (sv *StubParamVisitor) Reset()        { sv.params = nil }
