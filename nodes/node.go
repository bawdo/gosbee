// Package nodes defines the AST node types used to represent SQL query elements.
package nodes

// Node is the interface that all AST nodes implement.
type Node interface {
	Accept(visitor Visitor) string
}

// Visitor defines the interface for walking the AST and producing output.
// Concrete visitors (e.g., Postgres, MySQL) implement this interface.
type Visitor interface {
	VisitTable(node *Table) string
	VisitTableAlias(node *TableAlias) string
	VisitAttribute(node *Attribute) string
	VisitLiteral(node *LiteralNode) string
	VisitStar(node *StarNode) string
	VisitSqlLiteral(node *SqlLiteral) string
	VisitComparison(node *ComparisonNode) string
	VisitUnary(node *UnaryNode) string
	VisitAnd(node *AndNode) string
	VisitOr(node *OrNode) string
	VisitNot(node *NotNode) string
	VisitIn(node *InNode) string
	VisitBetween(node *BetweenNode) string
	VisitGrouping(node *GroupingNode) string
	VisitJoin(node *JoinNode) string
	VisitOrdering(node *OrderingNode) string
	VisitSelectCore(node *SelectCore) string
	VisitInsertStatement(node *InsertStatement) string
	VisitUpdateStatement(node *UpdateStatement) string
	VisitDeleteStatement(node *DeleteStatement) string
	VisitAssignment(node *AssignmentNode) string
	VisitOnConflict(node *OnConflictNode) string
	VisitInfix(node *InfixNode) string
	VisitUnaryMath(node *UnaryMathNode) string
	VisitAggregate(node *AggregateNode) string
	VisitExtract(node *ExtractNode) string
	VisitWindowFunction(node *WindowFuncNode) string
	VisitOver(node *OverNode) string
	VisitExists(node *ExistsNode) string
	VisitSetOperation(node *SetOperationNode) string
	VisitCTE(node *CTENode) string
	VisitNamedFunction(node *NamedFunctionNode) string
	VisitCase(node *CaseNode) string
	VisitGroupingSet(node *GroupingSetNode) string
	VisitAlias(node *AliasNode) string
	VisitBindParam(node *BindParamNode) string
	VisitCasted(node *CastedNode) string
}

// Parameterizer is implemented by visitors that support parameterized queries.
// Callers use type assertion to extract collected parameters after SQL generation.
type Parameterizer interface {
	Params() []any
	Reset()
}

// Literal wraps a raw Go value into a LiteralNode. If val already
// implements Node, it is returned as-is.
func Literal(val any) Node {
	if n, ok := val.(Node); ok {
		return n
	}
	lit := &LiteralNode{Value: val}
	lit.Predications.self = lit
	lit.Combinable.self = lit
	return lit
}

// OnConflictAction specifies the action for ON CONFLICT clauses.
type OnConflictAction int

const (
	DoNothing OnConflictAction = iota
	DoUpdate
)

// AssignmentNode represents a column = value pair in SET clauses.
type AssignmentNode struct {
	Left  Node // column (Attribute)
	Right Node // value
}

func (n *AssignmentNode) Accept(v Visitor) string { return v.VisitAssignment(n) }

// InsertStatement represents INSERT INTO ... VALUES / SELECT.
type InsertStatement struct {
	Into       Node            // *Table
	Columns    []Node          // column list
	Values     [][]Node        // rows of values (multi-row)
	Select     Node            // for INSERT FROM SELECT (mutually exclusive with Values)
	Returning  []Node          // RETURNING columns
	OnConflict *OnConflictNode // ON CONFLICT clause
}

func (n *InsertStatement) Accept(v Visitor) string { return v.VisitInsertStatement(n) }

// UpdateStatement represents UPDATE ... SET ... WHERE.
type UpdateStatement struct {
	Table       Node
	Assignments []*AssignmentNode
	Wheres      []Node
	Returning   []Node
}

func (n *UpdateStatement) Accept(v Visitor) string { return v.VisitUpdateStatement(n) }

// DeleteStatement represents DELETE FROM ... WHERE.
type DeleteStatement struct {
	From      Node
	Wheres    []Node
	Returning []Node
}

func (n *DeleteStatement) Accept(v Visitor) string { return v.VisitDeleteStatement(n) }

// OnConflictNode represents ON CONFLICT (...) DO NOTHING / DO UPDATE SET ...
type OnConflictNode struct {
	Columns     []Node            // conflict target columns
	Action      OnConflictAction  // DoNothing or DoUpdate
	Assignments []*AssignmentNode // SET for DO UPDATE
	Wheres      []Node            // WHERE for DO UPDATE
}

func (n *OnConflictNode) Accept(v Visitor) string { return v.VisitOnConflict(n) }
