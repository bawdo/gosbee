// Package visitors provides SQL dialect generators that walk the AST.
package visitors

import (
	"fmt"
	"strings"

	"github.com/bawdo/gosbee/internal/quoting"
	"github.com/bawdo/gosbee/nodes"
)

// Operator SQL strings for InfixOp values.
var infixOpSQL = [...]string{
	nodes.OpPlus:       "+",
	nodes.OpMinus:      "-",
	nodes.OpMultiply:   "*",
	nodes.OpDivide:     "/",
	nodes.OpBitwiseAnd: "&",
	nodes.OpBitwiseOr:  "|",
	nodes.OpBitwiseXor: "^",
	nodes.OpShiftLeft:  "<<",
	nodes.OpShiftRight: ">>",
	nodes.OpConcat:     "||",
}

// needsParens returns true if the node should be wrapped in parentheses
// when used as a child of an infix or unary math expression.
func needsParens(n nodes.Node) bool {
	switch n.(type) {
	case *nodes.InfixNode, *nodes.UnaryMathNode:
		return true
	}
	return false
}

// Operator SQL strings for ComparisonOp values.
var comparisonOpSQL = [...]string{
	nodes.OpEq:                "=",
	nodes.OpNotEq:             "!=",
	nodes.OpGt:                ">",
	nodes.OpGtEq:              ">=",
	nodes.OpLt:                "<",
	nodes.OpLtEq:              "<=",
	nodes.OpLike:              "LIKE",
	nodes.OpNotLike:           "NOT LIKE",
	nodes.OpRegexp:            "~",
	nodes.OpNotRegexp:         "!~",
	nodes.OpDistinctFrom:      "IS DISTINCT FROM",
	nodes.OpNotDistinctFrom:   "IS NOT DISTINCT FROM",
	nodes.OpCaseSensitiveEq:   "=",
	nodes.OpCaseInsensitiveEq: "=",
	nodes.OpContains:          "@>",
	nodes.OpOverlaps:          "&&",
}

// SQL keywords for JoinType values.
var joinTypeSQL = [...]string{
	nodes.InnerJoin:      "INNER JOIN",
	nodes.LeftOuterJoin:  "LEFT OUTER JOIN",
	nodes.RightOuterJoin: "RIGHT OUTER JOIN",
	nodes.FullOuterJoin:  "FULL OUTER JOIN",
	nodes.CrossJoin:      "CROSS JOIN",
	nodes.StringJoin:     "",
}

// SQL keywords for LockMode values.
var lockModeSQL = [...]string{
	nodes.NoLock:         "",
	nodes.ForUpdate:      "FOR UPDATE",
	nodes.ForShare:       "FOR SHARE",
	nodes.ForNoKeyUpdate: "FOR NO KEY UPDATE",
	nodes.ForKeyShare:    "FOR KEY SHARE",
}

// SQL keywords for SetOpType values.
var setOpTypeSQL = [...]string{
	nodes.Union:        "UNION",
	nodes.UnionAll:     "UNION ALL",
	nodes.Intersect:    "INTERSECT",
	nodes.IntersectAll: "INTERSECT ALL",
	nodes.Except:       "EXCEPT",
	nodes.ExceptAll:    "EXCEPT ALL",
}

// Option configures a visitor at construction time.
type Option func(*baseVisitor)

// WithParams enables parameterized query mode. When enabled, literal values
// are replaced with bind placeholders and collected for separate retrieval.
//
// Note: Parameterized mode is now enabled by default. This option is kept
// for backwards compatibility and has no effect.
func WithParams() Option {
	return func(b *baseVisitor) {
		b.parameterize = true
	}
}

// WithoutParams disables parameterized query mode.
//
// ⚠️ WARNING: Disables SQL injection protection. Only use for debugging or when
// you're certain all values are trusted. Production code should NEVER use this option.
//
// When disabled, literal values are interpolated directly into the SQL string
// with basic escaping only. This is convenient for debugging but creates serious
// security vulnerabilities with untrusted input.
func WithoutParams() Option {
	return func(b *baseVisitor) {
		b.parameterize = false
	}
}

// baseVisitor implements the shared SQL generation logic used by all dialects.
// Dialect-specific visitors embed *baseVisitor and set the outer field to
// themselves, enabling correct virtual dispatch through the Visitor interface.
type baseVisitor struct {
	// outer is the concrete dialect visitor. All recursive Accept calls
	// go through outer so that dialect overrides are respected.
	outer nodes.Visitor

	// quoteIdent quotes a SQL identifier (table name, column name).
	quoteIdent func(string) string

	// parameterize enables bind-parameter mode.
	parameterize bool

	// params accumulates bind parameter values during SQL generation.
	params []any

	// paramIndex tracks the next parameter number (1-based).
	paramIndex int

	// placeholder returns the bind placeholder for a given parameter index.
	// PostgreSQL uses $1, $2; MySQL/SQLite use ?.
	placeholder func(int) string
}

// applyOptions applies functional options to the baseVisitor.
func (b *baseVisitor) applyOptions(opts []Option) {
	for _, o := range opts {
		o(b)
	}
}

// Params returns the collected bind parameters from the last SQL generation.
func (b *baseVisitor) Params() []any {
	return b.params
}

// Reset clears collected parameters for reuse.
func (b *baseVisitor) Reset() {
	b.params = nil
	b.paramIndex = 0
}

func (b *baseVisitor) VisitTable(n *nodes.Table) string {
	return b.quoteIdent(n.Name)
}

func (b *baseVisitor) VisitTableAlias(n *nodes.TableAlias) string {
	if tbl, ok := n.Relation.(*nodes.Table); ok {
		return b.quoteIdent(tbl.Name) + " AS " + b.quoteIdent(n.AliasName)
	}
	return "(" + n.Relation.Accept(b.outer) + ") AS " + b.quoteIdent(n.AliasName)
}

func (b *baseVisitor) VisitAttribute(n *nodes.Attribute) string {
	return b.qualifierName(n.Relation) + "." + b.quoteIdent(n.Name)
}

// qualifierName returns the quoted name used to qualify a column reference.
func (b *baseVisitor) qualifierName(rel nodes.Node) string {
	return b.quoteIdent(nodes.RelationName(rel))
}

func (b *baseVisitor) VisitLiteral(n *nodes.LiteralNode) string {
	return b.literalToSQL(n.Value)
}

func (b *baseVisitor) literalToSQL(val any) string {
	// nil always renders as NULL keyword, never parameterized.
	if val == nil {
		return "NULL"
	}

	// In parameterize mode, emit a placeholder and collect the value.
	if b.parameterize {
		b.paramIndex++
		b.params = append(b.params, val)
		return b.placeholder(b.paramIndex)
	}

	switch v := val.(type) {
	case string:
		return "'" + quoting.EscapeString(v) + "'"
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case int:
		return fmt.Sprintf("%d", v)
	case int8:
		return fmt.Sprintf("%d", v)
	case int16:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case uint:
		return fmt.Sprintf("%d", v)
	case uint8:
		return fmt.Sprintf("%d", v)
	case uint16:
		return fmt.Sprintf("%d", v)
	case uint32:
		return fmt.Sprintf("%d", v)
	case uint64:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case float64:
		return fmt.Sprintf("%g", v)
	default:
		panic(fmt.Sprintf("gosbee: unsupported literal type %T", v))
	}
}

func (b *baseVisitor) VisitStar(n *nodes.StarNode) string {
	if n.Table != nil {
		return b.quoteIdent(n.Table.Name) + ".*"
	}
	return "*"
}

func (b *baseVisitor) VisitSqlLiteral(n *nodes.SqlLiteral) string {
	if b.parameterize && len(n.Binds) > 0 {
		b.params = append(b.params, n.Binds...)
		for range n.Binds {
			b.paramIndex++
		}
	}
	return n.Raw
}

func (b *baseVisitor) VisitComparison(n *nodes.ComparisonNode) string {
	left := n.Left.Accept(b.outer)
	right := n.Right.Accept(b.outer)
	if n.Op == nodes.OpCaseInsensitiveEq {
		return "LOWER(" + left + ") = LOWER(" + right + ")"
	}
	return left + " " + comparisonOpSQL[n.Op] + " " + right
}

func (b *baseVisitor) VisitUnary(n *nodes.UnaryNode) string {
	expr := n.Expr.Accept(b.outer)
	switch n.Op {
	case nodes.OpIsNull:
		return expr + " IS NULL"
	case nodes.OpIsNotNull:
		return expr + " IS NOT NULL"
	default:
		return expr
	}
}

func (b *baseVisitor) VisitAnd(n *nodes.AndNode) string {
	left := n.Left.Accept(b.outer)
	right := n.Right.Accept(b.outer)
	return left + " AND " + right
}

func (b *baseVisitor) VisitOr(n *nodes.OrNode) string {
	left := n.Left.Accept(b.outer)
	right := n.Right.Accept(b.outer)
	return left + " OR " + right
}

func (b *baseVisitor) VisitNot(n *nodes.NotNode) string {
	return "NOT (" + n.Expr.Accept(b.outer) + ")"
}

func (b *baseVisitor) VisitIn(n *nodes.InNode) string {
	expr := n.Expr.Accept(b.outer)
	vals := make([]string, len(n.Vals))
	for i, v := range n.Vals {
		vals[i] = v.Accept(b.outer)
	}
	keyword := "IN"
	if n.Negate {
		keyword = "NOT IN"
	}
	return expr + " " + keyword + " (" + strings.Join(vals, ", ") + ")"
}

func (b *baseVisitor) VisitBetween(n *nodes.BetweenNode) string {
	expr := n.Expr.Accept(b.outer)
	low := n.Low.Accept(b.outer)
	high := n.High.Accept(b.outer)
	keyword := "BETWEEN"
	if n.Negate {
		keyword = "NOT BETWEEN"
	}
	return expr + " " + keyword + " " + low + " AND " + high
}

func (b *baseVisitor) VisitGrouping(n *nodes.GroupingNode) string {
	return "(" + n.Expr.Accept(b.outer) + ")"
}

func (b *baseVisitor) VisitOrdering(n *nodes.OrderingNode) string {
	expr := n.Expr.Accept(b.outer)
	if n.Direction == nodes.Desc {
		expr += " DESC"
	} else {
		expr += " ASC"
	}
	switch n.Nulls {
	case nodes.NullsFirst:
		expr += " NULLS FIRST"
	case nodes.NullsLast:
		expr += " NULLS LAST"
	}
	return expr
}

func (b *baseVisitor) VisitJoin(n *nodes.JoinNode) string {
	// StringJoin: raw SQL fragment, output directly.
	if n.Type == nodes.StringJoin {
		return n.Right.Accept(b.outer)
	}

	rightSQL := n.Right.Accept(b.outer)

	// Wrap subqueries in parentheses.
	if _, ok := n.Right.(*nodes.SelectCore); ok {
		rightSQL = "(" + rightSQL + ")"
	}

	var sb strings.Builder
	sb.WriteString(joinTypeSQL[n.Type])
	if n.Lateral {
		sb.WriteString(" LATERAL")
	}
	sb.WriteString(" ")
	sb.WriteString(rightSQL)

	if n.On != nil {
		sb.WriteString(" ON ")
		sb.WriteString(n.On.Accept(b.outer))
	}

	return sb.String()
}

func (b *baseVisitor) VisitInsertStatement(n *nodes.InsertStatement) string {
	var sb strings.Builder

	sb.WriteString("INSERT INTO ")
	sb.WriteString(n.Into.Accept(b.outer))

	// Columns
	if len(n.Columns) > 0 {
		sb.WriteString(" (")
		cols := make([]string, len(n.Columns))
		for i, c := range n.Columns {
			cols[i] = b.quoteIdent(c.(*nodes.Attribute).Name)
		}
		sb.WriteString(strings.Join(cols, ", "))
		sb.WriteString(")")
	}

	// INSERT FROM SELECT
	if n.Select != nil {
		sb.WriteString(" ")
		sb.WriteString(n.Select.Accept(b.outer))
	} else if len(n.Values) > 0 {
		sb.WriteString(" VALUES ")
		rows := make([]string, len(n.Values))
		for i, row := range n.Values {
			vals := make([]string, len(row))
			for j, v := range row {
				vals[j] = v.Accept(b.outer)
			}
			rows[i] = "(" + strings.Join(vals, ", ") + ")"
		}
		sb.WriteString(strings.Join(rows, ", "))
	}

	// ON CONFLICT
	if n.OnConflict != nil {
		sb.WriteString(" ")
		sb.WriteString(n.OnConflict.Accept(b.outer))
	}

	// RETURNING
	if len(n.Returning) > 0 {
		sb.WriteString(" RETURNING ")
		rets := make([]string, len(n.Returning))
		for i, r := range n.Returning {
			rets[i] = r.Accept(b.outer)
		}
		sb.WriteString(strings.Join(rets, ", "))
	}

	return sb.String()
}

func (b *baseVisitor) VisitUpdateStatement(n *nodes.UpdateStatement) string {
	var sb strings.Builder

	sb.WriteString("UPDATE ")
	sb.WriteString(n.Table.Accept(b.outer))

	// SET
	if len(n.Assignments) > 0 {
		sb.WriteString(" SET ")
		assigns := make([]string, len(n.Assignments))
		for i, a := range n.Assignments {
			assigns[i] = a.Accept(b.outer)
		}
		sb.WriteString(strings.Join(assigns, ", "))
	}

	// WHERE
	if len(n.Wheres) > 0 {
		sb.WriteString(" WHERE ")
		wheres := make([]string, len(n.Wheres))
		for i, w := range n.Wheres {
			wheres[i] = w.Accept(b.outer)
		}
		sb.WriteString(strings.Join(wheres, " AND "))
	}

	// RETURNING
	if len(n.Returning) > 0 {
		sb.WriteString(" RETURNING ")
		rets := make([]string, len(n.Returning))
		for i, r := range n.Returning {
			rets[i] = r.Accept(b.outer)
		}
		sb.WriteString(strings.Join(rets, ", "))
	}

	return sb.String()
}

func (b *baseVisitor) VisitDeleteStatement(n *nodes.DeleteStatement) string {
	var sb strings.Builder

	sb.WriteString("DELETE FROM ")
	sb.WriteString(n.From.Accept(b.outer))

	// WHERE
	if len(n.Wheres) > 0 {
		sb.WriteString(" WHERE ")
		wheres := make([]string, len(n.Wheres))
		for i, w := range n.Wheres {
			wheres[i] = w.Accept(b.outer)
		}
		sb.WriteString(strings.Join(wheres, " AND "))
	}

	// RETURNING
	if len(n.Returning) > 0 {
		sb.WriteString(" RETURNING ")
		rets := make([]string, len(n.Returning))
		for i, r := range n.Returning {
			rets[i] = r.Accept(b.outer)
		}
		sb.WriteString(strings.Join(rets, ", "))
	}

	return sb.String()
}

func (b *baseVisitor) VisitAssignment(n *nodes.AssignmentNode) string {
	left := n.Left.Accept(b.outer)
	right := n.Right.Accept(b.outer)
	return left + " = " + right
}

func (b *baseVisitor) VisitOnConflict(n *nodes.OnConflictNode) string {
	var sb strings.Builder

	sb.WriteString("ON CONFLICT")

	if len(n.Columns) > 0 {
		sb.WriteString(" (")
		cols := make([]string, len(n.Columns))
		for i, c := range n.Columns {
			cols[i] = b.quoteIdent(c.(*nodes.Attribute).Name)
		}
		sb.WriteString(strings.Join(cols, ", "))
		sb.WriteString(")")
	}

	if n.Action == nodes.DoNothing {
		sb.WriteString(" DO NOTHING")
	} else {
		sb.WriteString(" DO UPDATE SET ")
		assigns := make([]string, len(n.Assignments))
		for i, a := range n.Assignments {
			assigns[i] = a.Accept(b.outer)
		}
		sb.WriteString(strings.Join(assigns, ", "))

		if len(n.Wheres) > 0 {
			sb.WriteString(" WHERE ")
			wheres := make([]string, len(n.Wheres))
			for i, w := range n.Wheres {
				wheres[i] = w.Accept(b.outer)
			}
			sb.WriteString(strings.Join(wheres, " AND "))
		}
	}

	return sb.String()
}

func (b *baseVisitor) VisitInfix(n *nodes.InfixNode) string {
	left := n.Left.Accept(b.outer)
	if needsParens(n.Left) {
		left = "(" + left + ")"
	}
	right := n.Right.Accept(b.outer)
	if needsParens(n.Right) {
		right = "(" + right + ")"
	}
	return left + " " + infixOpSQL[n.Op] + " " + right
}

func (b *baseVisitor) VisitUnaryMath(n *nodes.UnaryMathNode) string {
	expr := n.Expr.Accept(b.outer)
	if needsParens(n.Expr) {
		expr = "(" + expr + ")"
	}
	return "~" + expr
}

// Aggregate function SQL names.
var aggregateFuncSQL = [...]string{
	nodes.AggCount: "COUNT",
	nodes.AggSum:   "SUM",
	nodes.AggAvg:   "AVG",
	nodes.AggMin:   "MIN",
	nodes.AggMax:   "MAX",
}

func (b *baseVisitor) VisitAggregate(n *nodes.AggregateNode) string {
	var sb strings.Builder
	sb.WriteString(aggregateFuncSQL[n.Func])
	sb.WriteString("(")
	if n.Distinct {
		sb.WriteString("DISTINCT ")
	}
	if n.Expr == nil {
		sb.WriteString("*")
	} else {
		sb.WriteString(n.Expr.Accept(b.outer))
	}
	sb.WriteString(")")
	if n.Filter != nil {
		sb.WriteString(" FILTER (WHERE ")
		sb.WriteString(n.Filter.Accept(b.outer))
		sb.WriteString(")")
	}
	return sb.String()
}

// Extract field SQL names.
var extractFieldSQL = [...]string{
	nodes.ExtractYear:    "YEAR",
	nodes.ExtractMonth:   "MONTH",
	nodes.ExtractDay:     "DAY",
	nodes.ExtractHour:    "HOUR",
	nodes.ExtractMinute:  "MINUTE",
	nodes.ExtractSecond:  "SECOND",
	nodes.ExtractDow:     "DOW",
	nodes.ExtractDoy:     "DOY",
	nodes.ExtractEpoch:   "EPOCH",
	nodes.ExtractQuarter: "QUARTER",
	nodes.ExtractWeek:    "WEEK",
}

func (b *baseVisitor) VisitExtract(n *nodes.ExtractNode) string {
	return "EXTRACT(" + extractFieldSQL[n.Field] + " FROM " + n.Expr.Accept(b.outer) + ")"
}

// Window function SQL names.
var windowFuncSQL = [...]string{
	nodes.WinRowNumber:   "ROW_NUMBER",
	nodes.WinRank:        "RANK",
	nodes.WinDenseRank:   "DENSE_RANK",
	nodes.WinNtile:       "NTILE",
	nodes.WinLag:         "LAG",
	nodes.WinLead:        "LEAD",
	nodes.WinFirstValue:  "FIRST_VALUE",
	nodes.WinLastValue:   "LAST_VALUE",
	nodes.WinNthValue:    "NTH_VALUE",
	nodes.WinCumeDist:    "CUME_DIST",
	nodes.WinPercentRank: "PERCENT_RANK",
}

func (b *baseVisitor) VisitWindowFunction(n *nodes.WindowFuncNode) string {
	var sb strings.Builder
	sb.WriteString(windowFuncSQL[n.Func])
	sb.WriteString("(")
	for i, arg := range n.Args {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(arg.Accept(b.outer))
	}
	sb.WriteString(")")
	return sb.String()
}

func (b *baseVisitor) VisitOver(n *nodes.OverNode) string {
	var sb strings.Builder
	sb.WriteString(n.Expr.Accept(b.outer))
	sb.WriteString(" OVER ")
	if n.WindowName != "" {
		sb.WriteString(b.quoteIdent(n.WindowName))
	} else {
		sb.WriteString(b.renderWindowDef(n.Window))
	}
	return sb.String()
}

func (b *baseVisitor) VisitExists(n *nodes.ExistsNode) string {
	var sb strings.Builder
	if n.Negated {
		sb.WriteString("NOT ")
	}
	sb.WriteString("EXISTS (")
	sb.WriteString(n.Subquery.Accept(b.outer))
	sb.WriteString(")")
	return sb.String()
}

func (b *baseVisitor) VisitSetOperation(n *nodes.SetOperationNode) string {
	var sb strings.Builder
	sb.WriteString("(")
	sb.WriteString(n.Left.Accept(b.outer))
	sb.WriteString(") ")
	sb.WriteString(setOpTypeSQL[n.Type])
	sb.WriteString(" (")
	sb.WriteString(n.Right.Accept(b.outer))
	sb.WriteString(")")
	return sb.String()
}

func (b *baseVisitor) VisitCTE(n *nodes.CTENode) string {
	var sb strings.Builder
	sb.WriteString(b.quoteIdent(n.Name))
	if len(n.Columns) > 0 {
		sb.WriteString(" (")
		quoted := make([]string, len(n.Columns))
		for i, c := range n.Columns {
			quoted[i] = b.quoteIdent(c)
		}
		sb.WriteString(strings.Join(quoted, ", "))
		sb.WriteString(")")
	}
	sb.WriteString(" AS (")
	sb.WriteString(n.Query.Accept(b.outer))
	sb.WriteString(")")
	return sb.String()
}

func (b *baseVisitor) VisitNamedFunction(n *nodes.NamedFunctionNode) string {
	var sb strings.Builder
	validateSQLFunctionName(n.Name)
	// Special case: CAST(expr AS type)
	if n.Name == "CAST" && len(n.Args) == 2 {
		sb.WriteString("CAST(")
		sb.WriteString(n.Args[0].Accept(b.outer))
		sb.WriteString(" AS ")
		sb.WriteString(n.Args[1].Accept(b.outer))
		sb.WriteString(")")
		return sb.String()
	}
	sb.WriteString(n.Name)
	sb.WriteString("(")
	if n.Distinct {
		sb.WriteString("DISTINCT ")
	}
	for i, arg := range n.Args {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(arg.Accept(b.outer))
	}
	sb.WriteString(")")
	return sb.String()
}

func (b *baseVisitor) VisitCase(n *nodes.CaseNode) string {
	var sb strings.Builder
	sb.WriteString("CASE")
	if n.Operand != nil {
		sb.WriteString(" ")
		sb.WriteString(n.Operand.Accept(b.outer))
	}
	for _, w := range n.Whens {
		sb.WriteString(" WHEN ")
		sb.WriteString(w.Condition.Accept(b.outer))
		sb.WriteString(" THEN ")
		sb.WriteString(w.Result.Accept(b.outer))
	}
	if n.ElseVal != nil {
		sb.WriteString(" ELSE ")
		sb.WriteString(n.ElseVal.Accept(b.outer))
	}
	sb.WriteString(" END")
	return sb.String()
}

// Grouping set type SQL keywords.
var groupingSetTypeSQL = [...]string{
	nodes.Cube:         "CUBE",
	nodes.Rollup:       "ROLLUP",
	nodes.GroupingSets: "GROUPING SETS",
}

func (b *baseVisitor) VisitGroupingSet(n *nodes.GroupingSetNode) string {
	var sb strings.Builder
	sb.WriteString(groupingSetTypeSQL[n.Type])
	sb.WriteString("(")
	if n.Type == nodes.GroupingSets {
		// GROUPING SETS ((col1, col2), (col3), ())
		for i, set := range n.Sets {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(")
			for j, col := range set {
				if j > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(col.Accept(b.outer))
			}
			sb.WriteString(")")
		}
	} else {
		// CUBE(col1, col2) or ROLLUP(col1, col2)
		for i, col := range n.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col.Accept(b.outer))
		}
	}
	sb.WriteString(")")
	return sb.String()
}

func (b *baseVisitor) VisitAlias(n *nodes.AliasNode) string {
	return n.Expr.Accept(b.outer) + " AS " + b.quoteIdent(n.Name)
}

func (b *baseVisitor) VisitBindParam(n *nodes.BindParamNode) string {
	// Always parameterize if in param mode, otherwise render as literal.
	if b.parameterize {
		b.paramIndex++
		b.params = append(b.params, n.Value)
		return b.placeholder(b.paramIndex)
	}
	return b.literalToSQL(n.Value)
}

func (b *baseVisitor) VisitCasted(n *nodes.CastedNode) string {
	valSQL := b.literalToSQL(n.Value)
	if n.TypeName != "" {
		validateSQLTypeName(n.TypeName)
		return "CAST(" + valSQL + " AS " + n.TypeName + ")"
	}
	return valSQL
}

// validateSQLTypeName panics if the type name contains characters outside
// the set of letters, digits, spaces, parentheses, and commas.
// This prevents SQL injection through crafted type names.
func validateSQLTypeName(name string) {
	for _, c := range name {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') &&
			(c < '0' || c > '9') && c != ' ' && c != '(' &&
			c != ')' && c != ',' && c != '_' {
			panic(fmt.Sprintf("gosbee: invalid SQL type name character %q in %q", string(c), name))
		}
	}
}

// validateSQLFunctionName panics if the function name contains characters
// outside the set of letters, digits, and underscores.
// This prevents SQL injection through crafted function names.
func validateSQLFunctionName(name string) {
	for _, c := range name {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') &&
			(c < '0' || c > '9') && c != '_' {
			panic(fmt.Sprintf("gosbee: invalid SQL function name character %q in %q", string(c), name))
		}
	}
}

// renderWindowDef renders a window definition as SQL: (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)
func (b *baseVisitor) renderWindowDef(w *nodes.WindowDefinition) string {
	if w == nil {
		return "()"
	}
	var sb strings.Builder
	sb.WriteString("(")
	needSpace := false
	if len(w.PartitionBy) > 0 {
		sb.WriteString("PARTITION BY ")
		parts := make([]string, len(w.PartitionBy))
		for i, p := range w.PartitionBy {
			parts[i] = p.Accept(b.outer)
		}
		sb.WriteString(strings.Join(parts, ", "))
		needSpace = true
	}
	if len(w.OrderBy) > 0 {
		if needSpace {
			sb.WriteString(" ")
		}
		sb.WriteString("ORDER BY ")
		orders := make([]string, len(w.OrderBy))
		for i, o := range w.OrderBy {
			orders[i] = o.Accept(b.outer)
		}
		sb.WriteString(strings.Join(orders, ", "))
		needSpace = true
	}
	if w.Frame != nil {
		if needSpace {
			sb.WriteString(" ")
		}
		sb.WriteString(b.renderFrame(w.Frame))
	}
	sb.WriteString(")")
	return sb.String()
}

// Frame type SQL keywords.
var frameTypeSQL = [...]string{
	nodes.FrameRows:  "ROWS",
	nodes.FrameRange: "RANGE",
}

// renderFrame renders a window frame as SQL.
func (b *baseVisitor) renderFrame(f *nodes.WindowFrame) string {
	var sb strings.Builder
	sb.WriteString(frameTypeSQL[f.Type])
	if f.End != nil {
		sb.WriteString(" BETWEEN ")
		sb.WriteString(b.renderBound(f.Start))
		sb.WriteString(" AND ")
		sb.WriteString(b.renderBound(*f.End))
	} else {
		sb.WriteString(" ")
		sb.WriteString(b.renderBound(f.Start))
	}
	return sb.String()
}

// renderBound renders a single frame bound as SQL.
func (b *baseVisitor) renderBound(fb nodes.FrameBound) string {
	switch fb.Type {
	case nodes.BoundUnboundedPreceding:
		return "UNBOUNDED PRECEDING"
	case nodes.BoundPreceding:
		return fb.Offset.Accept(b.outer) + " PRECEDING"
	case nodes.BoundCurrentRow:
		return "CURRENT ROW"
	case nodes.BoundFollowing:
		return fb.Offset.Accept(b.outer) + " FOLLOWING"
	case nodes.BoundUnboundedFollowing:
		return "UNBOUNDED FOLLOWING"
	default:
		return ""
	}
}

func (b *baseVisitor) VisitSelectCore(n *nodes.SelectCore) string {
	var sb strings.Builder

	b.writeCTEs(&sb, n.CTEs)
	b.writeComment(&sb, n.Comment)
	sb.WriteString("SELECT ")
	b.writeHints(&sb, n.Hints)
	b.writeDistinct(&sb, n.Distinct, n.DistinctOn)
	b.writeProjections(&sb, n.Projections)
	b.writeFrom(&sb, n.From)
	b.writeJoins(&sb, n.Joins)
	b.writeClause(&sb, " WHERE ", n.Wheres, " AND ")
	b.writeClause(&sb, " GROUP BY ", n.Groups, ", ")
	b.writeClause(&sb, " HAVING ", n.Havings, " AND ")
	b.writeWindowClause(&sb, n.Windows)
	b.writeClause(&sb, " ORDER BY ", n.Orders, ", ")
	b.writeNodeClause(&sb, " LIMIT ", n.Limit)
	b.writeNodeClause(&sb, " OFFSET ", n.Offset)
	b.writeLock(&sb, n.Lock, n.SkipLocked)

	return sb.String()
}

// writeClause writes "keyword item1 sep item2 sep ..." if items is non-empty.
func (b *baseVisitor) writeClause(sb *strings.Builder, keyword string, items []nodes.Node, sep string) {
	if len(items) == 0 {
		return
	}
	sb.WriteString(keyword)
	for i, item := range items {
		if i > 0 {
			sb.WriteString(sep)
		}
		sb.WriteString(item.Accept(b.outer))
	}
}

// writeNodeClause writes "keyword node" if node is non-nil.
func (b *baseVisitor) writeNodeClause(sb *strings.Builder, keyword string, n nodes.Node) {
	if n != nil {
		sb.WriteString(keyword)
		sb.WriteString(n.Accept(b.outer))
	}
}

func (b *baseVisitor) writeCTEs(sb *strings.Builder, ctes []*nodes.CTENode) {
	if len(ctes) == 0 {
		return
	}
	hasRecursive := false
	for _, cte := range ctes {
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
	for i, cte := range ctes {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(cte.Accept(b.outer))
	}
	sb.WriteString(" ")
}

func (b *baseVisitor) writeComment(sb *strings.Builder, comment string) {
	if comment != "" {
		sb.WriteString("/* ")
		sb.WriteString(strings.ReplaceAll(comment, "*/", "* /"))
		sb.WriteString(" */ ")
	}
}

func (b *baseVisitor) writeHints(sb *strings.Builder, hints []string) {
	if len(hints) == 0 {
		return
	}
	sb.WriteString("/*+ ")
	for i, h := range hints {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(strings.ReplaceAll(h, "*/", "* /"))
	}
	sb.WriteString(" */ ")
}

func (b *baseVisitor) writeDistinct(sb *strings.Builder, distinct bool, distinctOn []nodes.Node) {
	if len(distinctOn) > 0 {
		sb.WriteString("DISTINCT ON (")
		for i, c := range distinctOn {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(c.Accept(b.outer))
		}
		sb.WriteString(") ")
	} else if distinct {
		sb.WriteString("DISTINCT ")
	}
}

func (b *baseVisitor) writeProjections(sb *strings.Builder, projections []nodes.Node) {
	if len(projections) == 0 {
		sb.WriteString("*")
		return
	}
	for i, p := range projections {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(p.Accept(b.outer))
	}
}

func (b *baseVisitor) writeFrom(sb *strings.Builder, from nodes.Node) {
	if from != nil {
		sb.WriteString(" FROM ")
		sb.WriteString(from.Accept(b.outer))
	}
}

func (b *baseVisitor) writeJoins(sb *strings.Builder, joins []*nodes.JoinNode) {
	for _, j := range joins {
		sb.WriteString(" ")
		sb.WriteString(j.Accept(b.outer))
	}
}

func (b *baseVisitor) writeWindowClause(sb *strings.Builder, windows []*nodes.WindowDefinition) {
	if len(windows) == 0 {
		return
	}
	sb.WriteString(" WINDOW ")
	for i, w := range windows {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(b.quoteIdent(w.Name))
		sb.WriteString(" AS ")
		sb.WriteString(b.renderWindowDef(&nodes.WindowDefinition{
			PartitionBy: w.PartitionBy,
			OrderBy:     w.OrderBy,
			Frame:       w.Frame,
		}))
	}
}

func (b *baseVisitor) writeLock(sb *strings.Builder, lock nodes.LockMode, skipLocked bool) {
	if lock != nodes.NoLock {
		sb.WriteString(" ")
		sb.WriteString(lockModeSQL[lock])
		if skipLocked {
			sb.WriteString(" SKIP LOCKED")
		}
	}
}
