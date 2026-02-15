package nodes

import "testing"

// --- Table / Attribute creation ---

func TestTableCreatesAttributes(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	col := users.Col("id")

	if col.Name != "id" {
		t.Errorf("expected col name %q, got %q", "id", col.Name)
	}
	if col.Relation != users {
		t.Error("expected attribute relation to be the users table")
	}
}

func TestTableAlias(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	u := users.Alias("u")

	if u.AliasName != "u" {
		t.Errorf("expected alias %q, got %q", "u", u.AliasName)
	}
	if u.Relation != users {
		t.Error("expected alias to reference the original table")
	}
}

func TestTableAliasCreatesAttributes(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	u := users.Alias("u")
	col := u.Col("name")

	if col.Name != "name" {
		t.Errorf("expected col name %q, got %q", "name", col.Name)
	}
	if col.Relation != u {
		t.Error("expected attribute relation to be the table alias")
	}
}

func TestTableStar(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	star := users.Star()

	if star.Table != users {
		t.Error("expected qualified star to reference the table")
	}
}

func TestUnqualifiedStar(t *testing.T) {
	t.Parallel()
	star := Star()
	if star.Table != nil {
		t.Error("expected unqualified star to have nil table")
	}
}

// --- Literal wrapping ---

func TestLiteralWrapsRawValues(t *testing.T) {
	t.Parallel()
	n := Literal(42)
	lit, ok := n.(*LiteralNode)
	if !ok {
		t.Fatalf("expected *LiteralNode, got %T", n)
	}
	if lit.Value != 42 {
		t.Errorf("expected value 42, got %v", lit.Value)
	}
}

func TestLiteralPassesThroughNodes(t *testing.T) {
	t.Parallel()
	attr := NewAttribute(NewTable("t"), "col")
	n := Literal(attr)
	if n != attr {
		t.Error("expected Literal to pass through an existing Node")
	}
}

func TestLiteralSetsSelfPointers(t *testing.T) {
	t.Parallel()
	n := Literal(42)
	lit := n.(*LiteralNode)

	// Predications.self must be set so chaining works without nil panic
	cmp := lit.Eq(10)
	if cmp.Left != lit {
		t.Error("expected Left to be the literal node")
	}

	// Combinable.self must be set so And/Or work
	other := NewAttribute(NewTable("t"), "col").Eq(1)
	andNode := lit.Eq(10).And(other)
	if andNode == nil {
		t.Error("expected And to produce a non-nil node")
	}
}

// --- Predications return correct node types ---

func TestEqReturnsComparisonNodeWithOpEq(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	col := users.Col("name")
	cmp := col.Eq("Alice")

	if cmp.Op != OpEq {
		t.Errorf("expected OpEq, got %d", cmp.Op)
	}
	if cmp.Left != col {
		t.Error("expected left to be the attribute")
	}
	right, ok := cmp.Right.(*LiteralNode)
	if !ok {
		t.Fatalf("expected right to be *LiteralNode, got %T", cmp.Right)
	}
	if right.Value != "Alice" {
		t.Errorf("expected right value %q, got %v", "Alice", right.Value)
	}
}

func TestComparisons(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("x")

	tests := []struct {
		name string
		node *ComparisonNode
		want ComparisonOp
	}{
		{"NotEq", col.NotEq(1), OpNotEq},
		{"Gt", col.Gt(10), OpGt},
		{"GtEq", col.GtEq(10), OpGtEq},
		{"Lt", col.Lt(5), OpLt},
		{"LtEq", col.LtEq(5), OpLtEq},
		{"Like", col.Like("%foo%"), OpLike},
		{"NotLike", col.NotLike("%bar%"), OpNotLike},
		{"MatchesRegexp", col.MatchesRegexp("^A.*"), OpRegexp},
		{"DoesNotMatchRegexp", col.DoesNotMatchRegexp("^A.*"), OpNotRegexp},
		{"IsDistinctFrom", col.IsDistinctFrom(nil), OpDistinctFrom},
		{"IsNotDistinctFrom", col.IsNotDistinctFrom(42), OpNotDistinctFrom},
		{"CaseSensitiveEq", col.CaseSensitiveEq("Alice"), OpCaseSensitiveEq},
		{"CaseInsensitiveEq", col.CaseInsensitiveEq("alice"), OpCaseInsensitiveEq},
		{"Contains", col.Contains("{1,2}"), OpContains},
		{"Overlaps", col.Overlaps("{3,4}"), OpOverlaps},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.node.Op != tt.want {
				t.Errorf("expected %v, got %v", tt.want, tt.node.Op)
			}
		})
	}
}

func TestNodeToNodePredicate(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	posts := NewTable("posts")
	cmp := users.Col("id").Eq(posts.Col("author_id"))

	if _, ok := cmp.Right.(*Attribute); !ok {
		t.Errorf("expected right to be *Attribute, got %T", cmp.Right)
	}
}

// --- Unary predicates ---

func TestIsNull(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("deleted_at")
	u := col.IsNull()

	if u.Op != OpIsNull {
		t.Errorf("expected OpIsNull, got %d", u.Op)
	}
	if u.Expr != col {
		t.Error("expected expr to be the attribute")
	}
}

func TestIsNotNull(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("deleted_at")
	u := col.IsNotNull()

	if u.Op != OpIsNotNull {
		t.Errorf("expected OpIsNotNull, got %d", u.Op)
	}
}

// --- In / NotIn ---

func TestIn(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("status")
	in := col.In("active", "pending")

	if in.Negate {
		t.Error("expected In to not be negated")
	}
	if len(in.Vals) != 2 {
		t.Fatalf("expected 2 values, got %d", len(in.Vals))
	}
	if in.Expr != col {
		t.Error("expected expr to be the attribute")
	}
}

func TestNotIn(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("status")
	in := col.NotIn("deleted")

	if !in.Negate {
		t.Error("expected NotIn to be negated")
	}
	if len(in.Vals) != 1 {
		t.Fatalf("expected 1 value, got %d", len(in.Vals))
	}
}

// --- Between ---

func TestBetween(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("age")
	b := col.Between(18, 65)

	if b.Expr != col {
		t.Error("expected expr to be the attribute")
	}
	low, ok := b.Low.(*LiteralNode)
	if !ok {
		t.Fatalf("expected low to be *LiteralNode, got %T", b.Low)
	}
	if low.Value != 18 {
		t.Errorf("expected low value 18, got %v", low.Value)
	}
	high, ok := b.High.(*LiteralNode)
	if !ok {
		t.Fatalf("expected high to be *LiteralNode, got %T", b.High)
	}
	if high.Value != 65 {
		t.Errorf("expected high value 65, got %v", high.Value)
	}
}

// --- Combinators ---

func TestAndChaining(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	cond1 := users.Col("active").Eq(true)
	cond2 := users.Col("age").Gt(18)
	and := cond1.And(cond2)

	if and.Left != cond1 {
		t.Error("expected left to be cond1")
	}
	if and.Right != cond2 {
		t.Error("expected right to be cond2")
	}
}

func TestOrWrapsInGrouping(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	cond1 := users.Col("role").Eq("admin")
	cond2 := users.Col("role").Eq("moderator")
	grouped := cond1.Or(cond2)

	// Or returns a GroupingNode wrapping an OrNode
	or, ok := grouped.Expr.(*OrNode)
	if !ok {
		t.Fatalf("expected GroupingNode.Expr to be *OrNode, got %T", grouped.Expr)
	}
	if or.Left != cond1 {
		t.Error("expected or.Left to be cond1")
	}
	if or.Right != cond2 {
		t.Error("expected or.Right to be cond2")
	}
}

func TestNotCombinator(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("active")
	cmp := col.Eq(true)
	not := cmp.Not()

	if not.Expr != cmp {
		t.Error("expected not.Expr to be the comparison")
	}
}

// --- Combinable on result nodes ---

func TestComparisonIsCombinableViaAnd(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	c1 := users.Col("a").Eq(1)
	c2 := users.Col("b").Eq(2)
	and := c1.And(c2)

	if and.Left != c1 || and.Right != c2 {
		t.Error("expected And to chain two comparisons")
	}
}

// --- SqlLiteral ---

func TestSqlLiteralPredications(t *testing.T) {
	t.Parallel()
	raw := NewSqlLiteral("COUNT(*)")
	cmp := raw.Eq(0)

	if cmp.Op != OpEq {
		t.Errorf("expected OpEq, got %d", cmp.Op)
	}
	if cmp.Left != raw {
		t.Error("expected left to be the SqlLiteral")
	}
}

// --- SelectCore ---

func TestSelectCoreHoldsData(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	posts := NewTable("posts")

	join := &JoinNode{
		Left:  users,
		Right: posts,
		Type:  InnerJoin,
		On:    users.Col("id").Eq(posts.Col("user_id")),
	}

	sc := &SelectCore{
		From:        users,
		Projections: []Node{users.Col("name"), users.Col("email")},
		Wheres:      []Node{users.Col("active").Eq(true)},
		Joins:       []*JoinNode{join},
	}

	if sc.From != users {
		t.Error("expected From to be users table")
	}
	if len(sc.Projections) != 2 {
		t.Errorf("expected 2 projections, got %d", len(sc.Projections))
	}
	if len(sc.Wheres) != 1 {
		t.Errorf("expected 1 where, got %d", len(sc.Wheres))
	}
	if len(sc.Joins) != 1 {
		t.Errorf("expected 1 join, got %d", len(sc.Joins))
	}
	if sc.Joins[0].Type != InnerJoin {
		t.Errorf("expected InnerJoin, got %d", sc.Joins[0].Type)
	}
}

// --- JoinNode ---

func TestJoinTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		joinType JoinType
	}{
		{"InnerJoin", InnerJoin},
		{"LeftOuterJoin", LeftOuterJoin},
		{"RightOuterJoin", RightOuterJoin},
		{"FullOuterJoin", FullOuterJoin},
		{"CrossJoin", CrossJoin},
	}

	users := NewTable("users")
	posts := NewTable("posts")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			join := &JoinNode{
				Left:  users,
				Right: posts,
				Type:  tt.joinType,
			}
			if join.Type != tt.joinType {
				t.Errorf("expected join type %d, got %d", tt.joinType, join.Type)
			}
		})
	}
}

// --- Ordering ---

func TestAscOrdering(t *testing.T) {
	t.Parallel()
	col := NewTable("users").Col("name")
	ord := col.Asc()

	if ord.Direction != Asc {
		t.Errorf("expected Asc, got %d", ord.Direction)
	}
	if ord.Expr != col {
		t.Error("expected expr to be the attribute")
	}
}

func TestDescOrdering(t *testing.T) {
	t.Parallel()
	col := NewTable("users").Col("created_at")
	ord := col.Desc()

	if ord.Direction != Desc {
		t.Errorf("expected Desc, got %d", ord.Direction)
	}
	if ord.Expr != col {
		t.Error("expected expr to be the attribute")
	}
}

// --- SelectCore with Orders, Limit, Offset ---

func TestSelectCoreHoldsOrdersLimitOffset(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	sc := &SelectCore{
		From:   users,
		Orders: []Node{users.Col("name").Asc(), users.Col("id").Desc()},
		Limit:  Literal(10),
		Offset: Literal(20),
	}

	if len(sc.Orders) != 2 {
		t.Errorf("expected 2 orders, got %d", len(sc.Orders))
	}
	if sc.Limit == nil {
		t.Error("expected limit to be set")
	}
	if sc.Offset == nil {
		t.Error("expected offset to be set")
	}
}

// --- SelectCore with Groups and Havings ---

func TestSelectCoreHoldsGroupsAndHavings(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	sc := &SelectCore{
		From:    users,
		Groups:  []Node{users.Col("status")},
		Havings: []Node{NewSqlLiteral("COUNT(*)").Gt(5)},
	}

	if len(sc.Groups) != 1 {
		t.Errorf("expected 1 group, got %d", len(sc.Groups))
	}
	if len(sc.Havings) != 1 {
		t.Errorf("expected 1 having, got %d", len(sc.Havings))
	}
}

// --- SelectCore with Distinct ---

func TestSelectCoreHoldsDistinct(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	sc := &SelectCore{
		From:     users,
		Distinct: true,
	}

	if !sc.Distinct {
		t.Error("expected Distinct to be true")
	}
}

// --- Accept methods (ensure all nodes implement Node) ---

// stubVisitor implements Visitor for compile-time verification.
// NOTE: The canonical shared StubVisitor is in internal/testutil. This copy
// exists only because nodes_test.go (package nodes) cannot import testutil
// without creating an import cycle. Keep return values in sync with
// internal/testutil/stub_visitor.go.
type stubVisitor struct{}

func (sv stubVisitor) VisitTable(n *Table) string             { return n.Name }
func (sv stubVisitor) VisitTableAlias(n *TableAlias) string   { return n.AliasName }
func (sv stubVisitor) VisitAttribute(*Attribute) string       { return "attr" }
func (sv stubVisitor) VisitLiteral(*LiteralNode) string       { return "lit" }
func (sv stubVisitor) VisitStar(*StarNode) string             { return "*" }
func (sv stubVisitor) VisitSqlLiteral(n *SqlLiteral) string   { return n.Raw }
func (sv stubVisitor) VisitComparison(n *ComparisonNode) string {
	if n.Left == nil || n.Right == nil {
		return "comparison"
	}
	return n.Left.Accept(sv) + "=?" + n.Right.Accept(sv)
}
func (sv stubVisitor) VisitUnary(*UnaryNode) string                    { return "unary" }
func (sv stubVisitor) VisitAnd(*AndNode) string                        { return "and" }
func (sv stubVisitor) VisitOr(*OrNode) string                          { return "or" }
func (sv stubVisitor) VisitNot(*NotNode) string                        { return "not" }
func (sv stubVisitor) VisitIn(*InNode) string                          { return "in" }
func (sv stubVisitor) VisitBetween(*BetweenNode) string                { return "between" }
func (sv stubVisitor) VisitGrouping(*GroupingNode) string              { return "grouping" }
func (sv stubVisitor) VisitJoin(*JoinNode) string                      { return "join" }
func (sv stubVisitor) VisitOrdering(*OrderingNode) string              { return "ordering" }
func (sv stubVisitor) VisitSelectCore(*SelectCore) string              { return "select_core" }
func (sv stubVisitor) VisitInsertStatement(*InsertStatement) string    { return "insert" }
func (sv stubVisitor) VisitUpdateStatement(*UpdateStatement) string    { return "update" }
func (sv stubVisitor) VisitDeleteStatement(*DeleteStatement) string    { return "delete" }
func (sv stubVisitor) VisitAssignment(*AssignmentNode) string          { return "assign" }
func (sv stubVisitor) VisitOnConflict(*OnConflictNode) string          { return "conflict" }
func (sv stubVisitor) VisitInfix(*InfixNode) string                    { return "infix" }
func (sv stubVisitor) VisitUnaryMath(*UnaryMathNode) string            { return "unary_math" }
func (sv stubVisitor) VisitAggregate(*AggregateNode) string            { return "aggregate" }
func (sv stubVisitor) VisitExtract(*ExtractNode) string                { return "extract" }
func (sv stubVisitor) VisitWindowFunction(*WindowFuncNode) string      { return "window_func" }
func (sv stubVisitor) VisitOver(*OverNode) string                      { return "over" }
func (sv stubVisitor) VisitExists(*ExistsNode) string                  { return "exists" }
func (sv stubVisitor) VisitSetOperation(*SetOperationNode) string      { return "set_op" }
func (sv stubVisitor) VisitCTE(*CTENode) string                        { return "cte" }
func (sv stubVisitor) VisitNamedFunction(*NamedFunctionNode) string    { return "named_func" }
func (sv stubVisitor) VisitCase(*CaseNode) string                      { return "case" }
func (sv stubVisitor) VisitGroupingSet(*GroupingSetNode) string        { return "grouping_set" }
func (sv stubVisitor) VisitAlias(*AliasNode) string                    { return "alias" }
func (sv stubVisitor) VisitBindParam(*BindParamNode) string            { return "bind_param" }
func (sv stubVisitor) VisitCasted(*CastedNode) string                  { return "casted" }

func TestAllNodesImplementNodeInterface(t *testing.T) {
	t.Parallel()
	sv := stubVisitor{}

	// Compile-time check: each type must implement Node
	var nodes []Node
	nodes = append(nodes, NewTable("t"))
	nodes = append(nodes, NewTable("t").Alias("a"))
	nodes = append(nodes, NewAttribute(NewTable("t"), "c"))
	nodes = append(nodes, &LiteralNode{Value: 1})
	nodes = append(nodes, &StarNode{})
	nodes = append(nodes, NewSqlLiteral("raw"))
	nodes = append(nodes, &ComparisonNode{})
	nodes = append(nodes, &UnaryNode{})
	nodes = append(nodes, &AndNode{})
	nodes = append(nodes, &OrNode{})
	nodes = append(nodes, &NotNode{})
	nodes = append(nodes, &InNode{})
	nodes = append(nodes, &BetweenNode{})
	nodes = append(nodes, &GroupingNode{})
	nodes = append(nodes, &JoinNode{})
	nodes = append(nodes, &OrderingNode{})
	nodes = append(nodes, &SelectCore{})
	nodes = append(nodes, &InfixNode{})
	nodes = append(nodes, &UnaryMathNode{})
	nodes = append(nodes, NewAggregateNode(AggCount, nil))
	nodes = append(nodes, NewExtractNode(ExtractYear, NewAttribute(NewTable("t"), "c")))
	nodes = append(nodes, RowNumber())
	nodes = append(nodes, RowNumber().Over(NewWindowDef()))
	nodes = append(nodes, Exists(&SelectCore{}))
	nodes = append(nodes, NotExists(&SelectCore{}))
	nodes = append(nodes, &SetOperationNode{Left: &SelectCore{}, Right: &SelectCore{}})
	nodes = append(nodes, &CTENode{Name: "cte", Query: &SelectCore{}})
	nodes = append(nodes, NewNamedFunction("COALESCE", Literal(1)))
	nodes = append(nodes, NewCase().When(Literal(true), Literal(1)))
	nodes = append(nodes, NewCube(NewAttribute(NewTable("t"), "c")))
	nodes = append(nodes, NewAliasNode(NewAttribute(NewTable("t"), "c"), "alias"))
	nodes = append(nodes, NewBindParam(42))
	nodes = append(nodes, NewCasted(42, "integer"))

	for _, n := range nodes {
		n.Accept(sv) // should not panic
	}
}

// --- DML node types ---

func TestAssignmentNodeAccept(t *testing.T) {
	t.Parallel()
	col := NewAttribute(NewTable("users"), "name")
	a := &AssignmentNode{Left: col, Right: Literal("Alice")}
	got := a.Accept(stubVisitor{})
	if got != "assign" {
		t.Errorf("expected %q from stub, got %q", "assign", got)
	}
	if a.Left != col {
		t.Error("Left not set correctly")
	}
}

func TestInsertStatementAccept(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	stmt := &InsertStatement{Into: users}
	got := stmt.Accept(stubVisitor{})
	if got != "insert" {
		t.Errorf("expected %q from stub, got %q", "insert", got)
	}
}

func TestInsertStatementFields(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	col := NewAttribute(users, "name")
	stmt := &InsertStatement{
		Into:    users,
		Columns: []Node{col},
		Values:  [][]Node{{Literal("Alice")}},
	}
	if stmt.Into != users {
		t.Error("Into not set")
	}
	if len(stmt.Columns) != 1 {
		t.Errorf("expected 1 column, got %d", len(stmt.Columns))
	}
	if len(stmt.Values) != 1 || len(stmt.Values[0]) != 1 {
		t.Error("Values not set correctly")
	}
}

func TestInsertStatementMultiRow(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	stmt := &InsertStatement{
		Into:    users,
		Columns: []Node{NewAttribute(users, "name")},
		Values: [][]Node{
			{Literal("Alice")},
			{Literal("Bob")},
			{Literal("Carol")},
		},
	}
	if len(stmt.Values) != 3 {
		t.Errorf("expected 3 rows, got %d", len(stmt.Values))
	}
}

func TestUpdateStatementAccept(t *testing.T) {
	t.Parallel()
	stmt := &UpdateStatement{Table: NewTable("users")}
	got := stmt.Accept(stubVisitor{})
	if got != "update" {
		t.Errorf("expected %q from stub, got %q", "update", got)
	}
}

func TestUpdateStatementFields(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	col := NewAttribute(users, "name")
	stmt := &UpdateStatement{
		Table:       users,
		Assignments: []*AssignmentNode{{Left: col, Right: Literal("Bob")}},
		Wheres:      []Node{col.Eq("Alice")},
	}
	if len(stmt.Assignments) != 1 {
		t.Errorf("expected 1 assignment, got %d", len(stmt.Assignments))
	}
	if len(stmt.Wheres) != 1 {
		t.Errorf("expected 1 where, got %d", len(stmt.Wheres))
	}
}

func TestDeleteStatementAccept(t *testing.T) {
	t.Parallel()
	stmt := &DeleteStatement{From: NewTable("users")}
	got := stmt.Accept(stubVisitor{})
	if got != "delete" {
		t.Errorf("expected %q from stub, got %q", "delete", got)
	}
}

func TestDeleteStatementFields(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	stmt := &DeleteStatement{
		From:   users,
		Wheres: []Node{users.Col("id").Eq(1)},
	}
	if len(stmt.Wheres) != 1 {
		t.Errorf("expected 1 where, got %d", len(stmt.Wheres))
	}
}

func TestOnConflictNodeAccept(t *testing.T) {
	t.Parallel()
	node := &OnConflictNode{Action: DoNothing}
	got := node.Accept(stubVisitor{})
	if got != "conflict" {
		t.Errorf("expected %q from stub, got %q", "conflict", got)
	}
}

func TestOnConflictNodeDoNothing(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	node := &OnConflictNode{
		Columns: []Node{NewAttribute(users, "email")},
		Action:  DoNothing,
	}
	if node.Action != DoNothing {
		t.Error("expected DoNothing action")
	}
}

func TestOnConflictNodeDoUpdate(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	node := &OnConflictNode{
		Columns:     []Node{NewAttribute(users, "email")},
		Action:      DoUpdate,
		Assignments: []*AssignmentNode{{Left: NewAttribute(users, "name"), Right: Literal("updated")}},
	}
	if node.Action != DoUpdate {
		t.Error("expected DoUpdate action")
	}
	if len(node.Assignments) != 1 {
		t.Errorf("expected 1 assignment, got %d", len(node.Assignments))
	}
}

// --- Arithmetic operations ---

func TestArithmeticOperations(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("x")

	tests := []struct {
		name string
		node *InfixNode
		want InfixOp
	}{
		{"Plus", col.Plus(5), OpPlus},
		{"Minus", col.Minus(3), OpMinus},
		{"Multiply", col.Multiply(2), OpMultiply},
		{"Divide", col.Divide(4), OpDivide},
		{"BitwiseAnd", col.BitwiseAnd(0xFF), OpBitwiseAnd},
		{"BitwiseOr", col.BitwiseOr(0x01), OpBitwiseOr},
		{"BitwiseXor", col.BitwiseXor(0x0F), OpBitwiseXor},
		{"ShiftLeft", col.ShiftLeft(2), OpShiftLeft},
		{"ShiftRight", col.ShiftRight(1), OpShiftRight},
		{"Concat", col.Concat(" "), OpConcat},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.node.Op != tt.want {
				t.Errorf("expected %v, got %v", tt.want, tt.node.Op)
			}
		})
	}
}

func TestPlusInfixNodeDetails(t *testing.T) {
	t.Parallel()
	col := NewTable("users").Col("age")
	n := col.Plus(5)
	if n.Left != col {
		t.Error("expected left to be the attribute")
	}
	right, ok := n.Right.(*LiteralNode)
	if !ok {
		t.Fatalf("expected right to be *LiteralNode, got %T", n.Right)
	}
	if right.Value != 5 {
		t.Errorf("expected right value 5, got %v", right.Value)
	}
}

func TestBitwiseNotCreatesUnaryMathNode(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("flags")
	n := col.BitwiseNot()
	if n.Op != OpBitwiseNot {
		t.Errorf("expected OpBitwiseNot, got %d", n.Op)
	}
	if n.Expr != col {
		t.Error("expected expr to be the attribute")
	}
}

func TestArithmeticNodeToNode(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	col1 := users.Col("age")
	col2 := users.Col("bonus")
	n := col1.Plus(col2)
	if _, ok := n.Right.(*Attribute); !ok {
		t.Errorf("expected right to be *Attribute, got %T", n.Right)
	}
}

func TestArithmeticChainingOnResult(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("x")
	// col.Plus(5).Multiply(3) should work
	n := col.Plus(5).Multiply(3)
	if n.Op != OpMultiply {
		t.Errorf("expected OpMultiply, got %d", n.Op)
	}
	inner, ok := n.Left.(*InfixNode)
	if !ok {
		t.Fatalf("expected left to be *InfixNode, got %T", n.Left)
	}
	if inner.Op != OpPlus {
		t.Errorf("expected inner op OpPlus, got %d", inner.Op)
	}
}

func TestArithmeticThenPredication(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("x")
	// col.Plus(5).Eq(10) should work
	cmp := col.Plus(5).Eq(10)
	if cmp.Op != OpEq {
		t.Errorf("expected OpEq, got %d", cmp.Op)
	}
	if _, ok := cmp.Left.(*InfixNode); !ok {
		t.Errorf("expected left to be *InfixNode, got %T", cmp.Left)
	}
}

func TestArithmeticThenCombinable(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("x")
	other := NewTable("t").Col("y").Eq(1)
	// col.Plus(5).Eq(10).And(other) should work
	and := col.Plus(5).Eq(10).And(other)
	if and.Left == nil || and.Right == nil {
		t.Error("expected And to have both sides")
	}
}

func TestUnaryMathThenPredication(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("flags")
	// col.BitwiseNot().Eq(0) should work
	cmp := col.BitwiseNot().Eq(0)
	if cmp.Op != OpEq {
		t.Errorf("expected OpEq, got %d", cmp.Op)
	}
}

func TestUnaryMathThenArithmetic(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("flags")
	// col.BitwiseNot().BitwiseAnd(0xFF) should work
	n := col.BitwiseNot().BitwiseAnd(0xFF)
	if n.Op != OpBitwiseAnd {
		t.Errorf("expected OpBitwiseAnd, got %d", n.Op)
	}
	if _, ok := n.Left.(*UnaryMathNode); !ok {
		t.Errorf("expected left to be *UnaryMathNode, got %T", n.Left)
	}
}

// --- NotBetween ---

func TestNotBetween(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("age")
	b := col.NotBetween(18, 65)

	if !b.Negate {
		t.Error("expected NotBetween to be negated")
	}
	if b.Expr != col {
		t.Error("expected expr to be the attribute")
	}
	low, ok := b.Low.(*LiteralNode)
	if !ok {
		t.Fatalf("expected low to be *LiteralNode, got %T", b.Low)
	}
	if low.Value != 18 {
		t.Errorf("expected low value 18, got %v", low.Value)
	}
	high, ok := b.High.(*LiteralNode)
	if !ok {
		t.Fatalf("expected high to be *LiteralNode, got %T", b.High)
	}
	if high.Value != 65 {
		t.Errorf("expected high value 65, got %v", high.Value)
	}
}

// --- Composite predications ---

func TestEqAny(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("status")
	g := col.EqAny("active", "pending")
	or, ok := g.Expr.(*OrNode)
	if !ok {
		t.Fatalf("expected OrNode inside GroupingNode, got %T", g.Expr)
	}
	left, ok := or.Left.(*ComparisonNode)
	if !ok {
		t.Fatalf("expected left to be ComparisonNode, got %T", or.Left)
	}
	if left.Op != OpEq {
		t.Errorf("expected OpEq, got %d", left.Op)
	}
}

func TestEqAnySingleValue(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("status")
	g := col.EqAny("active")
	// Single value: GroupingNode wrapping a ComparisonNode
	if _, ok := g.Expr.(*ComparisonNode); !ok {
		t.Fatalf("expected ComparisonNode inside GroupingNode, got %T", g.Expr)
	}
}

func TestEqAll(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("status")
	node := col.EqAll("active", "pending")
	and, ok := node.(*AndNode)
	if !ok {
		t.Fatalf("expected AndNode, got %T", node)
	}
	left, ok := and.Left.(*ComparisonNode)
	if !ok {
		t.Fatalf("expected left to be ComparisonNode, got %T", and.Left)
	}
	if left.Op != OpEq {
		t.Errorf("expected OpEq, got %d", left.Op)
	}
}

func TestMatchesAny(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("name")
	g := col.MatchesAny("%foo%", "%bar%")
	if _, ok := g.Expr.(*OrNode); !ok {
		t.Fatalf("expected OrNode inside GroupingNode, got %T", g.Expr)
	}
}

func TestMatchesAll(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("name")
	node := col.MatchesAll("%foo%", "%bar%")
	if _, ok := node.(*AndNode); !ok {
		t.Fatalf("expected AndNode, got %T", node)
	}
}

func TestInAny(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("id")
	g := col.InAny([]any{1, 2}, []any{3, 4})
	if _, ok := g.Expr.(*OrNode); !ok {
		t.Fatalf("expected OrNode inside GroupingNode, got %T", g.Expr)
	}
}

func TestInAll(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("id")
	node := col.InAll([]any{1, 2}, []any{3, 4})
	if _, ok := node.(*AndNode); !ok {
		t.Fatalf("expected AndNode, got %T", node)
	}
}

// --- Aggregate functions ---

func TestCountCreatesAggregateNode(t *testing.T) {
	t.Parallel()
	col := NewTable("users").Col("id")
	n := Count(col)
	if n.Func != AggCount {
		t.Errorf("expected AggCount, got %d", n.Func)
	}
	if n.Expr != col {
		t.Error("expected expr to be the attribute")
	}
	if n.Distinct {
		t.Error("expected Distinct to be false")
	}
}

func TestCountStar(t *testing.T) {
	t.Parallel()
	n := Count(nil)
	if n.Func != AggCount {
		t.Errorf("expected AggCount, got %d", n.Func)
	}
	if n.Expr != nil {
		t.Error("expected expr to be nil for COUNT(*)")
	}
}

func TestSumCreatesAggregateNode(t *testing.T) {
	t.Parallel()
	col := NewTable("orders").Col("total")
	n := Sum(col)
	if n.Func != AggSum {
		t.Errorf("expected AggSum, got %d", n.Func)
	}
	if n.Expr != col {
		t.Error("expected expr to be the attribute")
	}
}

func TestAvgCreatesAggregateNode(t *testing.T) {
	t.Parallel()
	col := NewTable("orders").Col("total")
	n := Avg(col)
	if n.Func != AggAvg {
		t.Errorf("expected AggAvg, got %d", n.Func)
	}
}

func TestMinCreatesAggregateNode(t *testing.T) {
	t.Parallel()
	col := NewTable("orders").Col("total")
	n := Min(col)
	if n.Func != AggMin {
		t.Errorf("expected AggMin, got %d", n.Func)
	}
}

func TestMaxCreatesAggregateNode(t *testing.T) {
	t.Parallel()
	col := NewTable("orders").Col("total")
	n := Max(col)
	if n.Func != AggMax {
		t.Errorf("expected AggMax, got %d", n.Func)
	}
}

func TestCountDistinct(t *testing.T) {
	t.Parallel()
	col := NewTable("users").Col("country")
	n := CountDistinct(col)
	if n.Func != AggCount {
		t.Errorf("expected AggCount, got %d", n.Func)
	}
	if !n.Distinct {
		t.Error("expected Distinct to be true")
	}
	if n.Expr != col {
		t.Error("expected expr to be the attribute")
	}
}

func TestAggregateWithFilter(t *testing.T) {
	t.Parallel()
	col := NewTable("orders").Col("total")
	cond := NewTable("orders").Col("status").Eq("completed")
	n := Sum(col).WithFilter(cond)
	if n.Func != AggSum {
		t.Errorf("expected AggSum, got %d", n.Func)
	}
	if n.Filter == nil {
		t.Error("expected Filter to be set")
	}
	if n.Expr != col {
		t.Error("expected expr to be preserved")
	}
}

func TestAggregateThenPredication(t *testing.T) {
	t.Parallel()
	n := Count(nil)
	cmp := n.Eq(0)
	if cmp.Op != OpEq {
		t.Errorf("expected OpEq, got %d", cmp.Op)
	}
	if _, ok := cmp.Left.(*AggregateNode); !ok {
		t.Errorf("expected left to be *AggregateNode, got %T", cmp.Left)
	}
}

func TestAggregateThenArithmetic(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("x")
	n := Sum(col).Plus(10)
	if n.Op != OpPlus {
		t.Errorf("expected OpPlus, got %d", n.Op)
	}
	if _, ok := n.Left.(*AggregateNode); !ok {
		t.Errorf("expected left to be *AggregateNode, got %T", n.Left)
	}
}

func TestAggregateThenCombinable(t *testing.T) {
	t.Parallel()
	cond := Count(nil).Gt(5)
	not := cond.Not()
	if not.Expr != cond {
		t.Error("expected not.Expr to be the comparison")
	}
}

// --- Extract ---

func TestExtractYear(t *testing.T) {
	t.Parallel()
	col := NewTable("orders").Col("created_at")
	n := Extract(ExtractYear, col)
	if n.Field != ExtractYear {
		t.Errorf("expected ExtractYear, got %d", n.Field)
	}
	if n.Expr != col {
		t.Error("expected expr to be the attribute")
	}
}

func TestExtractFields(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("ts")
	fields := []ExtractField{
		ExtractYear, ExtractMonth, ExtractDay, ExtractHour,
		ExtractMinute, ExtractSecond, ExtractDow, ExtractDoy,
		ExtractEpoch, ExtractQuarter, ExtractWeek,
	}
	for _, f := range fields {
		n := Extract(f, col)
		if n.Field != f {
			t.Errorf("expected field %d, got %d", f, n.Field)
		}
	}
}

func TestExtractThenPredication(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("ts")
	cmp := Extract(ExtractYear, col).Eq(2024)
	if cmp.Op != OpEq {
		t.Errorf("expected OpEq, got %d", cmp.Op)
	}
	if _, ok := cmp.Left.(*ExtractNode); !ok {
		t.Errorf("expected left to be *ExtractNode, got %T", cmp.Left)
	}
}

func TestExtractThenArithmetic(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("ts")
	n := Extract(ExtractMonth, col).Plus(1)
	if n.Op != OpPlus {
		t.Errorf("expected OpPlus, got %d", n.Op)
	}
}

// --- Window functions ---

func TestRowNumberCreatesWindowFuncNode(t *testing.T) {
	t.Parallel()
	n := RowNumber()
	if n.Func != WinRowNumber {
		t.Errorf("expected WinRowNumber, got %d", n.Func)
	}
	if len(n.Args) != 0 {
		t.Errorf("expected 0 args, got %d", len(n.Args))
	}
}

func TestRankCreatesWindowFuncNode(t *testing.T) {
	t.Parallel()
	n := Rank()
	if n.Func != WinRank {
		t.Errorf("expected WinRank, got %d", n.Func)
	}
}

func TestDenseRankCreatesWindowFuncNode(t *testing.T) {
	t.Parallel()
	n := DenseRank()
	if n.Func != WinDenseRank {
		t.Errorf("expected WinDenseRank, got %d", n.Func)
	}
}

func TestCumeDistCreatesWindowFuncNode(t *testing.T) {
	t.Parallel()
	n := CumeDist()
	if n.Func != WinCumeDist {
		t.Errorf("expected WinCumeDist, got %d", n.Func)
	}
}

func TestPercentRankCreatesWindowFuncNode(t *testing.T) {
	t.Parallel()
	n := PercentRank()
	if n.Func != WinPercentRank {
		t.Errorf("expected WinPercentRank, got %d", n.Func)
	}
}

func TestNtileCreatesWindowFuncNode(t *testing.T) {
	t.Parallel()
	n := Ntile(Literal(4))
	if n.Func != WinNtile {
		t.Errorf("expected WinNtile, got %d", n.Func)
	}
	if len(n.Args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(n.Args))
	}
}

func TestFirstValueCreatesWindowFuncNode(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("x")
	n := FirstValue(col)
	if n.Func != WinFirstValue {
		t.Errorf("expected WinFirstValue, got %d", n.Func)
	}
	if n.Args[0] != col {
		t.Error("expected arg to be the attribute")
	}
}

func TestLastValueCreatesWindowFuncNode(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("x")
	n := LastValue(col)
	if n.Func != WinLastValue {
		t.Errorf("expected WinLastValue, got %d", n.Func)
	}
}

func TestLagCreatesWindowFuncNode(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("x")
	n := Lag(col, Literal(1), Literal(0))
	if n.Func != WinLag {
		t.Errorf("expected WinLag, got %d", n.Func)
	}
	if len(n.Args) != 3 {
		t.Errorf("expected 3 args, got %d", len(n.Args))
	}
}

func TestLeadCreatesWindowFuncNode(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("x")
	n := Lead(col)
	if n.Func != WinLead {
		t.Errorf("expected WinLead, got %d", n.Func)
	}
	if len(n.Args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(n.Args))
	}
}

func TestNthValueCreatesWindowFuncNode(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("x")
	n := NthValue(col, Literal(3))
	if n.Func != WinNthValue {
		t.Errorf("expected WinNthValue, got %d", n.Func)
	}
	if len(n.Args) != 2 {
		t.Errorf("expected 2 args, got %d", len(n.Args))
	}
}

func TestWindowFuncOverCreatesOverNode(t *testing.T) {
	t.Parallel()
	def := NewWindowDef().Partition(NewTable("t").Col("dept"))
	over := RowNumber().Over(def)
	if over.Window != def {
		t.Error("expected Window to be the definition")
	}
	if over.WindowName != "" {
		t.Error("expected empty WindowName")
	}
	if _, ok := over.Expr.(*WindowFuncNode); !ok {
		t.Errorf("expected Expr to be *WindowFuncNode, got %T", over.Expr)
	}
}

func TestWindowFuncOverNameCreatesOverNode(t *testing.T) {
	t.Parallel()
	over := Rank().OverName("w")
	if over.WindowName != "w" {
		t.Errorf("expected WindowName %q, got %q", "w", over.WindowName)
	}
	if over.Window != nil {
		t.Error("expected nil Window")
	}
}

func TestAggregateOverCreatesOverNode(t *testing.T) {
	t.Parallel()
	def := NewWindowDef().Partition(NewTable("t").Col("dept"))
	over := Sum(NewTable("t").Col("salary")).Over(def)
	if over.Window != def {
		t.Error("expected Window to be the definition")
	}
	if _, ok := over.Expr.(*AggregateNode); !ok {
		t.Errorf("expected Expr to be *AggregateNode, got %T", over.Expr)
	}
}

func TestAggregateOverNameCreatesOverNode(t *testing.T) {
	t.Parallel()
	over := Count(nil).OverName("w")
	if over.WindowName != "w" {
		t.Errorf("expected WindowName %q, got %q", "w", over.WindowName)
	}
}

func TestOverNodePredications(t *testing.T) {
	t.Parallel()
	over := RowNumber().Over(NewWindowDef())
	cmp := over.Eq(1)
	if cmp.Op != OpEq {
		t.Errorf("expected OpEq, got %d", cmp.Op)
	}
}

func TestOverNodeArithmetics(t *testing.T) {
	t.Parallel()
	over := RowNumber().Over(NewWindowDef())
	n := over.Plus(1)
	if n.Op != OpPlus {
		t.Errorf("expected OpPlus, got %d", n.Op)
	}
}

func TestOverNodeCombinable(t *testing.T) {
	t.Parallel()
	over1 := RowNumber().Over(NewWindowDef())
	cond := over1.Gt(0)
	not := cond.Not()
	if not.Expr != cond {
		t.Error("expected not.Expr to be the comparison")
	}
}

func TestWindowDefinitionBuilder(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("dept")
	ord := NewTable("t").Col("salary").Desc()
	def := NewWindowDef("w").Partition(col).Order(ord).Rows(UnboundedPreceding(), CurrentRow())

	if def.Name != "w" {
		t.Errorf("expected name %q, got %q", "w", def.Name)
	}
	if len(def.PartitionBy) != 1 {
		t.Errorf("expected 1 partition, got %d", len(def.PartitionBy))
	}
	if len(def.OrderBy) != 1 {
		t.Errorf("expected 1 order, got %d", len(def.OrderBy))
	}
	if def.Frame == nil {
		t.Fatal("expected frame to be set")
	}
	if def.Frame.Type != FrameRows {
		t.Errorf("expected FrameRows, got %d", def.Frame.Type)
	}
	if def.Frame.Start.Type != BoundUnboundedPreceding {
		t.Errorf("expected BoundUnboundedPreceding, got %d", def.Frame.Start.Type)
	}
	if def.Frame.End == nil || def.Frame.End.Type != BoundCurrentRow {
		t.Error("expected end bound to be CurrentRow")
	}
}

func TestWindowDefinitionRangeFrame(t *testing.T) {
	t.Parallel()
	def := NewWindowDef().Range(UnboundedPreceding(), UnboundedFollowing())
	if def.Frame.Type != FrameRange {
		t.Errorf("expected FrameRange, got %d", def.Frame.Type)
	}
}

func TestFrameBoundPreceding(t *testing.T) {
	t.Parallel()
	fb := Preceding(Literal(3))
	if fb.Type != BoundPreceding {
		t.Errorf("expected BoundPreceding, got %d", fb.Type)
	}
	if fb.Offset == nil {
		t.Error("expected offset to be set")
	}
}

func TestFrameBoundFollowing(t *testing.T) {
	t.Parallel()
	fb := Following(Literal(5))
	if fb.Type != BoundFollowing {
		t.Errorf("expected BoundFollowing, got %d", fb.Type)
	}
}

func TestWindowFuncNodeAccept(t *testing.T) {
	t.Parallel()
	sv := stubVisitor{}
	n := RowNumber()
	n.Accept(sv) // should not panic
}

func TestOverNodeAccept(t *testing.T) {
	t.Parallel()
	sv := stubVisitor{}
	over := RowNumber().Over(NewWindowDef())
	over.Accept(sv) // should not panic
}

func TestAllWindowNodesImplementNodeInterface(t *testing.T) {
	t.Parallel()
	sv := stubVisitor{}
	var ns []Node
	ns = append(ns, RowNumber())
	ns = append(ns, RowNumber().Over(NewWindowDef()))
	for _, n := range ns {
		n.Accept(sv) // should not panic
	}
}

// --- NamedFunctionNode ---

func TestNewNamedFunction(t *testing.T) {
	t.Parallel()
	col := NewTable("users").Col("name")
	fn := NewNamedFunction("LOWER", col)
	if fn.Name != "LOWER" {
		t.Errorf("expected name LOWER, got %q", fn.Name)
	}
	if len(fn.Args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(fn.Args))
	}
	if fn.Args[0] != col {
		t.Error("expected arg to be the attribute")
	}
}

func TestNamedFunctionAccept(t *testing.T) {
	t.Parallel()
	fn := NewNamedFunction("UPPER", Literal("hello"))
	fn.Accept(stubVisitor{}) // should not panic
}

func TestCoalesceConvenience(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("x")
	fn := Coalesce(col, Literal(0))
	if fn.Name != "COALESCE" {
		t.Errorf("expected COALESCE, got %q", fn.Name)
	}
	if len(fn.Args) != 2 {
		t.Errorf("expected 2 args, got %d", len(fn.Args))
	}
}

func TestLowerConvenience(t *testing.T) {
	t.Parallel()
	fn := Lower(NewTable("t").Col("name"))
	if fn.Name != "LOWER" {
		t.Errorf("expected LOWER, got %q", fn.Name)
	}
}

func TestUpperConvenience(t *testing.T) {
	t.Parallel()
	fn := Upper(NewTable("t").Col("name"))
	if fn.Name != "UPPER" {
		t.Errorf("expected UPPER, got %q", fn.Name)
	}
}

func TestSubstringConvenience(t *testing.T) {
	t.Parallel()
	fn := Substring(NewTable("t").Col("name"), Literal(1), Literal(3))
	if fn.Name != "SUBSTRING" {
		t.Errorf("expected SUBSTRING, got %q", fn.Name)
	}
	if len(fn.Args) != 3 {
		t.Errorf("expected 3 args, got %d", len(fn.Args))
	}
}

func TestCastConvenience(t *testing.T) {
	t.Parallel()
	fn := Cast(NewTable("t").Col("age"), "VARCHAR")
	if fn.Name != "CAST" {
		t.Errorf("expected CAST, got %q", fn.Name)
	}
	if len(fn.Args) != 2 {
		t.Errorf("expected 2 args (expr + type), got %d", len(fn.Args))
	}
}

func TestNamedFunctionPredications(t *testing.T) {
	t.Parallel()
	fn := Lower(NewTable("t").Col("name"))
	cmp := fn.Eq("alice")
	if cmp.Op != OpEq {
		t.Errorf("expected OpEq, got %d", cmp.Op)
	}
}

func TestNamedFunctionArithmetics(t *testing.T) {
	t.Parallel()
	fn := NewNamedFunction("LENGTH", NewTable("t").Col("name"))
	n := fn.Plus(1)
	if n.Op != OpPlus {
		t.Errorf("expected OpPlus, got %d", n.Op)
	}
}

func TestNamedFunctionAs(t *testing.T) {
	t.Parallel()
	fn := Lower(NewTable("t").Col("name"))
	alias := fn.As("lower_name")
	if alias.Name != "lower_name" {
		t.Errorf("expected alias name %q, got %q", "lower_name", alias.Name)
	}
	if alias.Expr != fn {
		t.Error("expected expr to be the named function")
	}
}

// --- CaseNode ---

func TestNewCaseSearched(t *testing.T) {
	t.Parallel()
	c := NewCase()
	if c.Operand != nil {
		t.Error("expected nil operand for searched CASE")
	}
}

func TestNewCaseSimple(t *testing.T) {
	t.Parallel()
	col := NewTable("t").Col("status")
	c := NewCase(col)
	if c.Operand != col {
		t.Error("expected operand to be the attribute")
	}
}

func TestCaseWhenElse(t *testing.T) {
	t.Parallel()
	c := NewCase().
		When(Literal(true), Literal(1)).
		When(Literal(false), Literal(0)).
		Else(Literal(-1))
	if len(c.Whens) != 2 {
		t.Errorf("expected 2 whens, got %d", len(c.Whens))
	}
	if c.ElseVal == nil {
		t.Error("expected else value to be set")
	}
}

func TestCaseNodeAccept(t *testing.T) {
	t.Parallel()
	c := NewCase().When(Literal(true), Literal(1))
	c.Accept(stubVisitor{}) // should not panic
}

func TestCaseNodePredications(t *testing.T) {
	t.Parallel()
	c := NewCase().When(Literal(true), Literal(1))
	cmp := c.Eq(1)
	if cmp.Op != OpEq {
		t.Errorf("expected OpEq, got %d", cmp.Op)
	}
}

func TestCaseNodeAs(t *testing.T) {
	t.Parallel()
	c := NewCase().When(Literal(true), Literal(1))
	alias := c.As("result")
	if alias.Name != "result" {
		t.Errorf("expected alias %q, got %q", "result", alias.Name)
	}
}

// --- GroupingSetNode ---

func TestNewCube(t *testing.T) {
	t.Parallel()
	col1 := NewTable("t").Col("a")
	col2 := NewTable("t").Col("b")
	n := NewCube(col1, col2)
	if n.Type != Cube {
		t.Errorf("expected Cube, got %d", n.Type)
	}
	if len(n.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(n.Columns))
	}
}

func TestNewRollup(t *testing.T) {
	t.Parallel()
	col1 := NewTable("t").Col("a")
	n := NewRollup(col1)
	if n.Type != Rollup {
		t.Errorf("expected Rollup, got %d", n.Type)
	}
	if len(n.Columns) != 1 {
		t.Errorf("expected 1 column, got %d", len(n.Columns))
	}
}

func TestNewGroupingSets(t *testing.T) {
	t.Parallel()
	col1 := NewTable("t").Col("a")
	col2 := NewTable("t").Col("b")
	n := NewGroupingSets([]Node{col1, col2}, []Node{col1}, []Node{})
	if n.Type != GroupingSets {
		t.Errorf("expected GroupingSets, got %d", n.Type)
	}
	if len(n.Sets) != 3 {
		t.Errorf("expected 3 sets, got %d", len(n.Sets))
	}
}

func TestGroupingSetAccept(t *testing.T) {
	t.Parallel()
	n := NewCube(NewTable("t").Col("a"))
	n.Accept(stubVisitor{}) // should not panic
}

// --- AliasNode ---

func TestNewAliasNode(t *testing.T) {
	t.Parallel()
	col := NewTable("users").Col("name")
	alias := NewAliasNode(col, "user_name")
	if alias.Name != "user_name" {
		t.Errorf("expected name %q, got %q", "user_name", alias.Name)
	}
	if alias.Expr != col {
		t.Error("expected expr to be the attribute")
	}
}

func TestAliasNodeAccept(t *testing.T) {
	t.Parallel()
	alias := NewAliasNode(Literal(1), "one")
	alias.Accept(stubVisitor{}) // should not panic
}

func TestAliasNodePredications(t *testing.T) {
	t.Parallel()
	alias := NewAliasNode(NewTable("t").Col("x"), "col")
	cmp := alias.Eq(1)
	if cmp.Op != OpEq {
		t.Errorf("expected OpEq, got %d", cmp.Op)
	}
}

func TestAttributeAs(t *testing.T) {
	t.Parallel()
	col := NewTable("users").Col("name")
	alias := col.As("user_name")
	if alias.Name != "user_name" {
		t.Errorf("expected name %q, got %q", "user_name", alias.Name)
	}
	if alias.Expr != col {
		t.Error("expected expr to be the attribute")
	}
}

func TestAggregateAs(t *testing.T) {
	t.Parallel()
	agg := Count(nil)
	alias := agg.As("total")
	if alias.Name != "total" {
		t.Errorf("expected name %q, got %q", "total", alias.Name)
	}
}

func TestOverNodeAs(t *testing.T) {
	t.Parallel()
	over := RowNumber().Over(NewWindowDef())
	alias := over.As("rn")
	if alias.Name != "rn" {
		t.Errorf("expected name %q, got %q", "rn", alias.Name)
	}
}

// --- BindParamNode ---

func TestNewBindParam(t *testing.T) {
	t.Parallel()
	bp := NewBindParam(42)
	if bp.Value != 42 {
		t.Errorf("expected value 42, got %v", bp.Value)
	}
}

func TestBindParamAccept(t *testing.T) {
	t.Parallel()
	bp := NewBindParam("hello")
	bp.Accept(stubVisitor{}) // should not panic
}

// --- CastedNode ---

func TestNewCasted(t *testing.T) {
	t.Parallel()
	c := NewCasted(42, "integer")
	if c.Value != 42 {
		t.Errorf("expected value 42, got %v", c.Value)
	}
	if c.TypeName != "integer" {
		t.Errorf("expected type %q, got %q", "integer", c.TypeName)
	}
}

func TestCastedAccept(t *testing.T) {
	t.Parallel()
	c := NewCasted("hello", "text")
	c.Accept(stubVisitor{}) // should not panic
}

func TestCastedPredications(t *testing.T) {
	t.Parallel()
	c := NewCasted(42, "integer")
	cmp := c.Eq(42)
	if cmp.Op != OpEq {
		t.Errorf("expected OpEq, got %d", cmp.Op)
	}
}

// --- BoundSqlLiteral ---

func TestNewBoundSqlLiteral(t *testing.T) {
	t.Parallel()
	lit := NewBoundSqlLiteral("WHERE id = ?", 42)
	if lit.Raw != "WHERE id = ?" {
		t.Errorf("expected raw %q, got %q", "WHERE id = ?", lit.Raw)
	}
	if len(lit.Binds) != 1 {
		t.Fatalf("expected 1 bind, got %d", len(lit.Binds))
	}
	if lit.Binds[0] != 42 {
		t.Errorf("expected bind value 42, got %v", lit.Binds[0])
	}
}

// --- Type Coercion ---

func TestAttributeTyped(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	col := users.Col("age")
	typed := col.Typed("integer")

	if typed.TypeName != "integer" {
		t.Errorf("expected TypeName %q, got %q", "integer", typed.TypeName)
	}
	if typed.Name != "age" {
		t.Errorf("expected Name %q, got %q", "age", typed.Name)
	}
	if typed.Relation != users {
		t.Error("expected Relation to match original")
	}
	// Original should be unmodified.
	if col.TypeName != "" {
		t.Errorf("original TypeName should be empty, got %q", col.TypeName)
	}
	// Copy should have its own self pointer.
	if typed == col {
		t.Error("Typed should return a copy, not the same pointer")
	}
}

func TestAttributeCoerceWithType(t *testing.T) {
	t.Parallel()
	col := NewTable("users").Col("age").Typed("integer")
	node := col.Coerce(42)
	casted, ok := node.(*CastedNode)
	if !ok {
		t.Fatalf("expected *CastedNode, got %T", node)
	}
	if casted.Value != 42 {
		t.Errorf("expected Value 42, got %v", casted.Value)
	}
	if casted.TypeName != "integer" {
		t.Errorf("expected TypeName %q, got %q", "integer", casted.TypeName)
	}
}

func TestAttributeCoerceWithoutType(t *testing.T) {
	t.Parallel()
	col := NewTable("users").Col("age")
	node := col.Coerce(42)
	lit, ok := node.(*LiteralNode)
	if !ok {
		t.Fatalf("expected *LiteralNode, got %T", node)
	}
	if lit.Value != 42 {
		t.Errorf("expected Value 42, got %v", lit.Value)
	}
}

// --- RelationName / TableSourceName ---

func TestRelationNameTable(t *testing.T) {
	t.Parallel()
	tbl := NewTable("users")
	if got := RelationName(tbl); got != "users" {
		t.Errorf("expected %q, got %q", "users", got)
	}
}

func TestRelationNameAlias(t *testing.T) {
	t.Parallel()
	alias := NewTable("users").Alias("u")
	if got := RelationName(alias); got != "u" {
		t.Errorf("expected %q, got %q", "u", got)
	}
}

func TestRelationNameUnknown(t *testing.T) {
	t.Parallel()
	lit := &LiteralNode{Value: "x"}
	if got := RelationName(lit); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestTableSourceNameTable(t *testing.T) {
	t.Parallel()
	tbl := NewTable("users")
	if got := TableSourceName(tbl); got != "users" {
		t.Errorf("expected %q, got %q", "users", got)
	}
}

func TestTableSourceNameAliasDereferences(t *testing.T) {
	t.Parallel()
	alias := NewTable("users").Alias("u")
	if got := TableSourceName(alias); got != "users" {
		t.Errorf("expected %q (underlying table), got %q", "users", got)
	}
}

func TestTableSourceNameAliasSubquery(t *testing.T) {
	t.Parallel()
	// Alias wrapping a SelectCore (not a Table) falls back to alias name.
	sub := &SelectCore{From: NewTable("orders")}
	alias := &TableAlias{Relation: sub, AliasName: "sub"}
	if got := TableSourceName(alias); got != "sub" {
		t.Errorf("expected %q (alias name fallback), got %q", "sub", got)
	}
}

func TestTableSourceNameUnknown(t *testing.T) {
	t.Parallel()
	lit := &LiteralNode{Value: "x"}
	if got := TableSourceName(lit); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

// --- Empty variadic guards ---

func TestGroupOrEmpty(t *testing.T) {
	t.Parallel()
	result := groupOr(nil)
	if result != nil {
		t.Errorf("expected nil for empty groupOr, got %v", result)
	}
}

func TestChainAndEmpty(t *testing.T) {
	t.Parallel()
	result := chainAnd(nil)
	if result != nil {
		t.Errorf("expected nil for empty chainAnd, got %v", result)
	}
}

func TestEqAnyEmpty(t *testing.T) {
	t.Parallel()
	col := NewTable("users").Col("id")
	result := col.EqAny()
	if result != nil {
		t.Errorf("expected nil for empty EqAny, got %v", result)
	}
}

func TestInAnyEmpty(t *testing.T) {
	t.Parallel()
	col := NewTable("users").Col("id")
	result := col.InAny()
	if result != nil {
		t.Errorf("expected nil for empty InAny, got %v", result)
	}
}

// --- Arithmetic constructors ---

func TestNewInfixNode(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	left := users.Col("price")
	right := Literal(10)
	
	node := NewInfixNode(left, right, OpPlus)
	
	if node.Left != left {
		t.Error("expected Left to be the left operand")
	}
	if node.Right != right {
		t.Error("expected Right to be the right operand")
	}
	if node.Op != OpPlus {
		t.Errorf("expected OpPlus, got %d", node.Op)
	}
	
	// Verify self pointers are set for method chaining
	cmp := node.Eq(20)
	if cmp.Left != node {
		t.Error("expected Predications.self to be set correctly")
	}
}

func TestNewUnaryMathNode(t *testing.T) {
	t.Parallel()
	users := NewTable("users")
	expr := users.Col("flags")
	
	node := NewUnaryMathNode(expr, OpBitwiseNot)
	
	if node.Expr != expr {
		t.Error("expected Expr to be the expression")
	}
	if node.Op != OpBitwiseNot {
		t.Errorf("expected OpBitwiseNot, got %d", node.Op)
	}
	
	// Verify self pointers are set for method chaining
	result := node.Multiply(2)
	if result.Left != node {
		t.Error("expected Arithmetics.self to be set correctly")
	}
}

// --- Over/OverName for named functions ---

func TestNamedFunctionOver(t *testing.T) {
	t.Parallel()
	fn := Lower(Literal("NAME"))
	windowDef := NewWindowDef().Partition(Literal("category"))

	overNode := fn.Over(windowDef)

	if overNode.Expr != fn {
		t.Error("expected Expr to be the named function")
	}
	if overNode.Window != windowDef {
		t.Error("expected Window to be the window definition")
	}
}

func TestNamedFunctionOverName(t *testing.T) {
	t.Parallel()
	fn := Upper(Literal("text"))

	overNode := fn.OverName("my_window")

	if overNode.Expr != fn {
		t.Error("expected Expr to be the named function")
	}
	if overNode.WindowName != "my_window" {
		t.Errorf("expected WindowName %q, got %q", "my_window", overNode.WindowName)
	}
}

// --- String() debug helpers ---

func TestJoinTypeString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		joinType JoinType
		want     string
	}{
		{InnerJoin, "INNER JOIN"},
		{LeftOuterJoin, "LEFT OUTER JOIN"},
		{RightOuterJoin, "RIGHT OUTER JOIN"},
		{FullOuterJoin, "FULL OUTER JOIN"},
		{CrossJoin, "CROSS JOIN"},
	}
	for _, tt := range tests {
		got := tt.joinType.String()
		if got != tt.want {
			t.Errorf("JoinType(%d).String() = %q, want %q", tt.joinType, got, tt.want)
		}
	}
}

func TestSetOpTypeString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		opType SetOpType
		want   string
	}{
		{Union, "UNION"},
		{UnionAll, "UNION ALL"},
		{Intersect, "INTERSECT"},
		{IntersectAll, "INTERSECT ALL"},
		{Except, "EXCEPT"},
		{ExceptAll, "EXCEPT ALL"},
	}
	for _, tt := range tests {
		got := tt.opType.String()
		if got != tt.want {
			t.Errorf("SetOpType(%d).String() = %q, want %q", tt.opType, got, tt.want)
		}
	}
}
