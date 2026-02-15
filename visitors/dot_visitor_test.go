package visitors

import (
	"strings"
	"testing"

	"github.com/bawdo/gosbee/nodes"
)

func TestDotVisitTable(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	users.Accept(dv)
	dot := dv.ToDot()

	if !strings.Contains(dot, "digraph") {
		t.Error("expected DOT output to contain 'digraph'")
	}
	if !strings.Contains(dot, `label="Table\nusers"`) {
		t.Errorf("expected Table node label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `fillcolor="#6CA6CD"`) {
		t.Errorf("expected blue fill for Table, got:\n%s", dot)
	}
}

// --- Task 2: Leaf node tests ---

func TestDotVisitTableAlias(t *testing.T) {
	dv := NewDotVisitor()
	u := nodes.NewTable("users").Alias("u")
	u.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="TableAlias\nu"`) {
		t.Errorf("expected TableAlias label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="Table\nusers"`) {
		t.Errorf("expected inner Table node, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="RELATION"`) {
		t.Errorf("expected RELATION edge label, got:\n%s", dot)
	}
}

func TestDotVisitAttribute(t *testing.T) {
	dv := NewDotVisitor()
	col := nodes.NewTable("users").Col("name")
	col.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="Attribute\nusers.name"`) {
		t.Errorf("expected Attribute label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `fillcolor="#B0D4E8"`) {
		t.Errorf("expected light blue fill, got:\n%s", dot)
	}
}

func TestDotVisitLiteral(t *testing.T) {
	dv := NewDotVisitor()
	n := nodes.Literal(42)
	n.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="Literal\n42"`) {
		t.Errorf("expected Literal label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `fillcolor="#D3D3D3"`) {
		t.Errorf("expected grey fill, got:\n%s", dot)
	}
}

func TestDotVisitStar(t *testing.T) {
	dv := NewDotVisitor()
	s := nodes.Star()
	s.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="Star\n*"`) {
		t.Errorf("expected Star label, got:\n%s", dot)
	}
}

func TestDotVisitStarQualified(t *testing.T) {
	dv := NewDotVisitor()
	s := nodes.NewTable("users").Star()
	s.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="Star\nusers.*"`) {
		t.Errorf("expected qualified Star label, got:\n%s", dot)
	}
}

func TestDotVisitSqlLiteral(t *testing.T) {
	dv := NewDotVisitor()
	n := nodes.NewSqlLiteral("NOW()")
	n.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="SqlLiteral\nNOW()"`) {
		t.Errorf("expected SqlLiteral label, got:\n%s", dot)
	}
}

// --- Task 3: Expression node tests ---

func TestDotVisitComparison(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	cmp := users.Col("age").Gt(nodes.Literal(18))
	cmp.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"Comparison\n>"`) {
		t.Errorf("expected Comparison > label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="LEFT"`) {
		t.Errorf("expected LEFT edge, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="RIGHT"`) {
		t.Errorf("expected RIGHT edge, got:\n%s", dot)
	}
}

func TestDotVisitAnd(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	left := users.Col("a").Eq(nodes.Literal(1))
	right := users.Col("b").Eq(nodes.Literal(2))
	and := left.And(right)
	and.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="AND"`) {
		t.Errorf("expected AND node, got:\n%s", dot)
	}
	if !strings.Contains(dot, `fillcolor="#FFEB80"`) {
		t.Errorf("expected yellow fill, got:\n%s", dot)
	}
}

func TestDotVisitOr(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	left := users.Col("a").Eq(nodes.Literal(1))
	right := users.Col("b").Eq(nodes.Literal(2))
	or := left.Or(right)
	or.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="OR"`) {
		t.Errorf("expected OR node, got:\n%s", dot)
	}
}

func TestDotVisitNot(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	cond := users.Col("active").Eq(nodes.Literal(true))
	not := cond.Not()
	not.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="NOT"`) {
		t.Errorf("expected NOT node, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="EXPR"`) {
		t.Errorf("expected EXPR edge, got:\n%s", dot)
	}
}

func TestDotVisitIn(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	in := users.Col("id").In(nodes.Literal(1), nodes.Literal(2), nodes.Literal(3))
	in.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="IN"`) {
		t.Errorf("expected IN node, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="VAL[0]"`) {
		t.Errorf("expected VAL[0] edge, got:\n%s", dot)
	}
}

func TestDotVisitBetween(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	btwn := users.Col("age").Between(nodes.Literal(18), nodes.Literal(65))
	btwn.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="BETWEEN"`) {
		t.Errorf("expected BETWEEN node, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="LOW"`) {
		t.Errorf("expected LOW edge, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="HIGH"`) {
		t.Errorf("expected HIGH edge, got:\n%s", dot)
	}
}

func TestDotVisitUnary(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	isNull := users.Col("deleted_at").IsNull()
	isNull.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"Unary\nIS NULL"`) {
		t.Errorf("expected IS NULL label, got:\n%s", dot)
	}
}

func TestDotVisitGrouping(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	cond := users.Col("a").Eq(nodes.Literal(1))
	grp := &nodes.GroupingNode{Expr: cond}
	grp.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"Grouping\n( )"`) {
		t.Errorf("expected Grouping label, got:\n%s", dot)
	}
}

// --- Task 4: Join, Ordering, SelectCore tests ---

func TestDotVisitJoin(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	join := &nodes.JoinNode{
		Left:  users,
		Right: posts,
		Type:  nodes.InnerJoin,
		On:    users.Col("id").Eq(posts.Col("user_id")),
	}
	join.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"Join\nINNER JOIN"`) {
		t.Errorf("expected INNER JOIN label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `fillcolor="#77DD77"`) {
		t.Errorf("expected green fill, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="ON"`) {
		t.Errorf("expected ON edge, got:\n%s", dot)
	}
}

func TestDotVisitOrdering(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	ord := &nodes.OrderingNode{Expr: users.Col("name"), Direction: nodes.Desc}
	ord.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"Order\nDESC"`) {
		t.Errorf("expected DESC label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `fillcolor="#CDA0E0"`) {
		t.Errorf("expected purple fill, got:\n%s", dot)
	}
}

func TestDotVisitSelectCore(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("id"), users.Col("name")},
		Wheres:      []nodes.Node{users.Col("active").Eq(nodes.Literal(true))},
		Limit:       nodes.Literal(10),
	}
	core.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="SelectCore"`) {
		t.Errorf("expected SelectCore label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="FROM"`) {
		t.Errorf("expected FROM edge, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="SELECT[0]"`) {
		t.Errorf("expected SELECT[0] edge, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="WHERE[0]"`) {
		t.Errorf("expected WHERE[0] edge, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="LIMIT"`) {
		t.Errorf("expected LIMIT edge, got:\n%s", dot)
	}
}

func TestDotVisitSelectCoreDistinct(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{
		From:     users,
		Distinct: true,
	}
	core.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="DISTINCT"`) {
		t.Errorf("expected DISTINCT node, got:\n%s", dot)
	}
}

// --- Task 5: DML statement tests ---

func TestDotVisitInsertStatement(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:    users,
		Columns: []nodes.Node{users.Col("name"), users.Col("age")},
		Values:  [][]nodes.Node{{nodes.Literal("Alice"), nodes.Literal(30)}},
	}
	stmt.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="InsertStatement"`) {
		t.Errorf("expected InsertStatement label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `fillcolor="#FF6961"`) {
		t.Errorf("expected red fill, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="INTO"`) {
		t.Errorf("expected INTO edge, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="COLUMN[0]"`) {
		t.Errorf("expected COLUMN[0] edge, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="VALUES[0][0]"`) {
		t.Errorf("expected VALUES[0][0] edge, got:\n%s", dot)
	}
}

func TestDotVisitUpdateStatement(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	stmt := &nodes.UpdateStatement{
		Table:       users,
		Assignments: []*nodes.AssignmentNode{{Left: users.Col("name"), Right: nodes.Literal("Bob")}},
		Wheres:      []nodes.Node{users.Col("id").Eq(nodes.Literal(1))},
	}
	stmt.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="UpdateStatement"`) {
		t.Errorf("expected UpdateStatement label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="TABLE"`) {
		t.Errorf("expected TABLE edge, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="SET[0]"`) {
		t.Errorf("expected SET[0] edge, got:\n%s", dot)
	}
}

func TestDotVisitDeleteStatement(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	stmt := &nodes.DeleteStatement{
		From:   users,
		Wheres: []nodes.Node{users.Col("id").Eq(nodes.Literal(1))},
	}
	stmt.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="DeleteStatement"`) {
		t.Errorf("expected DeleteStatement label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="FROM"`) {
		t.Errorf("expected FROM edge, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="WHERE[0]"`) {
		t.Errorf("expected WHERE[0] edge, got:\n%s", dot)
	}
}

func TestDotVisitAssignment(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	assign := &nodes.AssignmentNode{Left: users.Col("name"), Right: nodes.Literal("Alice")}
	assign.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"Assignment\n="`) {
		t.Errorf("expected Assignment label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="COLUMN"`) {
		t.Errorf("expected COLUMN edge, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="VALUE"`) {
		t.Errorf("expected VALUE edge, got:\n%s", dot)
	}
}

func TestDotVisitOnConflict(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	oc := &nodes.OnConflictNode{
		Columns: []nodes.Node{users.Col("email")},
		Action:  nodes.DoUpdate,
		Assignments: []*nodes.AssignmentNode{
			{Left: users.Col("name"), Right: nodes.Literal("Updated")},
		},
	}
	oc.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"OnConflict\nDO UPDATE"`) {
		t.Errorf("expected DO UPDATE label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="TARGET[0]"`) {
		t.Errorf("expected TARGET[0] edge, got:\n%s", dot)
	}
}

// --- Task 6: Plugin cluster and provenance tests ---

func TestDotPluginCluster(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")

	// Build a simple tree with two nodes.
	users.Accept(dv)
	posts := nodes.NewTable("posts")
	posts.Accept(dv)

	// Manually add a cluster for the second node.
	dv.AddPluginCluster("softdelete", "#CC6666", []string{"n1"})

	dot := dv.ToDot()
	if !strings.Contains(dot, "subgraph cluster_0_softdelete") {
		t.Errorf("expected softdelete cluster, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="softdelete"`) {
		t.Errorf("expected softdelete label, got:\n%s", dot)
	}
	if !strings.Contains(dot, "style=dashed") {
		t.Errorf("expected dashed style, got:\n%s", dot)
	}
	if !strings.Contains(dot, `color="#CC6666"`) {
		t.Errorf("expected cluster color, got:\n%s", dot)
	}
}

func TestDotPluginClusterEmptySkipped(t *testing.T) {
	dv := NewDotVisitor()
	dv.AddPluginCluster("empty", "#000000", []string{})
	dot := dv.ToDot()
	if strings.Contains(dot, "subgraph") {
		t.Errorf("expected no cluster for empty nodeIDs, got:\n%s", dot)
	}
}

func TestDotNodeCountAndIDsSince(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	users.Accept(dv)
	if dv.NodeCount() != 1 {
		t.Errorf("expected 1 node, got %d", dv.NodeCount())
	}

	snapshot := dv.NodeCount()
	posts := nodes.NewTable("posts")
	posts.Accept(dv)
	ids := dv.NodeIDsSince(snapshot)
	if len(ids) != 1 || ids[0] != "n1" {
		t.Errorf("expected [n1], got %v", ids)
	}
}

func TestDotProvenanceWhere(t *testing.T) {
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{
		From: users,
		Wheres: []nodes.Node{
			users.Col("active").Eq(nodes.Literal(true)),    // user's WHERE [0]
			users.Col("deleted_at").IsNull(),                // plugin WHERE [1]
		},
	}

	prov := NewPluginProvenance()
	prov.AddWhere("softdelete", "#CC6666", 1) // index 1 is plugin-added

	dv := NewDotVisitor()
	dv.SetProvenance(prov)
	core.Accept(dv)
	dot := dv.ToDot()

	if !strings.Contains(dot, "subgraph cluster_") {
		t.Errorf("expected plugin cluster, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="softdelete"`) {
		t.Errorf("expected softdelete cluster label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `color="#CC6666"`) {
		t.Errorf("expected cluster color, got:\n%s", dot)
	}
}

func TestDotProvenanceJoin(t *testing.T) {
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	core := &nodes.SelectCore{
		From: users,
		Joins: []*nodes.JoinNode{
			{Left: users, Right: posts, Type: nodes.InnerJoin,
				On: users.Col("id").Eq(posts.Col("user_id"))}, // user's JOIN [0]
			{Left: users, Right: nodes.NewTable("audit_log"), Type: nodes.LeftOuterJoin,
				On: users.Col("id").Eq(nodes.NewTable("audit_log").Col("user_id"))}, // plugin JOIN [1]
		},
	}

	prov := NewPluginProvenance()
	prov.AddJoin("opa", "#9B59B6", 1)

	dv := NewDotVisitor()
	dv.SetProvenance(prov)
	core.Accept(dv)
	dot := dv.ToDot()

	if !strings.Contains(dot, `label="opa"`) {
		t.Errorf("expected opa cluster label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `color="#9B59B6"`) {
		t.Errorf("expected opa cluster color, got:\n%s", dot)
	}
}

// --- Arithmetic DOT tests ---

func TestDotVisitInfix(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	n := users.Col("age").Plus(nodes.Literal(5))
	n.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"Infix\n+"`) {
		t.Errorf("expected Infix + label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `fillcolor="#98FB98"`) {
		t.Errorf("expected mint green fill, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="LEFT"`) {
		t.Errorf("expected LEFT edge, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="RIGHT"`) {
		t.Errorf("expected RIGHT edge, got:\n%s", dot)
	}
}

func TestDotVisitUnaryMath(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	n := users.Col("flags").BitwiseNot()
	n.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"UnaryMath\n~"`) {
		t.Errorf("expected UnaryMath ~ label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `fillcolor="#98FB98"`) {
		t.Errorf("expected mint green fill, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="EXPR"`) {
		t.Errorf("expected EXPR edge, got:\n%s", dot)
	}
}

func TestDotVisitInfixAllOps(t *testing.T) {
	tests := []struct {
		name     string
		op       func(*nodes.Attribute) nodes.Node
		expected string
	}{
		{"Plus", func(a *nodes.Attribute) nodes.Node { return a.Plus(1) }, `"Infix\n+"`},
		{"Minus", func(a *nodes.Attribute) nodes.Node { return a.Minus(1) }, `"Infix\n-"`},
		{"Multiply", func(a *nodes.Attribute) nodes.Node { return a.Multiply(1) }, `"Infix\n*"`},
		{"Divide", func(a *nodes.Attribute) nodes.Node { return a.Divide(1) }, `"Infix\n/"`},
		{"BitwiseAnd", func(a *nodes.Attribute) nodes.Node { return a.BitwiseAnd(1) }, `"Infix\n&"`},
		{"BitwiseOr", func(a *nodes.Attribute) nodes.Node { return a.BitwiseOr(1) }, `"Infix\n|"`},
		{"BitwiseXor", func(a *nodes.Attribute) nodes.Node { return a.BitwiseXor(1) }, `"Infix\n^"`},
		{"ShiftLeft", func(a *nodes.Attribute) nodes.Node { return a.ShiftLeft(1) }, `"Infix\n<<"`},
		{"ShiftRight", func(a *nodes.Attribute) nodes.Node { return a.ShiftRight(1) }, `"Infix\n>>"`},
		{"Concat", func(a *nodes.Attribute) nodes.Node { return a.Concat(" ") }, `"Infix\n||"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dv := NewDotVisitor()
			col := nodes.NewTable("t").Col("x")
			n := tt.op(col)
			n.Accept(dv)
			dot := dv.ToDot()
			if !strings.Contains(dot, tt.expected) {
				t.Errorf("expected %s in DOT output, got:\n%s", tt.expected, dot)
			}
		})
	}
}

func TestDotProvenanceMultiplePlugins(t *testing.T) {
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{
		From: users,
		Wheres: []nodes.Node{
			users.Col("active").Eq(nodes.Literal(true)),    // user [0]
			users.Col("deleted_at").IsNull(),                // softdelete [1]
			users.Col("tenant_id").Eq(nodes.Literal(42)),   // opa [2]
		},
	}

	prov := NewPluginProvenance()
	prov.AddWhere("softdelete", "#CC6666", 1)
	prov.AddWhere("opa", "#9B59B6", 2)

	dv := NewDotVisitor()
	dv.SetProvenance(prov)
	core.Accept(dv)
	dot := dv.ToDot()

	if !strings.Contains(dot, `label="softdelete"`) {
		t.Errorf("expected softdelete cluster, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="opa"`) {
		t.Errorf("expected opa cluster, got:\n%s", dot)
	}
}

// --- Task 10: Full integration test ---

func TestDotFullSelectQuery(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	core := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("id"), users.Col("name"), posts.Col("title")},
		Wheres:      []nodes.Node{users.Col("active").Eq(nodes.Literal(true))},
		Joins: []*nodes.JoinNode{
			{Left: users, Right: posts, Type: nodes.InnerJoin,
				On: users.Col("id").Eq(posts.Col("user_id"))},
		},
		Orders: []nodes.Node{
			&nodes.OrderingNode{Expr: users.Col("name"), Direction: nodes.Asc},
		},
		Limit:  nodes.Literal(10),
		Offset: nodes.Literal(5),
	}
	core.Accept(dv)
	dot := dv.ToDot()

	// Verify structure.
	if !strings.Contains(dot, "digraph AST") {
		t.Error("missing digraph header")
	}
	if !strings.HasSuffix(strings.TrimSpace(dot), "}") {
		t.Error("missing closing brace")
	}
	// Verify key nodes exist.
	for _, expected := range []string{
		"SelectCore", `Table\nusers`, `Table\nposts`,
		`Attribute\nusers.id`, `Attribute\nusers.name`,
		`Comparison\n=`, `Literal\ntrue`, `Literal\n10`,
		`Join\nINNER JOIN`, `Order\nASC`,
	} {
		if !strings.Contains(dot, expected) {
			t.Errorf("expected %q in DOT output", expected)
		}
	}
	// Verify key edges.
	for _, expected := range []string{
		`label="FROM"`, `label="SELECT[0]"`, `label="JOIN[0]"`,
		`label="WHERE[0]"`, `label="ORDER[0]"`,
		`label="LIMIT"`, `label="OFFSET"`,
		`label="ON"`, `label="LEFT"`, `label="RIGHT"`,
	} {
		if !strings.Contains(dot, expected) {
			t.Errorf("expected edge %s in DOT output", expected)
		}
	}
}

// --- New predication DOT tests ---

func TestDotVisitNotBetween(t *testing.T) {
	dv := NewDotVisitor()
	users := nodes.NewTable("users")
	btwn := users.Col("age").NotBetween(nodes.Literal(18), nodes.Literal(65))
	btwn.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `label="NOT BETWEEN"`) {
		t.Errorf("expected NOT BETWEEN node, got:\n%s", dot)
	}
}

func TestDotVisitNewComparisonOps(t *testing.T) {
	tests := []struct {
		name     string
		node     nodes.Node
		expected string
	}{
		{"Regexp", nodes.NewTable("t").Col("x").MatchesRegexp("^A"), `"Comparison\n~"`},
		{"NotRegexp", nodes.NewTable("t").Col("x").DoesNotMatchRegexp("^A"), `"Comparison\n!~"`},
		{"DistinctFrom", nodes.NewTable("t").Col("x").IsDistinctFrom(1), `"Comparison\nIS DISTINCT FROM"`},
		{"NotDistinctFrom", nodes.NewTable("t").Col("x").IsNotDistinctFrom(1), `"Comparison\nIS NOT DISTINCT FROM"`},
		{"CaseSensitiveEq", nodes.NewTable("t").Col("x").CaseSensitiveEq("A"), `"Comparison\nCASE = (sensitive)"`},
		{"CaseInsensitiveEq", nodes.NewTable("t").Col("x").CaseInsensitiveEq("a"), `"Comparison\nCASE = (insensitive)"`},
		{"Contains", nodes.NewTable("t").Col("x").Contains("{1}"), `"Comparison\n@>"`},
		{"Overlaps", nodes.NewTable("t").Col("x").Overlaps("{1}"), `"Comparison\n&&"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dv := NewDotVisitor()
			tt.node.Accept(dv)
			dot := dv.ToDot()
			if !strings.Contains(dot, tt.expected) {
				t.Errorf("expected %s in DOT output, got:\n%s", tt.expected, dot)
			}
		})
	}
}

// --- Aggregate DOT tests ---

func TestDotVisitCountStar(t *testing.T) {
	dv := NewDotVisitor()
	nodes.Count(nil).Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"COUNT"`) {
		t.Errorf("expected COUNT label, got:\n%s", dot)
	}
	if !strings.Contains(dot, colorFunction) {
		t.Errorf("expected function color %s, got:\n%s", colorFunction, dot)
	}
	// COUNT(*) should have a star child node
	if !strings.Contains(dot, `"*"`) {
		t.Errorf("expected * child node for COUNT(*), got:\n%s", dot)
	}
}

func TestDotVisitCountColumn(t *testing.T) {
	col := nodes.NewTable("users").Col("id")
	dv := NewDotVisitor()
	nodes.Count(col).Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"COUNT"`) {
		t.Errorf("expected COUNT label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `"Attribute\nusers.id"`) {
		t.Errorf("expected Attribute child, got:\n%s", dot)
	}
}

func TestDotVisitCountDistinct(t *testing.T) {
	col := nodes.NewTable("users").Col("country")
	dv := NewDotVisitor()
	nodes.CountDistinct(col).Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"COUNT\nDISTINCT"`) {
		t.Errorf("expected COUNT DISTINCT label, got:\n%s", dot)
	}
}

func TestDotVisitAggregateWithFilter(t *testing.T) {
	col := nodes.NewTable("orders").Col("total")
	cond := nodes.NewTable("orders").Col("status").Eq("completed")
	n := nodes.Sum(col).WithFilter(cond)
	dv := NewDotVisitor()
	n.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"SUM"`) {
		t.Errorf("expected SUM label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `"FILTER"`) {
		t.Errorf("expected FILTER edge, got:\n%s", dot)
	}
}

func TestDotVisitAllAggregateFuncs(t *testing.T) {
	col := nodes.NewTable("t").Col("x")
	tests := []struct {
		name     string
		node     nodes.Node
		expected string
	}{
		{"Count", nodes.Count(col), `"COUNT"`},
		{"Sum", nodes.Sum(col), `"SUM"`},
		{"Avg", nodes.Avg(col), `"AVG"`},
		{"Min", nodes.Min(col), `"MIN"`},
		{"Max", nodes.Max(col), `"MAX"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dv := NewDotVisitor()
			tt.node.Accept(dv)
			dot := dv.ToDot()
			if !strings.Contains(dot, tt.expected) {
				t.Errorf("expected %s, got:\n%s", tt.expected, dot)
			}
		})
	}
}

func TestDotVisitExtract(t *testing.T) {
	col := nodes.NewTable("t").Col("ts")
	n := nodes.Extract(nodes.ExtractYear, col)
	dv := NewDotVisitor()
	n.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"EXTRACT\nYEAR"`) {
		t.Errorf("expected EXTRACT YEAR label, got:\n%s", dot)
	}
	if !strings.Contains(dot, colorFunction) {
		t.Errorf("expected function color, got:\n%s", dot)
	}
}

func TestDotVisitExtractMonth(t *testing.T) {
	col := nodes.NewTable("t").Col("ts")
	n := nodes.Extract(nodes.ExtractMonth, col)
	dv := NewDotVisitor()
	n.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"EXTRACT\nMONTH"`) {
		t.Errorf("expected EXTRACT MONTH label, got:\n%s", dot)
	}
}

// --- Window function DOT tests ---

func TestDotVisitWindowFunction(t *testing.T) {
	n := nodes.RowNumber()
	dv := NewDotVisitor()
	n.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"ROW_NUMBER"`) {
		t.Errorf("expected ROW_NUMBER label, got:\n%s", dot)
	}
	if !strings.Contains(dot, colorFunction) {
		t.Errorf("expected function color, got:\n%s", dot)
	}
}

func TestDotVisitWindowFunctionWithArgs(t *testing.T) {
	n := nodes.Ntile(nodes.Literal(4))
	dv := NewDotVisitor()
	n.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"NTILE"`) {
		t.Errorf("expected NTILE label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `"ARG[0]"`) {
		t.Errorf("expected ARG[0] edge, got:\n%s", dot)
	}
}

func TestDotVisitOverNode(t *testing.T) {
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().Partition(users.Col("dept"))
	over := nodes.RowNumber().Over(def)
	dv := NewDotVisitor()
	over.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"OVER"`) {
		t.Errorf("expected OVER label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `"EXPR"`) {
		t.Errorf("expected EXPR edge, got:\n%s", dot)
	}
	if !strings.Contains(dot, `"PARTITION[0]"`) {
		t.Errorf("expected PARTITION edge, got:\n%s", dot)
	}
}

func TestDotVisitOverNamedWindow(t *testing.T) {
	over := nodes.Rank().OverName("w")
	dv := NewDotVisitor()
	over.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"OVER\nw"`) {
		t.Errorf("expected OVER w label, got:\n%s", dot)
	}
}

func TestDotVisitOverWithFrame(t *testing.T) {
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().
		Order(users.Col("id").Asc()).
		Rows(nodes.UnboundedPreceding(), nodes.CurrentRow())
	over := nodes.Sum(users.Col("salary")).Over(def)
	dv := NewDotVisitor()
	over.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"Frame\nROWS"`) {
		t.Errorf("expected Frame ROWS label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `"FRAME"`) {
		t.Errorf("expected FRAME edge, got:\n%s", dot)
	}
}

func TestDotVisitSelectCoreWithWindow(t *testing.T) {
	users := nodes.NewTable("users")
	w := nodes.NewWindowDef("w").Order(users.Col("salary").Asc())
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{nodes.Rank().OverName("w")},
		Windows:     []*nodes.WindowDefinition{w},
	}
	dv := NewDotVisitor()
	sc.Accept(dv)
	dot := dv.ToDot()
	if !strings.Contains(dot, `"WINDOW\nw"`) {
		t.Errorf("expected WINDOW w label, got:\n%s", dot)
	}
	if !strings.Contains(dot, `"WINDOW[0]"`) {
		t.Errorf("expected WINDOW[0] edge, got:\n%s", dot)
	}
}
