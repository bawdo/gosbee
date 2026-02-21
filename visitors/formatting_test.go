package visitors

import (
	"strings"
	"testing"

	"github.com/bawdo/gosbee/internal/testutil"
	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
)

// fmtPG returns a FormattingVisitor wrapping a non-parameterised PostgresVisitor.
// Used throughout formatting tests for concise setup.
func fmtPG() *FormattingVisitor {
	return NewFormattingVisitor(NewPostgresVisitor(WithoutParams()))
}

// fmtMySQL returns a FormattingVisitor wrapping a non-parameterised MySQLVisitor.
func fmtMySQL() *FormattingVisitor {
	return NewFormattingVisitor(NewMySQLVisitor(WithoutParams()))
}

func TestFormattingVisitorDelegatesLeafNodes(t *testing.T) {
	t.Parallel()
	fv := fmtPG()
	users := nodes.NewTable("users")

	// VisitTable
	testutil.AssertSQL(t, fv, users, `"users"`)
	// VisitAttribute
	testutil.AssertSQL(t, fv, users.Col("id"), `"users"."id"`)
	// VisitLiteral
	testutil.AssertSQL(t, fv, nodes.Literal("alice"), `'alice'`)
	testutil.AssertSQL(t, fv, nodes.Literal(42), `42`)
	// VisitStar
	testutil.AssertSQL(t, fv, nodes.Star(), `*`)
}

func TestFormattingVisitorDelegatesMySQLQuoting(t *testing.T) {
	t.Parallel()
	fv := fmtMySQL()
	users := nodes.NewTable("users")
	testutil.AssertSQL(t, fv, users, "`users`")
	testutil.AssertSQL(t, fv, users.Col("id"), "`users`.`id`")
}

func TestFormattingVisitorParamsForwardedToInner(t *testing.T) {
	t.Parallel()
	inner := NewPostgresVisitor() // parameterised (default)
	fv := NewFormattingVisitor(inner)

	// FormattingVisitor must implement Parameterizer
	p, ok := nodes.Visitor(fv).(nodes.Parameterizer)
	if !ok {
		t.Fatal("FormattingVisitor does not implement Parameterizer")
	}

	p.Reset()
	_ = nodes.Literal("hello").Accept(fv)
	params := p.Params()
	if len(params) != 1 || params[0] != "hello" {
		t.Errorf("expected params [hello], got %v", params)
	}

	// Verify Reset clears the accumulated params
	p.Reset()
	if got := p.Params(); got != nil {
		t.Errorf("expected nil params after Reset, got %v", got)
	}
}

func TestFormattingVisitorParamsNilWhenInnerNotParameterizer(t *testing.T) {
	t.Parallel()
	fv := fmtPG() // WithoutParams — inner does not collect params
	p, ok := nodes.Visitor(fv).(nodes.Parameterizer)
	if !ok {
		t.Fatal("FormattingVisitor does not implement Parameterizer")
	}
	if got := p.Params(); got != nil {
		t.Errorf("expected nil params, got %v", got)
	}
}

func TestFormattingSelectSingleColumn(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"))

	want := "SELECT \"users\".\"id\"\nFROM \"users\""
	testutil.AssertSQL(t, fmtPG(), m.Core, want)
}

func TestFormattingSelectMultiColumn(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"), users.Col("name"), users.Col("email"))

	want := "SELECT \"users\".\"id\"\n\t,\"users\".\"name\"\n\t,\"users\".\"email\"\nFROM \"users\""
	testutil.AssertSQL(t, fmtPG(), m.Core, want)
}

func TestFormattingSelectStar(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	// No explicit projections — should default to *

	want := "SELECT *\nFROM \"users\""
	testutil.AssertSQL(t, fmtPG(), m.Core, want)
}

func TestFormattingSelectMySQLQuoting(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"), users.Col("name"))

	want := "SELECT `users`.`id`\n\t,`users`.`name`\nFROM `users`"
	testutil.AssertSQL(t, fmtMySQL(), m.Core, want)
}

func TestFormattingJoin(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"))
	m.Join(posts, nodes.InnerJoin).On(posts.Col("user_id").Eq(users.Col("id")))

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "\nINNER JOIN") {
		t.Errorf("expected JOIN on its own line, got:\n%s", got)
	}
}

func TestFormattingMultipleJoins(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	comments := nodes.NewTable("comments")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"))
	m.Join(posts, nodes.InnerJoin).On(posts.Col("user_id").Eq(users.Col("id")))
	m.Join(comments, nodes.LeftOuterJoin).On(comments.Col("post_id").Eq(posts.Col("id")))

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "\nINNER JOIN") {
		t.Errorf("expected INNER JOIN on own line, got:\n%s", got)
	}
	if !strings.Contains(got, "\nLEFT OUTER JOIN") {
		t.Errorf("expected LEFT OUTER JOIN on own line, got:\n%s", got)
	}
}

func TestFormattingWhereSingle(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"))
	m.Where(users.Col("active").Eq(nodes.Literal(true)))

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "\nWHERE ") {
		t.Errorf("expected WHERE on its own line, got:\n%s", got)
	}
	// Single condition — should NOT have AND continuation
	if strings.Contains(got, "\n\tAND") {
		t.Errorf("single WHERE should not have AND continuation, got:\n%s", got)
	}
}

func TestFormattingWhereMultiple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"))
	m.Where(users.Col("active").Eq(nodes.Literal(true)))
	m.Where(users.Col("age").Gt(nodes.Literal(18)))

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "\nWHERE ") {
		t.Errorf("expected WHERE on its own line, got:\n%s", got)
	}
	if !strings.Contains(got, "\n\tAND ") {
		t.Errorf("expected AND continuation, got:\n%s", got)
	}
}

func TestFormattingGroupByMultiple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("dept"), nodes.NewAggregateNode(nodes.AggCount, nil))
	m.Group(users.Col("dept"), users.Col("region"))

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "\nGROUP BY ") {
		t.Errorf("expected GROUP BY on own line, got:\n%s", got)
	}
	if !strings.Contains(got, "\n\t,") {
		t.Errorf("expected leading-comma continuation in GROUP BY, got:\n%s", got)
	}
}

func TestFormattingOrderByMultiple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"))
	m.Order(
		&nodes.OrderingNode{Expr: users.Col("name"), Direction: nodes.Asc},
		&nodes.OrderingNode{Expr: users.Col("id"), Direction: nodes.Desc},
	)

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "\nORDER BY ") {
		t.Errorf("expected ORDER BY on own line, got:\n%s", got)
	}
	if !strings.Contains(got, "\n\t,") {
		t.Errorf("expected leading-comma continuation in ORDER BY, got:\n%s", got)
	}
}

func TestFormattingLimitOffset(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"))
	m.Limit(10)
	m.Offset(5)

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "\nLIMIT 10") {
		t.Errorf("expected LIMIT on own line, got:\n%s", got)
	}
	if !strings.Contains(got, "\nOFFSET 5") {
		t.Errorf("expected OFFSET on own line, got:\n%s", got)
	}
}

func TestFormattingForUpdate(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"))
	m.Core.Lock = nodes.ForUpdate

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "\nFOR UPDATE") {
		t.Errorf("expected FOR UPDATE on own line, got:\n%s", got)
	}
}

func TestFormattingCTE(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	active := managers.NewSelectManager(users)
	active.Select(users.Col("id"))
	active.Where(users.Col("active").Eq(nodes.Literal(true)))

	cteTable := nodes.NewTable("active_users")
	main := managers.NewSelectManager(cteTable)
	main.Select(cteTable.Col("id"))
	main.With("active_users", active.Core)

	got := main.Core.Accept(fmtPG())
	if !strings.HasPrefix(got, "WITH ") {
		t.Errorf("expected WITH prefix, got:\n%s", got)
	}
	if !strings.Contains(got, "\nSELECT ") {
		t.Errorf("expected SELECT on own line after WITH, got:\n%s", got)
	}
}

func TestFormattingFullQuery(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"), users.Col("name"), users.Col("email"))
	m.Join(posts, nodes.InnerJoin).On(posts.Col("user_id").Eq(users.Col("id")))
	m.Where(users.Col("active").Eq(nodes.Literal(true)))
	m.Where(users.Col("age").Gt(nodes.Literal(18)))
	m.Group(users.Col("id"), users.Col("name"))
	m.Order(
		&nodes.OrderingNode{Expr: users.Col("name"), Direction: nodes.Asc},
		&nodes.OrderingNode{Expr: users.Col("id"), Direction: nodes.Desc},
	)
	m.Limit(10)
	m.Offset(5)

	want := strings.Join([]string{
		`SELECT "users"."id"`,
		`	,"users"."name"`,
		`	,"users"."email"`,
		`FROM "users"`,
		`INNER JOIN "posts" ON "posts"."user_id" = "users"."id"`,
		`WHERE "users"."active" = TRUE`,
		`	AND "users"."age" > 18`,
		`GROUP BY "users"."id"`,
		`	,"users"."name"`,
		`ORDER BY "users"."name" ASC`,
		`	,"users"."id" DESC`,
		`LIMIT 10`,
		`OFFSET 5`,
	}, "\n")

	testutil.AssertSQL(t, fmtPG(), m.Core, want)
}

func TestFormattingSetOperation(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")

	left := managers.NewSelectManager(users)
	left.Select(users.Col("id"))
	left.Where(users.Col("active").Eq(nodes.Literal(true)))

	right := managers.NewSelectManager(users)
	right.Select(users.Col("id"))
	right.Where(users.Col("role").Eq(nodes.Literal("admin")))

	op := &nodes.SetOperationNode{
		Left:  left.Core,
		Right: right.Core,
		Type:  nodes.UnionAll,
	}

	got := op.Accept(fmtPG())
	if !strings.Contains(got, "UNION ALL") {
		t.Errorf("expected UNION ALL, got:\n%s", got)
	}
	// Each leg should be wrapped in parens on separate lines
	if !strings.HasPrefix(got, "(") {
		t.Errorf("expected output to start with (, got:\n%s", got)
	}
	if !strings.Contains(got, "\nUNION ALL\n") {
		t.Errorf("expected UNION ALL on own line, got:\n%s", got)
	}
}

func TestFormattingDistinct(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("name"))
	m.Distinct()

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "SELECT DISTINCT ") {
		t.Errorf("expected SELECT DISTINCT, got:\n%s", got)
	}
}

func TestFormattingInsert(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewInsertManager(users)
	m.Columns(users.Col("name"), users.Col("email"))
	m.Values("alice", "alice@example.com")
	m.Returning(users.Col("id"))

	got := m.Statement.Accept(fmtPG())
	if !strings.HasPrefix(got, "INSERT INTO") {
		t.Errorf("expected INSERT INTO prefix, got:\n%s", got)
	}
	if !strings.Contains(got, "\nVALUES") {
		t.Errorf("expected VALUES on own line, got:\n%s", got)
	}
	if !strings.Contains(got, "\nRETURNING") {
		t.Errorf("expected RETURNING on own line, got:\n%s", got)
	}
}

func TestFormattingInsertColumnsAreUnqualified(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewInsertManager(users)
	m.Columns(users.Col("name"), users.Col("email"))
	m.Values("alice", "alice@example.com")

	got := m.Statement.Accept(fmtPG())
	// Column list must not include table qualifier in the column list position
	valuesIdx := strings.Index(got, "VALUES")
	nameIdx := strings.Index(got, `"users"."name"`)
	if nameIdx >= 0 && (valuesIdx < 0 || nameIdx < valuesIdx) {
		t.Errorf("INSERT column list must not include table qualifier, got:\n%s", got)
	}
	// Should have bare quoted names in column list
	if !strings.Contains(got, `("name", "email")`) {
		t.Errorf("expected bare column names in INSERT column list, got:\n%s", got)
	}
}

func TestFormattingUpdate(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewUpdateManager(users)
	m.Set(users.Col("name"), "alice")
	m.Set(users.Col("email"), "alice@example.com")
	m.Where(users.Col("id").Eq(nodes.Literal(1)))

	got := m.Statement.Accept(fmtPG())
	if !strings.HasPrefix(got, "UPDATE") {
		t.Errorf("expected UPDATE prefix, got:\n%s", got)
	}
	if !strings.Contains(got, "\nSET ") {
		t.Errorf("expected SET on own line, got:\n%s", got)
	}
	if !strings.Contains(got, "\n\t,") {
		t.Errorf("expected leading-comma in SET, got:\n%s", got)
	}
	if !strings.Contains(got, "\nWHERE ") {
		t.Errorf("expected WHERE on own line, got:\n%s", got)
	}
}

func TestFormattingDelete(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewDeleteManager(users)
	m.Where(users.Col("id").Eq(nodes.Literal(1)))
	m.Returning(users.Col("id"))

	got := m.Statement.Accept(fmtPG())
	if !strings.HasPrefix(got, "DELETE FROM") {
		t.Errorf("expected DELETE FROM prefix, got:\n%s", got)
	}
	if !strings.Contains(got, "\nWHERE ") {
		t.Errorf("expected WHERE on own line, got:\n%s", got)
	}
	if !strings.Contains(got, "\nRETURNING") {
		t.Errorf("expected RETURNING on own line, got:\n%s", got)
	}
}

func TestFormattingSetOperationWithOrderByAndLimit(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")

	left := managers.NewSelectManager(users)
	left.Select(users.Col("id"))

	right := managers.NewSelectManager(users)
	right.Select(users.Col("id"))
	right.Where(users.Col("active").Eq(nodes.Literal(true)))

	op := &nodes.SetOperationNode{
		Left:  left.Core,
		Right: right.Core,
		Type:  nodes.Union,
		Orders: []nodes.Node{
			&nodes.OrderingNode{Expr: users.Col("name"), Direction: nodes.Asc},
		},
		Limit:  nodes.Literal(10),
		Offset: nodes.Literal(5),
	}

	got := op.Accept(fmtPG())
	if !strings.Contains(got, "\nORDER BY ") {
		t.Errorf("expected ORDER BY in set operation output, got:\n%s", got)
	}
	if !strings.Contains(got, "\nLIMIT 10") {
		t.Errorf("expected LIMIT 10 in set operation output, got:\n%s", got)
	}
	if !strings.Contains(got, "\nOFFSET 5") {
		t.Errorf("expected OFFSET 5 in set operation output, got:\n%s", got)
	}
}

func TestFormattingSetOperationWithMultipleOrders(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")

	left := managers.NewSelectManager(users)
	left.Select(users.Col("id"))

	right := managers.NewSelectManager(users)
	right.Select(users.Col("id"))

	op := &nodes.SetOperationNode{
		Left:  left.Core,
		Right: right.Core,
		Type:  nodes.Intersect,
		Orders: []nodes.Node{
			&nodes.OrderingNode{Expr: users.Col("name"), Direction: nodes.Asc},
			&nodes.OrderingNode{Expr: users.Col("id"), Direction: nodes.Desc},
		},
	}

	got := op.Accept(fmtPG())
	if !strings.Contains(got, "\nORDER BY ") {
		t.Errorf("expected ORDER BY in set operation output, got:\n%s", got)
	}
	if !strings.Contains(got, "\n\t,") {
		t.Errorf("expected leading-comma continuation in ORDER BY, got:\n%s", got)
	}
}

func TestFormattingInsertMultipleReturning(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewInsertManager(users)
	m.Columns(users.Col("name"), users.Col("email"))
	m.Values("alice", "alice@example.com")
	m.Returning(users.Col("id"), users.Col("name"))

	got := m.Statement.Accept(fmtPG())
	if !strings.Contains(got, "\nRETURNING ") {
		t.Errorf("expected RETURNING on own line, got:\n%s", got)
	}
	if !strings.Contains(got, "\n\t,") {
		t.Errorf("expected leading-comma continuation for second RETURNING column, got:\n%s", got)
	}
}

func TestFormattingUpdateReturning(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewUpdateManager(users)
	m.Set(users.Col("name"), "bob")
	m.Where(users.Col("id").Eq(nodes.Literal(42)))
	m.Returning(users.Col("id"))

	got := m.Statement.Accept(fmtPG())
	if !strings.Contains(got, "\nWHERE ") {
		t.Errorf("expected WHERE on own line, got:\n%s", got)
	}
	if !strings.Contains(got, "\nRETURNING ") {
		t.Errorf("expected RETURNING on own line, got:\n%s", got)
	}
}

func TestFormattingUpdateMultipleWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewUpdateManager(users)
	m.Set(users.Col("name"), "carol")
	m.Where(users.Col("active").Eq(nodes.Literal(true)))
	m.Where(users.Col("age").Gt(nodes.Literal(21)))

	got := m.Statement.Accept(fmtPG())
	if !strings.Contains(got, "\nWHERE ") {
		t.Errorf("expected WHERE on own line, got:\n%s", got)
	}
	if !strings.Contains(got, "\n\tAND ") {
		t.Errorf("expected AND continuation for second WHERE condition, got:\n%s", got)
	}
}

func TestFormattingDeleteMultipleWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewDeleteManager(users)
	m.Where(users.Col("active").Eq(nodes.Literal(false)))
	m.Where(users.Col("age").Lt(nodes.Literal(18)))

	got := m.Statement.Accept(fmtPG())
	if !strings.Contains(got, "\nWHERE ") {
		t.Errorf("expected WHERE on own line, got:\n%s", got)
	}
	if !strings.Contains(got, "\n\tAND ") {
		t.Errorf("expected AND continuation for second WHERE condition, got:\n%s", got)
	}
}

// --- VisitSelectCore uncovered branches ---

func TestFormattingComment(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"))
	m.Comment("my query")

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "/* my query */") {
		t.Errorf("expected comment in SQL, got:\n%s", got)
	}
}

func TestFormattingHintSingle(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"))
	m.Hint("INDEX_SCAN(users)")

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "/*+ INDEX_SCAN(users) */") {
		t.Errorf("expected hint in SQL, got:\n%s", got)
	}
}

func TestFormattingHintMultiple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"))
	m.Hint("INDEX_SCAN(users)")
	m.Hint("NO_HASH_JOIN")

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "INDEX_SCAN(users) NO_HASH_JOIN") {
		t.Errorf("expected multiple hints in SQL, got:\n%s", got)
	}
}

func TestFormattingDistinctOn(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"), users.Col("email"))
	m.DistinctOn(users.Col("email"))

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "DISTINCT ON (") {
		t.Errorf("expected DISTINCT ON in SQL, got:\n%s", got)
	}
}

func TestFormattingHaving(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("dept"), nodes.NewAggregateNode(nodes.AggCount, nil))
	m.Group(users.Col("dept"))
	m.Having(nodes.NewAggregateNode(nodes.AggCount, nil).Gt(nodes.Literal(5)))

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "\nHAVING ") {
		t.Errorf("expected HAVING on own line, got:\n%s", got)
	}
}

func TestFormattingHavingMultiple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("dept"), nodes.NewAggregateNode(nodes.AggCount, nil))
	m.Group(users.Col("dept"))
	m.Having(nodes.NewAggregateNode(nodes.AggCount, nil).Gt(nodes.Literal(5)))
	m.Having(nodes.NewAggregateNode(nodes.AggCount, nil).Lt(nodes.Literal(100)))

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "\nHAVING ") {
		t.Errorf("expected HAVING on own line, got:\n%s", got)
	}
	if !strings.Contains(got, "\n\tAND ") {
		t.Errorf("expected AND continuation in HAVING, got:\n%s", got)
	}
}

func TestFormattingWindow(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"))
	m.Window(nodes.NewWindowDef("w").Partition(users.Col("dept")))

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "\nWINDOW ") {
		t.Errorf("expected WINDOW on own line, got:\n%s", got)
	}
	if !strings.Contains(got, `"w" AS`) {
		t.Errorf("expected window name in SQL, got:\n%s", got)
	}
}

func TestFormattingSkipLocked(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"))
	m.Core.Lock = nodes.ForUpdate
	m.Core.SkipLocked = true

	got := m.Core.Accept(fmtPG())
	if !strings.Contains(got, "SKIP LOCKED") {
		t.Errorf("expected SKIP LOCKED in SQL, got:\n%s", got)
	}
}

func TestFormattingWithRecursive(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	cteQuery := managers.NewSelectManager(users)
	cteQuery.Select(users.Col("id"), users.Col("manager_id"))

	hierarchy := nodes.NewTable("hierarchy")
	main := managers.NewSelectManager(hierarchy)
	main.Select(hierarchy.Col("id"))
	main.WithRecursive("hierarchy", cteQuery.Core)

	got := main.Core.Accept(fmtPG())
	if !strings.Contains(got, "WITH RECURSIVE ") {
		t.Errorf("expected WITH RECURSIVE in SQL, got:\n%s", got)
	}
}

func TestFormattingMultipleCTEs(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	orders := nodes.NewTable("orders")

	cte1 := managers.NewSelectManager(users)
	cte1.Select(users.Col("id"))

	cte2 := managers.NewSelectManager(orders)
	cte2.Select(orders.Col("user_id"))

	active := nodes.NewTable("active")
	main := managers.NewSelectManager(active)
	main.Select(active.Col("id"))
	main.With("active", cte1.Core)
	main.With("ordered", cte2.Core)

	got := main.Core.Accept(fmtPG())
	if !strings.HasPrefix(got, "WITH ") {
		t.Errorf("expected WITH prefix, got:\n%s", got)
	}
	if !strings.Contains(got, "active") || !strings.Contains(got, "ordered") {
		t.Errorf("expected both CTEs in SQL, got:\n%s", got)
	}
}
