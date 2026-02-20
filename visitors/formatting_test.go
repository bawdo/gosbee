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
