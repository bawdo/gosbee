package softdelete

import (
	"errors"
	"strings"
	"testing"

	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
	"github.com/bawdo/gosbee/visitors"
)

func toSQL(t *testing.T, core *nodes.SelectCore) string {
	t.Helper()
	return core.Accept(visitors.NewPostgresVisitor(visitors.WithoutParams()))
}

// --- Default behaviour ---

func TestDefaultColumnDeletedAt(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users}

	sd := New()
	result, err := sd.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "users" WHERE "users"."deleted_at" IS NULL`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

// --- Custom column name ---

func TestCustomColumnName(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users}

	sd := New(WithColumn("removed_at"))
	result, err := sd.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "users" WHERE "users"."removed_at" IS NULL`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

// --- Preserves existing WHERE conditions ---

func TestPreservesExistingWheres(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{
		From:   users,
		Wheres: []nodes.Node{users.Col("active").Eq(true)},
	}

	sd := New()
	result, err := sd.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "users" WHERE "users"."active" = TRUE AND "users"."deleted_at" IS NULL`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

// --- Applies to joined tables ---

func TestAppliedToJoinedTables(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	core := &nodes.SelectCore{
		From: users,
		Joins: []*nodes.JoinNode{
			{
				Left:  users,
				Right: posts,
				Type:  nodes.InnerJoin,
				On:    users.Col("id").Eq(posts.Col("user_id")),
			},
		},
	}

	sd := New()
	result, err := sd.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id" WHERE "users"."deleted_at" IS NULL AND "posts"."deleted_at" IS NULL`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

// --- Table filtering ---

func TestWithTablesFiltersToSpecifiedTables(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	core := &nodes.SelectCore{
		From: users,
		Joins: []*nodes.JoinNode{
			{
				Left:  users,
				Right: posts,
				Type:  nodes.InnerJoin,
				On:    users.Col("id").Eq(posts.Col("user_id")),
			},
		},
	}

	// Only apply soft-delete to users, not posts
	sd := New(WithTables("users"))
	result, err := sd.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id" WHERE "users"."deleted_at" IS NULL`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

// --- Table alias ---

func TestAppliedToTableAlias(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	u := users.Alias("u")
	core := &nodes.SelectCore{From: u}

	sd := New()
	result, err := sd.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	// Column should be qualified with the alias
	expected := `SELECT * FROM "users" AS "u" WHERE "u"."deleted_at" IS NULL`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

// --- WithTables matches by underlying table name, not alias ---

func TestWithTablesMatchesByUnderlyingName(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	u := users.Alias("u")
	core := &nodes.SelectCore{From: u}

	sd := New(WithTables("users"))
	result, err := sd.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Wheres) != 1 {
		t.Errorf("expected 1 where clause, got %d", len(result.Wheres))
	}
}

// --- No tables to process (nil From, no joins) ---

func TestNoTablesIsNoOp(t *testing.T) {
	t.Parallel()
	core := &nodes.SelectCore{}

	sd := New()
	result, err := sd.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Wheres) != 0 {
		t.Errorf("expected no wheres, got %d", len(result.Wheres))
	}
}

// --- Per-table column overrides ---

func TestWithTableColumn(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users}

	sd := New(WithTableColumn("users", "removed_at"))
	result, err := sd.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "users" WHERE "users"."removed_at" IS NULL`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestWithTableColumnMultiple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	core := &nodes.SelectCore{
		From: users,
		Joins: []*nodes.JoinNode{
			{
				Left:  users,
				Right: posts,
				Type:  nodes.InnerJoin,
				On:    users.Col("id").Eq(posts.Col("user_id")),
			},
		},
	}

	sd := New(
		WithTableColumn("users", "deleted_at"),
		WithTableColumn("posts", "removed_at"),
	)
	result, err := sd.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id" WHERE "users"."deleted_at" IS NULL AND "posts"."removed_at" IS NULL`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestWithTableColumnFallsBackToDefault(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	core := &nodes.SelectCore{
		From: users,
		Joins: []*nodes.JoinNode{
			{
				Left:  users,
				Right: posts,
				Type:  nodes.InnerJoin,
				On:    users.Col("id").Eq(posts.Col("user_id")),
			},
		},
	}

	// Only override posts; users should fall back to the default Column
	sd := New(
		WithTableColumn("posts", "removed_at"),
		WithTables("users", "posts"),
	)
	result, err := sd.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id" WHERE "users"."deleted_at" IS NULL AND "posts"."removed_at" IS NULL`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

// --- Implements Transformer interface ---

func TestImplementsTransformer(t *testing.T) {
	t.Parallel()
	// Compile-time check
	var _ interface {
		TransformSelect(*nodes.SelectCore) (*nodes.SelectCore, error)
	} = New()
}

// --- Nested queries, CTEs, set operations, raw SQL ---

func pgVisitor() *visitors.PostgresVisitor {
	return visitors.NewPostgresVisitor(visitors.WithoutParams())
}

func TestSoftDeleteFiltersSubquery(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	sub := managers.NewSelectManager(posts).Select(posts.Col("user_id"))
	q := managers.NewSelectManager(users).
		Where(&nodes.InNode{Expr: users.Col("id"), Vals: []nodes.Node{sub.Core}}).
		Use(New())

	got, _, err := q.ToSQL(pgVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `SELECT * FROM "users" WHERE "users"."id" IN (SELECT "posts"."user_id" FROM "posts" WHERE "posts"."deleted_at" IS NULL) AND "users"."deleted_at" IS NULL`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestSoftDeleteSkipsCTEReference(t *testing.T) {
	t.Parallel()
	posts := nodes.NewTable("posts")
	body := managers.NewSelectManager(posts)
	q := managers.NewSelectManager(nodes.NewTable("c")).With("c", body.Core).Use(New())

	got, _, err := q.ToSQL(pgVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `WITH "c" AS (SELECT * FROM "posts" WHERE "posts"."deleted_at" IS NULL) SELECT * FROM "c"`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestSoftDeleteFiltersUnionBranches(t *testing.T) {
	t.Parallel()
	a := managers.NewSelectManager(nodes.NewTable("users"))
	b := managers.NewSelectManager(nodes.NewTable("admins"))
	m := managers.NewSetOperationManager(a.Union(b)).Use(New())

	got, _, err := m.ToSQL(pgVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{`"users"."deleted_at" IS NULL`, `"admins"."deleted_at" IS NULL`} {
		if !strings.Contains(got, want) {
			t.Errorf("expected %s in:\n  %s", want, got)
		}
	}
}

func rawQuery() *managers.SelectManager {
	users := nodes.NewTable("users")
	return managers.NewSelectManager(users).Where(nodes.NewSqlLiteral("1 = 1"))
}

func TestSoftDeleteRefusesRawSQL(t *testing.T) {
	t.Parallel()
	_, _, err := rawQuery().Use(New()).ToSQL(pgVisitor())
	if !errors.Is(err, plugins.ErrUnpoliceable) {
		t.Fatalf("expected ErrUnpoliceable, got %v", err)
	}
}

func TestSoftDeleteAllowRawSQLOption(t *testing.T) {
	t.Parallel()
	got, _, err := rawQuery().Use(New(WithAllowRawSQL())).ToSQL(pgVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `SELECT * FROM "users" WHERE 1 = 1 AND "users"."deleted_at" IS NULL`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestSoftDeleteRepeatedToSQLIsStable(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	sub := managers.NewSelectManager(posts).Select(posts.Col("user_id"))
	q := managers.NewSelectManager(users).
		Where(&nodes.InNode{Expr: users.Col("id"), Vals: []nodes.Node{sub.Core}}).
		Use(New())

	first, _, err := q.ToSQL(pgVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, _, err := q.ToSQL(pgVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first != second {
		t.Errorf("repeated ToSQL differs:\n  %s\n  %s", first, second)
	}
	plain, _, err := sub.ToSQL(pgVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(plain, "deleted_at") {
		t.Errorf("subquery was mutated: %s", plain)
	}
}
