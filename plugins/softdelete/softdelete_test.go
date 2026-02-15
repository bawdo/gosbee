package softdelete

import (
	"testing"

	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/visitors"
)

func toSQL(t *testing.T, core *nodes.SelectCore) string {
	t.Helper()
	return core.Accept(visitors.NewPostgresVisitor())
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
