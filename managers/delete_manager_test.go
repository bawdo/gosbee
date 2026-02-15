package managers

import (
	"testing"

	"github.com/bawdo/gosbee/internal/testutil"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// --- NewDeleteManager ---

func TestNewDeleteManager(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewDeleteManager(users)
	if m.Statement.From != users {
		t.Error("expected From to be users table")
	}
}

// --- Where ---

func TestDeleteWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewDeleteManager(users).
		Where(users.Col("id").Eq(1))
	if len(m.Statement.Wheres) != 1 {
		t.Errorf("expected 1 where, got %d", len(m.Statement.Wheres))
	}
}

func TestDeleteWhereMultiple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewDeleteManager(users).
		Where(users.Col("active").Eq(false)).
		Where(users.Col("role").Eq("guest"))
	if len(m.Statement.Wheres) != 2 {
		t.Errorf("expected 2 wheres, got %d", len(m.Statement.Wheres))
	}
}

// --- Returning ---

func TestDeleteReturning(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewDeleteManager(users).
		Where(users.Col("id").Eq(1)).
		Returning(users.Col("id"))
	if len(m.Statement.Returning) != 1 {
		t.Errorf("expected 1 returning column, got %d", len(m.Statement.Returning))
	}
}

// --- Chaining ---

func TestDeleteChainingReturnsSelf(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewDeleteManager(users)
	if m.Where(users.Col("id").Eq(1)) != m {
		t.Error("Where should return self")
	}
	if m.Returning(users.Col("id")) != m {
		t.Error("Returning should return self")
	}
}

// --- Use / Transformers ---

func TestDeleteUseReturnsSelf(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewDeleteManager(users)
	ct := &countingTransformer{}
	if m.Use(ct) != m {
		t.Error("Use should return self")
	}
}

func TestDeleteTransformerCalled(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	ct := &countingTransformer{}
	m := NewDeleteManager(users).Where(users.Col("id").Eq(1))
	m.Use(ct)

	_, _, err := m.ToSQL(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ct.called != 1 {
		t.Errorf("expected transformer called once, got %d", ct.called)
	}
}

func TestDeleteTransformerDoesNotModifyOriginal(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewDeleteManager(users).
		Where(users.Col("id").Eq(1))

	appendingTransformer := &deleteAppendTransformer{}
	m.Use(appendingTransformer)

	_, _, _ = m.ToSQL(testutil.StubVisitor{})

	if len(m.Statement.Wheres) != 1 {
		t.Errorf("expected original to have 1 where, got %d", len(m.Statement.Wheres))
	}
}

type deleteAppendTransformer struct {
	plugins.BaseTransformer
}

func (t *deleteAppendTransformer) TransformDelete(stmt *nodes.DeleteStatement) (*nodes.DeleteStatement, error) {
	stmt.Wheres = append(stmt.Wheres, nodes.NewAttribute(stmt.From, "injected").Eq("by_plugin"))
	return stmt, nil
}

func TestDeleteTransformerErrorStopsGeneration(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewDeleteManager(users).Where(users.Col("id").Eq(1))
	m.Use(failingTransformer{})

	sql, _, err := m.ToSQL(testutil.StubVisitor{})
	if err == nil {
		t.Fatal("expected error from failing transformer")
	}
	if sql != "" {
		t.Errorf("expected empty SQL on error, got %q", sql)
	}
}

// --- ToSQL ---

func TestDeleteToSQL(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewDeleteManager(users).Where(users.Col("id").Eq(1))

	sql, _, err := m.ToSQL(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "delete" {
		t.Errorf("expected 'delete', got %q", sql)
	}
}

// --- ToSQLParams ---

func TestDeleteToSQLParamsWithParameterizer(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewDeleteManager(users).Where(users.Col("id").Eq(1))

	sv := &testutil.StubParamVisitor{}
	_, _, err := m.ToSQLParams(sv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteToSQLParamsWithoutParameterizer(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewDeleteManager(users).Where(users.Col("id").Eq(1))

	_, params, err := m.ToSQLParams(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if params != nil {
		t.Errorf("expected nil params, got %v", params)
	}
}
