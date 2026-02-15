package managers

import (
	"errors"
	"testing"

	"github.com/bawdo/gosbee/internal/testutil"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// --- NewUpdateManager ---

func TestNewUpdateManager(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewUpdateManager(users)
	if m.Statement.Table != users {
		t.Error("expected Table to be users")
	}
}

// --- Set ---

func TestUpdateSet(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewUpdateManager(users).
		Set(users.Col("name"), "Bob")
	if len(m.Statement.Assignments) != 1 {
		t.Errorf("expected 1 assignment, got %d", len(m.Statement.Assignments))
	}
}

func TestUpdateSetMultiple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewUpdateManager(users).
		Set(users.Col("name"), "Bob").
		Set(users.Col("age"), 30)
	if len(m.Statement.Assignments) != 2 {
		t.Errorf("expected 2 assignments, got %d", len(m.Statement.Assignments))
	}
}

func TestUpdateSetNodeValue(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	m := NewUpdateManager(users).
		Set(users.Col("post_count"), posts.Col("count"))
	a := m.Statement.Assignments[0]
	if _, ok := a.Right.(*nodes.Attribute); !ok {
		t.Error("expected Right to be Attribute node")
	}
}

// --- Where ---

func TestUpdateWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewUpdateManager(users).
		Set(users.Col("name"), "Bob").
		Where(users.Col("id").Eq(1))
	if len(m.Statement.Wheres) != 1 {
		t.Errorf("expected 1 where, got %d", len(m.Statement.Wheres))
	}
}

func TestUpdateWhereMultiple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewUpdateManager(users).
		Set(users.Col("active"), false).
		Where(users.Col("role").Eq("guest")).
		Where(users.Col("last_login").Lt("2025-01-01"))
	if len(m.Statement.Wheres) != 2 {
		t.Errorf("expected 2 wheres, got %d", len(m.Statement.Wheres))
	}
}

// --- Returning ---

func TestUpdateReturning(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewUpdateManager(users).
		Set(users.Col("name"), "Bob").
		Returning(users.Col("id"), users.Col("name"))
	if len(m.Statement.Returning) != 2 {
		t.Errorf("expected 2 returning columns, got %d", len(m.Statement.Returning))
	}
}

// --- Chaining ---

func TestUpdateChainingReturnsSelf(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewUpdateManager(users)
	if m.Set(users.Col("name"), "Bob") != m {
		t.Error("Set should return self")
	}
	if m.Where(users.Col("id").Eq(1)) != m {
		t.Error("Where should return self")
	}
	if m.Returning(users.Col("id")) != m {
		t.Error("Returning should return self")
	}
}

// --- Use / Transformers ---

func TestUpdateUseReturnsSelf(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewUpdateManager(users)
	ct := &countingTransformer{}
	if m.Use(ct) != m {
		t.Error("Use should return self")
	}
}

func TestUpdateTransformerCalled(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	ct := &updateCountingTransformer{}
	m := NewUpdateManager(users).Set(users.Col("name"), "Bob")
	m.Use(ct)

	_, _, err := m.ToSQL(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ct.called != 1 {
		t.Errorf("expected transformer called once, got %d", ct.called)
	}
}

// updateCountingTransformer counts TransformUpdate invocations.
type updateCountingTransformer struct {
	plugins.BaseTransformer
	called int
}

func (ct *updateCountingTransformer) TransformUpdate(stmt *nodes.UpdateStatement) (*nodes.UpdateStatement, error) {
	ct.called++
	return stmt, nil
}

func TestUpdateTransformerDoesNotModifyOriginal(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewUpdateManager(users).
		Set(users.Col("name"), "Bob").
		Where(users.Col("id").Eq(1))

	appendingTransformer := &updateAppendTransformer{}
	m.Use(appendingTransformer)

	_, _, _ = m.ToSQL(testutil.StubVisitor{})

	if len(m.Statement.Wheres) != 1 {
		t.Errorf("expected original to have 1 where, got %d", len(m.Statement.Wheres))
	}
}

type updateAppendTransformer struct {
	plugins.BaseTransformer
}

func (t *updateAppendTransformer) TransformUpdate(stmt *nodes.UpdateStatement) (*nodes.UpdateStatement, error) {
	stmt.Wheres = append(stmt.Wheres, nodes.NewAttribute(stmt.Table, "injected").Eq("by_plugin"))
	return stmt, nil
}

// updateFailingTransformer returns an error from TransformUpdate.
type updateFailingTransformer struct {
	plugins.BaseTransformer
}

func (ft updateFailingTransformer) TransformUpdate(stmt *nodes.UpdateStatement) (*nodes.UpdateStatement, error) {
	return nil, errors.New("policy violation: access denied")
}

func TestUpdateTransformerErrorStopsGeneration(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewUpdateManager(users).Set(users.Col("name"), "Bob")
	m.Use(updateFailingTransformer{})

	sql, _, err := m.ToSQL(testutil.StubVisitor{})
	if err == nil {
		t.Fatal("expected error from failing transformer")
	}
	if sql != "" {
		t.Errorf("expected empty SQL on error, got %q", sql)
	}
}

// --- ToSQL ---

func TestUpdateToSQL(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewUpdateManager(users).Set(users.Col("name"), "Bob")

	sql, _, err := m.ToSQL(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "update" {
		t.Errorf("expected 'update', got %q", sql)
	}
}

// --- ToSQLParams ---

func TestUpdateToSQLParamsWithParameterizer(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewUpdateManager(users).Set(users.Col("name"), "Bob")

	sv := &testutil.StubParamVisitor{}
	_, _, err := m.ToSQLParams(sv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpdateToSQLParamsWithoutParameterizer(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewUpdateManager(users).Set(users.Col("name"), "Bob")

	_, params, err := m.ToSQLParams(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if params != nil {
		t.Errorf("expected nil params, got %v", params)
	}
}
