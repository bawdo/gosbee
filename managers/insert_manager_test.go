package managers

import (
	"testing"

	"github.com/bawdo/gosbee/internal/testutil"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// --- NewInsertManager ---

func TestNewInsertManager(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users)
	if m.Statement.Into != users {
		t.Error("expected Into to be users table")
	}
}

// --- Columns ---

func TestInsertColumns(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users).
		Columns(users.Col("name"), users.Col("email"))
	if len(m.Statement.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(m.Statement.Columns))
	}
}

// --- Values ---

func TestInsertValues(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users).
		Columns(users.Col("name")).
		Values("Alice")
	if len(m.Statement.Values) != 1 {
		t.Errorf("expected 1 row, got %d", len(m.Statement.Values))
	}
	if len(m.Statement.Values[0]) != 1 {
		t.Errorf("expected 1 value in row, got %d", len(m.Statement.Values[0]))
	}
}

func TestInsertMultipleRows(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users).
		Columns(users.Col("name")).
		Values("Alice").
		Values("Bob").
		Values("Carol")
	if len(m.Statement.Values) != 3 {
		t.Errorf("expected 3 rows, got %d", len(m.Statement.Values))
	}
}

func TestInsertMultipleColumnsAndValues(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users).
		Columns(users.Col("name"), users.Col("age")).
		Values("Alice", 30).
		Values("Bob", 25)
	if len(m.Statement.Values) != 2 {
		t.Errorf("expected 2 rows, got %d", len(m.Statement.Values))
	}
	if len(m.Statement.Values[0]) != 2 {
		t.Errorf("expected 2 values in first row, got %d", len(m.Statement.Values[0]))
	}
}

// --- FromSelect ---

func TestInsertFromSelect(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	archive := nodes.NewTable("archive")
	sel := NewSelectManager(users).Select(users.Col("name"))

	m := NewInsertManager(archive).
		Columns(archive.Col("name")).
		FromSelect(sel)

	if m.Statement.Select == nil {
		t.Error("expected Select to be set")
	}
}

// --- Returning ---

func TestInsertReturning(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users).
		Columns(users.Col("name")).
		Values("Alice").
		Returning(users.Col("id"))
	if len(m.Statement.Returning) != 1 {
		t.Errorf("expected 1 returning column, got %d", len(m.Statement.Returning))
	}
}

// --- OnConflict ---

func TestInsertOnConflictDoNothing(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users).
		Columns(users.Col("email")).
		Values("a@b.com").
		OnConflict(users.Col("email")).DoNothing()

	if m.Statement.OnConflict == nil {
		t.Fatal("expected OnConflict to be set")
	}
	if m.Statement.OnConflict.Action != nodes.DoNothing {
		t.Error("expected DoNothing action")
	}
}

func TestInsertOnConflictDoUpdate(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	assign := &nodes.AssignmentNode{Left: users.Col("name"), Right: nodes.Literal("updated")}
	m := NewInsertManager(users).
		Columns(users.Col("email"), users.Col("name")).
		Values("a@b.com", "Alice")

	m.OnConflict(users.Col("email")).
		DoUpdate(assign).
		Where(users.Col("locked").Eq(false))

	oc := m.Statement.OnConflict
	if oc == nil {
		t.Fatal("expected OnConflict to be set")
	}
	if oc.Action != nodes.DoUpdate {
		t.Error("expected DoUpdate action")
	}
	if len(oc.Assignments) != 1 {
		t.Errorf("expected 1 assignment, got %d", len(oc.Assignments))
	}
	if len(oc.Wheres) != 1 {
		t.Errorf("expected 1 where, got %d", len(oc.Wheres))
	}
}

// --- Chaining ---

func TestInsertChainingReturnsSelf(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users)
	if m.Columns(users.Col("name")) != m {
		t.Error("Columns should return self")
	}
	if m.Values("Alice") != m {
		t.Error("Values should return self")
	}
	if m.Returning(users.Col("id")) != m {
		t.Error("Returning should return self")
	}
}

// --- Use / Transformers ---

func TestInsertUseReturnsSelf(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users)
	ct := &countingTransformer{}
	if m.Use(ct) != m {
		t.Error("Use should return self")
	}
}

func TestInsertTransformerCalled(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	ct := &countingTransformer{}
	m := NewInsertManager(users).
		Columns(users.Col("name")).
		Values("Alice")
	m.Use(ct)

	_, _, err := m.ToSQL(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ct.called != 1 {
		t.Errorf("expected transformer called once, got %d", ct.called)
	}
}

func TestInsertTransformerDoesNotModifyOriginal(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users).
		Columns(users.Col("name")).
		Values("Alice")

	appendingTransformer := &insertAppendTransformer{}
	m.Use(appendingTransformer)

	_, _, _ = m.ToSQL(testutil.StubVisitor{})

	// Original should be unchanged
	if len(m.Statement.Values) != 1 {
		t.Errorf("expected original to have 1 row, got %d", len(m.Statement.Values))
	}
}

type insertAppendTransformer struct {
	plugins.BaseTransformer
}

func (t *insertAppendTransformer) TransformInsert(stmt *nodes.InsertStatement) (*nodes.InsertStatement, error) {
	stmt.Values = append(stmt.Values, []nodes.Node{nodes.Literal("injected")})
	return stmt, nil
}

func TestInsertTransformerErrorStopsGeneration(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users).
		Columns(users.Col("name")).
		Values("Alice")
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

func TestInsertToSQL(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users).
		Columns(users.Col("name")).
		Values("Alice")

	sql, _, err := m.ToSQL(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "insert" {
		t.Errorf("expected 'insert', got %q", sql)
	}
}

// --- ToSQLParams ---

func TestInsertToSQLParamsWithParameterizer(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users).
		Columns(users.Col("name")).
		Values("Alice")

	sv := &testutil.StubParamVisitor{}
	sql, params, err := m.ToSQLParams(sv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "insert" {
		t.Errorf("expected 'insert', got %q", sql)
	}
	// After reset, params should be empty
	_ = params
}

func TestInsertToSQLParamsWithoutParameterizer(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewInsertManager(users).
		Columns(users.Col("name")).
		Values("Alice")

	sql, params, err := m.ToSQLParams(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "insert" {
		t.Errorf("expected 'insert', got %q", sql)
	}
	if params != nil {
		t.Errorf("expected nil params, got %v", params)
	}
}
