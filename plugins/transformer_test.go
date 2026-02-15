package plugins

import (
	"testing"

	"github.com/bawdo/gosbee/nodes"
)

// --- BaseTransformer no-op behaviour ---

func TestBaseTransformerSelect(t *testing.T) {
	t.Parallel()
	bt := BaseTransformer{}
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("id")},
		Wheres:      []nodes.Node{users.Col("active").Eq(true)},
	}

	result, err := bt.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != core {
		t.Error("expected BaseTransformer.TransformSelect to return input unchanged")
	}
}

func TestBaseTransformerInsert(t *testing.T) {
	t.Parallel()
	bt := BaseTransformer{}
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:    users,
		Columns: []nodes.Node{users.Col("name")},
		Values:  [][]nodes.Node{{nodes.Literal("Alice")}},
	}

	result, err := bt.TransformInsert(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != stmt {
		t.Error("expected BaseTransformer.TransformInsert to return input unchanged")
	}
}

func TestBaseTransformerUpdate(t *testing.T) {
	t.Parallel()
	bt := BaseTransformer{}
	users := nodes.NewTable("users")
	stmt := &nodes.UpdateStatement{
		Table: users,
		Assignments: []*nodes.AssignmentNode{
			{
				Left:  users.Col("name"),
				Right: nodes.Literal("Bob"),
			},
		},
		Wheres: []nodes.Node{users.Col("id").Eq(1)},
	}

	result, err := bt.TransformUpdate(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != stmt {
		t.Error("expected BaseTransformer.TransformUpdate to return input unchanged")
	}
}

func TestBaseTransformerDelete(t *testing.T) {
	t.Parallel()
	bt := BaseTransformer{}
	users := nodes.NewTable("users")
	stmt := &nodes.DeleteStatement{
		From:   users,
		Wheres: []nodes.Node{users.Col("id").Eq(1)},
	}

	result, err := bt.TransformDelete(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != stmt {
		t.Error("expected BaseTransformer.TransformDelete to return input unchanged")
	}
}

// --- BaseTransformer with nil inputs ---

func TestBaseTransformerNilSelect(t *testing.T) {
	t.Parallel()
	bt := BaseTransformer{}

	result, err := bt.TransformSelect(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("expected nil input to return nil")
	}
}

func TestBaseTransformerNilInsert(t *testing.T) {
	t.Parallel()
	bt := BaseTransformer{}

	result, err := bt.TransformInsert(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("expected nil input to return nil")
	}
}

func TestBaseTransformerNilUpdate(t *testing.T) {
	t.Parallel()
	bt := BaseTransformer{}

	result, err := bt.TransformUpdate(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("expected nil input to return nil")
	}
}

func TestBaseTransformerNilDelete(t *testing.T) {
	t.Parallel()
	bt := BaseTransformer{}

	result, err := bt.TransformDelete(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("expected nil input to return nil")
	}
}
