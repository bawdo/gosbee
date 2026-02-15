package plugins

import (
	"testing"

	"github.com/bawdo/gosbee/nodes"
)

func TestCollectTablesFromTable(t *testing.T) {
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users}

	refs := CollectTables(core)
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Name != "users" {
		t.Errorf("expected name 'users', got %q", refs[0].Name)
	}
	if refs[0].Relation != users {
		t.Error("expected relation to be the table")
	}
}

func TestCollectTablesFromAlias(t *testing.T) {
	u := nodes.NewTable("users").Alias("u")
	core := &nodes.SelectCore{From: u}

	refs := CollectTables(core)
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Name != "users" {
		t.Errorf("expected underlying name 'users', got %q", refs[0].Name)
	}
	if refs[0].Relation != u {
		t.Error("expected relation to be the alias")
	}
}

func TestCollectTablesIncludesJoins(t *testing.T) {
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	comments := nodes.NewTable("comments")
	core := &nodes.SelectCore{
		From: users,
		Joins: []*nodes.JoinNode{
			{Right: posts},
			{Right: comments},
		},
	}

	refs := CollectTables(core)
	if len(refs) != 3 {
		t.Fatalf("expected 3 refs, got %d", len(refs))
	}
	names := make([]string, len(refs))
	for i, r := range refs {
		names[i] = r.Name
	}
	if names[0] != "users" || names[1] != "posts" || names[2] != "comments" {
		t.Errorf("unexpected names: %v", names)
	}
}

func TestCollectTablesSkipsSubquery(t *testing.T) {
	users := nodes.NewTable("users")
	subquery := &nodes.SelectCore{From: nodes.NewTable("posts")}
	core := &nodes.SelectCore{
		From:  users,
		Joins: []*nodes.JoinNode{{Right: subquery}},
	}

	refs := CollectTables(core)
	// Should only find 'users', subquery is skipped
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref (subquery skipped), got %d", len(refs))
	}
	if refs[0].Name != "users" {
		t.Errorf("expected 'users', got %q", refs[0].Name)
	}
}

func TestCollectTablesNilFrom(t *testing.T) {
	core := &nodes.SelectCore{}

	refs := CollectTables(core)
	if len(refs) != 0 {
		t.Errorf("expected 0 refs, got %d", len(refs))
	}
}
