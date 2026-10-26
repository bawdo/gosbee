package managers

import (
	"errors"
	"reflect"
	"testing"

	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
	"github.com/bawdo/gosbee/visitors"
)

// tenantTransformer adds `tenant = <val>` to every SelectCore it sees.
type tenantTransformer struct {
	plugins.BaseTransformer
	val any
	err error
}

func (tt *tenantTransformer) TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error) {
	if tt.err != nil {
		return nil, tt.err
	}
	core.Wheres = append(core.Wheres, nodes.NewAttribute(core.From, "tenant").Eq(tt.val))
	return core, nil
}

func TestSetOperationManagerAppliesTransformers(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	op := NewSelectManager(users).Union(NewSelectManager(posts))
	m := NewSetOperationManager(op).Use(&tenantTransformer{val: "a"})

	got, _, err := m.ToSQL(visitors.NewPostgresVisitor(visitors.WithoutParams()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `(SELECT * FROM "users" WHERE "users"."tenant" = 'a') UNION (SELECT * FROM "posts" WHERE "posts"."tenant" = 'a')`
	if got != want {
		t.Errorf("\nwant: %s\n got: %s", want, got)
	}
}

func TestSetOperationManagerParamOrder(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	op := NewSelectManager(users).Union(NewSelectManager(posts))
	m := NewSetOperationManager(op).Use(&tenantTransformer{val: 7})

	sql, params, err := m.ToSQL(visitors.NewPostgresVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `(SELECT * FROM "users" WHERE "users"."tenant" = $1) UNION (SELECT * FROM "posts" WHERE "posts"."tenant" = $2)`
	if sql != want {
		t.Errorf("\nwant: %s\n got: %s", want, sql)
	}
	if !reflect.DeepEqual(params, []any{7, 7}) {
		t.Errorf("unexpected params: %v", params)
	}
}

func TestSetOperationManagerDoesNotMutateNode(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	op := NewSelectManager(users).Union(NewSelectManager(posts))
	m := NewSetOperationManager(op).Use(&tenantTransformer{val: "a"})
	v := visitors.NewPostgresVisitor(visitors.WithoutParams())

	first, _, _ := m.ToSQL(v)
	second, _, _ := m.ToSQL(v)
	if first != second {
		t.Errorf("repeat ToSQL differs:\n%s\n%s", first, second)
	}
}

func TestSetOperationManagerTransformerError(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	op := NewSelectManager(users).Union(NewSelectManager(posts))
	boom := errors.New("policy violation")
	m := NewSetOperationManager(op).Use(&tenantTransformer{err: boom})

	sql, params, err := m.ToSQL(visitors.NewPostgresVisitor(visitors.WithoutParams()))
	if !errors.Is(err, boom) {
		t.Fatalf("expected policy error, got %v", err)
	}
	if sql != "" || params != nil {
		t.Errorf("expected empty result, got %q %v", sql, params)
	}
}
