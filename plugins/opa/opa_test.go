package opa

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/visitors"
)

func toSQL(t *testing.T, core *nodes.SelectCore) string {
	t.Helper()
	return core.Accept(visitors.NewPostgresVisitor(visitors.WithoutParams()))
}

// --- Condition injection ---

func TestInjectsConditionsForTable(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users}

	policy := func(tableName string) ([]nodes.Node, error) {
		if tableName == "users" {
			return []nodes.Node{
				nodes.NewAttribute(nodes.NewTable("users"), "tenant_id").Eq(5),
			}, nil
		}
		return nil, nil
	}

	o := New(policy)
	result, err := o.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "users" WHERE "users"."tenant_id" = 5`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

// --- Multiple conditions from policy ---

func TestInjectsMultipleConditions(t *testing.T) {
	t.Parallel()
	posts := nodes.NewTable("posts")
	core := &nodes.SelectCore{From: posts}

	policy := func(tableName string) ([]nodes.Node, error) {
		if tableName == "posts" {
			t := nodes.NewTable("posts")
			return []nodes.Node{
				t.Col("tenant_id").Eq(5),
				t.Col("status").NotEq("draft"),
			}, nil
		}
		return nil, nil
	}

	o := New(policy)
	result, err := o.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "posts" WHERE "posts"."tenant_id" = 5 AND "posts"."status" != 'draft'`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

// --- Query rejection ---

func TestRejectsQueryOnPolicyError(t *testing.T) {
	t.Parallel()
	secrets := nodes.NewTable("secrets")
	core := &nodes.SelectCore{From: secrets}

	policy := func(tableName string) ([]nodes.Node, error) {
		if tableName == "secrets" {
			return nil, errors.New("access denied: table 'secrets' is restricted")
		}
		return nil, nil
	}

	o := New(policy)
	_, err := o.TransformSelect(core)
	if err == nil {
		t.Fatal("expected error from policy rejection")
	}
	if err.Error() != "access denied: table 'secrets' is restricted" {
		t.Errorf("unexpected error: %v", err)
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

	policy := func(tableName string) ([]nodes.Node, error) {
		return []nodes.Node{
			nodes.NewAttribute(nodes.NewTable(tableName), "tenant_id").Eq(1),
		}, nil
	}

	o := New(policy)
	result, err := o.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "users" WHERE "users"."active" = TRUE AND "users"."tenant_id" = 1`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

// --- Multi-table: policies applied per table ---

func TestPolicyAppliedToEachJoinedTable(t *testing.T) {
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

	policy := func(tableName string) ([]nodes.Node, error) {
		return []nodes.Node{
			nodes.NewAttribute(nodes.NewTable(tableName), "tenant_id").Eq(42),
		}, nil
	}

	o := New(policy)
	result, err := o.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id" WHERE "users"."tenant_id" = 42 AND "posts"."tenant_id" = 42`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

// --- Error on second table short-circuits ---

func TestErrorOnJoinedTableShortCircuits(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	secrets := nodes.NewTable("secrets")
	core := &nodes.SelectCore{
		From: users,
		Joins: []*nodes.JoinNode{
			{
				Left:  users,
				Right: secrets,
				Type:  nodes.InnerJoin,
				On:    users.Col("id").Eq(secrets.Col("user_id")),
			},
		},
	}

	policy := func(tableName string) ([]nodes.Node, error) {
		if tableName == "secrets" {
			return nil, errors.New("access denied")
		}
		return nil, nil
	}

	o := New(policy)
	_, err := o.TransformSelect(core)
	if err == nil {
		t.Fatal("expected error when joined table is restricted")
	}
}

// --- No conditions returned is no-op ---

func TestNilConditionsIsNoOp(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users}

	policy := func(tableName string) ([]nodes.Node, error) {
		return nil, nil
	}

	o := New(policy)
	result, err := o.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Wheres) != 0 {
		t.Errorf("expected no wheres, got %d", len(result.Wheres))
	}
}

// --- Implements Transformer interface ---

func TestImplementsTransformer(t *testing.T) {
	t.Parallel()
	var _ interface {
		TransformSelect(*nodes.SelectCore) (*nodes.SelectCore, error)
	} = New(func(string) ([]nodes.Node, error) { return nil, nil })
}

// --- NewFromServer: server-backed OPA ---

func TestNewFromServerTransform(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			var req map[string]any
			_ = json.NewDecoder(r.Body).Decode(&req)
			unknowns, _ := req["unknowns"].([]any)
			tableName := ""
			if len(unknowns) > 0 {
				s, _ := unknowns[0].(string)
				if len(s) > 5 {
					tableName = s[5:]
				}
			}
			if tableName == "users" {
				_, _ = w.Write([]byte(`{"result":{"queries":[[{"index":0,"terms":[{"type":"ref","value":[{"type":"var","value":"eq"}]},{"type":"ref","value":[{"type":"var","value":"data"},{"type":"string","value":"users"},{"type":"var","value":"$0"},{"type":"string","value":"tenant_id"}]},{"type":"number","value":42}]}]]}}`))
			} else {
				_, _ = w.Write([]byte(`{"result":{"queries":[[]]}}`))
			}
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	o := NewFromServer(srv.URL, "data.authz.allow", map[string]any{"subject": map[string]any{"tenant_id": 42}})
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users}
	result, err := o.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "users" WHERE "users"."tenant_id" = 42`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestNewFromServerDeny(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			// Empty result = access denied.
			_, _ = w.Write([]byte(`{"result":{}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	o := NewFromServer(srv.URL, "data.authz.allow", nil)
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users}
	_, err := o.TransformSelect(core)
	if err == nil {
		t.Fatal("expected error for deny response")
	}
}

func TestNewFromServerImplementsTransformer(t *testing.T) {
	t.Parallel()
	var _ interface {
		TransformSelect(*nodes.SelectCore) (*nodes.SelectCore, error)
	} = NewFromServer("http://localhost:8181", "data.authz.allow", nil)
}

// --- Masking: star expansion ---

func TestMaskExpandsStar(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{"result":{"queries":[[]]}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":"<MASKED>"}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	resolver := func(table string) ([]string, error) {
		if table == "consignments" {
			return []string{"id", "account_name", "billed_total"}, nil
		}
		return nil, fmt.Errorf("unknown table %q", table)
	}

	o := NewFromServer(srv.URL, "data.authz.allow", nil, WithColumnResolver(resolver))
	core := &nodes.SelectCore{From: nodes.NewTable("consignments")}
	result, err := o.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT "consignments"."id", "consignments"."account_name", '<MASKED>' AS "billed_total" FROM "consignments"`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestMaskReplacesExplicitProjection(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{"result":{"queries":[[]]}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":"<MASKED>"}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	o := NewFromServer(srv.URL, "data.authz.allow", nil)
	tbl := nodes.NewTable("consignments")
	core := &nodes.SelectCore{
		From:        tbl,
		Projections: []nodes.Node{tbl.Col("id"), tbl.Col("billed_total")},
	}
	result, err := o.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT "consignments"."id", '<MASKED>' AS "billed_total" FROM "consignments"`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestNoMasksLeavesProjectionsUntouched(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{"result":{"queries":[[]]}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	o := NewFromServer(srv.URL, "data.authz.allow", nil)
	core := &nodes.SelectCore{From: nodes.NewTable("users")}
	result, err := o.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT * FROM "users"`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestMaskStarWithNilResolverErrors(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{"result":{"queries":[[]]}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":"<MASKED>"}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	o := NewFromServer(srv.URL, "data.authz.allow", nil)
	core := &nodes.SelectCore{From: nodes.NewTable("consignments")}
	_, err := o.TransformSelect(core)
	if err == nil {
		t.Fatal("expected error for nil column resolver with masks and star")
	}
}

func TestPolicyFuncModeNoMasks(t *testing.T) {
	t.Parallel()
	policy := func(tableName string) ([]nodes.Node, error) {
		return nil, nil
	}
	o := New(policy)
	core := &nodes.SelectCore{From: nodes.NewTable("users")}
	result, err := o.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := toSQL(t, result)
	expected := `SELECT * FROM "users"`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestMaskedValuesNotParameterized(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{"result":{"queries":[[{"index":0,"terms":[{"type":"ref","value":[{"type":"var","value":"eq"}]},{"type":"ref","value":[{"type":"var","value":"data"},{"type":"string","value":"consignments"},{"type":"var","value":"$0"},{"type":"string","value":"account_name"}]},{"type":"string","value":"acme"}]}]]}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":"<MASKED>"}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	resolver := func(table string) ([]string, error) {
		return []string{"id", "account_name", "billed_total"}, nil
	}

	o := NewFromServer(srv.URL, "data.authz.allow", nil, WithColumnResolver(resolver))
	core := &nodes.SelectCore{From: nodes.NewTable("consignments")}
	result, err := o.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	v := visitors.NewPostgresVisitor()
	sql := result.Accept(v)
	params := v.Params()

	// The masked value '<MASKED>' should appear as a literal in SQL, not as $1.
	if !strings.Contains(sql, "'<MASKED>'") {
		t.Errorf("expected literal '<MASKED>' in SQL, got: %s", sql)
	}
	// Only the condition value 'acme' should be parameterized.
	if len(params) != 1 {
		t.Fatalf("expected 1 param (acme), got %d: %v", len(params), params)
	}
	if params[0] != "acme" {
		t.Errorf("expected param[0]='acme', got %v", params[0])
	}
}

func TestMaskAppliedPerJoinedTable(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{"result":{"queries":[[]]}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			// Masks are fetched once; return masks for orders only.
			_, _ = w.Write([]byte(`{"result":{"orders":{"total":{"replace":{"value":"***"}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	resolver := func(table string) ([]string, error) {
		switch table {
		case "orders":
			return []string{"id", "customer_id", "total"}, nil
		case "customers":
			return []string{"id", "name"}, nil
		}
		return nil, fmt.Errorf("unknown table %q", table)
	}

	o := NewFromServer(srv.URL, "data.authz.allow", nil, WithColumnResolver(resolver))
	orders := nodes.NewTable("orders")
	customers := nodes.NewTable("customers")
	core := &nodes.SelectCore{
		From: orders,
		Joins: []*nodes.JoinNode{{
			Left:  orders,
			Right: customers,
			Type:  nodes.InnerJoin,
			On:    orders.Col("customer_id").Eq(customers.Col("id")),
		}},
	}
	result, err := o.TransformSelect(core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := toSQL(t, result)
	expected := `SELECT "orders"."id", "orders"."customer_id", '***' AS "total", "customers"."id", "customers"."name" FROM "orders" INNER JOIN "customers" ON "orders"."customer_id" = "customers"."id"`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}
