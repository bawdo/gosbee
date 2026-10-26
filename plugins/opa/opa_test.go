package opa

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
	"github.com/bawdo/gosbee/plugins/softdelete"
	"github.com/bawdo/gosbee/visitors"
)

var (
	_ plugins.ScopedTransformer = (*OPA)(nil)
	_ plugins.StrictTransformer = (*OPA)(nil)
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

func TestMaskedValuesParameterized(t *testing.T) {
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

	// The masked value is a bind parameter, projected before the WHERE clause.
	if !strings.Contains(sql, `$1 AS "billed_total"`) {
		t.Errorf("expected $1 AS \"billed_total\" in SQL, got: %s", sql)
	}
	if len(params) != 2 {
		t.Fatalf("expected 2 params (<MASKED>, acme), got %d: %v", len(params), params)
	}
	if params[0] != "<MASKED>" || params[1] != "acme" {
		t.Errorf("expected params [<MASKED> acme], got %v", params)
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

// --- Nested queries, CTEs, raw SQL, memoisation ---

func pgVisitor() *visitors.PostgresVisitor {
	return visitors.NewPostgresVisitor(visitors.WithoutParams())
}

// tenantPolicy returns a PolicyFunc that restricts every table to tenant_id = 1
// and records each table name it is asked about.
func tenantPolicy(calls *[]string) PolicyFunc {
	return func(name string) ([]nodes.Node, error) {
		*calls = append(*calls, name)
		return []nodes.Node{nodes.NewTable(name).Col("tenant_id").Eq(1)}, nil
	}
}

func TestOPAFiltersSubqueryAndJoinedSubquery(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	orders := nodes.NewTable("orders")
	sub := managers.NewSelectManager(posts).Select(posts.Col("user_id"))
	joined := managers.NewSelectManager(orders).As("o")
	var calls []string
	q := managers.NewSelectManager(users).
		Where(&nodes.InNode{Expr: users.Col("id"), Vals: []nodes.Node{sub.Core}}).
		Join(joined).On(users.Col("id").Eq(joined.Col("user_id"))).
		Use(New(tenantPolicy(&calls)))

	got, _, err := q.ToSQL(pgVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{
		`(SELECT "posts"."user_id" FROM "posts" WHERE "posts"."tenant_id" = 1)`,
		`(SELECT * FROM "orders" WHERE "orders"."tenant_id" = 1) AS "o"`,
		`"users"."tenant_id" = 1`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("expected %s in:\n  %s", want, got)
		}
	}
	if strings.Contains(got, `"o"."tenant_id"`) {
		t.Errorf("derived table alias was policed as a table in:\n  %s", got)
	}
	for _, name := range calls {
		if name == "o" {
			t.Errorf("policy was called with the derived table alias; calls: %v", calls)
		}
	}
}

func TestOPASkipsCTEName(t *testing.T) {
	t.Parallel()
	posts := nodes.NewTable("posts")
	body := managers.NewSelectManager(posts)
	var calls []string
	q := managers.NewSelectManager(nodes.NewTable("c")).
		With("c", body.Core).
		Use(New(tenantPolicy(&calls)))

	if _, _, err := q.ToSQL(pgVisitor()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, name := range calls {
		if name == "c" {
			t.Errorf("policy was called with the CTE name; calls: %v", calls)
		}
	}
	if len(calls) != 1 || calls[0] != "posts" {
		t.Errorf("expected one call for posts, got %v", calls)
	}
}

func TestOPARefusesRawSQL(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	var calls []string
	q := managers.NewSelectManager(users).
		StringJoin(nodes.RawSQL("INNER JOIN secrets ON 1 = 1")).
		Use(New(tenantPolicy(&calls)))

	_, _, err := q.ToSQL(pgVisitor())
	if !errors.Is(err, plugins.ErrUnpoliceable) {
		t.Fatalf("expected ErrUnpoliceable, got %v", err)
	}
}

func TestOPACompilesEachTableOncePerWalk(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sub1 := managers.NewSelectManager(users).Select(users.Col("id"))
	sub2 := managers.NewSelectManager(users).Select(users.Col("id"))
	var calls []string
	q := managers.NewSelectManager(users).
		Where(&nodes.InNode{Expr: users.Col("id"), Vals: []nodes.Node{sub1.Core}}).
		Where(nodes.Exists(sub2)).
		Use(New(tenantPolicy(&calls)))

	if _, _, err := q.ToSQL(pgVisitor()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(calls) != 1 {
		t.Errorf("expected 1 policy call per walk, got %d: %v", len(calls), calls)
	}
	if _, _, err := q.ToSQL(pgVisitor()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(calls) != 2 {
		t.Errorf("expected 2 policy calls after a second ToSQL, got %d: %v", len(calls), calls)
	}
}

// --- Masks policed across the tree ---

// allowAll is a PolicyFunc that adds no conditions to any table.
func allowAll(string) ([]nodes.Node, error) { return nil, nil }

// maskedSSN returns an OPA transformer that masks users.ssn with "XXX",
// injected through the test-only masks override so no server is needed.
func maskedSSN() *OPA {
	o := New(allowAll)
	o.masks = map[string]map[string]MaskAction{
		"users": {"ssn": {Replace: &ReplaceAction{Value: "XXX"}}},
	}
	return o
}

func TestMaskReplacesBareProjectionWithAlias(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	q := managers.NewSelectManager(users).Select(users.Col("ssn")).Use(maskedSSN())

	got, params, err := q.ToSQL(visitors.NewPostgresVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := `SELECT $1 AS "ssn" FROM "users"`; got != want {
		t.Errorf("expected:\n  %s\ngot:\n  %s", want, got)
	}
	if len(params) != 1 || params[0] != "XXX" {
		t.Errorf("expected params [XXX], got %v", params)
	}

	got, _, err = q.ToSQL(visitors.NewMySQLVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "SELECT ? AS `ssn` FROM `users`"; got != want {
		t.Errorf("expected:\n  %s\ngot:\n  %s", want, got)
	}
}

func TestMaskReplacesBareProjectionInSubquery(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	sub := managers.NewSelectManager(users).Select(users.Col("ssn"))
	q := managers.NewSelectManager(posts).
		Select(posts.Col("id")).
		Where(posts.Col("author_ssn").In(sub.Core)).
		Use(maskedSSN())

	got, params, err := q.ToSQL(visitors.NewPostgresVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `SELECT "posts"."id" FROM "posts" WHERE "posts"."author_ssn" IN (SELECT $1 AS "ssn" FROM "users")`
	if got != want {
		t.Errorf("expected:\n  %s\ngot:\n  %s", want, got)
	}
	if len(params) != 1 || params[0] != "XXX" {
		t.Errorf("expected params [XXX], got %v", params)
	}
}

func TestMaskRefusesUseOutsideProjection(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	cases := map[string]func() *managers.SelectManager{
		"alias": func() *managers.SelectManager {
			return managers.NewSelectManager(users).Select(users.Col("ssn").As("x"))
		},
		"function": func() *managers.SelectManager {
			return managers.NewSelectManager(users).Select(nodes.Lower(users.Col("ssn")))
		},
		"where": func() *managers.SelectManager {
			return managers.NewSelectManager(users).Select(users.Col("id")).Where(users.Col("ssn").Eq(1))
		},
		"order by": func() *managers.SelectManager {
			return managers.NewSelectManager(users).Select(users.Col("id")).Order(users.Col("ssn").Asc())
		},
		"group by": func() *managers.SelectManager {
			return managers.NewSelectManager(users).Select(users.Col("id")).Group(users.Col("ssn"))
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, _, err := build().Use(maskedSSN()).ToSQL(pgVisitor())
			if !errors.Is(err, ErrMaskedColumnUse) {
				t.Fatalf("expected ErrMaskedColumnUse, got %v (sql %q)", err, got)
			}
			if got != "" {
				t.Errorf("expected no SQL on refusal, got %q", got)
			}
		})
	}
}

func TestMaskAllowsPolicyFilterOnMaskedColumn(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	o := maskedSSN()
	o.evalPolicy = func(name string) ([]nodes.Node, error) {
		return []nodes.Node{nodes.NewTable(name).Col("ssn").NotEq("")}, nil
	}
	q := managers.NewSelectManager(users).Select(users.Col("id")).Use(o)

	got, _, err := q.ToSQL(pgVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := `SELECT "users"."id" FROM "users" WHERE "users"."ssn" != ''`; got != want {
		t.Errorf("expected:\n  %s\ngot:\n  %s", want, got)
	}
}

func TestMaskOutputPassesStrictTransformer(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	q := managers.NewSelectManager(users).
		Select(users.Col("id"), users.Col("ssn")).
		Use(maskedSSN()).
		Use(softdelete.New())

	got, _, err := q.ToSQL(pgVisitor())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, `'XXX' AS "ssn"`) {
		t.Errorf("expected masked projection in:\n  %s", got)
	}
}

func TestOPANestedPolicyDenialFailsQuery(t *testing.T) {
	t.Parallel()
	errDenied := errors.New("access denied to posts")
	policy := func(name string) ([]nodes.Node, error) {
		if name == "posts" {
			return nil, errDenied
		}
		return nil, nil
	}
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	sub := managers.NewSelectManager(posts).Select(posts.Col("user_id"))
	q := managers.NewSelectManager(users).
		Where(users.Col("id").In(sub.Core)).
		Use(New(policy))

	got, _, err := q.ToSQL(pgVisitor())
	if !errors.Is(err, errDenied) {
		t.Fatalf("expected the policy error, got %v", err)
	}
	if got != "" {
		t.Errorf("expected no SQL on denial, got %q", got)
	}
}
