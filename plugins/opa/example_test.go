package opa_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins/opa"
	"github.com/bawdo/gosbee/visitors"
)

// fakeOPA is an in-process stand-in for an OPA server, so the examples need
// no policy server and no network beyond the loopback listener httptest
// opens. It answers the two calls the plugin makes:
//
//   - POST /v1/compile, the partial evaluation of the row filter, with one
//     residual: tenant_id = 42.
//   - POST /v1/data/<package>/masks, the column masks, replacing
//     billed_total with "<MASKED>".
//
// A real deployment points NewFromServer at its OPA server instead.
func fakeOPA() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{"result":{"queries":[[{"index":0,"terms":[` +
				`{"type":"ref","value":[{"type":"var","value":"eq"}]},` +
				`{"type":"ref","value":[{"type":"var","value":"data"},{"type":"string","value":"consignments"},` +
				`{"type":"var","value":"$0"},{"type":"string","value":"tenant_id"}]},` +
				`{"type":"number","value":42}]}]]}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":"<MASKED>"}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

// In PolicyFunc mode a Go function plays the policy engine: it returns the
// conditions each table must satisfy, and the plugin appends them to WHERE.
func ExampleNew() {
	policy := func(table string) ([]nodes.Node, error) {
		if table == "orders" {
			return []nodes.Node{nodes.NewTable("orders").Col("tenant_id").Eq(42)}, nil
		}
		return nil, nil
	}

	orders := nodes.NewTable("orders")
	query := managers.NewSelectManager(orders).
		Select(orders.Col("id"), orders.Col("total")).
		Use(opa.New(policy))

	sql, params, err := query.ToSQL(visitors.NewPostgresVisitor())
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(sql)
	fmt.Println(params)
	// Output:
	// SELECT "orders"."id", "orders"."total" FROM "orders" WHERE "orders"."tenant_id" = $1
	// [42]
}

// In server mode the plugin asks OPA for both the row filter and the column
// masks. Masking a SELECT * needs the table's columns, which a
// ColumnResolver supplies: the star is expanded and the masked column is
// replaced by its mask value, bound as a parameter under the column's name.
func ExampleNewFromServer() {
	srv := fakeOPA()
	defer srv.Close()

	columns := func(table string) ([]string, error) {
		if table == "consignments" {
			return []string{"id", "account_name", "billed_total"}, nil
		}
		return nil, fmt.Errorf("unknown table %q", table)
	}

	plugin := opa.NewFromServer(srv.URL, "data.policies.consignments.include",
		map[string]any{"subject": map[string]any{"tenant_id": 42}},
		opa.WithColumnResolver(columns))

	query := managers.NewSelectManager(nodes.NewTable("consignments")).
		Use(plugin)

	sql, params, err := query.ToSQL(visitors.NewPostgresVisitor())
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(sql)
	fmt.Println(params)
	// Output:
	// SELECT "consignments"."id", "consignments"."account_name", $1 AS "billed_total" FROM "consignments" WHERE "consignments"."tenant_id" = $2
	// [<MASKED> 42]
}

// A masked column can only be replaced where it is projected bare. Anywhere
// else, here a WHERE condition, replacing it would change what the query
// means, so the query is refused with ErrMaskedColumnUse rather than
// rendered unmasked.
func ExampleNewFromServer_maskedColumnRefused() {
	srv := fakeOPA()
	defer srv.Close()

	plugin := opa.NewFromServer(srv.URL, "data.policies.consignments.include", nil)

	consignments := nodes.NewTable("consignments")
	query := managers.NewSelectManager(consignments).
		Select(consignments.Col("id")).
		Where(consignments.Col("billed_total").Gt(1000)).
		Use(plugin)

	_, _, err := query.ToSQL(visitors.NewPostgresVisitor())
	fmt.Println(errors.Is(err, opa.ErrMaskedColumnUse))
	// Output:
	// true
}
