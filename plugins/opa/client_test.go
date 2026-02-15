package opa

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/visitors"
)

func toClientSQL(t *testing.T, n nodes.Node) string {
	t.Helper()
	return n.Accept(visitors.NewPostgresVisitor(visitors.WithoutParams()))
}

// --- Task 1: Compile response parsing ---

func TestCompileResponseParsesTerms(t *testing.T) {
	jsonBody := `{
		"result": {
			"queries": [[{
				"index": 0,
				"terms": [
					{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
					{"type": "ref", "value": [
						{"type": "var", "value": "data"},
						{"type": "string", "value": "users"},
						{"type": "var", "value": "$0"},
						{"type": "string", "value": "tenant_id"}
					]},
					{"type": "number", "value": 42}
				]
			}]]
		}
	}`
	resp, err := parseCompileResponse([]byte(jsonBody))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(resp.Result.Queries) != 1 {
		t.Fatalf("expected 1 query, got %d", len(resp.Result.Queries))
	}
	if len(resp.Result.Queries[0]) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(resp.Result.Queries[0]))
	}
	expr := resp.Result.Queries[0][0]
	if len(expr.Terms) != 3 {
		t.Fatalf("expected 3 terms, got %d", len(expr.Terms))
	}
}

func TestCompileTermString(t *testing.T) {
	jsonBody := `{
		"result": {
			"queries": [[{
				"index": 0,
				"terms": [
					{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
					{"type": "ref", "value": [
						{"type": "var", "value": "data"},
						{"type": "string", "value": "users"},
						{"type": "var", "value": "$0"},
						{"type": "string", "value": "name"}
					]},
					{"type": "string", "value": "Alice"}
				]
			}]]
		}
	}`
	resp, err := parseCompileResponse([]byte(jsonBody))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	term := resp.Result.Queries[0][0].Terms[2]
	if term.Type != "string" {
		t.Errorf("expected type string, got %s", term.Type)
	}
	if s, ok := term.Value.(string); !ok || s != "Alice" {
		t.Errorf("expected value Alice, got %v", term.Value)
	}
}

func TestCompileTermNumber(t *testing.T) {
	jsonBody := `{
		"result": {
			"queries": [[{
				"index": 0,
				"terms": [
					{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
					{"type": "ref", "value": [
						{"type": "var", "value": "data"},
						{"type": "string", "value": "users"},
						{"type": "var", "value": "$0"},
						{"type": "string", "value": "id"}
					]},
					{"type": "number", "value": 42}
				]
			}]]
		}
	}`
	resp, err := parseCompileResponse([]byte(jsonBody))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	term := resp.Result.Queries[0][0].Terms[2]
	if term.Type != "number" {
		t.Errorf("expected type number, got %s", term.Type)
	}
	if v, ok := term.Value.(int); !ok || v != 42 {
		t.Errorf("expected int 42, got %v (%T)", term.Value, term.Value)
	}
}

func TestCompileTermNumberFloat(t *testing.T) {
	jsonBody := `{
		"result": {
			"queries": [[{
				"index": 0,
				"terms": [
					{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
					{"type": "ref", "value": [
						{"type": "var", "value": "data"},
						{"type": "string", "value": "users"},
						{"type": "var", "value": "$0"},
						{"type": "string", "value": "score"}
					]},
					{"type": "number", "value": 3.14}
				]
			}]]
		}
	}`
	resp, err := parseCompileResponse([]byte(jsonBody))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	term := resp.Result.Queries[0][0].Terms[2]
	if term.Type != "number" {
		t.Errorf("expected type number, got %s", term.Type)
	}
	if v, ok := term.Value.(float64); !ok || v != 3.14 {
		t.Errorf("expected float64 3.14, got %v (%T)", term.Value, term.Value)
	}
}

func TestCompileTermBoolean(t *testing.T) {
	jsonBody := `{
		"result": {
			"queries": [[{
				"index": 0,
				"terms": [
					{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
					{"type": "ref", "value": [
						{"type": "var", "value": "data"},
						{"type": "string", "value": "users"},
						{"type": "var", "value": "$0"},
						{"type": "string", "value": "active"}
					]},
					{"type": "boolean", "value": true}
				]
			}]]
		}
	}`
	resp, err := parseCompileResponse([]byte(jsonBody))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	term := resp.Result.Queries[0][0].Terms[2]
	if term.Type != "boolean" {
		t.Errorf("expected type boolean, got %s", term.Type)
	}
	if v, ok := term.Value.(bool); !ok || v != true {
		t.Errorf("expected true, got %v (%T)", term.Value, term.Value)
	}
}

func TestCompileTermVar(t *testing.T) {
	jsonBody := `{
		"result": {
			"queries": [[{
				"index": 0,
				"terms": [
					{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
					{"type": "ref", "value": [
						{"type": "var", "value": "data"},
						{"type": "string", "value": "users"},
						{"type": "var", "value": "$0"},
						{"type": "string", "value": "id"}
					]},
					{"type": "var", "value": "somevar"}
				]
			}]]
		}
	}`
	resp, err := parseCompileResponse([]byte(jsonBody))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	term := resp.Result.Queries[0][0].Terms[2]
	if term.Type != "var" {
		t.Errorf("expected type var, got %s", term.Type)
	}
	if s, ok := term.Value.(string); !ok || s != "somevar" {
		t.Errorf("expected somevar, got %v", term.Value)
	}
}

func TestCompileTermRef(t *testing.T) {
	jsonBody := `{
		"result": {
			"queries": [[{
				"index": 0,
				"terms": [
					{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
					{"type": "ref", "value": [
						{"type": "var", "value": "data"},
						{"type": "string", "value": "users"},
						{"type": "var", "value": "$0"},
						{"type": "string", "value": "tenant_id"}
					]},
					{"type": "number", "value": 1}
				]
			}]]
		}
	}`
	resp, err := parseCompileResponse([]byte(jsonBody))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	refTerm := resp.Result.Queries[0][0].Terms[1]
	if refTerm.Type != "ref" {
		t.Fatalf("expected type ref, got %s", refTerm.Type)
	}
	parts, ok := refTerm.Value.([]compileTerm)
	if !ok {
		t.Fatalf("expected []compileTerm, got %T", refTerm.Value)
	}
	if len(parts) != 4 {
		t.Fatalf("expected 4 ref parts, got %d", len(parts))
	}
	if parts[0].Type != "var" || parts[0].Value != "data" {
		t.Errorf("ref[0]: expected var/data, got %s/%v", parts[0].Type, parts[0].Value)
	}
	if parts[3].Type != "string" || parts[3].Value != "tenant_id" {
		t.Errorf("ref[3]: expected string/tenant_id, got %s/%v", parts[3].Type, parts[3].Value)
	}
}

// --- Mask parsing ---

func TestParseMasksReplace(t *testing.T) {
	jsonBody := `{
		"result": {
			"queries": [[]],
			"masks": {
				"consignments": {
					"billed_total": {
						"replace": {"value": "<MASKED>"}
					}
				}
			}
		}
	}`
	resp, err := parseCompileResponse([]byte(jsonBody))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	masks := resp.Result.Masks
	if masks == nil {
		t.Fatal("expected masks to be populated")
	}
	tbl, ok := masks["consignments"]
	if !ok {
		t.Fatal("expected consignments table in masks")
	}
	col, ok := tbl["billed_total"]
	if !ok {
		t.Fatal("expected billed_total column in masks")
	}
	if col.Replace == nil {
		t.Fatal("expected replace action")
	}
	if col.Replace.Value != "<MASKED>" {
		t.Errorf("expected '<MASKED>', got %q", col.Replace.Value)
	}
}

func TestParseMasksEmptyObjectMeansNoMask(t *testing.T) {
	jsonBody := `{
		"result": {
			"queries": [[]],
			"masks": {
				"consignments": {
					"billed_total": {}
				}
			}
		}
	}`
	resp, err := parseCompileResponse([]byte(jsonBody))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	col := resp.Result.Masks["consignments"]["billed_total"]
	if col.Replace != nil {
		t.Errorf("expected nil Replace for empty object, got %+v", col.Replace)
	}
}

func TestParseMasksAbsent(t *testing.T) {
	jsonBody := `{
		"result": {
			"queries": [[]]
		}
	}`
	resp, err := parseCompileResponse([]byte(jsonBody))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if resp.Result.Masks != nil {
		t.Errorf("expected nil masks, got %v", resp.Result.Masks)
	}
}

func TestParseMasksMultipleColumns(t *testing.T) {
	jsonBody := `{
		"result": {
			"queries": [[]],
			"masks": {
				"consignments": {
					"billed_total": {"replace": {"value": "<MASKED>"}},
					"cost_price": {"replace": {"value": "0.00"}}
				}
			}
		}
	}`
	resp, err := parseCompileResponse([]byte(jsonBody))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	masks := resp.Result.Masks
	if len(masks["consignments"]) != 2 {
		t.Fatalf("expected 2 masked columns, got %d", len(masks["consignments"]))
	}
	if masks["consignments"]["cost_price"].Replace.Value != "0.00" {
		t.Errorf("expected '0.00', got %q", masks["consignments"]["cost_price"].Replace.Value)
	}
}

// --- Task 2: Translate single expressions ---

func TestTranslateEq(t *testing.T) {
	table := nodes.NewTable("users")
	expr := compileExpression{
		Terms: []compileTerm{
			{Type: "ref", Value: []compileTerm{{Type: "var", Value: "eq"}}},
			{Type: "ref", Value: []compileTerm{
				{Type: "var", Value: "data"},
				{Type: "string", Value: "users"},
				{Type: "var", Value: "$0"},
				{Type: "string", Value: "tenant_id"},
			}},
			{Type: "number", Value: 42},
		},
	}
	node, err := translateExpression(expr, table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := toClientSQL(t, node)
	expected := `"users"."tenant_id" = 42`
	if got != expected {
		t.Errorf("expected %s, got %s", expected, got)
	}
}

func TestTranslateEqSwappedOperands(t *testing.T) {
	table := nodes.NewTable("consignments")
	// OPA may return the value in Terms[1] and the data ref in Terms[2].
	expr := compileExpression{
		Terms: []compileTerm{
			{Type: "ref", Value: []compileTerm{{Type: "var", Value: "eq"}}},
			{Type: "string", Value: "reader"},
			{Type: "ref", Value: []compileTerm{
				{Type: "var", Value: "data"},
				{Type: "string", Value: "consignments"},
				{Type: "var", Value: "$0"},
				{Type: "string", Value: "origin"},
			}},
		},
	}
	node, err := translateExpression(expr, table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := toClientSQL(t, node)
	expected := `"consignments"."origin" = 'reader'`
	if got != expected {
		t.Errorf("expected %s, got %s", expected, got)
	}
}

func TestTranslateNeq(t *testing.T) {
	table := nodes.NewTable("users")
	expr := compileExpression{
		Terms: []compileTerm{
			{Type: "ref", Value: []compileTerm{{Type: "var", Value: "neq"}}},
			{Type: "ref", Value: []compileTerm{
				{Type: "var", Value: "data"},
				{Type: "string", Value: "users"},
				{Type: "var", Value: "$0"},
				{Type: "string", Value: "status"},
			}},
			{Type: "string", Value: "banned"},
		},
	}
	node, err := translateExpression(expr, table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := toClientSQL(t, node)
	expected := `"users"."status" != 'banned'`
	if got != expected {
		t.Errorf("expected %s, got %s", expected, got)
	}
}

func TestTranslateLt(t *testing.T) {
	table := nodes.NewTable("users")
	expr := compileExpression{
		Terms: []compileTerm{
			{Type: "ref", Value: []compileTerm{{Type: "var", Value: "lt"}}},
			{Type: "ref", Value: []compileTerm{
				{Type: "var", Value: "data"},
				{Type: "string", Value: "users"},
				{Type: "var", Value: "$0"},
				{Type: "string", Value: "age"},
			}},
			{Type: "number", Value: 18},
		},
	}
	node, err := translateExpression(expr, table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := toClientSQL(t, node)
	expected := `"users"."age" < 18`
	if got != expected {
		t.Errorf("expected %s, got %s", expected, got)
	}
}

func TestTranslateLte(t *testing.T) {
	table := nodes.NewTable("users")
	expr := compileExpression{
		Terms: []compileTerm{
			{Type: "ref", Value: []compileTerm{{Type: "var", Value: "lte"}}},
			{Type: "ref", Value: []compileTerm{
				{Type: "var", Value: "data"},
				{Type: "string", Value: "users"},
				{Type: "var", Value: "$0"},
				{Type: "string", Value: "age"},
			}},
			{Type: "number", Value: 65},
		},
	}
	node, err := translateExpression(expr, table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := toClientSQL(t, node)
	expected := `"users"."age" <= 65`
	if got != expected {
		t.Errorf("expected %s, got %s", expected, got)
	}
}

func TestTranslateGt(t *testing.T) {
	table := nodes.NewTable("users")
	expr := compileExpression{
		Terms: []compileTerm{
			{Type: "ref", Value: []compileTerm{{Type: "var", Value: "gt"}}},
			{Type: "ref", Value: []compileTerm{
				{Type: "var", Value: "data"},
				{Type: "string", Value: "users"},
				{Type: "var", Value: "$0"},
				{Type: "string", Value: "score"},
			}},
			{Type: "number", Value: 100},
		},
	}
	node, err := translateExpression(expr, table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := toClientSQL(t, node)
	expected := `"users"."score" > 100`
	if got != expected {
		t.Errorf("expected %s, got %s", expected, got)
	}
}

func TestTranslateGte(t *testing.T) {
	table := nodes.NewTable("users")
	expr := compileExpression{
		Terms: []compileTerm{
			{Type: "ref", Value: []compileTerm{{Type: "var", Value: "gte"}}},
			{Type: "ref", Value: []compileTerm{
				{Type: "var", Value: "data"},
				{Type: "string", Value: "users"},
				{Type: "var", Value: "$0"},
				{Type: "string", Value: "score"},
			}},
			{Type: "number", Value: 50},
		},
	}
	node, err := translateExpression(expr, table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := toClientSQL(t, node)
	expected := `"users"."score" >= 50`
	if got != expected {
		t.Errorf("expected %s, got %s", expected, got)
	}
}

// --- Task 3: String operations ---

func TestTranslateStartsWith(t *testing.T) {
	table := nodes.NewTable("users")
	expr := compileExpression{
		Terms: []compileTerm{
			{Type: "ref", Value: []compileTerm{{Type: "var", Value: "startswith"}}},
			{Type: "ref", Value: []compileTerm{
				{Type: "var", Value: "data"},
				{Type: "string", Value: "users"},
				{Type: "var", Value: "$0"},
				{Type: "string", Value: "name"},
			}},
			{Type: "string", Value: "Jo"},
		},
	}
	node, err := translateExpression(expr, table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := toClientSQL(t, node)
	expected := `"users"."name" LIKE 'Jo%'`
	if got != expected {
		t.Errorf("expected %s, got %s", expected, got)
	}
}

func TestTranslateEndsWith(t *testing.T) {
	table := nodes.NewTable("users")
	expr := compileExpression{
		Terms: []compileTerm{
			{Type: "ref", Value: []compileTerm{{Type: "var", Value: "endswith"}}},
			{Type: "ref", Value: []compileTerm{
				{Type: "var", Value: "data"},
				{Type: "string", Value: "users"},
				{Type: "var", Value: "$0"},
				{Type: "string", Value: "email"},
			}},
			{Type: "string", Value: "@acme.com"},
		},
	}
	node, err := translateExpression(expr, table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := toClientSQL(t, node)
	expected := `"users"."email" LIKE '%@acme.com'`
	if got != expected {
		t.Errorf("expected %s, got %s", expected, got)
	}
}

func TestTranslateContains(t *testing.T) {
	table := nodes.NewTable("users")
	expr := compileExpression{
		Terms: []compileTerm{
			{Type: "ref", Value: []compileTerm{{Type: "var", Value: "contains"}}},
			{Type: "ref", Value: []compileTerm{
				{Type: "var", Value: "data"},
				{Type: "string", Value: "users"},
				{Type: "var", Value: "$0"},
				{Type: "string", Value: "role"},
			}},
			{Type: "string", Value: "engineer"},
		},
	}
	node, err := translateExpression(expr, table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := toClientSQL(t, node)
	expected := `"users"."role" LIKE '%engineer%'`
	if got != expected {
		t.Errorf("expected %s, got %s", expected, got)
	}
}

func TestTranslateStartsWithRejectsNonString(t *testing.T) {
	table := nodes.NewTable("users")
	expr := compileExpression{
		Terms: []compileTerm{
			{Type: "ref", Value: []compileTerm{{Type: "var", Value: "startswith"}}},
			{Type: "ref", Value: []compileTerm{
				{Type: "var", Value: "data"},
				{Type: "string", Value: "users"},
				{Type: "var", Value: "$0"},
				{Type: "string", Value: "name"},
			}},
			{Type: "number", Value: 42},
		},
	}
	_, err := translateExpression(expr, table)
	if err == nil {
		t.Fatal("expected error for non-string value in startswith")
	}
}

// --- Task 4: Translate full query sets ---

func TestTranslateQueriesSingleAnd(t *testing.T) {
	table := nodes.NewTable("users")
	queries := [][]compileExpression{{
		{Terms: []compileTerm{
			{Type: "ref", Value: []compileTerm{{Type: "var", Value: "eq"}}},
			{Type: "ref", Value: []compileTerm{
				{Type: "var", Value: "data"},
				{Type: "string", Value: "users"},
				{Type: "var", Value: "$0"},
				{Type: "string", Value: "tenant_id"},
			}},
			{Type: "number", Value: 42},
		}},
		{Terms: []compileTerm{
			{Type: "ref", Value: []compileTerm{{Type: "var", Value: "eq"}}},
			{Type: "ref", Value: []compileTerm{
				{Type: "var", Value: "data"},
				{Type: "string", Value: "users"},
				{Type: "var", Value: "$0"},
				{Type: "string", Value: "active"},
			}},
			{Type: "boolean", Value: true},
		}},
	}}
	conditions, err := translateQueries(queries, table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(conditions) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(conditions))
	}
	got0 := toClientSQL(t, conditions[0])
	if got0 != `"users"."tenant_id" = 42` {
		t.Errorf("condition[0]: expected \"users\".\"tenant_id\" = 42, got %s", got0)
	}
	got1 := toClientSQL(t, conditions[1])
	if got1 != `"users"."active" = TRUE` {
		t.Errorf("condition[1]: expected \"users\".\"active\" = TRUE, got %s", got1)
	}
}

func TestTranslateQueriesMultipleOrGroups(t *testing.T) {
	table := nodes.NewTable("users")
	queries := [][]compileExpression{
		// Query 0: tenant_id = 1 AND active = true
		{
			{Terms: []compileTerm{
				{Type: "ref", Value: []compileTerm{{Type: "var", Value: "eq"}}},
				{Type: "ref", Value: []compileTerm{
					{Type: "var", Value: "data"},
					{Type: "string", Value: "users"},
					{Type: "var", Value: "$0"},
					{Type: "string", Value: "tenant_id"},
				}},
				{Type: "number", Value: 1},
			}},
			{Terms: []compileTerm{
				{Type: "ref", Value: []compileTerm{{Type: "var", Value: "eq"}}},
				{Type: "ref", Value: []compileTerm{
					{Type: "var", Value: "data"},
					{Type: "string", Value: "users"},
					{Type: "var", Value: "$0"},
					{Type: "string", Value: "active"},
				}},
				{Type: "boolean", Value: true},
			}},
		},
		// Query 1: tenant_id = 2
		{
			{Terms: []compileTerm{
				{Type: "ref", Value: []compileTerm{{Type: "var", Value: "eq"}}},
				{Type: "ref", Value: []compileTerm{
					{Type: "var", Value: "data"},
					{Type: "string", Value: "users"},
					{Type: "var", Value: "$0"},
					{Type: "string", Value: "tenant_id"},
				}},
				{Type: "number", Value: 2},
			}},
		},
	}
	conditions, err := translateQueries(queries, table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(conditions) != 1 {
		t.Fatalf("expected 1 condition (OR group), got %d", len(conditions))
	}
	got := toClientSQL(t, conditions[0])
	expected := `("users"."tenant_id" = 1 AND "users"."active" = TRUE OR "users"."tenant_id" = 2)`
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestTranslateQueriesUnconditionalAllow(t *testing.T) {
	table := nodes.NewTable("users")
	queries := [][]compileExpression{{}} // empty inner = unconditional allow
	conditions, err := translateQueries(queries, table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conditions != nil {
		t.Errorf("expected nil conditions for unconditional allow, got %v", conditions)
	}
}

func TestTranslateQueriesEmptyDeny(t *testing.T) {
	table := nodes.NewTable("users")

	// nil queries
	_, err := translateQueries(nil, table)
	if err == nil {
		t.Fatal("expected error for nil queries")
	}

	// empty queries
	_, err = translateQueries([][]compileExpression{}, table)
	if err == nil {
		t.Fatal("expected error for empty queries")
	}
}

// --- Task 5: HTTP Client Compile method ---

func TestClientCompileSuccess(t *testing.T) {
	respBody := `{
		"result": {
			"queries": [[{
				"index": 0,
				"terms": [
					{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
					{"type": "ref", "value": [
						{"type": "var", "value": "data"},
						{"type": "string", "value": "users"},
						{"type": "var", "value": "$0"},
						{"type": "string", "value": "tenant_id"}
					]},
					{"type": "number", "value": 42}
				]
			}]]
		}
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/compile" {
			t.Errorf("expected path /v1/compile, got %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		// Verify request body structure.
		body, _ := io.ReadAll(r.Body)
		var req compileRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("failed to parse request body: %v", err)
		}
		if req.Query != "data.app.allow == true" {
			t.Errorf("expected query 'data.app.allow == true', got %q", req.Query)
		}
		if len(req.Unknowns) != 1 || req.Unknowns[0] != "data.users" {
			t.Errorf("expected unknowns [data.users], got %v", req.Unknowns)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(respBody))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "data.app.allow", map[string]any{"subject": map[string]any{"role": "admin"}})
	conditions, err := client.Compile("users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(conditions))
	}
	got := toClientSQL(t, conditions[0])
	expected := `"users"."tenant_id" = 42`
	if got != expected {
		t.Errorf("expected %s, got %s", expected, got)
	}
}

func TestClientCompileDeny(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result": {}}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "data.app.allow", map[string]any{})
	_, err := client.Compile("users")
	if err == nil {
		t.Fatal("expected error for deny response")
	}
	if !strings.Contains(err.Error(), "access denied") {
		t.Errorf("expected 'access denied' in error, got: %v", err)
	}
}

func TestClientCompileUnconditionalAllow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result": {"queries": [[]]}}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "data.app.allow", map[string]any{})
	conditions, err := client.Compile("users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conditions != nil {
		t.Errorf("expected nil conditions for unconditional allow, got %v", conditions)
	}
}

func TestClientCompileServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code": "internal_error", "message": "something broke"}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "data.app.allow", map[string]any{})
	_, err := client.Compile("users")
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("expected status code 500 in error, got: %v", err)
	}
}

func TestClientCompileUnreachable(t *testing.T) {
	client := NewClient("http://127.0.0.1:19999", "data.app.allow", map[string]any{})
	_, err := client.Compile("users")
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
}

func TestClientCompileNormalizesDataPrefix(t *testing.T) {
	respBody := `{
		"result": {
			"queries": [[{
				"index": 0,
				"terms": [
					{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
					{"type": "ref", "value": [
						{"type": "var", "value": "data"},
						{"type": "string", "value": "users"},
						{"type": "var", "value": "$0"},
						{"type": "string", "value": "tenant_id"}
					]},
					{"type": "number", "value": 1}
				]
			}]]
		}
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req compileRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("failed to parse request body: %v", err)
		}
		// Path without data. prefix should be normalized.
		if req.Query != "data.policies.filtering.allow == true" {
			t.Errorf("expected query 'data.policies.filtering.allow == true', got %q", req.Query)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(respBody))
	}))
	defer srv.Close()

	// Pass path WITHOUT data. prefix.
	client := NewClient(srv.URL, "policies.filtering.allow", map[string]any{})
	_, err := client.Compile("users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- Task 6: DiscoverInputs ---

func TestDiscoverInputs(t *testing.T) {
	// OPA response with refs to input.subject.role and input.subject.tenant_id.
	respBody := `{
		"result": {
			"queries": [[
				{
					"index": 0,
					"terms": [
						{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
						{"type": "ref", "value": [
							{"type": "var", "value": "input"},
							{"type": "string", "value": "subject"},
							{"type": "string", "value": "role"}
						]},
						{"type": "string", "value": "admin"}
					]
				},
				{
					"index": 1,
					"terms": [
						{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
						{"type": "ref", "value": [
							{"type": "var", "value": "input"},
							{"type": "string", "value": "subject"},
							{"type": "string", "value": "tenant_id"}
						]},
						{"type": "ref", "value": [
							{"type": "var", "value": "data"},
							{"type": "string", "value": "users"},
							{"type": "var", "value": "$0"},
							{"type": "string", "value": "tenant_id"}
						]}
					]
				}
			]]
		}
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the request sends unknowns=["input"].
		body, _ := io.ReadAll(r.Body)
		var req compileRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("failed to parse request: %v", err)
		}
		if len(req.Unknowns) != 1 || req.Unknowns[0] != "input" {
			t.Errorf("expected unknowns [input], got %v", req.Unknowns)
		}
		// Input should be an empty object for DiscoverInputs.
		inputMap, ok := req.Input.(map[string]any)
		if !ok || len(inputMap) != 0 {
			t.Errorf("expected empty input object, got %v", req.Input)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(respBody))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "data.app.allow", nil)
	paths, err := client.DiscoverInputs()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(paths) != 2 {
		t.Fatalf("expected 2 paths, got %d: %v", len(paths), paths)
	}
	// Results should be sorted.
	if paths[0] != "subject.role" {
		t.Errorf("paths[0]: expected 'subject.role', got %q", paths[0])
	}
	if paths[1] != "subject.tenant_id" {
		t.Errorf("paths[1]: expected 'subject.tenant_id', got %q", paths[1])
	}
}

func TestDiscoverInputsWithDataUnknowns(t *testing.T) {
	respBody := `{
		"result": {
			"queries": [[
				{
					"index": 0,
					"terms": [
						{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
						{"type": "ref", "value": [
							{"type": "var", "value": "input"},
							{"type": "string", "value": "user"},
							{"type": "string", "value": "role"}
						]},
						{"type": "string", "value": "reader"}
					]
				},
				{
					"index": 1,
					"terms": [
						{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
						{"type": "ref", "value": [
							{"type": "var", "value": "data"},
							{"type": "string", "value": "consignments"},
							{"type": "var", "value": "$0"},
							{"type": "string", "value": "origin"}
						]},
						{"type": "ref", "value": [
							{"type": "var", "value": "input"},
							{"type": "string", "value": "user"},
							{"type": "string", "value": "region"}
						]}
					]
				}
			]]
		}
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req compileRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("failed to parse request: %v", err)
		}
		if len(req.Unknowns) != 2 || req.Unknowns[0] != "input" || req.Unknowns[1] != "data.consignments" {
			t.Errorf("expected unknowns [input data.consignments], got %v", req.Unknowns)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(respBody))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "data.app.allow", nil)
	paths, err := client.DiscoverInputs("data.consignments")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(paths) != 2 {
		t.Fatalf("expected 2 paths, got %d: %v", len(paths), paths)
	}
	if paths[0] != "user.region" {
		t.Errorf("paths[0]: expected 'user.region', got %q", paths[0])
	}
	if paths[1] != "user.role" {
		t.Errorf("paths[1]: expected 'user.role', got %q", paths[1])
	}
}

func TestDiscoverInputsNoInputs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result": {"queries": [[]]}}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "data.app.allow", nil)
	paths, err := client.DiscoverInputs()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(paths) != 0 {
		t.Errorf("expected 0 paths, got %d: %v", len(paths), paths)
	}
}

func TestDiscoverInputsServerUnreachable(t *testing.T) {
	client := NewClient("http://127.0.0.1:19999", "data.app.allow", nil)
	_, err := client.DiscoverInputs()
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
}

// --- Explain method ---

func TestExplainCompact(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{
				"result": {
					"queries": [[
						{
							"index": 0,
							"terms": [
								{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
								{"type": "ref", "value": [
									{"type": "var", "value": "data"},
									{"type": "string", "value": "users"},
									{"type": "string", "value": "tenant_id"}
								]},
								{"type": "string", "value": "acme"}
							]
						}
					]]
				}
			}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := NewClient(srv.URL, "data.authz.allow", map[string]any{"subject": map[string]any{"role": "admin"}})
	result, err := client.Explain("users")
	if err != nil {
		t.Fatalf("Explain failed: %v", err)
	}
	if result.QueryCount != 1 {
		t.Errorf("expected 1 query, got %d", result.QueryCount)
	}
	if result.ExpressionCount != 1 {
		t.Errorf("expected 1 expression, got %d", result.ExpressionCount)
	}
	if len(result.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(result.Conditions))
	}
	if result.RawJSON == "" {
		t.Error("expected raw JSON to be populated")
	}
}

func TestExplainVerboseTranslations(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{
				"result": {
					"queries": [[
						{
							"index": 0,
							"terms": [
								{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
								{"type": "ref", "value": [
									{"type": "var", "value": "data"},
									{"type": "string", "value": "users"},
									{"type": "string", "value": "tenant_id"}
								]},
								{"type": "string", "value": "acme"}
							]
						}
					]]
				}
			}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := NewClient(srv.URL, "data.authz.allow", nil)
	result, err := client.Explain("users")
	if err != nil {
		t.Fatalf("Explain failed: %v", err)
	}
	if len(result.Translations) != 1 {
		t.Fatalf("expected 1 translation, got %d", len(result.Translations))
	}
	tr := result.Translations[0]
	if tr.Operator != "eq" {
		t.Errorf("expected operator eq, got %s", tr.Operator)
	}
	if tr.Column != "tenant_id" {
		t.Errorf("expected column tenant_id, got %s", tr.Column)
	}
}

func TestExplainAccessDenied(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{"result": {}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := NewClient(srv.URL, "data.authz.allow", nil)
	result, err := client.Explain("users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.AccessDenied {
		t.Error("expected AccessDenied to be true")
	}
	if result.RawJSON == "" {
		t.Error("expected RawJSON to be populated for diagnostics")
	}
	if result.RequestJSON == "" {
		t.Error("expected RequestJSON to be populated for diagnostics")
	}
}

// --- CompileWithMasks ---

func TestCompileResultIncludesMasks(t *testing.T) {
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

	client := NewClient(srv.URL, "data.app.allow", map[string]any{})
	result, err := client.CompileWithMasks("consignments")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(result.Conditions))
	}
	if result.Masks == nil {
		t.Fatal("expected masks to be populated")
	}
	if result.Masks["consignments"]["billed_total"].Replace.Value != "<MASKED>" {
		t.Error("expected mask value '<MASKED>'")
	}
}

func TestCompileResultNoMasks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{"result": {"queries": [[]]}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "data.app.allow", map[string]any{})
	result, err := client.CompileWithMasks("users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Conditions != nil {
		t.Errorf("expected nil conditions, got %v", result.Conditions)
	}
	if result.Masks != nil {
		t.Errorf("expected nil masks, got %v", result.Masks)
	}
}

func TestExplainIncludesMasks(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{
				"result": {
					"queries": [[
						{
							"index": 0,
							"terms": [
								{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
								{"type": "ref", "value": [
									{"type": "var", "value": "data"},
									{"type": "string", "value": "consignments"},
									{"type": "string", "value": "account_name"}
								]},
								{"type": "string", "value": "acme"}
							]
						}
					]]
				}
			}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":"<MASKED>"}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := NewClient(srv.URL, "data.authz.allow", nil)
	result, err := client.Explain("consignments")
	if err != nil {
		t.Fatalf("Explain failed: %v", err)
	}
	if result.Masks == nil {
		t.Fatal("expected masks to be populated")
	}
	if result.Masks["consignments"]["billed_total"].Replace.Value != "<MASKED>" {
		t.Error("expected mask value '<MASKED>'")
	}
}

func TestExplainNoMasks(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{"result": {"queries": [[]]}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := NewClient(srv.URL, "data.authz.allow", nil)
	result, err := client.Explain("users")
	if err != nil {
		t.Fatalf("Explain failed: %v", err)
	}
	if result.Masks != nil {
		t.Errorf("expected nil masks, got %v", result.Masks)
	}
}

func TestExplainUnconditionalAllow(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{"result": {"queries": [[]]}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := NewClient(srv.URL, "data.authz.allow", nil)
	result, err := client.Explain("users")
	if err != nil {
		t.Fatalf("Explain failed: %v", err)
	}
	if len(result.Conditions) != 0 {
		t.Errorf("expected no conditions for unconditional allow, got %d", len(result.Conditions))
	}
	if !result.UnconditionalAllow {
		t.Error("expected UnconditionalAllow to be true")
	}
}

func TestCompileResultMasksExported(t *testing.T) {
	var m MaskAction
	m.Replace = &ReplaceAction{Value: "test"}
	if m.Replace.Value != "test" {
		t.Errorf("expected 'test', got %q", m.Replace.Value)
	}
}

// --- FetchMasks from Data API ---

func TestFetchMasksFromDataAPI(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/data/authz/masks" {
			_, _ = w.Write([]byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":"<MASKED>"}}}}}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "data.authz.allow", map[string]any{"user": map[string]any{"role": "reader"}})
	masks, err := client.FetchMasks()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if masks == nil {
		t.Fatal("expected masks")
	}
	if masks["consignments"]["billed_total"].Replace.Value != "<MASKED>" {
		t.Error("expected '<MASKED>'")
	}
}

func TestFetchMasksNoMasksDefined(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// OPA returns empty result when masks rule is undefined.
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "data.authz.allow", nil)
	masks, err := client.FetchMasks()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if masks != nil {
		t.Errorf("expected nil masks, got %v", masks)
	}
}

func TestFetchMasksSuperadminNoMask(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// When value is {}, it means "no mask" (superadmin case).
		_, _ = w.Write([]byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":{}}}}}}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "data.authz.allow", map[string]any{"user": map[string]any{"role": "superadmin"}})
	masks, err := client.FetchMasks()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if masks != nil {
		t.Errorf("expected nil masks for superadmin, got %v", masks)
	}
}

func TestMasksDataPath(t *testing.T) {
	tests := []struct {
		policyPath string
		expected   string
	}{
		{"data.authz.allow", "authz/masks"},
		{"data.app.allow", "app/masks"},
		{"data.policies.filtering.platform.consignment.include", "policies/filtering/platform/consignment/masks"},
	}
	for _, tt := range tests {
		client := NewClient("http://localhost", tt.policyPath, nil)
		got := client.masksDataPath()
		if got != tt.expected {
			t.Errorf("masksDataPath(%q) = %q, want %q", tt.policyPath, got, tt.expected)
		}
	}
}

func TestParseMasksResponseReplace(t *testing.T) {
	data := []byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":"<MASKED>"}}}}}`)
	masks, err := parseMasksResponse(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if masks["consignments"]["billed_total"].Replace.Value != "<MASKED>" {
		t.Error("expected '<MASKED>'")
	}
}

func TestParseMasksResponseEmptyValueMeansNoMask(t *testing.T) {
	data := []byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":{}}}}}}`)
	masks, err := parseMasksResponse(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if masks != nil {
		t.Errorf("expected nil masks for empty value, got %v", masks)
	}
}

// --- extractOperator edge cases ---

func TestExtractOperatorNonRefType(t *testing.T) {
	term := compileTerm{Type: "string", Value: "eq"}
	_, err := extractOperator(term)
	if err == nil {
		t.Error("expected error for non-ref type")
	}
	if !strings.Contains(err.Error(), "must be ref") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExtractOperatorEmptyParts(t *testing.T) {
	term := compileTerm{Type: "ref", Value: []compileTerm{}}
	_, err := extractOperator(term)
	if err == nil {
		t.Error("expected error for empty ref parts")
	}
	if !strings.Contains(err.Error(), "has no parts") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExtractOperatorNonVarFirstPart(t *testing.T) {
	term := compileTerm{
		Type: "ref",
		Value: []compileTerm{
			{Type: "string", Value: "eq"},
		},
	}
	_, err := extractOperator(term)
	if err == nil {
		t.Error("expected error for non-var first part")
	}
	if !strings.Contains(err.Error(), "must be var") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExtractOperatorNonStringValue(t *testing.T) {
	term := compileTerm{
		Type: "ref",
		Value: []compileTerm{
			{Type: "var", Value: 123}, // number instead of string
		},
	}
	_, err := extractOperator(term)
	if err == nil {
		t.Error("expected error for non-string var value")
	}
	if !strings.Contains(err.Error(), "not a string") {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- extractColumnName edge cases ---

func TestExtractColumnNameNonRefType(t *testing.T) {
	term := compileTerm{Type: "var", Value: "x"}
	_, err := extractColumnName(term)
	if err == nil {
		t.Error("expected error for non-ref type")
	}
	if !strings.Contains(err.Error(), "must be ref") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExtractColumnNameEmptyParts(t *testing.T) {
	term := compileTerm{Type: "ref", Value: []compileTerm{}}
	_, err := extractColumnName(term)
	if err == nil {
		t.Error("expected error for empty ref parts")
	}
	if !strings.Contains(err.Error(), "has no parts") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExtractColumnNameNoStringElement(t *testing.T) {
	term := compileTerm{
		Type: "ref",
		Value: []compileTerm{
			{Type: "var", Value: "data"},
			{Type: "number", Value: 42},
		},
	}
	_, err := extractColumnName(term)
	if err == nil {
		t.Error("expected error when no string-typed element exists")
	}
	if !strings.Contains(err.Error(), "no string-typed element") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExtractColumnNameStringValueNotString(t *testing.T) {
	term := compileTerm{
		Type: "ref",
		Value: []compileTerm{
			{Type: "string", Value: 123}, // number instead of string
		},
	}
	_, err := extractColumnName(term)
	if err == nil {
		t.Error("expected error for string element with non-string value")
	}
	if !strings.Contains(err.Error(), "not a string") {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- isDataRef edge cases ---

func TestIsDataRefNonRefType(t *testing.T) {
	term := compileTerm{Type: "string", Value: "data"}
	if isDataRef(term) {
		t.Error("expected false for non-ref type")
	}
}

func TestIsDataRefInvalidValue(t *testing.T) {
	term := compileTerm{Type: "ref", Value: "not-a-slice"}
	if isDataRef(term) {
		t.Error("expected false for invalid Value type")
	}
}

func TestIsDataRefEmptyParts(t *testing.T) {
	term := compileTerm{Type: "ref", Value: []compileTerm{}}
	if isDataRef(term) {
		t.Error("expected false for empty parts")
	}
}

func TestIsDataRefNonVarFirstPart(t *testing.T) {
	term := compileTerm{
		Type: "ref",
		Value: []compileTerm{
			{Type: "string", Value: "data"},
		},
	}
	if isDataRef(term) {
		t.Error("expected false when first part is not var")
	}
}

func TestIsDataRefNonDataVar(t *testing.T) {
	term := compileTerm{
		Type: "ref",
		Value: []compileTerm{
			{Type: "var", Value: "input"},
		},
	}
	if isDataRef(term) {
		t.Error("expected false when var is not 'data'")
	}
}

func TestIsDataRefValidDataRef(t *testing.T) {
	term := compileTerm{
		Type: "ref",
		Value: []compileTerm{
			{Type: "var", Value: "data"},
			{Type: "string", Value: "users"},
		},
	}
	if !isDataRef(term) {
		t.Error("expected true for valid data ref")
	}
}

// --- parseCompileResponse edge cases ---

func TestParseCompileResponseInvalidJSON(t *testing.T) {
	_, err := parseCompileResponse([]byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParseCompileResponseEmptyResult(t *testing.T) {
	data := []byte(`{"result":{}}`)
	resp, err := parseCompileResponse(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Result.Queries != nil {
		t.Error("expected nil queries for empty result")
	}
}
