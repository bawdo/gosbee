package opa

import (
	"reflect"
	"strings"
	"testing"

	"github.com/bawdo/gosbee/internal/sqllex"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/visitors"
)

type paramVisitor interface {
	nodes.Visitor
	Params() []any
}

// FuzzMaskedProjection drives the mask path (applyMasks, maskedProjection,
// then the visitor's bind parameter) with a fuzzed mask value and column
// name. With params, the only parameter is the value, the SQL carries no
// string literal, and the alias reads back as the column name. Without
// params, the SQL reads back with exactly one string literal, whose value is
// the mask value under MySQL, and under PostgreSQL and SQLite when it has no
// backslash (the known EscapeString defect).
func FuzzMaskedProjection(f *testing.F) {
	f.Add("<MASKED>", "ssn")
	f.Add("$1", "a")
	f.Add("?", `b"c`)
	f.Add("it's", "`")
	f.Add(`\`, "x")
	f.Add("\xff\x00", "\xff")
	f.Add("", "y")
	f.Add("'; DROP TABLE users; --", "z")

	dialects := []struct {
		name string
		lex  sqllex.Dialect
		new  func(...visitors.Option) paramVisitor
	}{
		{"postgres", sqllex.ANSI, func(o ...visitors.Option) paramVisitor { return visitors.NewPostgresVisitor(o...) }},
		{"mysql", sqllex.MySQL, func(o ...visitors.Option) paramVisitor { return visitors.NewMySQLVisitor(o...) }},
		{"sqlite", sqllex.ANSI, func(o ...visitors.Option) paramVisitor { return visitors.NewSQLiteVisitor(o...) }},
	}

	f.Fuzz(func(t *testing.T, value, col string) {
		if col == "" {
			return // not a column a policy can name
		}
		o := New(func(string) ([]nodes.Node, error) { return nil, nil })
		o.masks = map[string]map[string]MaskAction{
			"t": {col: {Replace: &ReplaceAction{Value: value}}},
		}
		tbl := nodes.NewTable("t")
		core, err := o.TransformSelect(&nodes.SelectCore{From: tbl, Projections: []nodes.Node{tbl.Col(col)}})
		if err != nil {
			t.Fatalf("TransformSelect: %v", err)
		}

		for _, d := range dialects {
			v := d.new()
			sql := core.Accept(v)
			ts, err := sqllex.Lex(d.lex, sql)
			if err != nil {
				t.Fatalf("%s params: Lex(%q): %v", d.name, sql, err)
			}
			if got := v.Params(); !reflect.DeepEqual(got, []any{value}) {
				t.Fatalf("%s params: %q has Params() %#v, want [%q]", d.name, sql, got, value)
			}
			if strs := sqllex.Of(ts, sqllex.String); len(strs) != 0 {
				t.Fatalf("%s params: %q has string literals %q, want none", d.name, sql, strs)
			}
			if got, want := sqllex.Of(ts, sqllex.Ident), []string{col, "t"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("%s params: %q has identifiers %q, want %q", d.name, sql, got, want)
			}

			sql = core.Accept(d.new(visitors.WithoutParams()))
			ts, err = sqllex.Lex(d.lex, sql)
			if err != nil {
				t.Fatalf("%s: Lex(%q): %v", d.name, sql, err)
			}
			strs := sqllex.Of(ts, sqllex.String)
			if len(strs) != 1 {
				t.Fatalf("%s: %q has string literals %q, want one", d.name, sql, strs)
			}
			if (d.lex == sqllex.MySQL || !strings.Contains(value, `\`)) && strs[0] != value {
				t.Fatalf("%s: %q reads back as %q, want %q", d.name, sql, strs[0], value)
			}
		}
	})
}
