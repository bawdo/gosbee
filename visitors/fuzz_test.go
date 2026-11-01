package visitors_test

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

// dialects pairs each visitor with the quoting rules its output is read
// back under.
var dialects = []struct {
	name string
	lex  sqllex.Dialect
	new  func(...visitors.Option) paramVisitor
}{
	{"postgres", sqllex.ANSI, func(o ...visitors.Option) paramVisitor { return visitors.NewPostgresVisitor(o...) }},
	{"mysql", sqllex.MySQL, func(o ...visitors.Option) paramVisitor { return visitors.NewMySQLVisitor(o...) }},
	{"sqlite", sqllex.ANSI, func(o ...visitors.Option) paramVisitor { return visitors.NewSQLiteVisitor(o...) }},
}

var visitorSeeds = []string{
	"", "users", `a"b`, "a`b", "it's", `\`, "\xff\x00", "$1", "?",
	`users"."passwords`, "'; DROP TABLE users; --",
}

func lex(t *testing.T, d sqllex.Dialect, sql string) []sqllex.Token {
	t.Helper()
	ts, err := sqllex.Lex(d, sql)
	if err != nil {
		t.Fatalf("Lex(%q): %v", sql, err)
	}
	return ts
}

// FuzzIdentifierQuoting: SELECT t.c FROM t, with both names fuzzed, reads
// back under each dialect's rules with exactly the identifiers t, c, t, in
// that order, so neither name can close its quotes early.
func FuzzIdentifierQuoting(f *testing.F) {
	for _, s := range visitorSeeds {
		f.Add(s, s)
		f.Add(s, "c")
	}
	f.Fuzz(func(t *testing.T, table, col string) {
		tbl := nodes.NewTable(table)
		core := &nodes.SelectCore{From: tbl, Projections: []nodes.Node{tbl.Col(col)}}
		for _, d := range dialects {
			sql := core.Accept(d.new(visitors.WithoutParams()))
			got := sqllex.Of(lex(t, d.lex, sql), sqllex.Ident)
			if want := []string{table, col, table}; !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: %q has identifiers %q, want %q", d.name, sql, got, want)
			}
		}
	})
}

// FuzzStringLiteral: WHERE t.c = s. Without params, the SQL reads back with
// exactly one string literal, whose value is s under MySQL, and under
// PostgreSQL and SQLite when s has no backslash (the known EscapeString
// defect). With params, the SQL has no string literal and the only parameter
// is s.
func FuzzStringLiteral(f *testing.F) {
	for _, s := range visitorSeeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		tbl := nodes.NewTable("t")
		core := &nodes.SelectCore{
			From:        tbl,
			Projections: []nodes.Node{tbl.Col("c")},
			Wheres:      []nodes.Node{tbl.Col("c").Eq(s)},
		}
		for _, d := range dialects {
			sql := core.Accept(d.new(visitors.WithoutParams()))
			ts := lex(t, d.lex, sql)
			strs := sqllex.Of(ts, sqllex.String)
			if len(strs) != 1 || ts[len(ts)-1].Kind != sqllex.String {
				t.Fatalf("%s: %q has string literals %q, want one, last", d.name, sql, strs)
			}
			if (d.lex == sqllex.MySQL || !strings.Contains(s, `\`)) && strs[0] != s {
				t.Fatalf("%s: %q reads back as %q, want %q", d.name, sql, strs[0], s)
			}

			v := d.new()
			sql = core.Accept(v)
			if strs := sqllex.Of(lex(t, d.lex, sql), sqllex.String); len(strs) != 0 {
				t.Fatalf("%s params: %q has string literals %q, want none", d.name, sql, strs)
			}
			if got := v.Params(); !reflect.DeepEqual(got, []any{s}) {
				t.Fatalf("%s params: Params() = %#v, want [%q]", d.name, got, s)
			}
		}
	})
}
