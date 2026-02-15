package main

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/bawdo/gosbee/plugins"
	"github.com/bawdo/gosbee/plugins/softdelete"
)

// configureSoftdelete parses softdelete arguments, registers the plugin
// in the registry, and rebuilds the query if one exists.
func configureSoftdelete(s *Session, args string) error {
	rest := strings.TrimSpace(args)
	var opts []softdelete.Option
	var statusFn func() string

	switch {
	case strings.Contains(rest, "."):
		// Per-table columns: users.deleted_at, posts.removed_at
		pairs := strings.Split(rest, ",")
		columns := map[string]string{}
		for _, pair := range pairs {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			dot := strings.IndexByte(pair, '.')
			if dot < 0 || dot == 0 || dot == len(pair)-1 {
				return fmt.Errorf("invalid table.column pair: %q", pair)
			}
			table := pair[:dot]
			col := pair[dot+1:]
			opts = append(opts, softdelete.WithTableColumn(table, col))
			columns[table] = col
		}
		statusFn = func() string {
			pairs := make([]string, 0, len(columns))
			for t, c := range columns {
				pairs = append(pairs, t+"."+c)
			}
			sort.Strings(pairs)
			return strings.Join(pairs, ", ")
		}
		_, _ = fmt.Fprintln(s.out,"  Soft-delete enabled (per-table columns)")

	case strings.Contains(strings.ToLower(rest), " on "):
		// Single column on specific tables: removed_at on users posts
		idx := strings.Index(strings.ToLower(rest), " on ")
		col := strings.TrimSpace(rest[:idx])
		tableList := strings.Fields(rest[idx+4:])
		if col == "" || len(tableList) == 0 {
			return errors.New("usage: plugin softdelete <column> on <table1> [table2 ...]")
		}
		opts = append(opts, softdelete.WithColumn(col), softdelete.WithTables(tableList...))
		statusFn = func() string {
			return fmt.Sprintf("column: %s, tables: %s", col, strings.Join(tableList, ", "))
		}
		_, _ = fmt.Fprintf(s.out,"  Soft-delete enabled (column: %s, tables: %s)\n", col, strings.Join(tableList, ", "))

	case rest != "":
		// Single custom column for all tables
		col := strings.Fields(rest)[0]
		opts = append(opts, softdelete.WithColumn(col))
		statusFn = func() string { return "column: " + col }
		_, _ = fmt.Fprintf(s.out,"  Soft-delete enabled (column: %s)\n", col)

	default:
		// No args — default column for all tables
		statusFn = func() string { return "column: deleted_at" }
		_, _ = fmt.Fprintln(s.out,"  Soft-delete enabled (column: deleted_at)")
	}

	s.plugins.register(pluginEntry{
		name:    "softdelete",
		factory: func() plugins.Transformer { return softdelete.New(opts...) },
		status:  statusFn,
		color:   "#CC6666",
	})

	if s.query != nil {
		s.rebuildQueryWithPlugins()
	}
	return nil
}
