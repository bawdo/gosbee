// Package softdelete provides a Transformer that automatically injects
// "column IS NULL" conditions into SELECT queries, filtering out
// soft-deleted rows.
//
// By default it appends WHERE "deleted_at" IS NULL for every table
// referenced in the FROM and JOIN clauses. Both the column name and the
// set of tables can be customised via options.
//
// # Basic usage
//
//	sd := softdelete.New()
//	query := managers.NewSelectManager(table)
//	query.Use(sd)
//	// SELECT * FROM "users" WHERE "users"."deleted_at" IS NULL
//
// # Custom column
//
//	sd := softdelete.New(softdelete.WithColumn("removed_at"))
//	// ... WHERE "users"."removed_at" IS NULL
//
// # Restrict to specific tables
//
// When a query joins multiple tables but only some use soft-delete:
//
//	sd := softdelete.New(softdelete.WithTables("users"))
//	// Only "users" gets the IS NULL condition; other joined tables are unchanged.
//
// # Per-table columns
//
// Different tables may use different column names for soft-delete:
//
//	sd := softdelete.New(
//	    softdelete.WithTableColumn("users", "deleted_at"),
//	    softdelete.WithTableColumn("posts", "removed_at"),
//	)
//	// users gets "deleted_at" IS NULL; posts gets "removed_at" IS NULL
//
// # Nested queries and raw SQL
//
// Via a manager, the plugin applies inside subqueries, CTE bodies and
// set-operation branches. A name that refers to a CTE is not a table and is
// skipped. Queries containing raw SQL (SqlLiteral, StringJoin) are refused
// with plugins.ErrUnpoliceable unless WithAllowRawSQL is given.
//
// # REPL usage
//
//	gosbee> plugin softdelete
//	gosbee> plugin softdelete removed_at
//	gosbee> plugin softdelete removed_at on users posts
//	gosbee> plugin softdelete users.deleted_at, posts.removed_at
//	gosbee> plugin off softdelete
//	gosbee> plugins
package softdelete

import (
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// SoftDelete is a Transformer that appends IS NULL conditions for a
// soft-delete column on every referenced table (or a configured subset).
type SoftDelete struct {
	plugins.BaseTransformer
	Column   string
	Columns  map[string]string // per-table column overrides (table name → column name)
	tables   map[string]bool   // nil means apply to all tables
	allowRaw bool              // when false, raw SQL in a query is refused
}

// Option configures a SoftDelete transformer.
type Option func(*SoftDelete)

// WithColumn sets the soft-delete column name. Default is "deleted_at".
func WithColumn(name string) Option {
	return func(sd *SoftDelete) { sd.Column = name }
}

// WithTables restricts the plugin to only the named tables.
// By default, the plugin applies to every table in the query.
func WithTables(names ...string) Option {
	return func(sd *SoftDelete) {
		sd.tables = make(map[string]bool, len(names))
		for _, n := range names {
			sd.tables[n] = true
		}
	}
}

// WithTableColumn sets a per-table column override. The table is
// automatically added to the whitelist, restricting the plugin's scope.
func WithTableColumn(table, column string) Option {
	return func(sd *SoftDelete) {
		if sd.Columns == nil {
			sd.Columns = make(map[string]string)
		}
		sd.Columns[table] = column
		if sd.tables == nil {
			sd.tables = make(map[string]bool)
		}
		sd.tables[table] = true
	}
}

// WithAllowRawSQL lets queries containing raw SQL fragments (SqlLiteral,
// StringJoin) through. By default the plugin refuses them with an error
// wrapping plugins.ErrUnpoliceable, because it cannot tell whether a raw
// fragment reads from a table it should be filtering. Only enable this when
// every raw fragment is trusted not to bypass soft-delete.
func WithAllowRawSQL() Option {
	return func(sd *SoftDelete) { sd.allowRaw = true }
}

// New creates a SoftDelete transformer with the given options.
func New(opts ...Option) *SoftDelete {
	sd := &SoftDelete{Column: "deleted_at"}
	for _, o := range opts {
		o(sd)
	}
	return sd
}

// TransformSelect appends "column IS NULL" to the WHERE clause for each
// matching table referenced in the query (FROM and JOINs), and in every query
// nested in it, via plugins.ApplySelect. A direct call outside a manager gets
// the same scope and raw SQL refusal as the plugin walker. The input is not
// modified.
func (sd *SoftDelete) TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error) {
	return plugins.ApplySelect(sd, core)
}

// TransformSelectScoped is TransformSelect aware of the walker's scope: names
// that refer to a CTE, and derived-table aliases, are not treated as tables.
func (sd *SoftDelete) TransformSelectScoped(core *nodes.SelectCore, scope plugins.Scope) (*nodes.SelectCore, error) {
	for _, ref := range scope.Tables(core) {
		if sd.appliesTo(ref.Name) {
			attr := nodes.NewAttribute(ref.Relation, sd.columnFor(ref.Name))
			core.Wheres = append(core.Wheres, attr.IsNull())
		}
	}
	return core, nil
}

// RefusesRawSQL reports whether the walker should refuse raw SQL fragments.
// It is true unless WithAllowRawSQL was given.
func (sd *SoftDelete) RefusesRawSQL() bool { return !sd.allowRaw }

func (sd *SoftDelete) appliesTo(tableName string) bool {
	if sd.tables == nil {
		return true
	}
	return sd.tables[tableName]
}

// columnFor returns the column name to use for the given table.
// It checks Columns for a per-table override, falling back to Column.
func (sd *SoftDelete) columnFor(tableName string) string {
	if sd.Columns != nil {
		if col, ok := sd.Columns[tableName]; ok {
			return col
		}
	}
	return sd.Column
}
