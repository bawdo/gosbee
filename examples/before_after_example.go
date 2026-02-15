package examples

// This file demonstrates the difference between using subpackages directly
// versus using the convenience package.

// ============================================================
// BEFORE: Using subpackages directly (still valid!)
// ============================================================

/*
import (
	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/visitors"
)

func buildQueryOldWay() (string, []any) {
	users := nodes.NewTable("users")

	sm := managers.NewSelectManager(users)
	sm.Select(users.Col("id"), users.Col("name"))
	sm.Where(users.Col("active").Eq(nodes.Literal(true)))

	visitor := visitors.NewPostgresVisitor()
	sql := sm.ToSQL(visitor)

	return sql, visitor.Binds()
}
*/

// ============================================================
// AFTER: Using the convenience package
// ============================================================

/*
import "github.com/bawdo/gosbee"

func buildQueryNewWay() (string, []any, error) {
	users := gosbee.NewTable("users")

	sm := gosbee.NewSelect(users)
	sm.Select(users.Col("id"), users.Col("name"))
	sm.Where(users.Col("active").Eq(gosbee.BindParam(true)))

	visitor := gosbee.NewPostgresVisitor()
	sql, params, err := sm.ToSQL(visitor)

	return sql, params, err
}
*/

// ============================================================
// ADVANCED: Mix and match as needed
// ============================================================

/*
import (
	"github.com/bawdo/gosbee"
	"github.com/bawdo/gosbee/nodes"  // Advanced node types
	"github.com/bawdo/gosbee/plugins" // Plugins not re-exported
)

func advancedQuery() string {
	// Common stuff from convenience package
	users := gosbee.NewTable("users")
	sm := gosbee.NewSelect(users)

	// Advanced nodes from subpackage
	sm.Select(nodes.NewCase().
		When(users.Col("age").Gt(gosbee.Literal(18)), gosbee.Literal("adult")).
		Else(gosbee.Literal("minor")))

	// Plugins always from subpackage
	sm.UsePlugin(plugins.NewSoftDeletePlugin())

	visitor := gosbee.NewPostgresVisitor()
	return sm.ToSQL(visitor)
}
*/
