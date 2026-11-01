package managers_test

import (
	"fmt"

	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/visitors"
)

// printSQL prints a statement and its bind parameters, or the error that
// stopped it rendering.
func printSQL(sql string, params []any, err error) {
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(sql)
	fmt.Println(params)
}

// A projection names the columns the query returns.
func ExampleSelectManager_Select() {
	users := nodes.NewTable("users")

	query := managers.NewSelectManager(users).
		Select(users.Col("id"), users.Col("name"))

	printSQL(query.ToSQL(visitors.NewPostgresVisitor()))
	// Output:
	// SELECT "users"."id", "users"."name" FROM "users"
	// []
}

// Join returns a JoinContext; On supplies the join condition and hands the
// SelectManager back so the chain can carry on.
func ExampleSelectManager_Join() {
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")

	query := managers.NewSelectManager(users).
		Select(users.Col("name"), posts.Col("title")).
		Join(posts).On(posts.Col("user_id").Eq(users.Col("id")))

	printSQL(query.ToSQL(visitors.NewPostgresVisitor()))
	// Output:
	// SELECT "users"."name", "posts"."title" FROM "users" INNER JOIN "posts" ON "posts"."user_id" = "users"."id"
	// []
}

// Several conditions passed to Where are joined with AND. Or groups its
// operands in parentheses so the precedence reads as written. Values are
// bound as parameters, never spliced into the SQL.
func ExampleSelectManager_Where() {
	users := nodes.NewTable("users")

	query := managers.NewSelectManager(users).
		Select(users.Col("id")).
		Where(
			users.Col("active").Eq(true),
			users.Col("role").Eq("admin").Or(users.Col("age").GtEq(18)),
		)

	printSQL(query.ToSQL(visitors.NewPostgresVisitor()))
	// Output:
	// SELECT "users"."id" FROM "users" WHERE "users"."active" = $1 AND ("users"."role" = $2 OR "users"."age" >= $3)
	// [true admin 18]
}

func ExampleInsertManager_Values() {
	users := nodes.NewTable("users")

	insert := managers.NewInsertManager(users).
		Columns(users.Col("name"), users.Col("email")).
		Values("Alice", "alice@example.com").
		Values("Bob", "bob@example.com")

	printSQL(insert.ToSQL(visitors.NewPostgresVisitor()))
	// Output:
	// INSERT INTO "users" ("name", "email") VALUES ($1, $2), ($3, $4)
	// [Alice alice@example.com Bob bob@example.com]
}

func ExampleUpdateManager_Set() {
	users := nodes.NewTable("users")

	update := managers.NewUpdateManager(users).
		Set(users.Col("name"), "Alice").
		Set(users.Col("active"), false).
		Where(users.Col("id").Eq(42))

	printSQL(update.ToSQL(visitors.NewPostgresVisitor()))
	// Output:
	// UPDATE "users" SET "name" = $1, "active" = $2 WHERE "users"."id" = $3
	// [Alice false 42]
}

func ExampleDeleteManager_Where() {
	sessions := nodes.NewTable("sessions")

	del := managers.NewDeleteManager(sessions).
		Where(sessions.Col("expires_at").Lt("2026-01-01"))

	printSQL(del.ToSQL(visitors.NewPostgresVisitor()))
	// Output:
	// DELETE FROM "sessions" WHERE "sessions"."expires_at" < $1
	// [2026-01-01]
}
