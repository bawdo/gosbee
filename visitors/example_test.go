package visitors_test

import (
	"fmt"

	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/visitors"
)

// lookupQuery builds one query for every dialect example, so the dialect
// is the only thing that changes between them.
func lookupQuery() *managers.SelectManager {
	users := nodes.NewTable("users")
	return managers.NewSelectManager(users).
		Select(users.Col("id"), users.Col("email")).
		Where(
			users.Col("email").CaseInsensitiveEq("Alice@Example.com"),
			users.Col("id").Gt(100),
		)
}

func render(v nodes.Visitor) {
	sql, params, err := lookupQuery().ToSQL(v)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(sql)
	fmt.Println(params)
}

// PostgreSQL quotes identifiers with double quotes, numbers its placeholders,
// and compares case-insensitively by lowering both sides.
func ExampleNewPostgresVisitor() {
	render(visitors.NewPostgresVisitor())
	// Output:
	// SELECT "users"."id", "users"."email" FROM "users" WHERE LOWER("users"."email") = LOWER($1) AND "users"."id" > $2
	// [Alice@Example.com 100]
}

// MySQL quotes identifiers with backticks and uses ? placeholders. Its
// default collations already compare case-insensitively, so a plain = is
// emitted.
func ExampleNewMySQLVisitor() {
	render(visitors.NewMySQLVisitor())
	// Output:
	// SELECT `users`.`id`, `users`.`email` FROM `users` WHERE `users`.`email` = ? AND `users`.`id` > ?
	// [Alice@Example.com 100]
}

// SQLite quotes identifiers with double quotes, uses ? placeholders, and
// spells a case-insensitive comparison as a collation.
func ExampleNewSQLiteVisitor() {
	render(visitors.NewSQLiteVisitor())
	// Output:
	// SELECT "users"."id", "users"."email" FROM "users" WHERE "users"."email" = ? COLLATE NOCASE AND "users"."id" > ?
	// [Alice@Example.com 100]
}
