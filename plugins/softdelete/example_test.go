package softdelete_test

import (
	"fmt"

	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins/softdelete"
	"github.com/bawdo/gosbee/visitors"
)

func printSQL(sql string, params []any, err error) {
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(sql)
	fmt.Println(params)
}

// By default every table in FROM and the joins gets a deleted_at IS NULL
// condition.
func ExampleNew() {
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")

	query := managers.NewSelectManager(users).
		Select(users.Col("name"), posts.Col("title")).
		Join(posts).On(posts.Col("user_id").Eq(users.Col("id"))).
		Use(softdelete.New())

	printSQL(query.ToSQL(visitors.NewPostgresVisitor()))
	// Output:
	// SELECT "users"."name", "posts"."title" FROM "users" INNER JOIN "posts" ON "posts"."user_id" = "users"."id" WHERE "users"."deleted_at" IS NULL AND "posts"."deleted_at" IS NULL
	// []
}

// Tables that name their soft-delete column differently get a column each.
// A table with no entry is left alone.
func ExampleWithTableColumn() {
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	tags := nodes.NewTable("tags")

	query := managers.NewSelectManager(users).
		Select(users.Col("name"), posts.Col("title"), tags.Col("label")).
		Join(posts).On(posts.Col("user_id").Eq(users.Col("id"))).
		Join(tags).On(tags.Col("post_id").Eq(posts.Col("id"))).
		Use(softdelete.New(
			softdelete.WithTableColumn("users", "deleted_at"),
			softdelete.WithTableColumn("posts", "removed_at"),
		))

	printSQL(query.ToSQL(visitors.NewPostgresVisitor()))
	// Output:
	// SELECT "users"."name", "posts"."title", "tags"."label" FROM "users" INNER JOIN "posts" ON "posts"."user_id" = "users"."id" INNER JOIN "tags" ON "tags"."post_id" = "posts"."id" WHERE "users"."deleted_at" IS NULL AND "posts"."removed_at" IS NULL
	// []
}
