package visitors

import (
	"strings"
	"testing"

	"github.com/bawdo/gosbee/internal/testutil"
	"github.com/bawdo/gosbee/nodes"
)

func assertContains(t *testing.T, s, substr string) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf("expected output to contain %q, got:\n%s", substr, s)
	}
}

// --- Table ---

func TestVisitTable(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), users, `"users"`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), users, "`users`")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), users, `"users"`)
}

// --- TableAlias ---

func TestVisitTableAlias(t *testing.T) {
	t.Parallel()
	u := nodes.NewTable("users").Alias("u")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), u, `"users" AS "u"`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), u, "`users` AS `u`")
}

// --- Attribute ---

func TestVisitAttribute(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("name")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), col, `"users"."name"`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), col, "`users`.`name`")
}

func TestVisitAttributeOnAlias(t *testing.T) {
	t.Parallel()
	u := nodes.NewTable("users").Alias("u")
	col := u.Col("name")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), col, `"u"."name"`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), col, "`u`.`name`")
}

// --- Literals ---

func TestVisitLiteralString(t *testing.T) {
	t.Parallel()
	n := nodes.Literal("Alice")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `'Alice'`)
}

func TestVisitLiteralStringEscapesSingleQuotes(t *testing.T) {
	t.Parallel()
	n := nodes.Literal("O'Brien")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `'O''Brien'`)
}

func TestVisitLiteralInt(t *testing.T) {
	t.Parallel()
	n := nodes.Literal(42)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `42`)
}

func TestVisitLiteralFloat(t *testing.T) {
	t.Parallel()
	n := nodes.Literal(3.14)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `3.14`)
}

func TestVisitLiteralBoolTrue(t *testing.T) {
	t.Parallel()
	n := nodes.Literal(true)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `TRUE`)
}

func TestVisitLiteralBoolFalse(t *testing.T) {
	t.Parallel()
	n := nodes.Literal(false)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `FALSE`)
}

func TestVisitLiteralNil(t *testing.T) {
	t.Parallel()
	n := &nodes.LiteralNode{Value: nil}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `NULL`)
}

func TestVisitLiteralInt64(t *testing.T) {
	t.Parallel()
	n := nodes.Literal(int64(9999999999))
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `9999999999`)
}

// --- Star ---

func TestVisitUnqualifiedStar(t *testing.T) {
	t.Parallel()
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), nodes.Star(), `*`)
}

func TestVisitQualifiedStar(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), users.Star(), `"users".*`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), users.Star(), "`users`.*")
}

// --- SqlLiteral ---

func TestVisitSqlLiteral(t *testing.T) {
	t.Parallel()
	raw := nodes.NewSqlLiteral("COUNT(*)")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), raw, `COUNT(*)`)
}

// --- Comparison ---

func TestVisitEq(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("name")
	cmp := col.Eq("Alice")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"users"."name" = 'Alice'`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), cmp, "`users`.`name` = 'Alice'")
}

func TestVisitNotEq(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("x").NotEq(1)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."x" != 1`)
}

func TestVisitGt(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("age").Gt(18)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."age" > 18`)
}

func TestVisitGtEq(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("age").GtEq(18)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."age" >= 18`)
}

func TestVisitLt(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("age").Lt(65)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."age" < 65`)
}

func TestVisitLtEq(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("age").LtEq(65)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."age" <= 65`)
}

func TestVisitLike(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").Like("%foo%")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."name" LIKE '%foo%'`)
}

func TestVisitNotLike(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").NotLike("%bar%")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."name" NOT LIKE '%bar%'`)
}

func TestVisitNodeToNodeComparison(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	cmp := users.Col("id").Eq(posts.Col("author_id"))
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"users"."id" = "posts"."author_id"`)
}

// --- Unary ---

func TestVisitIsNull(t *testing.T) {
	t.Parallel()
	u := nodes.NewTable("t").Col("deleted_at").IsNull()
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), u, `"t"."deleted_at" IS NULL`)
}

func TestVisitIsNotNull(t *testing.T) {
	t.Parallel()
	u := nodes.NewTable("t").Col("deleted_at").IsNotNull()
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), u, `"t"."deleted_at" IS NOT NULL`)
}

// --- Logical ---

func TestVisitAnd(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	and := users.Col("active").Eq(true).And(users.Col("age").Gt(18))
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), and,
		`"users"."active" = TRUE AND "users"."age" > 18`)
}

func TestVisitOr(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	or := users.Col("role").Eq("admin").Or(users.Col("role").Eq("mod"))
	// Or wraps in GroupingNode
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), or,
		`("users"."role" = 'admin' OR "users"."role" = 'mod')`)
}

func TestVisitNot(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("active").Eq(true)
	not := cmp.Not()
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), not, `NOT ("t"."active" = TRUE)`)
}

// --- In / NotIn ---

func TestVisitIn(t *testing.T) {
	t.Parallel()
	in := nodes.NewTable("t").Col("status").In("active", "pending")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), in,
		`"t"."status" IN ('active', 'pending')`)
}

func TestVisitNotIn(t *testing.T) {
	t.Parallel()
	in := nodes.NewTable("t").Col("status").NotIn("deleted", "banned")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), in,
		`"t"."status" NOT IN ('deleted', 'banned')`)
}

// --- Between ---

func TestVisitBetween(t *testing.T) {
	t.Parallel()
	b := nodes.NewTable("t").Col("age").Between(18, 65)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), b,
		`"t"."age" BETWEEN 18 AND 65`)
}

// --- Grouping ---

func TestVisitGrouping(t *testing.T) {
	t.Parallel()
	inner := nodes.NewTable("t").Col("x").Eq(1)
	g := &nodes.GroupingNode{Expr: inner}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), g, `("t"."x" = 1)`)
}

// --- Join ---

func TestVisitInnerJoin(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	join := &nodes.JoinNode{
		Left:  users,
		Right: posts,
		Type:  nodes.InnerJoin,
		On:    users.Col("id").Eq(posts.Col("user_id")),
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), join,
		`INNER JOIN "posts" ON "users"."id" = "posts"."user_id"`)
}

func TestVisitLeftOuterJoin(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	join := &nodes.JoinNode{
		Left:  users,
		Right: posts,
		Type:  nodes.LeftOuterJoin,
		On:    users.Col("id").Eq(posts.Col("user_id")),
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), join,
		`LEFT OUTER JOIN "posts" ON "users"."id" = "posts"."user_id"`)
}

func TestVisitCrossJoin(t *testing.T) {
	t.Parallel()
	join := &nodes.JoinNode{
		Left:  nodes.NewTable("a"),
		Right: nodes.NewTable("b"),
		Type:  nodes.CrossJoin,
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), join, `CROSS JOIN "b"`)
}

func TestVisitJoinWithAlias(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	p := nodes.NewTable("posts").Alias("p")
	join := &nodes.JoinNode{
		Left:  users,
		Right: p,
		Type:  nodes.InnerJoin,
		On:    users.Col("id").Eq(p.Col("user_id")),
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), join,
		`INNER JOIN "posts" AS "p" ON "users"."id" = "p"."user_id"`)
}

// --- SelectCore ---

func TestVisitSelectCoreSimple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("id"), users.Col("name")},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT "users"."id", "users"."name" FROM "users"`)
}

func TestVisitSelectCoreDefaultStar(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{From: users}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT * FROM "users"`)
}

func TestVisitSelectCoreWithWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{nodes.Star()},
		Wheres:      []nodes.Node{users.Col("active").Eq(true)},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT * FROM "users" WHERE "users"."active" = TRUE`)
}

func TestVisitSelectCoreWithMultipleWheres(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:   users,
		Wheres: []nodes.Node{users.Col("active").Eq(true), users.Col("age").Gt(18)},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT * FROM "users" WHERE "users"."active" = TRUE AND "users"."age" > 18`)
}

func TestVisitSelectCoreWithJoin(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("name"), posts.Col("title")},
		Joins: []*nodes.JoinNode{
			{
				Left:  users,
				Right: posts,
				Type:  nodes.InnerJoin,
				On:    users.Col("id").Eq(posts.Col("user_id")),
			},
		},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT "users"."name", "posts"."title" FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id"`)
}

func TestVisitSelectCoreWithJoinAndWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("name")},
		Joins: []*nodes.JoinNode{
			{
				Left:  users,
				Right: posts,
				Type:  nodes.LeftOuterJoin,
				On:    users.Col("id").Eq(posts.Col("user_id")),
			},
		},
		Wheres: []nodes.Node{users.Col("active").Eq(true)},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT "users"."name" FROM "users" LEFT OUTER JOIN "posts" ON "users"."id" = "posts"."user_id" WHERE "users"."active" = TRUE`)
}

// --- Full query: end-to-end through SelectManager ---

func TestEndToEndPostgres(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")

	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("name"), users.Col("email")},
		Joins: []*nodes.JoinNode{
			{
				Left:  users,
				Right: posts,
				Type:  nodes.InnerJoin,
				On:    users.Col("id").Eq(posts.Col("author_id")),
			},
		},
		Wheres: []nodes.Node{
			users.Col("active").Eq(true),
			posts.Col("published").Eq(true),
		},
	}

	expected := `SELECT "users"."name", "users"."email" FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."author_id" WHERE "users"."active" = TRUE AND "posts"."published" = TRUE`
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc, expected)
}

func TestEndToEndMySQL(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")

	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("name"), users.Col("email")},
		Joins: []*nodes.JoinNode{
			{
				Left:  users,
				Right: posts,
				Type:  nodes.InnerJoin,
				On:    users.Col("id").Eq(posts.Col("author_id")),
			},
		},
		Wheres: []nodes.Node{
			users.Col("active").Eq(true),
			posts.Col("published").Eq(true),
		},
	}

	expected := "SELECT `users`.`name`, `users`.`email` FROM `users` INNER JOIN `posts` ON `users`.`id` = `posts`.`author_id` WHERE `users`.`active` = TRUE AND `posts`.`published` = TRUE"
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), sc, expected)
}

// --- Complex WHERE with combinators ---

func TestComplexWhereWithOrAndGrouping(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")

	// (status = 'active' OR role = 'admin') AND last_login > '2025-01-01'
	filter1 := users.Col("status").Eq("active").Or(users.Col("role").Eq("admin"))
	filter2 := users.Col("last_login").Gt("2025-01-01")

	sc := &nodes.SelectCore{
		From:   users,
		Wheres: []nodes.Node{filter1.And(filter2)},
	}

	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT * FROM "users" WHERE ("users"."status" = 'active' OR "users"."role" = 'admin') AND "users"."last_login" > '2025-01-01'`)
}

// --- Subquery in JOIN ---

func TestSubqueryInJoin(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")

	subquery := &nodes.SelectCore{
		From:        posts,
		Projections: []nodes.Node{nodes.Star()},
		Wheres:      []nodes.Node{posts.Col("created_at").Gt("2025-01-01")},
	}

	sc := &nodes.SelectCore{
		From: users,
		Joins: []*nodes.JoinNode{
			{
				Left:  users,
				Right: subquery,
				Type:  nodes.InnerJoin,
				On:    users.Col("id").Eq(posts.Col("author_id")),
			},
		},
	}

	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT * FROM "users" INNER JOIN (SELECT * FROM "posts" WHERE "posts"."created_at" > '2025-01-01') ON "users"."id" = "posts"."author_id"`)
}

// --- Identifier escaping ---

func TestIdentifierWithSpecialChars(t *testing.T) {
	t.Parallel()
	// Double quotes inside identifier names are escaped by doubling
	table := nodes.NewTable(`my"table`)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), table, `"my""table"`)

	// Backticks inside identifier names are escaped by doubling
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), nodes.NewTable("my`table"), "`my``table`")
}

// --- All dialects produce valid SQL for same AST ---

func TestDialectConsistency(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("id")},
		Wheres:      []nodes.Node{users.Col("active").Eq(true)},
	}

	pg := NewPostgresVisitor(WithoutParams())
	my := NewMySQLVisitor(WithoutParams())
	sl := NewSQLiteVisitor(WithoutParams())

	pgSQL := sc.Accept(pg)
	mySQL := sc.Accept(my)
	slSQL := sc.Accept(sl)

	if pgSQL != slSQL {
		t.Errorf("Postgres and SQLite should produce identical SQL:\n  PG: %s\n  SL: %s", pgSQL, slSQL)
	}

	// MySQL should differ only in quoting
	expectedMySQL := "SELECT `users`.`id` FROM `users` WHERE `users`.`active` = TRUE"
	if mySQL != expectedMySQL {
		t.Errorf("MySQL SQL mismatch:\n  expected: %s\n  got:      %s", expectedMySQL, mySQL)
	}
}

// --- GROUP BY ---

func TestVisitSelectCoreWithGroupBy(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("status"), nodes.NewSqlLiteral("COUNT(*)")},
		Groups:      []nodes.Node{users.Col("status")},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT "users"."status", COUNT(*) FROM "users" GROUP BY "users"."status"`)
}

func TestVisitSelectCoreWithMultipleGroupBy(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("status"), users.Col("role"), nodes.NewSqlLiteral("COUNT(*)")},
		Groups:      []nodes.Node{users.Col("status"), users.Col("role")},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT "users"."status", "users"."role", COUNT(*) FROM "users" GROUP BY "users"."status", "users"."role"`)
}

// --- HAVING ---

func TestVisitSelectCoreWithHaving(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("status"), nodes.NewSqlLiteral("COUNT(*)")},
		Groups:      []nodes.Node{users.Col("status")},
		Havings:     []nodes.Node{nodes.NewSqlLiteral("COUNT(*)").Gt(5)},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT "users"."status", COUNT(*) FROM "users" GROUP BY "users"."status" HAVING COUNT(*) > 5`)
}

func TestVisitSelectCoreWithMultipleHavings(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:    users,
		Groups:  []nodes.Node{users.Col("status")},
		Havings: []nodes.Node{nodes.NewSqlLiteral("COUNT(*)").Gt(5), nodes.NewSqlLiteral("COUNT(*)").Lt(100)},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT * FROM "users" GROUP BY "users"."status" HAVING COUNT(*) > 5 AND COUNT(*) < 100`)
}

// --- GROUP BY + HAVING + WHERE + ORDER BY ---

func TestVisitSelectCoreGroupByFullQuery(t *testing.T) {
	t.Parallel()
	orders := nodes.NewTable("orders")
	sc := &nodes.SelectCore{
		From:        orders,
		Projections: []nodes.Node{orders.Col("customer_id"), nodes.NewSqlLiteral("SUM(amount)")},
		Wheres:      []nodes.Node{orders.Col("status").Eq("completed")},
		Groups:      []nodes.Node{orders.Col("customer_id")},
		Havings:     []nodes.Node{nodes.NewSqlLiteral("SUM(amount)").Gt(100)},
		Orders:      []nodes.Node{orders.Col("customer_id").Asc()},
		Limit:       nodes.Literal(10),
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT "orders"."customer_id", SUM(amount) FROM "orders" WHERE "orders"."status" = 'completed' GROUP BY "orders"."customer_id" HAVING SUM(amount) > 100 ORDER BY "orders"."customer_id" ASC LIMIT 10`)
}

func TestVisitSelectCoreGroupByMySQL(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("status"), nodes.NewSqlLiteral("COUNT(*)")},
		Groups:      []nodes.Node{users.Col("status")},
		Havings:     []nodes.Node{nodes.NewSqlLiteral("COUNT(*)").Gt(5)},
	}
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), sc,
		"SELECT `users`.`status`, COUNT(*) FROM `users` GROUP BY `users`.`status` HAVING COUNT(*) > 5")
}

// --- Ordering ---

func TestVisitOrderingAsc(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("name")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), col.Asc(), `"users"."name" ASC`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), col.Asc(), "`users`.`name` ASC")
}

func TestVisitOrderingDesc(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("created_at")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), col.Desc(), `"users"."created_at" DESC`)
}

// --- SelectCore with ORDER BY ---

func TestVisitSelectCoreWithOrderBy(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:   users,
		Orders: []nodes.Node{users.Col("name").Asc()},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT * FROM "users" ORDER BY "users"."name" ASC`)
}

func TestVisitSelectCoreWithMultipleOrders(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:   users,
		Orders: []nodes.Node{users.Col("name").Asc(), users.Col("id").Desc()},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT * FROM "users" ORDER BY "users"."name" ASC, "users"."id" DESC`)
}

// --- SelectCore with LIMIT ---

func TestVisitSelectCoreWithLimit(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:  users,
		Limit: nodes.Literal(10),
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT * FROM "users" LIMIT 10`)
}

// --- SelectCore with OFFSET ---

func TestVisitSelectCoreWithOffset(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:   users,
		Limit:  nodes.Literal(10),
		Offset: nodes.Literal(20),
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT * FROM "users" LIMIT 10 OFFSET 20`)
}

// --- Full query with ORDER BY, LIMIT, OFFSET ---

func TestVisitSelectCoreFullWithOrdering(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("id"), users.Col("name")},
		Wheres:      []nodes.Node{users.Col("active").Eq(true)},
		Orders:      []nodes.Node{users.Col("name").Asc()},
		Limit:       nodes.Literal(25),
		Offset:      nodes.Literal(50),
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT "users"."id", "users"."name" FROM "users" WHERE "users"."active" = TRUE ORDER BY "users"."name" ASC LIMIT 25 OFFSET 50`)
}

func TestVisitSelectCoreOrderByMySQL(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:   users,
		Orders: []nodes.Node{users.Col("name").Desc()},
		Limit:  nodes.Literal(10),
	}
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), sc,
		"SELECT * FROM `users` ORDER BY `users`.`name` DESC LIMIT 10")
}

// --- DISTINCT ---

func TestVisitSelectCoreWithDistinct(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:     users,
		Distinct: true,
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT DISTINCT * FROM "users"`)
}

func TestVisitSelectCoreDistinctWithProjections(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("name"), users.Col("email")},
		Distinct:    true,
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT DISTINCT "users"."name", "users"."email" FROM "users"`)
}

func TestVisitSelectCoreDistinctMySQL(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:     users,
		Distinct: true,
	}
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), sc,
		"SELECT DISTINCT * FROM `users`")
}

func TestVisitSelectCoreDistinctFalse(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:     users,
		Distinct: false,
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT * FROM "users"`)
}

// --- Compile-time: all visitors implement nodes.Visitor ---

func TestVisitorsImplementInterface(t *testing.T) {
	t.Parallel()
	var _ nodes.Visitor = NewPostgresVisitor(WithoutParams())
	var _ nodes.Visitor = NewMySQLVisitor(WithoutParams())
	var _ nodes.Visitor = NewSQLiteVisitor(WithoutParams())
}

// --- Multiple joins ---

func TestMultipleJoins(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	comments := nodes.NewTable("comments")

	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("name"), posts.Col("title"), comments.Col("body")},
		Joins: []*nodes.JoinNode{
			{
				Left:  users,
				Right: posts,
				Type:  nodes.InnerJoin,
				On:    users.Col("id").Eq(posts.Col("user_id")),
			},
			{
				Left:  posts,
				Right: comments,
				Type:  nodes.LeftOuterJoin,
				On:    posts.Col("id").Eq(comments.Col("post_id")),
			},
		},
	}

	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT "users"."name", "posts"."title", "comments"."body" FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id" LEFT OUTER JOIN "comments" ON "posts"."id" = "comments"."post_id"`)
}

// --- SqlLiteral in projections ---

func TestSqlLiteralInProjection(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{nodes.NewSqlLiteral("COUNT(*)")},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc, `SELECT COUNT(*) FROM "users"`)
}

// --- SqlLiteral in comparisons ---

func TestSqlLiteralInComparison(t *testing.T) {
	t.Parallel()
	raw := nodes.NewSqlLiteral("COUNT(*)")
	cmp := raw.Gt(0)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `COUNT(*) > 0`)
}

// --- IN with node values ---

func TestInWithNodeValues(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	in := &nodes.InNode{
		Expr: users.Col("id"),
		Vals: []nodes.Node{
			nodes.Literal(1),
			nodes.Literal(2),
			nodes.Literal(3),
		},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), in, `"users"."id" IN (1, 2, 3)`)
}

// --- Parameterized queries ---

func assertParams(t *testing.T, v nodes.Visitor, node nodes.Node, expectedSQL string, expectedParams []any) {
	t.Helper()
	if p, ok := v.(nodes.Parameterizer); ok {
		p.Reset()
	}
	got := node.Accept(v)
	if got != expectedSQL {
		t.Errorf("SQL:\n  expected: %s\n  got:      %s", expectedSQL, got)
	}
	if p, ok := v.(nodes.Parameterizer); ok {
		params := p.Params()
		if len(params) != len(expectedParams) {
			t.Errorf("params count: expected %d, got %d: %v", len(expectedParams), len(params), params)
			return
		}
		for i, ep := range expectedParams {
			if params[i] != ep {
				t.Errorf("params[%d]: expected %v (%T), got %v (%T)", i, ep, ep, params[i], params[i])
			}
		}
	}
}

func TestParamPostgresSimpleWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:   users,
		Wheres: []nodes.Node{users.Col("name").Eq("Alice")},
	}
	v := NewPostgresVisitor(WithParams())
	assertParams(t, v, sc,
		`SELECT * FROM "users" WHERE "users"."name" = $1`,
		[]any{"Alice"})
}

func TestParamMySQLSimpleWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:   users,
		Wheres: []nodes.Node{users.Col("name").Eq("Alice")},
	}
	v := NewMySQLVisitor(WithParams())
	assertParams(t, v, sc,
		"SELECT * FROM `users` WHERE `users`.`name` = ?",
		[]any{"Alice"})
}

func TestParamSQLiteSimpleWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:   users,
		Wheres: []nodes.Node{users.Col("name").Eq("Alice")},
	}
	v := NewSQLiteVisitor(WithParams())
	assertParams(t, v, sc,
		`SELECT * FROM "users" WHERE "users"."name" = ?`,
		[]any{"Alice"})
}

func TestParamPostgresMultipleValues(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:   users,
		Wheres: []nodes.Node{users.Col("name").Eq("Alice"), users.Col("age").Gt(30)},
	}
	v := NewPostgresVisitor(WithParams())
	assertParams(t, v, sc,
		`SELECT * FROM "users" WHERE "users"."name" = $1 AND "users"."age" > $2`,
		[]any{"Alice", 30})
}

func TestParamIn(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:   users,
		Wheres: []nodes.Node{users.Col("status").In("active", "pending")},
	}
	v := NewPostgresVisitor(WithParams())
	assertParams(t, v, sc,
		`SELECT * FROM "users" WHERE "users"."status" IN ($1, $2)`,
		[]any{"active", "pending"})
}

func TestParamBetween(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:   users,
		Wheres: []nodes.Node{users.Col("age").Between(18, 65)},
	}
	v := NewPostgresVisitor(WithParams())
	assertParams(t, v, sc,
		`SELECT * FROM "users" WHERE "users"."age" BETWEEN $1 AND $2`,
		[]any{18, 65})
}

func TestParamLimitOffset(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:   users,
		Limit:  nodes.Literal(10),
		Offset: nodes.Literal(20),
	}
	v := NewPostgresVisitor(WithParams())
	assertParams(t, v, sc,
		`SELECT * FROM "users" LIMIT $1 OFFSET $2`,
		[]any{10, 20})
}

func TestParamHaving(t *testing.T) {
	t.Parallel()
	orders := nodes.NewTable("orders")
	sc := &nodes.SelectCore{
		From:    orders,
		Groups:  []nodes.Node{orders.Col("customer_id")},
		Havings: []nodes.Node{orders.Col("total").Gt(100)},
	}
	v := NewPostgresVisitor(WithParams())
	assertParams(t, v, sc,
		`SELECT * FROM "orders" GROUP BY "orders"."customer_id" HAVING "orders"."total" > $1`,
		[]any{100})
}

func TestParamNullNotParameterized(t *testing.T) {
	t.Parallel()
	n := &nodes.LiteralNode{Value: nil}
	v := NewPostgresVisitor(WithParams())
	v.Reset()
	got := n.Accept(v)
	if got != "NULL" {
		t.Errorf("expected NULL, got %s", got)
	}
	if len(v.Params()) != 0 {
		t.Errorf("expected no params for NULL, got %v", v.Params())
	}
}

func TestParamSqlLiteralNotParameterized(t *testing.T) {
	t.Parallel()
	raw := nodes.NewSqlLiteral("COUNT(*)")
	v := NewPostgresVisitor(WithParams())
	v.Reset()
	got := raw.Accept(v)
	if got != "COUNT(*)" {
		t.Errorf("expected COUNT(*), got %s", got)
	}
	if len(v.Params()) != 0 {
		t.Errorf("expected no params for SqlLiteral, got %v", v.Params())
	}
}

func TestParamNodeToNodeNoParams(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	cmp := users.Col("id").Eq(posts.Col("author_id"))
	v := NewPostgresVisitor(WithParams())
	v.Reset()
	got := cmp.Accept(v)
	if got != `"users"."id" = "posts"."author_id"` {
		t.Errorf("expected node-to-node SQL, got %s", got)
	}
	if len(v.Params()) != 0 {
		t.Errorf("expected no params for node-to-node comparison, got %v", v.Params())
	}
}

func TestParamReset(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	cmp := users.Col("name").Eq("Alice")
	v := NewPostgresVisitor(WithParams())

	// First generation
	v.Reset()
	cmp.Accept(v)
	if len(v.Params()) != 1 {
		t.Fatalf("expected 1 param after first Accept, got %d", len(v.Params()))
	}

	// Reset and regenerate
	v.Reset()
	if len(v.Params()) != 0 {
		t.Fatalf("expected 0 params after Reset, got %d", len(v.Params()))
	}
	got := cmp.Accept(v)
	if got != `"users"."name" = $1` {
		t.Errorf("expected $1 after reset, got %s", got)
	}
	if len(v.Params()) != 1 || v.Params()[0] != "Alice" {
		t.Errorf("expected [Alice], got %v", v.Params())
	}
}

func TestParamSubqueryInJoin(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")

	subquery := &nodes.SelectCore{
		From:        posts,
		Projections: []nodes.Node{nodes.Star()},
		Wheres:      []nodes.Node{posts.Col("created_at").Gt("2025-01-01")},
	}

	sc := &nodes.SelectCore{
		From: users,
		Joins: []*nodes.JoinNode{
			{
				Left:  users,
				Right: subquery,
				Type:  nodes.InnerJoin,
				On:    users.Col("id").Eq(posts.Col("author_id")),
			},
		},
	}

	v := NewPostgresVisitor(WithParams())
	assertParams(t, v, sc,
		`SELECT * FROM "users" INNER JOIN (SELECT * FROM "posts" WHERE "posts"."created_at" > $1) ON "users"."id" = "posts"."author_id"`,
		[]any{"2025-01-01"})
}

func TestParamMySQLMultiple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:   users,
		Wheres: []nodes.Node{users.Col("name").Eq("Bob"), users.Col("active").Eq(true)},
		Limit:  nodes.Literal(5),
	}
	v := NewMySQLVisitor(WithParams())
	assertParams(t, v, sc,
		"SELECT * FROM `users` WHERE `users`.`name` = ? AND `users`.`active` = ? LIMIT ?",
		[]any{"Bob", true, 5})
}

func TestParamFullQuery(t *testing.T) {
	t.Parallel()
	orders := nodes.NewTable("orders")
	sc := &nodes.SelectCore{
		From:        orders,
		Projections: []nodes.Node{orders.Col("customer_id")},
		Wheres:      []nodes.Node{orders.Col("status").Eq("completed")},
		Groups:      []nodes.Node{orders.Col("customer_id")},
		Havings:     []nodes.Node{orders.Col("total").Gt(100)},
		Orders:      []nodes.Node{orders.Col("customer_id").Asc()},
		Limit:       nodes.Literal(10),
		Offset:      nodes.Literal(20),
	}
	v := NewPostgresVisitor(WithParams())
	assertParams(t, v, sc,
		`SELECT "orders"."customer_id" FROM "orders" WHERE "orders"."status" = $1 GROUP BY "orders"."customer_id" HAVING "orders"."total" > $2 ORDER BY "orders"."customer_id" ASC LIMIT $3 OFFSET $4`,
		[]any{"completed", 100, 10, 20})
}

// Compile-time: parameterized visitors implement Parameterizer.
func TestParameterizedVisitorsImplementParameterizer(t *testing.T) {
	t.Parallel()
	var _ nodes.Parameterizer = NewPostgresVisitor(WithParams())
	var _ nodes.Parameterizer = NewMySQLVisitor(WithParams())
	var _ nodes.Parameterizer = NewSQLiteVisitor(WithParams())
}

func TestNonParameterizedVisitorAlsoImplementsParameterizer(t *testing.T) {
	t.Parallel()
	// Even without WithParams, the visitor has the methods (just returns nil params).
	var _ nodes.Parameterizer = NewPostgresVisitor(WithoutParams())
}

// --- INSERT ---

func TestVisitInsertSingleRow(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:    users,
		Columns: []nodes.Node{nodes.NewAttribute(users, "name"), nodes.NewAttribute(users, "email")},
		Values:  [][]nodes.Node{{nodes.Literal("Alice"), nodes.Literal("alice@example.com")}},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`INSERT INTO "users" ("name", "email") VALUES ('Alice', 'alice@example.com')`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), stmt,
		"INSERT INTO `users` (`name`, `email`) VALUES ('Alice', 'alice@example.com')")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), stmt,
		`INSERT INTO "users" ("name", "email") VALUES ('Alice', 'alice@example.com')`)
}

func TestVisitInsertMultiRow(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:    users,
		Columns: []nodes.Node{nodes.NewAttribute(users, "name")},
		Values: [][]nodes.Node{
			{nodes.Literal("Alice")},
			{nodes.Literal("Bob")},
			{nodes.Literal("Carol")},
		},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`INSERT INTO "users" ("name") VALUES ('Alice'), ('Bob'), ('Carol')`)
}

func TestVisitInsertMultiColumn(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:    users,
		Columns: []nodes.Node{nodes.NewAttribute(users, "name"), nodes.NewAttribute(users, "age")},
		Values:  [][]nodes.Node{{nodes.Literal("Alice"), nodes.Literal(30)}, {nodes.Literal("Bob"), nodes.Literal(25)}},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`INSERT INTO "users" ("name", "age") VALUES ('Alice', 30), ('Bob', 25)`)
}

func TestVisitInsertFromSelect(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	archive := nodes.NewTable("archive")
	subquery := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("name"), users.Col("email")},
		Wheres:      []nodes.Node{users.Col("active").Eq(false)},
	}
	stmt := &nodes.InsertStatement{
		Into:    archive,
		Columns: []nodes.Node{nodes.NewAttribute(archive, "name"), nodes.NewAttribute(archive, "email")},
		Select:  subquery,
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`INSERT INTO "archive" ("name", "email") SELECT "users"."name", "users"."email" FROM "users" WHERE "users"."active" = FALSE`)
}

func TestVisitInsertReturning(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:      users,
		Columns:   []nodes.Node{nodes.NewAttribute(users, "name")},
		Values:    [][]nodes.Node{{nodes.Literal("Alice")}},
		Returning: []nodes.Node{users.Col("id"), users.Col("name")},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`INSERT INTO "users" ("name") VALUES ('Alice') RETURNING "users"."id", "users"."name"`)
}

func TestVisitInsertOnConflictDoNothing(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:    users,
		Columns: []nodes.Node{nodes.NewAttribute(users, "email"), nodes.NewAttribute(users, "name")},
		Values:  [][]nodes.Node{{nodes.Literal("a@b.com"), nodes.Literal("Alice")}},
		OnConflict: &nodes.OnConflictNode{
			Columns: []nodes.Node{nodes.NewAttribute(users, "email")},
			Action:  nodes.DoNothing,
		},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`INSERT INTO "users" ("email", "name") VALUES ('a@b.com', 'Alice') ON CONFLICT ("email") DO NOTHING`)
}

func TestVisitInsertOnConflictDoUpdate(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:    users,
		Columns: []nodes.Node{nodes.NewAttribute(users, "email"), nodes.NewAttribute(users, "name")},
		Values:  [][]nodes.Node{{nodes.Literal("a@b.com"), nodes.Literal("Alice")}},
		OnConflict: &nodes.OnConflictNode{
			Columns: []nodes.Node{nodes.NewAttribute(users, "email")},
			Action:  nodes.DoUpdate,
			Assignments: []*nodes.AssignmentNode{
				{Left: nodes.NewAttribute(users, "name"), Right: nodes.Literal("Alice")},
			},
		},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`INSERT INTO "users" ("email", "name") VALUES ('a@b.com', 'Alice') ON CONFLICT ("email") DO UPDATE SET "users"."name" = 'Alice'`)
}

func TestVisitInsertOnConflictDoUpdateWithWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:    users,
		Columns: []nodes.Node{nodes.NewAttribute(users, "email"), nodes.NewAttribute(users, "name")},
		Values:  [][]nodes.Node{{nodes.Literal("a@b.com"), nodes.Literal("Alice")}},
		OnConflict: &nodes.OnConflictNode{
			Columns: []nodes.Node{nodes.NewAttribute(users, "email")},
			Action:  nodes.DoUpdate,
			Assignments: []*nodes.AssignmentNode{
				{Left: nodes.NewAttribute(users, "name"), Right: nodes.Literal("Alice")},
			},
			Wheres: []nodes.Node{users.Col("locked").Eq(false)},
		},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`INSERT INTO "users" ("email", "name") VALUES ('a@b.com', 'Alice') ON CONFLICT ("email") DO UPDATE SET "users"."name" = 'Alice' WHERE "users"."locked" = FALSE`)
}

// --- UPDATE ---

func TestVisitUpdateSimple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.UpdateStatement{
		Table: users,
		Assignments: []*nodes.AssignmentNode{
			{Left: users.Col("name"), Right: nodes.Literal("Bob")},
		},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`UPDATE "users" SET "users"."name" = 'Bob'`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), stmt,
		"UPDATE `users` SET `users`.`name` = 'Bob'")
}

func TestVisitUpdateMultipleColumns(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.UpdateStatement{
		Table: users,
		Assignments: []*nodes.AssignmentNode{
			{Left: users.Col("name"), Right: nodes.Literal("Bob")},
			{Left: users.Col("age"), Right: nodes.Literal(30)},
		},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`UPDATE "users" SET "users"."name" = 'Bob', "users"."age" = 30`)
}

func TestVisitUpdateWithWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.UpdateStatement{
		Table: users,
		Assignments: []*nodes.AssignmentNode{
			{Left: users.Col("active"), Right: nodes.Literal(false)},
		},
		Wheres: []nodes.Node{users.Col("id").Eq(1)},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`UPDATE "users" SET "users"."active" = FALSE WHERE "users"."id" = 1`)
}

func TestVisitUpdateWithMultipleWheres(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.UpdateStatement{
		Table: users,
		Assignments: []*nodes.AssignmentNode{
			{Left: users.Col("active"), Right: nodes.Literal(false)},
		},
		Wheres: []nodes.Node{users.Col("role").Eq("guest"), users.Col("last_login").Lt("2025-01-01")},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`UPDATE "users" SET "users"."active" = FALSE WHERE "users"."role" = 'guest' AND "users"."last_login" < '2025-01-01'`)
}

func TestVisitUpdateReturning(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.UpdateStatement{
		Table: users,
		Assignments: []*nodes.AssignmentNode{
			{Left: users.Col("name"), Right: nodes.Literal("Bob")},
		},
		Wheres:    []nodes.Node{users.Col("id").Eq(1)},
		Returning: []nodes.Node{users.Col("id"), users.Col("name")},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`UPDATE "users" SET "users"."name" = 'Bob' WHERE "users"."id" = 1 RETURNING "users"."id", "users"."name"`)
}

// --- DELETE ---

func TestVisitDeleteSimple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.DeleteStatement{From: users}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`DELETE FROM "users"`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), stmt,
		"DELETE FROM `users`")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), stmt,
		`DELETE FROM "users"`)
}

func TestVisitDeleteWithWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.DeleteStatement{
		From:   users,
		Wheres: []nodes.Node{users.Col("id").Eq(1)},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`DELETE FROM "users" WHERE "users"."id" = 1`)
}

func TestVisitDeleteWithMultipleWheres(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.DeleteStatement{
		From:   users,
		Wheres: []nodes.Node{users.Col("active").Eq(false), users.Col("role").Eq("guest")},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`DELETE FROM "users" WHERE "users"."active" = FALSE AND "users"."role" = 'guest'`)
}

func TestVisitDeleteReturning(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.DeleteStatement{
		From:      users,
		Wheres:    []nodes.Node{users.Col("id").Eq(1)},
		Returning: []nodes.Node{users.Col("id")},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), stmt,
		`DELETE FROM "users" WHERE "users"."id" = 1 RETURNING "users"."id"`)
}

// --- Assignment ---

func TestVisitAssignment(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	a := &nodes.AssignmentNode{Left: users.Col("name"), Right: nodes.Literal("Alice")}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), a, `"users"."name" = 'Alice'`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), a, "`users`.`name` = 'Alice'")
}

// --- Parameterized DML ---

func TestParamInsertSingleRow(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:    users,
		Columns: []nodes.Node{nodes.NewAttribute(users, "name"), nodes.NewAttribute(users, "age")},
		Values:  [][]nodes.Node{{nodes.Literal("Alice"), nodes.Literal(30)}},
	}
	assertParams(t, NewPostgresVisitor(WithParams()), stmt,
		`INSERT INTO "users" ("name", "age") VALUES ($1, $2)`,
		[]any{"Alice", 30})
}

func TestParamInsertMultiRow(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:    users,
		Columns: []nodes.Node{nodes.NewAttribute(users, "name")},
		Values: [][]nodes.Node{
			{nodes.Literal("Alice")},
			{nodes.Literal("Bob")},
		},
	}
	assertParams(t, NewPostgresVisitor(WithParams()), stmt,
		`INSERT INTO "users" ("name") VALUES ($1), ($2)`,
		[]any{"Alice", "Bob"})
}

func TestParamInsertMySQL(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:    users,
		Columns: []nodes.Node{nodes.NewAttribute(users, "name"), nodes.NewAttribute(users, "age")},
		Values:  [][]nodes.Node{{nodes.Literal("Alice"), nodes.Literal(30)}},
	}
	assertParams(t, NewMySQLVisitor(WithParams()), stmt,
		"INSERT INTO `users` (`name`, `age`) VALUES (?, ?)",
		[]any{"Alice", 30})
}

func TestParamInsertNullValue(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:    users,
		Columns: []nodes.Node{nodes.NewAttribute(users, "name"), nodes.NewAttribute(users, "email")},
		Values:  [][]nodes.Node{{nodes.Literal("Alice"), &nodes.LiteralNode{Value: nil}}},
	}
	assertParams(t, NewPostgresVisitor(WithParams()), stmt,
		`INSERT INTO "users" ("name", "email") VALUES ($1, NULL)`,
		[]any{"Alice"})
}

func TestParamUpdateWithWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.UpdateStatement{
		Table: users,
		Assignments: []*nodes.AssignmentNode{
			{Left: users.Col("name"), Right: nodes.Literal("Bob")},
		},
		Wheres: []nodes.Node{users.Col("id").Eq(1)},
	}
	assertParams(t, NewPostgresVisitor(WithParams()), stmt,
		`UPDATE "users" SET "users"."name" = $1 WHERE "users"."id" = $2`,
		[]any{"Bob", 1})
}

func TestParamUpdateMySQL(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.UpdateStatement{
		Table: users,
		Assignments: []*nodes.AssignmentNode{
			{Left: users.Col("name"), Right: nodes.Literal("Bob")},
			{Left: users.Col("age"), Right: nodes.Literal(30)},
		},
		Wheres: []nodes.Node{users.Col("id").Eq(1)},
	}
	assertParams(t, NewMySQLVisitor(WithParams()), stmt,
		"UPDATE `users` SET `users`.`name` = ?, `users`.`age` = ? WHERE `users`.`id` = ?",
		[]any{"Bob", 30, 1})
}

func TestParamDeleteWithWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.DeleteStatement{
		From:   users,
		Wheres: []nodes.Node{users.Col("id").Eq(1)},
	}
	assertParams(t, NewPostgresVisitor(WithParams()), stmt,
		`DELETE FROM "users" WHERE "users"."id" = $1`,
		[]any{1})
}

func TestParamDeleteMySQL(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.DeleteStatement{
		From:   users,
		Wheres: []nodes.Node{users.Col("active").Eq(false), users.Col("role").Eq("guest")},
	}
	assertParams(t, NewMySQLVisitor(WithParams()), stmt,
		"DELETE FROM `users` WHERE `users`.`active` = ? AND `users`.`role` = ?",
		[]any{false, "guest"})
}

func TestParamInsertOnConflictDoUpdate(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:    users,
		Columns: []nodes.Node{nodes.NewAttribute(users, "email"), nodes.NewAttribute(users, "name")},
		Values:  [][]nodes.Node{{nodes.Literal("a@b.com"), nodes.Literal("Alice")}},
		OnConflict: &nodes.OnConflictNode{
			Columns: []nodes.Node{nodes.NewAttribute(users, "email")},
			Action:  nodes.DoUpdate,
			Assignments: []*nodes.AssignmentNode{
				{Left: users.Col("name"), Right: nodes.Literal("Alice")},
			},
		},
	}
	assertParams(t, NewPostgresVisitor(WithParams()), stmt,
		`INSERT INTO "users" ("email", "name") VALUES ($1, $2) ON CONFLICT ("email") DO UPDATE SET "users"."name" = $3`,
		[]any{"a@b.com", "Alice", "Alice"})
}

func TestParamInsertReturning(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.InsertStatement{
		Into:      users,
		Columns:   []nodes.Node{nodes.NewAttribute(users, "name")},
		Values:    [][]nodes.Node{{nodes.Literal("Alice")}},
		Returning: []nodes.Node{users.Col("id")},
	}
	assertParams(t, NewPostgresVisitor(WithParams()), stmt,
		`INSERT INTO "users" ("name") VALUES ($1) RETURNING "users"."id"`,
		[]any{"Alice"})
}

func TestParamUpdateReturning(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	stmt := &nodes.UpdateStatement{
		Table: users,
		Assignments: []*nodes.AssignmentNode{
			{Left: users.Col("active"), Right: nodes.Literal(false)},
		},
		Wheres:    []nodes.Node{users.Col("id").Eq(1)},
		Returning: []nodes.Node{users.Col("id")},
	}
	assertParams(t, NewPostgresVisitor(WithParams()), stmt,
		`UPDATE "users" SET "users"."active" = $1 WHERE "users"."id" = $2 RETURNING "users"."id"`,
		[]any{false, 1})
}

// --- Arithmetic / Math operations ---

func TestVisitPlus(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("age")
	n := col.Plus(5)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `"users"."age" + 5`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), n, "`users`.`age` + 5")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), n, `"users"."age" + 5`)
}

func TestVisitMinus(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("x")
	n := col.Minus(3)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `"t"."x" - 3`)
}

func TestVisitMultiply(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("x")
	n := col.Multiply(2)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `"t"."x" * 2`)
}

func TestVisitDivide(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("x")
	n := col.Divide(4)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `"t"."x" / 4`)
}

func TestVisitBitwiseAnd(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("flags")
	n := col.BitwiseAnd(0xFF)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `"t"."flags" & 255`)
}

func TestVisitBitwiseOr(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("flags")
	n := col.BitwiseOr(0x01)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `"t"."flags" | 1`)
}

func TestVisitBitwiseXor(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("flags")
	n := col.BitwiseXor(0x0F)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `"t"."flags" ^ 15`)
}

func TestVisitShiftLeft(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("x")
	n := col.ShiftLeft(2)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `"t"."x" << 2`)
}

func TestVisitShiftRight(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("x")
	n := col.ShiftRight(1)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `"t"."x" >> 1`)
}

func TestVisitConcat(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("first")
	n := col.Concat(" ")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `"t"."first" || ' '`)
}

func TestVisitBitwiseNot(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("flags")
	n := col.BitwiseNot()
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `~"t"."flags"`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), n, "~`t`.`flags`")
}

func TestVisitArithmeticChained(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("age")
	// (age + 5) * 3 — auto-parenthesized
	n := col.Plus(5).Multiply(3)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `("users"."age" + 5) * 3`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), n, "(`users`.`age` + 5) * 3")
}

func TestVisitArithmeticInWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	// WHERE age + 5 > 10
	cond := users.Col("age").Plus(5).Gt(10)
	sc := &nodes.SelectCore{
		From:   users,
		Wheres: []nodes.Node{cond},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT * FROM "users" WHERE "users"."age" + 5 > 10`)
}

func TestVisitArithmeticNodeToNode(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	n := users.Col("age").Plus(users.Col("bonus"))
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `"users"."age" + "users"."bonus"`)
}

func TestVisitConcatNodeToNode(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	n := users.Col("first").Concat(users.Col("last"))
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `"users"."first" || "users"."last"`)
}

func TestVisitBitwiseNotChained(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("flags")
	// ~flags & 0xFF — auto-parenthesized
	n := col.BitwiseNot().BitwiseAnd(0xFF)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `(~"t"."flags") & 255`)
}

func TestVisitArithmeticDeepChain(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("x")
	// ((x + 1) * 2) - 3
	n := col.Plus(1).Multiply(2).Minus(3)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `(("t"."x" + 1) * 2) - 3`)
}

func TestVisitArithmeticAllDialects(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("price")
	n := col.Multiply(100).Divide(100)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `("t"."price" * 100) / 100`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), n, "(`t`.`price` * 100) / 100")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), n, `("t"."price" * 100) / 100`)
}

// --- Parameterized arithmetic ---

func TestParamArithmeticPlus(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("age")
	n := col.Plus(5)
	assertParams(t, NewPostgresVisitor(WithParams()), n,
		`"users"."age" + $1`, []any{5})
}

func TestParamArithmeticChained(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("age")
	n := col.Plus(5).Multiply(3)
	assertParams(t, NewPostgresVisitor(WithParams()), n,
		`("users"."age" + $1) * $2`, []any{5, 3})
}

func TestParamArithmeticInWhere(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	cond := users.Col("age").Plus(5).Gt(10)
	sc := &nodes.SelectCore{
		From:   users,
		Wheres: []nodes.Node{cond},
	}
	assertParams(t, NewPostgresVisitor(WithParams()), sc,
		`SELECT * FROM "users" WHERE "users"."age" + $1 > $2`,
		[]any{5, 10})
}

func TestParamArithmeticMySQL(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("x")
	n := col.Plus(10)
	assertParams(t, NewMySQLVisitor(WithParams()), n,
		"`t`.`x` + ?", []any{10})
}

func TestParamBitwiseNot(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("flags")
	n := col.BitwiseNot()
	v := NewPostgresVisitor(WithParams())
	v.Reset()
	got := n.Accept(v)
	if got != `~"t"."flags"` {
		t.Errorf("expected ~\"t\".\"flags\", got %s", got)
	}
	if len(v.Params()) != 0 {
		t.Errorf("expected no params for bitwise not, got %v", v.Params())
	}
}

func TestParamArithmeticNodeToNodeNoParams(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	n := users.Col("age").Plus(users.Col("bonus"))
	v := NewPostgresVisitor(WithParams())
	v.Reset()
	got := n.Accept(v)
	if got != `"users"."age" + "users"."bonus"` {
		t.Errorf("expected node-to-node arithmetic, got %s", got)
	}
	if len(v.Params()) != 0 {
		t.Errorf("expected no params for node-to-node, got %v", v.Params())
	}
}

// --- NOT BETWEEN ---

func TestVisitNotBetween(t *testing.T) {
	t.Parallel()
	b := nodes.NewTable("t").Col("age").NotBetween(18, 65)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), b, `"t"."age" NOT BETWEEN 18 AND 65`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), b, "`t`.`age` NOT BETWEEN 18 AND 65")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), b, `"t"."age" NOT BETWEEN 18 AND 65`)
}

func TestParamNotBetween(t *testing.T) {
	t.Parallel()
	b := nodes.NewTable("t").Col("age").NotBetween(18, 65)
	assertParams(t, NewPostgresVisitor(WithParams()), b,
		`"t"."age" NOT BETWEEN $1 AND $2`, []any{18, 65})
	assertParams(t, NewMySQLVisitor(WithParams()), b,
		"`t`.`age` NOT BETWEEN ? AND ?", []any{18, 65})
}

// --- REGEXP ---

func TestVisitRegexpPostgres(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").MatchesRegexp("^A.*")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."name" ~ '^A.*'`)
}

func TestVisitRegexpMySQL(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").MatchesRegexp("^A.*")
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), cmp, "`t`.`name` REGEXP '^A.*'")
}

func TestVisitRegexpSQLite(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").MatchesRegexp("^A.*")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), cmp, `"t"."name" REGEXP '^A.*'`)
}

func TestVisitNotRegexpPostgres(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").DoesNotMatchRegexp("^A.*")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."name" !~ '^A.*'`)
}

func TestVisitNotRegexpMySQL(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").DoesNotMatchRegexp("^A.*")
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), cmp, "`t`.`name` NOT REGEXP '^A.*'")
}

func TestVisitNotRegexpSQLite(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").DoesNotMatchRegexp("^A.*")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), cmp, `"t"."name" NOT REGEXP '^A.*'`)
}

// --- IS DISTINCT FROM ---

func TestVisitIsDistinctFrom(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("x").IsDistinctFrom(nil)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."x" IS DISTINCT FROM NULL`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), cmp, "`t`.`x` IS DISTINCT FROM NULL")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), cmp, `"t"."x" IS DISTINCT FROM NULL`)
}

func TestVisitIsNotDistinctFrom(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("x").IsNotDistinctFrom(42)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."x" IS NOT DISTINCT FROM 42`)
}

func TestParamIsDistinctFrom(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("x").IsDistinctFrom(5)
	assertParams(t, NewPostgresVisitor(WithParams()), cmp,
		`"t"."x" IS DISTINCT FROM $1`, []any{5})
}

// --- CASE SENSITIVE / INSENSITIVE EQ ---

func TestVisitCaseSensitiveEqPostgres(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").CaseSensitiveEq("Alice")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."name" = 'Alice'`)
}

func TestVisitCaseSensitiveEqMySQL(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").CaseSensitiveEq("Alice")
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), cmp, "`t`.`name` = BINARY 'Alice'")
}

func TestVisitCaseSensitiveEqSQLite(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").CaseSensitiveEq("Alice")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), cmp, `"t"."name" = 'Alice' COLLATE BINARY`)
}

func TestVisitCaseInsensitiveEqPostgres(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").CaseInsensitiveEq("alice")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `LOWER("t"."name") = LOWER('alice')`)
}

func TestVisitCaseInsensitiveEqMySQL(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").CaseInsensitiveEq("alice")
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), cmp, "`t`.`name` = 'alice'")
}

func TestVisitCaseInsensitiveEqSQLite(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").CaseInsensitiveEq("alice")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), cmp, `"t"."name" = 'alice' COLLATE NOCASE`)
}

func TestParamCaseSensitiveEqMySQL(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").CaseSensitiveEq("Alice")
	assertParams(t, NewMySQLVisitor(WithParams()), cmp,
		"`t`.`name` = BINARY ?", []any{"Alice"})
}

func TestParamCaseInsensitiveEqPostgres(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").CaseInsensitiveEq("alice")
	assertParams(t, NewPostgresVisitor(WithParams()), cmp,
		`LOWER("t"."name") = LOWER($1)`, []any{"alice"})
}

// --- CONTAINS (@>) ---

func TestVisitContains(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("tags").Contains("{1,2}")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."tags" @> '{1,2}'`)
}

func TestParamContains(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("tags").Contains("{1,2}")
	assertParams(t, NewPostgresVisitor(WithParams()), cmp,
		`"t"."tags" @> $1`, []any{"{1,2}"})
}

// --- OVERLAPS (&&) ---

func TestVisitOverlaps(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("tags").Overlaps("{3,4}")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp, `"t"."tags" && '{3,4}'`)
}

func TestParamOverlaps(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("tags").Overlaps("{3,4}")
	assertParams(t, NewPostgresVisitor(WithParams()), cmp,
		`"t"."tags" && $1`, []any{"{3,4}"})
}

// --- Composite predications SQL generation ---

func TestVisitEqAny(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("status")
	n := col.EqAny("active", "pending")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n,
		`("t"."status" = 'active' OR "t"."status" = 'pending')`)
}

func TestVisitEqAll(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("status")
	n := col.EqAll("active", "pending")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n,
		`"t"."status" = 'active' AND "t"."status" = 'pending'`)
}

func TestVisitMatchesAny(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("name")
	n := col.MatchesAny("%foo%", "%bar%")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n,
		`("t"."name" LIKE '%foo%' OR "t"."name" LIKE '%bar%')`)
}

func TestVisitMatchesAll(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("name")
	n := col.MatchesAll("%foo%", "%bar%")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n,
		`"t"."name" LIKE '%foo%' AND "t"."name" LIKE '%bar%'`)
}

func TestVisitInAny(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("id")
	n := col.InAny([]any{1, 2}, []any{3, 4})
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n,
		`("t"."id" IN (1, 2) OR "t"."id" IN (3, 4))`)
}

func TestVisitInAll(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("id")
	n := col.InAll([]any{1, 2}, []any{3, 4})
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n,
		`"t"."id" IN (1, 2) AND "t"."id" IN (3, 4)`)
}

func TestParamEqAny(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("status")
	n := col.EqAny("active", "pending")
	assertParams(t, NewPostgresVisitor(WithParams()), n,
		`("t"."status" = $1 OR "t"."status" = $2)`,
		[]any{"active", "pending"})
}

func TestParamInAny(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("id")
	n := col.InAny([]any{1, 2}, []any{3, 4})
	assertParams(t, NewPostgresVisitor(WithParams()), n,
		`("t"."id" IN ($1, $2) OR "t"."id" IN ($3, $4))`,
		[]any{1, 2, 3, 4})
}

func TestParamRegexpPostgres(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").MatchesRegexp("^A.*")
	assertParams(t, NewPostgresVisitor(WithParams()), cmp,
		`"t"."name" ~ $1`, []any{"^A.*"})
}

func TestParamRegexpMySQL(t *testing.T) {
	t.Parallel()
	cmp := nodes.NewTable("t").Col("name").MatchesRegexp("^A.*")
	assertParams(t, NewMySQLVisitor(WithParams()), cmp,
		"`t`.`name` REGEXP ?", []any{"^A.*"})
}

// --- Aggregate functions ---

func TestVisitCountStar(t *testing.T) {
	t.Parallel()
	n := nodes.Count(nil)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, "COUNT(*)")
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), n, "COUNT(*)")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), n, "COUNT(*)")
}

func TestVisitCountColumn(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("id")
	n := nodes.Count(col)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `COUNT("users"."id")`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), n, "COUNT(`users`.`id`)")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), n, `COUNT("users"."id")`)
}

func TestVisitSum(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("orders").Col("total")
	n := nodes.Sum(col)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `SUM("orders"."total")`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), n, "SUM(`orders`.`total`)")
}

func TestVisitAvg(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("orders").Col("total")
	n := nodes.Avg(col)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `AVG("orders"."total")`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), n, "AVG(`orders`.`total`)")
}

func TestVisitMin(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("orders").Col("total")
	n := nodes.Min(col)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `MIN("orders"."total")`)
}

func TestVisitMax(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("orders").Col("total")
	n := nodes.Max(col)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `MAX("orders"."total")`)
}

func TestVisitCountDistinct(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("country")
	n := nodes.CountDistinct(col)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `COUNT(DISTINCT "users"."country")`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), n, "COUNT(DISTINCT `users`.`country`)")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), n, `COUNT(DISTINCT "users"."country")`)
}

func TestVisitAggregateWithFilter(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("orders").Col("total")
	cond := nodes.NewTable("orders").Col("status").Eq("completed")
	n := nodes.Sum(col).WithFilter(cond)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n,
		`SUM("orders"."total") FILTER (WHERE "orders"."status" = 'completed')`)
}

func TestVisitAggregateInSelectCore(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("status"), nodes.Count(nil)},
		Groups:      []nodes.Node{users.Col("status")},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT "users"."status", COUNT(*) FROM "users" GROUP BY "users"."status"`)
}

func TestVisitAggregateInHaving(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("status"), nodes.Count(nil)},
		Groups:      []nodes.Node{users.Col("status")},
		Havings:     []nodes.Node{nodes.Count(nil).Gt(5)},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT "users"."status", COUNT(*) FROM "users" GROUP BY "users"."status" HAVING COUNT(*) > 5`)
}

func TestVisitAggregateWithArithmetic(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("price")
	n := nodes.Sum(col).Plus(10)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `SUM("t"."price") + 10`)
}

func TestVisitExtractYear(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("orders").Col("created_at")
	n := nodes.Extract(nodes.ExtractYear, col)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `EXTRACT(YEAR FROM "orders"."created_at")`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), n, "EXTRACT(YEAR FROM `orders`.`created_at`)")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), n, `EXTRACT(YEAR FROM "orders"."created_at")`)
}

func TestVisitExtractMonth(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("orders").Col("created_at")
	n := nodes.Extract(nodes.ExtractMonth, col)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `EXTRACT(MONTH FROM "orders"."created_at")`)
}

func TestVisitExtractDay(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("orders").Col("created_at")
	n := nodes.Extract(nodes.ExtractDay, col)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `EXTRACT(DAY FROM "orders"."created_at")`)
}

func TestVisitExtractEpoch(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("ts")
	n := nodes.Extract(nodes.ExtractEpoch, col)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `EXTRACT(EPOCH FROM "t"."ts")`)
}

func TestVisitExtractInComparison(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("orders").Col("created_at")
	cmp := nodes.Extract(nodes.ExtractYear, col).Eq(2024)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cmp,
		`EXTRACT(YEAR FROM "orders"."created_at") = 2024`)
}

func TestVisitExtractWithArithmetic(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("ts")
	n := nodes.Extract(nodes.ExtractMonth, col).Plus(1)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `EXTRACT(MONTH FROM "t"."ts") + 1`)
}

// --- Parameterized aggregate tests ---

func TestParamCountStar(t *testing.T) {
	t.Parallel()
	n := nodes.Count(nil)
	v := NewPostgresVisitor(WithParams())
	got := n.Accept(v)
	if got != "COUNT(*)" {
		t.Errorf("expected COUNT(*), got %s", got)
	}
	if len(v.Params()) != 0 {
		t.Errorf("expected no params, got %v", v.Params())
	}
}

func TestParamAggregateInHaving(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{nodes.Count(nil)},
		Groups:      []nodes.Node{users.Col("status")},
		Havings:     []nodes.Node{nodes.Count(nil).Gt(5)},
	}
	assertParams(t, NewPostgresVisitor(WithParams()), sc,
		`SELECT COUNT(*) FROM "users" GROUP BY "users"."status" HAVING COUNT(*) > $1`,
		[]any{5})
}

func TestParamAggregateWithFilter(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("orders").Col("total")
	cond := nodes.NewTable("orders").Col("status").Eq("completed")
	n := nodes.Sum(col).WithFilter(cond)
	assertParams(t, NewPostgresVisitor(WithParams()), n,
		`SUM("orders"."total") FILTER (WHERE "orders"."status" = $1)`,
		[]any{"completed"})
}

func TestParamExtract(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("ts")
	cmp := nodes.Extract(nodes.ExtractYear, col).Eq(2024)
	assertParams(t, NewPostgresVisitor(WithParams()), cmp,
		`EXTRACT(YEAR FROM "t"."ts") = $1`, []any{2024})
}

// --- Window functions ---

func TestVisitRowNumber(t *testing.T) {
	t.Parallel()
	over := nodes.RowNumber().Over(nodes.NewWindowDef())
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over, `ROW_NUMBER() OVER ()`)
}

func TestVisitRank(t *testing.T) {
	t.Parallel()
	over := nodes.Rank().Over(nodes.NewWindowDef())
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over, `RANK() OVER ()`)
}

func TestVisitDenseRank(t *testing.T) {
	t.Parallel()
	over := nodes.DenseRank().Over(nodes.NewWindowDef())
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over, `DENSE_RANK() OVER ()`)
}

func TestVisitCumeDist(t *testing.T) {
	t.Parallel()
	over := nodes.CumeDist().Over(nodes.NewWindowDef())
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over, `CUME_DIST() OVER ()`)
}

func TestVisitPercentRank(t *testing.T) {
	t.Parallel()
	over := nodes.PercentRank().Over(nodes.NewWindowDef())
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over, `PERCENT_RANK() OVER ()`)
}

func TestVisitNtile(t *testing.T) {
	t.Parallel()
	over := nodes.Ntile(nodes.Literal(4)).Over(nodes.NewWindowDef())
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over, `NTILE(4) OVER ()`)
}

func TestVisitFirstValue(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("salary")
	over := nodes.FirstValue(col).Over(nodes.NewWindowDef())
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over, `FIRST_VALUE("users"."salary") OVER ()`)
}

func TestVisitLastValue(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("salary")
	over := nodes.LastValue(col).Over(nodes.NewWindowDef())
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over, `LAST_VALUE("users"."salary") OVER ()`)
}

func TestVisitLag(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("salary")
	over := nodes.Lag(col, nodes.Literal(1), nodes.Literal(0)).Over(nodes.NewWindowDef())
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over, `LAG("users"."salary", 1, 0) OVER ()`)
}

func TestVisitLead(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("salary")
	over := nodes.Lead(col).Over(nodes.NewWindowDef())
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over, `LEAD("users"."salary") OVER ()`)
}

func TestVisitNthValue(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("salary")
	over := nodes.NthValue(col, nodes.Literal(3)).Over(nodes.NewWindowDef())
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over, `NTH_VALUE("users"."salary", 3) OVER ()`)
}

func TestVisitOverWithPartitionBy(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().Partition(users.Col("dept"))
	over := nodes.RowNumber().Over(def)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over,
		`ROW_NUMBER() OVER (PARTITION BY "users"."dept")`)
}

func TestVisitOverWithOrderBy(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().Order(users.Col("salary").Desc())
	over := nodes.Rank().Over(def)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over,
		`RANK() OVER (ORDER BY "users"."salary" DESC)`)
}

func TestVisitOverWithPartitionAndOrder(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().
		Partition(users.Col("dept")).
		Order(users.Col("salary").Desc())
	over := nodes.Rank().Over(def)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over,
		`RANK() OVER (PARTITION BY "users"."dept" ORDER BY "users"."salary" DESC)`)
}

func TestVisitOverWithRowsFrame(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().
		Order(users.Col("salary").Asc()).
		Rows(nodes.UnboundedPreceding(), nodes.CurrentRow())
	over := nodes.Sum(users.Col("salary")).Over(def)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over,
		`SUM("users"."salary") OVER (ORDER BY "users"."salary" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`)
}

func TestVisitOverWithRangeFrame(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().
		Order(users.Col("id").Asc()).
		Range(nodes.UnboundedPreceding(), nodes.UnboundedFollowing())
	over := nodes.Count(nil).Over(def)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over,
		`COUNT(*) OVER (ORDER BY "users"."id" ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`)
}

func TestVisitOverWithPrecedingFollowing(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().
		Order(users.Col("id").Asc()).
		Rows(nodes.Preceding(nodes.Literal(3)), nodes.Following(nodes.Literal(3)))
	over := nodes.Sum(users.Col("salary")).Over(def)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over,
		`SUM("users"."salary") OVER (ORDER BY "users"."id" ASC ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING)`)
}

func TestVisitOverNamedWindow(t *testing.T) {
	t.Parallel()
	over := nodes.Rank().OverName("w")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over, `RANK() OVER "w"`)
}

func TestVisitAggregateOver(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().Partition(users.Col("dept"))
	over := nodes.Sum(users.Col("salary")).Over(def)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over,
		`SUM("users"."salary") OVER (PARTITION BY "users"."dept")`)
}

func TestVisitAggregateOverName(t *testing.T) {
	t.Parallel()
	over := nodes.Count(nil).OverName("w")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over, `COUNT(*) OVER "w"`)
}

func TestVisitSelectCoreWithWindow(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	w := nodes.NewWindowDef("w").
		Partition(users.Col("dept")).
		Order(users.Col("salary").Asc())
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{nodes.Rank().OverName("w")},
		Windows:     []*nodes.WindowDefinition{w},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT RANK() OVER "w" FROM "users" WINDOW "w" AS (PARTITION BY "users"."dept" ORDER BY "users"."salary" ASC)`)
}

func TestVisitSelectCoreWithMultipleWindows(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	w1 := nodes.NewWindowDef("w1").Order(users.Col("salary").Asc())
	w2 := nodes.NewWindowDef("w2").Partition(users.Col("dept"))
	sc := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{nodes.RowNumber().OverName("w1"), nodes.Rank().OverName("w2")},
		Windows:     []*nodes.WindowDefinition{w1, w2},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), sc,
		`SELECT ROW_NUMBER() OVER "w1", RANK() OVER "w2" FROM "users" WINDOW "w1" AS (ORDER BY "users"."salary" ASC), "w2" AS (PARTITION BY "users"."dept")`)
}

// Window functions with MySQL
func TestVisitWindowFuncMySQL(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().Order(users.Col("salary").Desc())
	over := nodes.RowNumber().Over(def)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), over,
		"ROW_NUMBER() OVER (ORDER BY `users`.`salary` DESC)")
}

// Window functions with SQLite
func TestVisitWindowFuncSQLite(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().Order(users.Col("salary").Desc())
	over := nodes.RowNumber().Over(def)
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), over,
		`ROW_NUMBER() OVER (ORDER BY "users"."salary" DESC)`)
}

// Parameterized window functions
func TestParamWindowFunction(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().Order(users.Col("salary").Asc())
	over := nodes.Ntile(nodes.Literal(4)).Over(def)
	assertParams(t, NewPostgresVisitor(WithParams()), over,
		`NTILE($1) OVER (ORDER BY "users"."salary" ASC)`, []any{4})
}

func TestParamLagWithArgs(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().Order(users.Col("salary").Asc())
	over := nodes.Lag(users.Col("salary"), nodes.Literal(1), nodes.Literal(0)).Over(def)
	assertParams(t, NewPostgresVisitor(WithParams()), over,
		`LAG("users"."salary", $1, $2) OVER (ORDER BY "users"."salary" ASC)`, []any{1, 0})
}

func TestParamWindowFrameOffset(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().
		Order(users.Col("id").Asc()).
		Rows(nodes.Preceding(nodes.Literal(3)), nodes.Following(nodes.Literal(5)))
	over := nodes.Sum(users.Col("salary")).Over(def)
	assertParams(t, NewPostgresVisitor(WithParams()), over,
		`SUM("users"."salary") OVER (ORDER BY "users"."id" ASC ROWS BETWEEN $1 PRECEDING AND $2 FOLLOWING)`,
		[]any{3, 5})
}

func TestVisitRowsFrameWithoutBetween(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	def := nodes.NewWindowDef().
		Order(users.Col("id").Asc()).
		Rows(nodes.UnboundedPreceding())
	over := nodes.Sum(users.Col("salary")).Over(def)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), over,
		`SUM("users"."salary") OVER (ORDER BY "users"."id" ASC ROWS UNBOUNDED PRECEDING)`)
}

// --- NULLS FIRST / LAST ---

func TestVisitOrderingNullsFirst(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("name")
	ord := &nodes.OrderingNode{Expr: col, Direction: nodes.Asc, Nulls: nodes.NullsFirst}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), ord, `"users"."name" ASC NULLS FIRST`)
}

func TestVisitOrderingNullsLast(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("name")
	ord := &nodes.OrderingNode{Expr: col, Direction: nodes.Desc, Nulls: nodes.NullsLast}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), ord, `"users"."name" DESC NULLS LAST`)
}

func TestVisitOrderingNullsDefault(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("name")
	ord := &nodes.OrderingNode{Expr: col, Direction: nodes.Asc}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), ord, `"users"."name" ASC`)
}

// --- DISTINCT ON ---

func TestVisitDistinctOn(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{
		From:       users,
		DistinctOn: []nodes.Node{users.Col("email")},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core,
		`SELECT DISTINCT ON ("users"."email") * FROM "users"`)
}

func TestVisitDistinctOnMultipleColumns(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{
		From:        users,
		DistinctOn:  []nodes.Node{users.Col("dept"), users.Col("role")},
		Projections: []nodes.Node{users.Col("name")},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core,
		`SELECT DISTINCT ON ("users"."dept", "users"."role") "users"."name" FROM "users"`)
}

// --- FOR UPDATE / FOR SHARE / SKIP LOCKED ---

func TestVisitForUpdate(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users, Lock: nodes.ForUpdate}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core,
		`SELECT * FROM "users" FOR UPDATE`)
}

func TestVisitForShare(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users, Lock: nodes.ForShare}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core,
		`SELECT * FROM "users" FOR SHARE`)
}

func TestVisitForUpdateSkipLocked(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users, Lock: nodes.ForUpdate, SkipLocked: true}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core,
		`SELECT * FROM "users" FOR UPDATE SKIP LOCKED`)
}

func TestVisitForNoKeyUpdate(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users, Lock: nodes.ForNoKeyUpdate}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core,
		`SELECT * FROM "users" FOR NO KEY UPDATE`)
}

func TestVisitForKeyShare(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users, Lock: nodes.ForKeyShare}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core,
		`SELECT * FROM "users" FOR KEY SHARE`)
}

// --- Query Comments ---

func TestVisitComment(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users, Comment: "load active users"}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core,
		`/* load active users */ SELECT * FROM "users"`)
}

func TestVisitCommentSanitizesCloseSequence(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users, Comment: "test */ ; DROP TABLE users; /*"}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core,
		`/* test * / ; DROP TABLE users; /* */ SELECT * FROM "users"`)
}

// --- Optimizer Hints ---

func TestVisitHints(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users, Hints: []string{"SeqScan(users)", "Parallel(users 4)"}}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core,
		`SELECT /*+ SeqScan(users) Parallel(users 4) */ * FROM "users"`)
}

func TestVisitHintsSanitizesCloseSequence(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users, Hints: []string{"USE_HASH_JOIN */ ; DROP TABLE users; /*+"}}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core,
		`SELECT /*+ USE_HASH_JOIN * / ; DROP TABLE users; /*+ */ * FROM "users"`)
}

// --- LATERAL JOIN ---

func TestVisitLateralJoin(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sub := &nodes.SqlLiteral{Raw: "subquery"}
	join := &nodes.JoinNode{Left: users, Right: sub, Type: nodes.InnerJoin, Lateral: true}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), join,
		`INNER JOIN LATERAL subquery`)
}

func TestVisitLateralLeftJoinWithOn(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	orders := nodes.NewTable("orders")
	cond := users.Col("id").Eq(orders.Col("user_id"))
	join := &nodes.JoinNode{Left: users, Right: orders, Type: nodes.LeftOuterJoin, Lateral: true, On: cond}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), join,
		`LEFT OUTER JOIN LATERAL "orders" ON "users"."id" = "orders"."user_id"`)
}

// --- String JOIN ---

func TestVisitStringJoin(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	join := &nodes.JoinNode{Left: users, Right: &nodes.SqlLiteral{Raw: "INNER JOIN orders ON orders.user_id = users.id"}, Type: nodes.StringJoin}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), join,
		`INNER JOIN orders ON orders.user_id = users.id`)
}

// --- EXISTS / NOT EXISTS ---

func TestVisitExists(t *testing.T) {
	t.Parallel()
	sub := &nodes.SelectCore{From: nodes.NewTable("orders")}
	ex := nodes.Exists(sub)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), ex,
		`EXISTS (SELECT * FROM "orders")`)
}

func TestVisitNotExists(t *testing.T) {
	t.Parallel()
	sub := &nodes.SelectCore{From: nodes.NewTable("orders")}
	ex := nodes.NotExists(sub)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), ex,
		`NOT EXISTS (SELECT * FROM "orders")`)
}

// --- Set Operations ---

func TestVisitUnion(t *testing.T) {
	t.Parallel()
	left := &nodes.SelectCore{From: nodes.NewTable("users")}
	right := &nodes.SelectCore{From: nodes.NewTable("admins")}
	op := &nodes.SetOperationNode{Left: left, Right: right, Type: nodes.Union}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), op,
		`(SELECT * FROM "users") UNION (SELECT * FROM "admins")`)
}

func TestVisitUnionAll(t *testing.T) {
	t.Parallel()
	left := &nodes.SelectCore{From: nodes.NewTable("users")}
	right := &nodes.SelectCore{From: nodes.NewTable("admins")}
	op := &nodes.SetOperationNode{Left: left, Right: right, Type: nodes.UnionAll}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), op,
		`(SELECT * FROM "users") UNION ALL (SELECT * FROM "admins")`)
}

func TestVisitIntersect(t *testing.T) {
	t.Parallel()
	left := &nodes.SelectCore{From: nodes.NewTable("users")}
	right := &nodes.SelectCore{From: nodes.NewTable("admins")}
	op := &nodes.SetOperationNode{Left: left, Right: right, Type: nodes.Intersect}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), op,
		`(SELECT * FROM "users") INTERSECT (SELECT * FROM "admins")`)
}

func TestVisitExcept(t *testing.T) {
	t.Parallel()
	left := &nodes.SelectCore{From: nodes.NewTable("users")}
	right := &nodes.SelectCore{From: nodes.NewTable("admins")}
	op := &nodes.SetOperationNode{Left: left, Right: right, Type: nodes.Except}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), op,
		`(SELECT * FROM "users") EXCEPT (SELECT * FROM "admins")`)
}

func TestVisitIntersectAll(t *testing.T) {
	t.Parallel()
	left := &nodes.SelectCore{From: nodes.NewTable("users")}
	right := &nodes.SelectCore{From: nodes.NewTable("admins")}
	op := &nodes.SetOperationNode{Left: left, Right: right, Type: nodes.IntersectAll}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), op,
		`(SELECT * FROM "users") INTERSECT ALL (SELECT * FROM "admins")`)
}

func TestVisitExceptAll(t *testing.T) {
	t.Parallel()
	left := &nodes.SelectCore{From: nodes.NewTable("users")}
	right := &nodes.SelectCore{From: nodes.NewTable("admins")}
	op := &nodes.SetOperationNode{Left: left, Right: right, Type: nodes.ExceptAll}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), op,
		`(SELECT * FROM "users") EXCEPT ALL (SELECT * FROM "admins")`)
}

func TestVisitSetOperationWithOrderByLimitOffset(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	left := &nodes.SelectCore{From: users}
	right := &nodes.SelectCore{From: nodes.NewTable("admins")}
	op := &nodes.SetOperationNode{
		Left:   left,
		Right:  right,
		Type:   nodes.Union,
		Orders: []nodes.Node{users.Col("id").Asc()},
		Limit:  nodes.Literal(10),
		Offset: nodes.Literal(5),
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), op,
		`(SELECT * FROM "users") UNION (SELECT * FROM "admins") ORDER BY "users"."id" ASC LIMIT 10 OFFSET 5`)
}

// --- CTE ---

func TestVisitCTE(t *testing.T) {
	t.Parallel()
	sub := &nodes.SelectCore{From: nodes.NewTable("orders")}
	cte := &nodes.CTENode{Name: "recent_orders", Query: sub}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cte,
		`"recent_orders" AS (SELECT * FROM "orders")`)
}

func TestVisitCTEWithColumns(t *testing.T) {
	t.Parallel()
	sub := &nodes.SelectCore{From: nodes.NewTable("orders")}
	cte := &nodes.CTENode{Name: "recent_orders", Query: sub, Columns: []string{"id", "total"}}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), cte,
		`"recent_orders" ("id", "total") AS (SELECT * FROM "orders")`)
}

func TestVisitSelectCoreWithCTE(t *testing.T) {
	t.Parallel()
	sub := &nodes.SelectCore{From: nodes.NewTable("orders")}
	cte := &nodes.CTENode{Name: "recent_orders", Query: sub}
	core := &nodes.SelectCore{
		From: nodes.NewTable("recent_orders"),
		CTEs: []*nodes.CTENode{cte},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core,
		`WITH "recent_orders" AS (SELECT * FROM "orders") SELECT * FROM "recent_orders"`)
}

func TestVisitSelectCoreWithRecursiveCTE(t *testing.T) {
	t.Parallel()
	sub := &nodes.SelectCore{From: nodes.NewTable("categories")}
	cte := &nodes.CTENode{Name: "tree", Query: sub, Recursive: true}
	core := &nodes.SelectCore{
		From: nodes.NewTable("tree"),
		CTEs: []*nodes.CTENode{cte},
	}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core,
		`WITH RECURSIVE "tree" AS (SELECT * FROM "categories") SELECT * FROM "tree"`)
}

// --- DOT Visitor: new node types ---

func TestDotVisitOrderingNullsFirst(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("name")
	ord := &nodes.OrderingNode{Expr: col, Direction: nodes.Asc, Nulls: nodes.NullsFirst}
	dv := NewDotVisitor()
	ord.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "NULLS FIRST")
}

func TestDotVisitLateralJoin(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sub := &nodes.SqlLiteral{Raw: "sub"}
	join := &nodes.JoinNode{Left: users, Right: sub, Type: nodes.InnerJoin, Lateral: true}
	dv := NewDotVisitor()
	join.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "LATERAL")
}

func TestDotVisitExists(t *testing.T) {
	t.Parallel()
	sub := &nodes.SelectCore{From: nodes.NewTable("orders")}
	ex := nodes.Exists(sub)
	dv := NewDotVisitor()
	ex.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "EXISTS")
}

func TestDotVisitNotExists(t *testing.T) {
	t.Parallel()
	sub := &nodes.SelectCore{From: nodes.NewTable("orders")}
	ex := nodes.NotExists(sub)
	dv := NewDotVisitor()
	ex.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "NOT EXISTS")
}

func TestDotVisitSetOperation(t *testing.T) {
	t.Parallel()
	left := &nodes.SelectCore{From: nodes.NewTable("users")}
	right := &nodes.SelectCore{From: nodes.NewTable("admins")}
	op := &nodes.SetOperationNode{Left: left, Right: right, Type: nodes.Union}
	dv := NewDotVisitor()
	op.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "UNION")
}

func TestDotVisitCTE(t *testing.T) {
	t.Parallel()
	sub := &nodes.SelectCore{From: nodes.NewTable("orders")}
	cte := &nodes.CTENode{Name: "recent_orders", Query: sub}
	dv := NewDotVisitor()
	cte.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "recent_orders")
}

func TestDotVisitDistinctOn(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{
		From:       users,
		DistinctOn: []nodes.Node{users.Col("email")},
	}
	dv := NewDotVisitor()
	core.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "DISTINCT ON")
}

func TestDotVisitForUpdate(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users, Lock: nodes.ForUpdate}
	dv := NewDotVisitor()
	core.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "FOR UPDATE")
}

func TestDotVisitComment(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users, Comment: "test"}
	dv := NewDotVisitor()
	core.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "Comment")
}

func TestDotVisitHints(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	core := &nodes.SelectCore{From: users, Hints: []string{"SeqScan"}}
	dv := NewDotVisitor()
	core.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "Hint")
}

// --- NamedFunction SQL ---

func TestVisitNamedFunction(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("name")
	fn := nodes.NewNamedFunction("LOWER", col)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), fn, `LOWER("users"."name")`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), fn, "LOWER(`users`.`name`)")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), fn, `LOWER("users"."name")`)
}

func TestVisitNamedFunctionMultiArg(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("x")
	fn := nodes.Coalesce(col, nodes.Literal(0))
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), fn, `COALESCE("t"."x", 0)`)
}

func TestVisitNamedFunctionDistinct(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("x")
	fn := nodes.NewNamedFunction("GROUP_CONCAT", col)
	fn.Distinct = true
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), fn, `GROUP_CONCAT(DISTINCT "t"."x")`)
}

func TestVisitCastSpecialRendering(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("age")
	fn := nodes.Cast(col, "VARCHAR")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), fn, `CAST("t"."age" AS VARCHAR)`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), fn, "CAST(`t`.`age` AS VARCHAR)")
}

func TestVisitNamedFunctionNoArgs(t *testing.T) {
	t.Parallel()
	fn := nodes.NewNamedFunction("NOW")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), fn, "NOW()")
}

// --- CASE SQL ---

func TestVisitCaseSearched(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("status")
	c := nodes.NewCase().
		When(col.Eq("active"), nodes.Literal(1)).
		When(col.Eq("inactive"), nodes.Literal(0)).
		Else(nodes.Literal(-1))
	expected := `CASE WHEN "t"."status" = 'active' THEN 1 WHEN "t"."status" = 'inactive' THEN 0 ELSE -1 END`
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), c, expected)
}

func TestVisitCaseSimple(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("status")
	c := nodes.NewCase(col).
		When(nodes.Literal("active"), nodes.Literal(1)).
		When(nodes.Literal("inactive"), nodes.Literal(0))
	expected := `CASE "t"."status" WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 END`
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), c, expected)
}

func TestVisitCaseNoElse(t *testing.T) {
	t.Parallel()
	c := nodes.NewCase().
		When(nodes.Literal(true), nodes.Literal(1))
	expected := "CASE WHEN TRUE THEN 1 END"
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), c, expected)
}

// --- GroupingSet SQL ---

func TestVisitCube(t *testing.T) {
	t.Parallel()
	col1 := nodes.NewTable("t").Col("a")
	col2 := nodes.NewTable("t").Col("b")
	n := nodes.NewCube(col1, col2)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `CUBE("t"."a", "t"."b")`)
}

func TestVisitRollup(t *testing.T) {
	t.Parallel()
	col1 := nodes.NewTable("t").Col("a")
	n := nodes.NewRollup(col1)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `ROLLUP("t"."a")`)
}

func TestVisitGroupingSets(t *testing.T) {
	t.Parallel()
	col1 := nodes.NewTable("t").Col("a")
	col2 := nodes.NewTable("t").Col("b")
	n := nodes.NewGroupingSets([]nodes.Node{col1, col2}, []nodes.Node{col1}, []nodes.Node{})
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), n, `GROUPING SETS(("t"."a", "t"."b"), ("t"."a"), ())`)
}

func TestVisitGroupingSetInGroupBy(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("a")
	cube := nodes.NewCube(col)
	core := &nodes.SelectCore{
		From:   nodes.NewTable("t"),
		Groups: []nodes.Node{cube},
	}
	expected := `SELECT * FROM "t" GROUP BY CUBE("t"."a")`
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core, expected)
}

// --- Alias SQL ---

func TestVisitAlias(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("t").Col("x")
	alias := nodes.NewAliasNode(col, "col_x")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), alias, `"t"."x" AS "col_x"`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), alias, "`t`.`x` AS `col_x`")
}

func TestVisitAliasInProjection(t *testing.T) {
	t.Parallel()
	col := nodes.NewTable("users").Col("name")
	alias := col.As("user_name")
	core := &nodes.SelectCore{
		From:        nodes.NewTable("users"),
		Projections: []nodes.Node{alias},
	}
	expected := `SELECT "users"."name" AS "user_name" FROM "users"`
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), core, expected)
}

func TestVisitAliasOnFunction(t *testing.T) {
	t.Parallel()
	fn := nodes.Lower(nodes.NewTable("t").Col("name"))
	alias := fn.As("lower_name")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), alias, `LOWER("t"."name") AS "lower_name"`)
}

func TestVisitAliasOnAggregate(t *testing.T) {
	t.Parallel()
	agg := nodes.Count(nil)
	alias := agg.As("total")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), alias, `COUNT(*) AS "total"`)
}

// --- BindParam SQL ---

func TestVisitBindParamNonParam(t *testing.T) {
	t.Parallel()
	bp := nodes.NewBindParam(42)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), bp, "42")
}

func TestVisitBindParamStringNonParam(t *testing.T) {
	t.Parallel()
	bp := nodes.NewBindParam("hello")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), bp, "'hello'")
}

func TestVisitBindParamParameterized(t *testing.T) {
	t.Parallel()
	v := NewPostgresVisitor(WithParams())
	bp := nodes.NewBindParam(42)
	got := bp.Accept(v)
	if got != "$1" {
		t.Errorf("expected $1, got %s", got)
	}
	params := v.Params()
	if len(params) != 1 || params[0] != 42 {
		t.Errorf("expected params [42], got %v", params)
	}
}

func TestVisitBindParamMySQL(t *testing.T) {
	t.Parallel()
	v := NewMySQLVisitor(WithParams())
	bp := nodes.NewBindParam("test")
	got := bp.Accept(v)
	if got != "?" {
		t.Errorf("expected ?, got %s", got)
	}
	params := v.Params()
	if len(params) != 1 || params[0] != "test" {
		t.Errorf("expected params [test], got %v", params)
	}
}

// --- Casted SQL ---

func TestVisitCasted(t *testing.T) {
	t.Parallel()
	c := nodes.NewCasted(42, "integer")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), c, "CAST(42 AS integer)")
}

func TestVisitCastedNoType(t *testing.T) {
	t.Parallel()
	c := nodes.NewCasted(42, "")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), c, "42")
}

func TestVisitCastedString(t *testing.T) {
	t.Parallel()
	c := nodes.NewCasted("hello", "text")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), c, "CAST('hello' AS text)")
}

func TestVisitCastedParameterized(t *testing.T) {
	t.Parallel()
	v := NewPostgresVisitor(WithParams())
	c := nodes.NewCasted(42, "integer")
	got := c.Accept(v)
	if got != "CAST($1 AS integer)" {
		t.Errorf("expected CAST($1 AS integer), got %s", got)
	}
}

// --- BoundSqlLiteral SQL ---

func TestVisitBoundSqlLiteralNoParams(t *testing.T) {
	t.Parallel()
	lit := nodes.NewBoundSqlLiteral("1 = 1")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), lit, "1 = 1")
}

func TestVisitBoundSqlLiteralWithParams(t *testing.T) {
	t.Parallel()
	lit := nodes.NewBoundSqlLiteral("WHERE id = ?", 42)
	v := NewPostgresVisitor(WithParams())
	got := lit.Accept(v)
	if got != "WHERE id = ?" {
		t.Errorf("expected raw SQL preserved, got %s", got)
	}
	params := v.Params()
	if len(params) != 1 || params[0] != 42 {
		t.Errorf("expected params [42], got %v", params)
	}
}

// --- DOT visitor for new nodes ---

func TestDotVisitNamedFunction(t *testing.T) {
	t.Parallel()
	fn := nodes.NewNamedFunction("LOWER", nodes.NewTable("t").Col("name"))
	dv := NewDotVisitor()
	fn.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "LOWER")
}

func TestDotVisitCase(t *testing.T) {
	t.Parallel()
	c := nodes.NewCase().When(nodes.Literal(true), nodes.Literal(1))
	dv := NewDotVisitor()
	c.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "CASE")
}

func TestDotVisitCube(t *testing.T) {
	t.Parallel()
	n := nodes.NewCube(nodes.NewTable("t").Col("a"))
	dv := NewDotVisitor()
	n.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "CUBE")
}

func TestDotVisitAlias(t *testing.T) {
	t.Parallel()
	alias := nodes.NewAliasNode(nodes.Literal(1), "one")
	dv := NewDotVisitor()
	alias.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "Alias")
	assertContains(t, dot, "one")
}

func TestDotVisitBindParam(t *testing.T) {
	t.Parallel()
	bp := nodes.NewBindParam(42)
	dv := NewDotVisitor()
	bp.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "BindParam")
}

func TestDotVisitCasted(t *testing.T) {
	t.Parallel()
	c := nodes.NewCasted(42, "integer")
	dv := NewDotVisitor()
	c.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "Casted")
}

// --- VisitCasted with type coercion ---

// --- Subquery alias ---

func TestVisitTableAliasWithSubquery(t *testing.T) {
	t.Parallel()
	// Build a subquery: SELECT "users"."id" FROM "users"
	users := nodes.NewTable("users")
	sub := &nodes.SelectCore{
		From:        users,
		Projections: []nodes.Node{users.Col("id")},
	}
	alias := &nodes.TableAlias{Relation: sub, AliasName: "sub"}
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), alias, `(SELECT "users"."id" FROM "users") AS "sub"`)
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), alias, "(SELECT `users`.`id` FROM `users`) AS `sub`")
}

func TestVisitCastedWithTypeName(t *testing.T) {
	t.Parallel()
	c := nodes.NewCasted(42, "integer")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), c, "CAST(42 AS integer)")
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), c, "CAST(42 AS integer)")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), c, "CAST(42 AS integer)")
}

func TestVisitCastedWithoutTypeName(t *testing.T) {
	t.Parallel()
	c := nodes.NewCasted(42, "")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), c, "42")
	testutil.AssertSQL(t, NewMySQLVisitor(WithoutParams()), c, "42")
	testutil.AssertSQL(t, NewSQLiteVisitor(WithoutParams()), c, "42")
}

func TestVisitCastedStringWithTypeName(t *testing.T) {
	t.Parallel()
	c := nodes.NewCasted("hello", "text")
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), c, "CAST('hello' AS text)")
}

func TestVisitCastedWithParams(t *testing.T) {
	t.Parallel()
	c := nodes.NewCasted(42, "integer")
	v := NewPostgresVisitor(WithParams())
	sql := c.Accept(v)
	if sql != "CAST($1 AS integer)" {
		t.Errorf("expected CAST($1 AS integer), got %s", sql)
	}
	params := v.Params()
	if len(params) != 1 || params[0] != 42 {
		t.Errorf("expected params [42], got %v", params)
	}
}

// --- Literal type coverage ---

func TestLiteralAllNumericTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"int8", int8(42), "42"},
		{"int16", int16(1000), "1000"},
		{"int32", int32(100000), "100000"},
		{"int64", int64(9223372036854775807), "9223372036854775807"},
		{"uint", uint(42), "42"},
		{"uint8", uint8(255), "255"},
		{"uint16", uint16(65535), "65535"},
		{"uint32", uint32(4294967295), "4294967295"},
		{"uint64", uint64(18446744073709551615), "18446744073709551615"},
		{"float32", float32(3.14), "3.14"},
		{"float64", float64(2.718281828), "2.718281828"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lit := nodes.Literal(tt.value)
			v := NewPostgresVisitor(WithoutParams())
			sql := lit.Accept(v)
			if sql != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, sql)
			}
		})
	}
}

// --- Validation panic tests ---

func TestValidateSQLTypeNameValid(t *testing.T) {
	t.Parallel()
	// Should not panic
	validateSQLTypeName("VARCHAR(255)")
	validateSQLTypeName("DECIMAL(10, 2)")
	validateSQLTypeName("user_defined_type")
}

func TestValidateSQLTypeNameInvalidChars(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		typeName string
	}{
		{"semicolon", "VARCHAR;DROP TABLE"},
		{"single quote", "VARCHAR'"},
		{"dash", "VARCHAR-100"},
		{"equals", "VARCHAR=100"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("expected panic for type name %q", tt.typeName)
				}
			}()
			validateSQLTypeName(tt.typeName)
		})
	}
}

func TestValidateSQLFunctionNameValid(t *testing.T) {
	t.Parallel()
	// Should not panic
	validateSQLFunctionName("my_function")
	validateSQLFunctionName("COUNT")
	validateSQLFunctionName("func123")
}

func TestValidateSQLFunctionNameInvalidChars(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		functionName string
	}{
		{"semicolon", "func;DROP"},
		{"single quote", "func'"},
		{"space", "func name"},
		{"dash", "func-name"},
		{"parentheses", "func()"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("expected panic for function name %q", tt.functionName)
				}
			}()
			validateSQLFunctionName(tt.functionName)
		})
	}
}

// --- UnaryMath parentheses coverage ---

func TestVisitUnaryMathWithParens(t *testing.T) {
	t.Parallel()
	// Test that expressions needing parentheses get them
	users := nodes.NewTable("users")
	// a + b wrapped in unary math should have parens
	expr := nodes.NewInfixNode(users.Col("a"), users.Col("b"), nodes.OpPlus)
	unary := nodes.NewUnaryMathNode(expr, nodes.OpBitwiseNot)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), unary, `~("users"."a" + "users"."b")`)
}

func TestVisitUnaryMathNoParens(t *testing.T) {
	t.Parallel()
	// Simple column doesn't need parens
	users := nodes.NewTable("users")
	unary := nodes.NewUnaryMathNode(users.Col("flags"), nodes.OpBitwiseNot)
	testutil.AssertSQL(t, NewPostgresVisitor(WithoutParams()), unary, `~"users"."flags"`)
}

// --- DOT visitor window coverage ---

func TestDotVisitWindowDefinitionWithFrame(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	id := users.Col("id")
	w := &nodes.WindowDefinition{
		Name:        "w",
		PartitionBy: []nodes.Node{id},
		Frame: &nodes.WindowFrame{
			Type: nodes.FrameRows,
		},
	}
	core := &nodes.SelectCore{
		From:    users,
		Windows: []*nodes.WindowDefinition{w},
	}
	dv := NewDotVisitor()
	core.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "WINDOW")
	assertContains(t, dot, "ROWS")
}

func TestDotVisitWindowDefinitionRangeFrame(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	w := &nodes.WindowDefinition{
		Name: "w",
		Frame: &nodes.WindowFrame{
			Type: nodes.FrameRange,
		},
	}
	core := &nodes.SelectCore{
		From:    users,
		Windows: []*nodes.WindowDefinition{w},
	}
	dv := NewDotVisitor()
	core.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "RANGE")
}

func TestDotVisitWindowDefinitionWithOrderBy(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	created := users.Col("created_at")
	w := &nodes.WindowDefinition{
		Name:    "w",
		OrderBy: []nodes.Node{created.Asc()},
	}
	core := &nodes.SelectCore{
		From:    users,
		Windows: []*nodes.WindowDefinition{w},
	}
	dv := NewDotVisitor()
	core.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "WINDOW")
	assertContains(t, dot, "ORDER")
}

func TestDotVisitGroupingSets(t *testing.T) {
	t.Parallel()
	col1 := nodes.NewTable("t").Col("a")
	col2 := nodes.NewTable("t").Col("b")
	n := nodes.NewGroupingSets([]nodes.Node{col1, col2}, []nodes.Node{col1})
	dv := NewDotVisitor()
	n.Accept(dv)
	dot := dv.ToDot()
	assertContains(t, dot, "GROUPING SETS")
}

// --- NEW TESTS: Params enabled by default ---

func TestParameterisedByDefault(t *testing.T) {
	t.Parallel()

	// Test that visitors enable parameterisation by default
	users := nodes.NewTable("users")
	query := users.Col("name").Eq(nodes.NewBindParam("Alice"))

	// PostgreSQL - should produce $1 placeholder by default
	pgVisitor := NewPostgresVisitor()
	pgVisitor.Reset()
	sql := query.Accept(pgVisitor)
	if sql != `"users"."name" = $1` {
		t.Errorf("PostgreSQL: expected parameterised SQL with $1, got %s", sql)
	}
	params := pgVisitor.Params()
	if len(params) != 1 || params[0] != "Alice" {
		t.Errorf("PostgreSQL: expected params [Alice], got %v", params)
	}

	// MySQL - should produce ? placeholder by default
	mysqlVisitor := NewMySQLVisitor()
	mysqlVisitor.Reset()
	sql = query.Accept(mysqlVisitor)
	if sql != "`users`.`name` = ?" {
		t.Errorf("MySQL: expected parameterised SQL with ?, got %s", sql)
	}
	params = mysqlVisitor.Params()
	if len(params) != 1 || params[0] != "Alice" {
		t.Errorf("MySQL: expected params [Alice], got %v", params)
	}

	// SQLite - should produce ? placeholder by default
	sqliteVisitor := NewSQLiteVisitor()
	sqliteVisitor.Reset()
	sql = query.Accept(sqliteVisitor)
	if sql != `"users"."name" = ?` {
		t.Errorf("SQLite: expected parameterised SQL with ?, got %s", sql)
	}
	params = sqliteVisitor.Params()
	if len(params) != 1 || params[0] != "Alice" {
		t.Errorf("SQLite: expected params [Alice], got %v", params)
	}
}

func TestWithoutParamsDisablesParameterisation(t *testing.T) {
	t.Parallel()

	users := nodes.NewTable("users")
	query := users.Col("name").Eq(nodes.NewBindParam("Alice"))

	// Test WithoutParams() option disables parameterisation
	visitor := NewPostgresVisitor(WithoutParams())
	sql := query.Accept(visitor)

	// Should get interpolated value 'Alice', not placeholder $1
	if sql != `"users"."name" = 'Alice'` {
		t.Errorf("expected interpolated SQL 'Alice', got %s", sql)
	}

	// Params should be empty
	params := visitor.Params()
	if len(params) != 0 {
		t.Errorf("expected no params, got %v", params)
	}
}

func TestRenderWindowDefEmpty(t *testing.T) {
	t.Parallel()
	got := RenderWindowDef(NewPostgresVisitor(WithoutParams()), nil)
	if got != "()" {
		t.Errorf("expected (), got %s", got)
	}
}

func TestRenderWindowDefPartitionAndOrder(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	w := &nodes.WindowDefinition{
		PartitionBy: []nodes.Node{users.Col("dept")},
		OrderBy:     []nodes.Node{&nodes.OrderingNode{Expr: users.Col("salary"), Direction: nodes.Desc}},
	}
	got := RenderWindowDef(NewPostgresVisitor(WithoutParams()), w)
	want := `(PARTITION BY "users"."dept" ORDER BY "users"."salary" DESC)`
	if got != want {
		t.Errorf("expected:\n  %s\ngot:\n  %s", want, got)
	}
}
