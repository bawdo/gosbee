package visitors

import (
	"testing"

	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins/softdelete"
)

// BenchmarkSimpleSelect benchmarks a basic single-table SELECT query.
func BenchmarkSimpleSelect(b *testing.B) {
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users).
		Select(users.Col("id"), users.Col("name"), users.Col("email")).
		Where(users.Col("active").Eq(true)).
		Order(users.Col("name").Asc()).
		Limit(10)
	v := NewPostgresVisitor()

	b.ResetTimer()
	for b.Loop() {
		_, _ = m.ToSQL(v)
	}
}

// BenchmarkComplexJoinQuery benchmarks a multi-join query with subqueries.
func BenchmarkComplexJoinQuery(b *testing.B) {
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	comments := nodes.NewTable("comments")

	m := managers.NewSelectManager(users)
	m.Select(
		users.Col("name"),
		nodes.Count(posts.Col("id")).As("post_count"),
		nodes.Count(comments.Col("id")).As("comment_count"),
	)
	m.Join(posts).On(users.Col("id").Eq(posts.Col("user_id")))
	m.Join(comments, nodes.LeftOuterJoin).On(posts.Col("id").Eq(comments.Col("post_id")))
	m.Where(users.Col("active").Eq(true))
	m.Where(posts.Col("published").Eq(true))
	m.Group(users.Col("name"))
	m.Having(nodes.Count(posts.Col("id")).Gt(5))
	m.Order(users.Col("name").Asc())
	m.Limit(20)
	m.Offset(10)
	v := NewPostgresVisitor()

	b.ResetTimer()
	for b.Loop() {
		_, _ = m.ToSQL(v)
	}
}

// BenchmarkParameterizedQuery benchmarks parameterized mode overhead.
func BenchmarkParameterizedQuery(b *testing.B) {
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users).
		Select(users.Col("id"), users.Col("name")).
		Where(users.Col("active").Eq(true)).
		Where(users.Col("age").Gt(18)).
		Where(users.Col("role").In("admin", "editor")).
		Order(users.Col("name").Asc()).
		Limit(10)
	v := NewPostgresVisitor(WithParams())

	b.ResetTimer()
	for b.Loop() {
		_, _, _ = m.ToSQLParams(v)
	}
}

// BenchmarkCloneCore benchmarks the cost of cloning a SelectCore.
func BenchmarkCloneCore(b *testing.B) {
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")

	m := managers.NewSelectManager(users)
	m.Select(users.Col("id"), users.Col("name"), users.Col("email"))
	m.Join(posts).On(users.Col("id").Eq(posts.Col("user_id")))
	m.Where(users.Col("active").Eq(true))
	m.Where(posts.Col("published").Eq(true))
	m.Group(users.Col("role"))
	m.Having(nodes.Count(nil).Gt(1))
	m.Order(users.Col("name").Asc())
	m.Limit(50)
	m.Offset(10)

	b.ResetTimer()
	for b.Loop() {
		m.CloneCore()
	}
}

// BenchmarkWithTransformers benchmarks the plugin pipeline cost.
func BenchmarkWithTransformers(b *testing.B) {
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users).
		Select(users.Col("id"), users.Col("name")).
		Where(users.Col("role").Eq("admin")).
		Use(softdelete.New(softdelete.WithTables("users")))
	v := NewPostgresVisitor()

	b.ResetTimer()
	for b.Loop() {
		_, _ = m.ToSQL(v)
	}
}

// BenchmarkMySQL benchmarks MySQL dialect output.
func BenchmarkMySQL(b *testing.B) {
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users).
		Select(users.Col("id"), users.Col("name"), users.Col("email")).
		Where(users.Col("active").Eq(true)).
		Order(users.Col("name").Asc()).
		Limit(10)
	v := NewMySQLVisitor()

	b.ResetTimer()
	for b.Loop() {
		_, _ = m.ToSQL(v)
	}
}
