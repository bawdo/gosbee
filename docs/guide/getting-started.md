# Getting Started with gosbee

gosbee is a Go SQL AST builder inspired by Ruby's Arel. It lets you build
SQL queries programmatically using a composable, type-safe API — then render
them for PostgreSQL, MySQL, or SQLite.

## Installation

```bash
go get github.com/bawdo/gosbee
```

The core library has **zero external dependencies**. Database drivers are only
required if you use the interactive REPL (see [The REPL](#the-repl) below).

## Core concepts

gosbee has three layers:

| Layer | Package | Purpose |
|-------|---------|---------|
| **Nodes** | `nodes` | AST building blocks — tables, attributes, predicates, literals |
| **Managers** | `managers` | DSL for composing queries (SELECT, INSERT, UPDATE, DELETE) |
| **Visitors** | `visitors` | Render the AST into dialect-specific SQL |

Optionally, **plugins** transform the AST before rendering — see the
[Plugins guide](plugins.md).

## Building your first query

```go
package main

import (
    "fmt"

    "github.com/bawdo/gosbee/managers"
    "github.com/bawdo/gosbee/nodes"
    "github.com/bawdo/gosbee/visitors"
)

func main() {
    // 1. Define tables and columns
    users := nodes.NewTable("users")

    // 2. Build a SELECT query
    query := managers.NewSelectManager(users).
        Select(users.Col("id"), users.Col("name"), users.Col("email")).
        Where(users.Col("status").Eq("active")).
        Order(users.Col("name").Asc()).
        Limit(10)

    // 3. Render SQL for your database
    v := visitors.NewPostgresVisitor()
    sql, err := query.ToSQL(v)
    if err != nil {
        panic(err)
    }

    fmt.Println(sql)
    // SELECT "users"."id", "users"."name", "users"."email"
    // FROM "users"
    // WHERE "users"."status" = 'active'
    // ORDER BY "users"."name" ASC
    // LIMIT 10
}
```

For details on switching between PostgreSQL, MySQL, and SQLite visitors, see the
[Visitors guide](visitors.md).

## Tables and attributes

Tables and their columns are the foundation of every query.

```go
users := nodes.NewTable("users")
posts := nodes.NewTable("posts")

// Column references
id    := users.Col("id")
name  := users.Col("name")
email := users.Col("email")

// Qualified star — "users".*
users.Star()

// Unqualified star — *
nodes.Star()

// Table alias
u := users.Alias("u")
u.Col("name") // "u"."name"
```

## Predicates (WHERE conditions)

Attributes expose predicate methods that return AST nodes:

```go
col := users.Col("age")

// Equality
col.Eq(25)                    // "users"."age" = 25
col.NotEq(25)                 // "users"."age" != 25

// Comparisons
col.Gt(18)                    // "users"."age" > 18
col.GtEq(18)                 // "users"."age" >= 18
col.Lt(65)                    // "users"."age" < 65
col.LtEq(65)                 // "users"."age" <= 65

// Ranges
col.Between(18, 65)           // "users"."age" BETWEEN 18 AND 65
col.In(1, 2, 3)              // "users"."age" IN (1, 2, 3)
col.NotIn(1, 2, 3)           // "users"."age" NOT IN (1, 2, 3)

// Pattern matching
users.Col("name").Like("A%")         // LIKE 'A%'
users.Col("name").NotLike("A%")      // NOT LIKE 'A%'

// NULL checks
col.Eq(nil)                   // "users"."age" IS NULL
col.NotEq(nil)                // "users"."age" IS NOT NULL
```

### Combining conditions

```go
active := users.Col("status").Eq("active")
adult  := users.Col("age").GtEq(18)

// AND
active.And(adult)

// OR (automatically wrapped in parentheses)
active.Or(adult)

// NOT
active.Not()
```

Multiple calls to `Where()` are AND'ed together:

```go
query := managers.NewSelectManager(users).
    Where(users.Col("status").Eq("active")).
    Where(users.Col("age").GtEq(18))
// ... WHERE "users"."status" = 'active' AND "users"."age" >= 18
```

## Joins

```go
posts := nodes.NewTable("posts")

// INNER JOIN
query := managers.NewSelectManager(users).
    Select(users.Col("name"), posts.Col("title")).
    Join(posts).On(users.Col("id").Eq(posts.Col("user_id")))

// LEFT OUTER JOIN
query = managers.NewSelectManager(users).
    OuterJoin(posts).On(users.Col("id").Eq(posts.Col("user_id")))

// CROSS JOIN
query = managers.NewSelectManager(users).CrossJoin(posts)

// Raw SQL join
query = managers.NewSelectManager(users).
    StringJoin("LEFT JOIN posts ON users.id = posts.user_id")
```

## Ordering, grouping, and pagination

```go
query := managers.NewSelectManager(users).
    Select(users.Col("department"), nodes.Count(nil).As("total")).
    Group(users.Col("department")).
    Having(nodes.Count(nil).Gt(5)).
    Order(users.Col("department").Asc()).
    Limit(20).
    Offset(40)
```

### NULLS FIRST / LAST

```go
users.Col("name").Asc().NullsFirst()
users.Col("name").Desc().NullsLast()
```

## Aggregate functions

```go
nodes.Count(nil)                         // COUNT(*)
nodes.Count(users.Col("id"))             // COUNT("users"."id")
nodes.CountDistinct(users.Col("email"))  // COUNT(DISTINCT "users"."email")
nodes.Sum(users.Col("total"))            // SUM(...)
nodes.Avg(users.Col("age"))              // AVG(...)
nodes.Min(users.Col("score"))            // MIN(...)
nodes.Max(users.Col("score"))            // MAX(...)
```

## Column aliasing

```go
query := managers.NewSelectManager(users).
    Select(
        users.Col("id"),
        users.Col("name").As("user_name"),
        nodes.Count(nil).As("total"),
    )
```

## Named functions

```go
nodes.Coalesce(users.Col("nickname"), "Anonymous")
nodes.Lower(users.Col("email"))
nodes.Upper(users.Col("city"))
nodes.Substring(users.Col("name"), 1, 3)
nodes.Cast(users.Col("age"), "TEXT")

// Arbitrary SQL functions
nodes.NewNamedFunction("MY_FUNC", users.Col("id"), nodes.Literal(42))
```

## CASE expressions

```go
// Searched CASE
caseExpr := nodes.NewCase().
    When(users.Col("age").Lt(18), nodes.Literal("minor")).
    When(users.Col("age").GtEq(18), nodes.Literal("adult")).
    Else(nodes.Literal("unknown"))

query := managers.NewSelectManager(users).
    Select(users.Col("name"), caseExpr.As("age_group"))
```

## Window functions

```go
def := nodes.NewWindowDef().
    Partition(users.Col("department")).
    Order(users.Col("salary").Desc())

query := managers.NewSelectManager(users).
    Select(
        users.Col("name"),
        nodes.Sum(users.Col("salary")).Over(def).As("running_total"),
    )
```

## Parameterised queries

Use parameterised queries to guard against SQL injection when passing values to
your database driver.

```go
v := visitors.NewPostgresVisitor(visitors.WithParams())

query := managers.NewSelectManager(users).
    Where(users.Col("name").Eq("Alice")).
    Where(users.Col("age").Gt(18))

sql, params, err := query.ToSQLParams(v)
// sql:    ... WHERE "users"."name" = $1 AND "users"."age" > $2
// params: []any{"Alice", 18}

// Pass directly to your database driver
// rows, err := db.Query(sql, params...)
```

Placeholder style is dialect-specific: PostgreSQL uses `$1, $2, ...` while
MySQL and SQLite use `?, ?, ...`.

## DML operations

gosbee supports INSERT, UPDATE, and DELETE in addition to SELECT.

### INSERT

```go
m := managers.NewInsertManager(users).
    Columns(users.Col("name"), users.Col("email")).
    Values("Alice", "alice@example.com").
    Values("Bob", "bob@example.com").
    Returning(users.Col("id"))

sql, err := m.ToSQL(v)
```

### UPDATE

```go
m := managers.NewUpdateManager(users).
    Set(users.Col("status"), "inactive").
    Where(users.Col("last_login").Lt(nodes.NewSqlLiteral("NOW() - INTERVAL '90 days'"))).
    Returning(users.Col("id"))

sql, err := m.ToSQL(v)
```

### DELETE

```go
m := managers.NewDeleteManager(users).
    Where(users.Col("status").Eq("deleted")).
    Returning(users.Col("id"))

sql, err := m.ToSQL(v)
```

### UPSERT (ON CONFLICT)

```go
m := managers.NewInsertManager(users).
    Columns(users.Col("email"), users.Col("name")).
    Values("alice@example.com", "Alice").
    OnConflict(users.Col("email")).DoNothing()

// Or DO UPDATE
m = managers.NewInsertManager(users).
    Columns(users.Col("email"), users.Col("name")).
    Values("alice@example.com", "Alice").
    OnConflict(users.Col("email")).
    DoUpdate(&nodes.AssignmentNode{
        Left:  users.Col("name"),
        Right: nodes.Literal("Alice"),
    })
```

## Plugins

Plugins transform the AST before SQL is rendered — for example, automatically
filtering soft-deleted rows or injecting access-control policies.

```go
import "github.com/bawdo/gosbee/plugins/softdelete"

query := managers.NewSelectManager(users).
    Select(users.Star()).
    Use(softdelete.New())

sql, _ := query.ToSQL(v)
// SELECT "users".* FROM "users" WHERE "users"."deleted_at" IS NULL
```

For full details on the built-in plugins and writing your own, see the
[Plugins guide](plugins.md).

## Set operations

```go
active := managers.NewSelectManager(users).
    Where(users.Col("status").Eq("active"))

admins := managers.NewSelectManager(users).
    Where(users.Col("role").Eq("admin"))

union := active.Union(admins)        // UNION
all   := active.UnionAll(admins)     // UNION ALL
inter := active.Intersect(admins)    // INTERSECT
diff  := active.Except(admins)       // EXCEPT
```

## Common Table Expressions (CTEs)

```go
activeCore := managers.NewSelectManager(users).
    Where(users.Col("status").Eq("active")).Core

activeUsers := nodes.NewTable("active_users")
query := managers.NewSelectManager(activeUsers).
    With("active_users", activeCore).
    Select(activeUsers.Col("name"))
```

## Subqueries

```go
subquery := managers.NewSelectManager(users).
    Select(users.Col("id")).
    Where(users.Col("status").Eq("active")).
    As("active_ids")

query := managers.NewSelectManager(subquery).
    Select(subquery.Col("id"))
```

## Raw SQL

When you need to embed raw SQL fragments, use `SqlLiteral`:

```go
lit := nodes.NewSqlLiteral("NOW()")
query := managers.NewSelectManager(users).
    Where(users.Col("created_at").Lt(lit))
```

## The REPL

gosbee ships with an interactive REPL for exploring queries. It connects to
live databases and provides tab completion for table and column names.

```bash
go install github.com/bawdo/gosbee/cmd/repl@latest
```

Set the SQL dialect and optionally a connection string:

```bash
export GOSBEE_ENGINE=postgres   # postgres | mysql | sqlite
export DATABASE_URL="postgres://user:pass@localhost/mydb"
repl
```

The REPL supports all query types (SELECT, INSERT, UPDATE, DELETE), plugin
toggling, DOT/Graphviz visualisation of the AST, parameterised output, and
more. Type `help` inside the REPL for the full command list.

The REPL is a useful companion for learning the library and prototyping
queries, but the primary audience for this guide is developers integrating
gosbee into their Go projects.

## Next steps

- **[Visitor Dialects](visitors.md)** — switching between PostgreSQL, MySQL, and
  SQLite, identifier quoting, and parameterisation options.
- **[Plugins](plugins.md)** — using the built-in soft-delete and OPA plugins, and
  writing your own transformers.
