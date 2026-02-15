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

## Import styles

gosbee supports two import styles:

### Simple imports (Recommended)

Use the convenience package for cleaner code:

```go
import "github.com/bawdo/gosbee"

users := gosbee.NewTable("users")
query := gosbee.NewSelect(users)
visitor := gosbee.NewPostgresVisitor()
```

**Pros:** Shorter imports, cleaner code, easier to get started
**Best for:** Most users, especially those new to gosbee

### Explicit imports (Advanced)

Import subpackages directly for full control:

```go
import (
    "github.com/bawdo/gosbee/managers"
    "github.com/bawdo/gosbee/nodes"
    "github.com/bawdo/gosbee/visitors"
)

users := nodes.NewTable("users")
query := managers.NewSelectManager(users)
visitor := visitors.NewPostgresVisitor()
```

**Pros:** Explicit about which package each type comes from, access to advanced features
**Best for:** Library developers, advanced usage, when you need access to all node types

### Mixing both approaches

You can combine both styles — use the convenience package for common operations and import subpackages for advanced features:

```go
import (
    "github.com/bawdo/gosbee"
    "github.com/bawdo/gosbee/nodes"  // For advanced node types
)

users := gosbee.NewTable("users")
query := gosbee.NewSelect(users)

// Advanced: window functions from nodes package
query.Select(
    nodes.NewWindowFunction("row_number", nil).
        Over(nodes.NewWindow().PartitionBy(users.Col("department"))),
)
```

**Examples in this guide use the simple import style.** For the explicit style, simply replace `gosbee.NewTable()` with `nodes.NewTable()`, `gosbee.NewSelect()` with `managers.NewSelectManager()`, etc.

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

    "github.com/bawdo/gosbee"
)

func main() {
    // 1. Define tables and columns
    users := gosbee.NewTable("users")

    // 2. Build a SELECT query
    query := gosbee.NewSelect(users).
        Select(users.Col("id"), users.Col("name"), users.Col("email")).
        Where(users.Col("status").Eq(gosbee.BindParam("active"))).
        Order(users.Col("name").Asc()).
        Limit(10)

    // 3. Render SQL for your database (parameterised by default)
    visitor := gosbee.NewPostgresVisitor()
    sql, params, err := query.ToSQL(visitor)
    if err != nil {
        panic(err)
    }

    fmt.Println(sql)
    // SELECT "users"."id", "users"."name", "users"."email"
    // FROM "users"
    // WHERE "users"."status" = $1
    // ORDER BY "users"."name" ASC
    // LIMIT 10
    fmt.Println(params) // []any{"active"}
}
```

For details on switching between PostgreSQL, MySQL, and SQLite visitors, see the
[Visitors guide](visitors.md).

## Tables and attributes

Tables and their columns are the foundation of every query.

```go
users := gosbee.NewTable("users")
posts := gosbee.NewTable("posts")

// Column references
id    := users.Col("id")
name  := users.Col("name")
email := users.Col("email")

// Qualified star — "users".*
users.Star()

// Unqualified star — *
gosbee.Star()

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
active := users.Col("status").Eq(gosbee.BindParam("active"))
adult  := users.Col("age").GtEq(gosbee.BindParam(18))

// AND
active.And(adult)

// OR (automatically wrapped in parentheses)
active.Or(adult)

// NOT
active.Not()
```

Multiple calls to `Where()` are AND'ed together:

```go
query := gosbee.NewSelect(users).
    Where(users.Col("status").Eq(gosbee.BindParam("active"))).
    Where(users.Col("age").GtEq(gosbee.BindParam(18)))
// ... WHERE "users"."status" = $1 AND "users"."age" >= $2
```

## Joins

```go
posts := gosbee.NewTable("posts")

// INNER JOIN
query := gosbee.NewSelect(users).
    Select(users.Col("name"), posts.Col("title")).
    Join(posts).On(users.Col("id").Eq(posts.Col("user_id")))

// LEFT OUTER JOIN
query = gosbee.NewSelect(users).
    OuterJoin(posts).On(users.Col("id").Eq(posts.Col("user_id")))

// CROSS JOIN
query = gosbee.NewSelect(users).CrossJoin(posts)

// Raw SQL join
query = gosbee.NewSelect(users).
    StringJoin("LEFT JOIN posts ON users.id = posts.user_id")
```

## Ordering, grouping, and pagination

```go
query := gosbee.NewSelect(users).
    Select(users.Col("department"), gosbee.Count(gosbee.Star()).As("total")).
    Group(users.Col("department")).
    Having(gosbee.Count(gosbee.Star()).Gt(gosbee.BindParam(5))).
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
gosbee.Count(gosbee.Star())               // COUNT(*)
gosbee.Count(users.Col("id"))             // COUNT("users"."id")
gosbee.CountDistinct(users.Col("email"))  // COUNT(DISTINCT "users"."email")
gosbee.Sum(users.Col("total"))            // SUM(...)
gosbee.Avg(users.Col("age"))              // AVG(...)
gosbee.Min(users.Col("score"))            // MIN(...)
gosbee.Max(users.Col("score"))            // MAX(...)
```

## Column aliasing

```go
query := gosbee.NewSelect(users).
    Select(
        users.Col("id"),
        users.Col("name").As("user_name"),
        gosbee.Count(gosbee.Star()).As("total"),
    )
```

## Named functions

For advanced node types like named functions, CASE expressions, and window functions, import the `nodes` package alongside the convenience package:

```go
import (
    "github.com/bawdo/gosbee"
    "github.com/bawdo/gosbee/nodes"
)

nodes.Coalesce(users.Col("nickname"), gosbee.BindParam("Anonymous"))
nodes.Lower(users.Col("email"))
nodes.Upper(users.Col("city"))
nodes.Substring(users.Col("name"), gosbee.BindParam(1), gosbee.BindParam(3))
nodes.Cast(users.Col("age"), "TEXT")

// Arbitrary SQL functions
nodes.NewNamedFunction("MY_FUNC", users.Col("id"), gosbee.BindParam(42))
```

## CASE expressions

```go
import (
    "github.com/bawdo/gosbee"
    "github.com/bawdo/gosbee/nodes"
)

// Searched CASE
caseExpr := nodes.NewCase().
    When(users.Col("age").Lt(gosbee.BindParam(18)), gosbee.BindParam("minor")).
    When(users.Col("age").GtEq(gosbee.BindParam(18)), gosbee.BindParam("adult")).
    Else(gosbee.BindParam("unknown"))

query := gosbee.NewSelect(users).
    Select(users.Col("name"), caseExpr.As("age_group"))
```

## Window functions

```go
import (
    "github.com/bawdo/gosbee"
    "github.com/bawdo/gosbee/nodes"
)

def := nodes.NewWindowDef().
    Partition(users.Col("department")).
    Order(users.Col("salary").Desc())

query := gosbee.NewSelect(users).
    Select(
        users.Col("name"),
        gosbee.Sum(users.Col("salary")).Over(def).As("running_total"),
    )
```

## Parameterised queries

Use parameterised queries to guard against SQL injection when passing values to
your database driver.

```go
query := gosbee.NewSelect(users).
    Where(users.Col("name").Eq(gosbee.BindParam("Alice"))).
    Where(users.Col("age").Gt(gosbee.BindParam(18)))

// Parameterisation is enabled by default
visitor := gosbee.NewPostgresVisitor()
sql, params, err := query.ToSQL(visitor)
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
m := gosbee.NewInsert(users).
    Columns(users.Col("name"), users.Col("email")).
    Values(gosbee.BindParam("Alice"), gosbee.BindParam("alice@example.com")).
    Values(gosbee.BindParam("Bob"), gosbee.BindParam("bob@example.com")).
    Returning(users.Col("id"))

sql, params, err := m.ToSQL(v)
```

### UPDATE

```go
import (
    "github.com/bawdo/gosbee"
    "github.com/bawdo/gosbee/nodes"
)

m := gosbee.NewUpdate(users).
    Set(users.Col("status"), gosbee.BindParam("inactive")).
    Where(users.Col("last_login").Lt(nodes.NewSqlLiteral("NOW() - INTERVAL '90 days'"))).
    Returning(users.Col("id"))

visitor := gosbee.NewPostgresVisitor()
sql, params, err := m.ToSQL(visitor)
```

### DELETE

```go
m := gosbee.NewDelete(users).
    Where(users.Col("status").Eq(gosbee.BindParam("deleted"))).
    Returning(users.Col("id"))

visitor := gosbee.NewPostgresVisitor()
sql, params, err := m.ToSQL(visitor)
```

### UPSERT (ON CONFLICT)

```go
import (
    "github.com/bawdo/gosbee"
    "github.com/bawdo/gosbee/nodes"
)

m := gosbee.NewInsert(users).
    Columns(users.Col("email"), users.Col("name")).
    Values(gosbee.BindParam("alice@example.com"), gosbee.BindParam("Alice")).
    OnConflict(users.Col("email")).DoNothing()

// Or DO UPDATE
m = gosbee.NewInsert(users).
    Columns(users.Col("email"), users.Col("name")).
    Values(gosbee.BindParam("alice@example.com"), gosbee.BindParam("Alice")).
    OnConflict(users.Col("email")).
    DoUpdate(&nodes.AssignmentNode{
        Left:  users.Col("name"),
        Right: gosbee.BindParam("Alice"),
    })
```

## Plugins

Plugins transform the AST before SQL is rendered — for example, automatically
filtering soft-deleted rows or injecting access-control policies.

```go
import (
    "github.com/bawdo/gosbee"
    "github.com/bawdo/gosbee/plugins/softdelete"
)

query := gosbee.NewSelect(users).
    Select(users.Star()).
    Use(softdelete.New())

visitor := gosbee.NewPostgresVisitor()
sql, _, _ := query.ToSQL(visitor)
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
