# gosbee
---
![gosbee with plugins](gosbee_with_plugins.png)

**gosbee is a Go SQL Builder** — a powerful SQL AST (Abstract Syntax Tree) library
inspired by Ruby's [Arel](https://github.com/rails/arel).

Build SQL queries programmatically using composable, type-safe Go code. Instead
of concatenating strings, you construct a tree of nodes that is only converted
to SQL at the last moment by a database-specific visitor. This gives you
semantic understanding of your queries, dialect-agnostic query building, and the
ability to transform queries through middleware before SQL generation.

## Features

- 🌳 **AST-based query building** — queries are trees, not strings
- 🗄️ **Multi-dialect support** — PostgreSQL, MySQL, SQLite via the Visitor pattern
- 🔗 **Composable** — subqueries, complex JOINs, CTEs, and set operations
- 🔌 **Plugin system** — transform the AST with middleware (access control,
  soft-delete, multi-tenancy)
- ⚡ **Late binding** — SQL is only generated when you call `ToSQL()`
- 🛡️ **Parameterised queries** — built-in SQL injection protection
- 💻 **Interactive REPL** — explore and test queries against live databases
- ✅ **100% Ruby Arel feature parity** — all core SQL features supported

## Quick Start

### Installation

```bash
go get github.com/bawdo/gosbee
```

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/bawdo/gosbee/managers"
    "github.com/bawdo/gosbee/nodes"
    "github.com/bawdo/gosbee/visitors"
    "github.com/jackc/pgx/v5"
)

func main() {
    // Connect to PostgreSQL
    conn, err := pgx.Connect(context.Background(), "postgres://user:pass@localhost/dbname")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close(context.Background())

    // Define tables and columns
    users := nodes.NewTable("users")
    posts := nodes.NewTable("posts")

    // Build a query
    query := managers.NewSelectManager(users).
        Select(users.Col("id"), users.Col("name"), posts.Col("title")).
        Join(posts).On(users.Col("id").Eq(posts.Col("user_id"))).
        Where(users.Col("active").Eq(true)).
        Order(posts.Col("created_at").Desc()).
        Limit(10)

    // Generate SQL for PostgreSQL
    visitor := visitors.NewPostgresVisitor()
    sql, err := query.ToSQL(visitor)
    if err != nil {
        panic(err)
    }

    fmt.Println(sql)
    // SELECT "users"."id", "users"."name", "posts"."title"
    // FROM "users"
    // INNER JOIN "posts" ON "users"."id" = "posts"."user_id"
    // WHERE "users"."active" = TRUE
    // ORDER BY "posts"."created_at" DESC
    // LIMIT 10

    // Execute the query
    rows, err := conn.Query(context.Background(), sql)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    // Process results
    for rows.Next() {
        var id int
        var name, title string
        if err := rows.Scan(&id, &name, &title); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("User %d: %s - Post: %s\n", id, name, title)
    }
}
```

### Parameterised Queries

Protect against SQL injection with parameterised queries:

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/bawdo/gosbee/managers"
    "github.com/bawdo/gosbee/nodes"
    "github.com/bawdo/gosbee/visitors"
    "github.com/jackc/pgx/v5"
)

func main() {
    // Connect to PostgreSQL
    conn, err := pgx.Connect(context.Background(), "postgres://user:pass@localhost/dbname")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close(context.Background())

    users := nodes.NewTable("users")

    // Build parameterised query
    visitor := visitors.NewPostgresVisitor(visitors.WithParams())
    query := managers.NewSelectManager(users).
        Select(users.Col("id"), users.Col("name"), users.Col("age")).
        Where(users.Col("name").Eq("Alice")).
        Where(users.Col("age").Gt(18))

    sql, params, err := query.ToSQLParams(visitor)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println(sql)
    // SELECT "users"."id", "users"."name", "users"."age"
    // FROM "users"
    // WHERE "users"."name" = $1 AND "users"."age" > $2
    fmt.Println(params) // []any{"Alice", 18}

    // Execute with parameters (safe from SQL injection)
    rows, err := conn.Query(context.Background(), sql, params...)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    // Process results
    for rows.Next() {
        var id, age int
        var name string
        if err := rows.Scan(&id, &name, &age); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("User %d: %s (age %d)\n", id, name, age)
    }
}
```

### Using Plugins

Transform queries with plugins before SQL generation:

```go
import "github.com/bawdo/gosbee/plugins/softdelete"

query := managers.NewSelectManager(users).
    Select(users.Star()).
    Use(softdelete.New())

sql, _ := query.ToSQL(visitor)
// SELECT "users".* FROM "users" WHERE "users"."deleted_at" IS NULL
```

## Architecture

gosbee follows a layered architecture inspired by Ruby's Arel:

| Layer | Package | Purpose |
|-------|---------|---------|
| **Nodes** | `nodes/` | AST building blocks — tables, attributes, predicates, literals |
| **Managers** | `managers/` | Fluent DSL for composing queries (SELECT, INSERT, UPDATE, DELETE) |
| **Visitors** | `visitors/` | Render the AST into dialect-specific SQL |
| **Plugins** | `plugins/` | Transform the AST before SQL generation (optional) |

```
Manager → [Plugin 1] → [Plugin 2] → ... → Visitor → SQL string
           Transform    Transform         Accept
```

This architecture allows you to:
- Build queries once, render for different databases
- Transform queries with middleware (access control, logging, etc.)
- Compose complex queries from smaller pieces
- Introspect and manipulate queries programmatically

## Supported SQL Features

gosbee supports the full range of modern/ANSI SQL features:

### Queries
- SELECT, INSERT, UPDATE, DELETE
- Projections (SELECT columns)
- WHERE conditions with predicates (=, !=, >, <, LIKE, IN, BETWEEN, etc.)
- JOINs (INNER, LEFT/RIGHT/FULL OUTER, CROSS, LATERAL)
- GROUP BY / HAVING
- ORDER BY with NULLS FIRST/LAST
- LIMIT / OFFSET
- DISTINCT / DISTINCT ON

### Advanced Features
- Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.) with frames
- Common Table Expressions (WITH / WITH RECURSIVE)
- Set operations (UNION, INTERSECT, EXCEPT)
- Subqueries and table aliases
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Named functions (COALESCE, CAST, LOWER, UPPER, etc.)
- CASE expressions (searched and simple)
- Advanced grouping (CUBE, ROLLUP, GROUPING SETS)
- EXISTS / NOT EXISTS
- Query comments and optimizer hints
- Locking clauses (FOR UPDATE, FOR SHARE, SKIP LOCKED)

### DML Operations
- Multi-row INSERT
- INSERT FROM SELECT
- UPSERT (ON CONFLICT DO NOTHING / DO UPDATE)
- RETURNING clause (PostgreSQL, SQLite)

## SQL Dialects

Built-in support for three major databases:

| Dialect | Visitor | Identifier Quoting | Placeholders |
|---------|---------|-------------------|--------------|
| **PostgreSQL** | `NewPostgresVisitor()` | `"table"."column"` | `$1, $2, $3` |
| **MySQL** | `NewMySQLVisitor()` | `` `table`.`column` `` | `?, ?, ?` |
| **SQLite** | `NewSQLiteVisitor()` | `"table"."column"` | `?, ?, ?` |

Dialect-specific features (DISTINCT ON, LATERAL JOIN, RETURNING, etc.) are
handled automatically by the visitors.

See the [Visitor Dialects guide](docs/guide/visitors.md) for details.

## Interactive REPL

gosbee includes an interactive shell for exploring queries:

```bash
# Install
go install github.com/bawdo/gosbee/cmd/repl@latest

# Run
export GOSBEE_ENGINE=postgres
export DATABASE_URL="postgres://user:pass@localhost/mydb"
repl
```

The REPL provides:
- Tab completion for tables and columns
- Live query execution
- Plugin support
- DOT/Graphviz visualisation
- Expression evaluation
- Command history

See the [REPL guide](cmd/repl/README.md) for the full feature set.

## Documentation

### For Library Users

- **[Getting Started](docs/guide/getting-started.md)** — comprehensive API guide
  with examples
- **[Visitor Dialects](docs/guide/visitors.md)** — SQL dialect selection and
  parameterisation
- **[Using Plugins](docs/guide/plugins.md)** — registering and using plugins

### For Contributors

- **[Contributing Guide](CONTRIBUTING.md)** — how to contribute to the project
- **[Architecture](docs/development/architecture.md)** — core design patterns and
  visitor pattern
- **[Writing Plugins](docs/development/writing-plugins.md)** — plugin development
  guide

### Plugin Documentation

- **[Plugin System](plugins/README.md)** — transformer architecture
- **[Soft Delete Plugin](plugins/softdelete/README.md)** — soft-delete filtering
  (proof of concept)
- **[OPA Plugin](plugins/opa/README.md)** — Open Policy Agent integration (proof
  of concept)

## Acknowledgements

This project is heavily inspired by Ruby's
[Arel](https://github.com/rails/arel) library, which pioneered the SQL AST
builder pattern. Arel was created by Bryan Helmkamp and has been the foundation
of ActiveRecord's query interface since Rails 3.0.

gosbee aims to bring Arel's elegant design to the Go ecosystem with:
- Idiomatic Go patterns and conventions
- Strong typing throughout
- Zero external dependencies in the core library
- Extended plugin architecture for middleware

We're grateful to the Arel maintainers and the Ruby community for proving out
this approach to SQL query building.

## Project Status

gosbee has achieved **100% feature parity** with Ruby Arel's core SQL features.
The library is under active development with ~1096 tests covering all major
functionality.

Current focus areas:
- Performance optimisation
- Additional dialect support
- Production-ready plugin implementations
- Extended REPL features

## Contributing

We welcome contributions! Whether you're fixing a bug, improving documentation,
or adding a new feature, please see our [Contributing Guide](CONTRIBUTING.md)
for guidelines.

Common ways to contribute:
- 🐛 Report bugs or suggest features via issues
- 📖 Improve documentation and examples
- 🔌 Write new plugins for common use cases
- 🗄️ Add support for additional SQL dialects
- ✅ Add tests for edge cases

## License

TBD

## Related Projects

- [Arel](https://github.com/rails/arel) — The Ruby library that inspired this
  project
- [squirrel](https://github.com/Masterminds/squirrel) — Another Go SQL builder
  (string-based, not AST-based)
- [goqu](https://github.com/doug-martin/goqu) — SQL builder with dialect support

## Links

- **Repository:** https://github.com/bawdo/gosbee
- **Documentation:** [docs/guide/getting-started.md](docs/guide/getting-started.md)
- **Issues:** https://github.com/bawdo/gosbee/issues
