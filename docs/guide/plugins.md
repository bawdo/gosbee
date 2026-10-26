# Using Plugins

> This guide covers registering and using AST transformer plugins. For an
> introduction to gosbee, see the [Getting Started guide](getting-started.md).

Plugins transform the AST before a visitor renders it into SQL. They implement
the `Transformer` interface and are registered on managers with `Use()`.

## Registering a plugin

Call `Use()` on any manager — SELECT, INSERT, UPDATE, or DELETE:

```go
import (
    "github.com/bawdo/gosbee"
    "github.com/bawdo/gosbee/plugins/softdelete"
)

users := gosbee.NewTable("users")
query := gosbee.NewSelect(users).
    Select(users.Star()).
    Use(softdelete.New())
```

Multiple plugins can be chained. They are applied in registration order:

```go
query := gosbee.NewSelect(users).
    Select(users.Star()).
    Use(softdelete.New()).
    Use(myCustomPlugin)
```

Plugins run automatically when you call `ToSQL()`. The original AST is cloned
before transformation, so the source manager is never mutated.

## Built-in plugins

### Soft Delete

The `softdelete` plugin automatically adds `WHERE deleted_at IS NULL`
conditions to queries, ensuring soft-deleted rows are excluded without manual
filtering.

```go
import "github.com/bawdo/gosbee/plugins/softdelete"

// Default: filters on "deleted_at" column for all tables
sd := softdelete.New()

// Custom column name
sd = softdelete.New(softdelete.WithColumn("removed_at"))

// Restrict to specific tables only
sd = softdelete.New(softdelete.WithTables("users", "posts"))

// Per-table column mapping
sd = softdelete.New(
    softdelete.WithTableColumn("users", "deleted_at"),
    softdelete.WithTableColumn("posts", "removed_at"),
)
```

The plugin filters SELECT queries only. It does not touch INSERT, UPDATE or
DELETE statements, or subqueries inside them, so add the condition yourself on
those.

When a query involves joins, the soft-delete condition is added for each joined
table (unless restricted with `WithTables`).

#### Nested queries

The condition is applied to every SELECT in the query, not just the outermost
one: subqueries, CTE bodies, joined subqueries and each branch of a
`UNION`, `INTERSECT` or `EXCEPT`. A name that refers to a CTE is not a table,
so `FROM "c"` is left alone (the filter lives inside the CTE body instead).

```go
import (
    "github.com/bawdo/gosbee"
    "github.com/bawdo/gosbee/plugins/softdelete"
)

sd := softdelete.New()
users := gosbee.NewTable("users")
posts := gosbee.NewTable("posts")

sub := gosbee.NewSelect(posts).Select(posts.Col("user_id"))
q := gosbee.NewSelect(users).
    Where(users.Col("id").In(sub)).
    Use(sd)
// SELECT * FROM "users" WHERE "users"."id" IN
//   (SELECT "posts"."user_id" FROM "posts" WHERE "posts"."deleted_at" IS NULL)
//   AND "users"."deleted_at" IS NULL
```

#### Raw SQL is refused by default

The plugin cannot see inside a raw SQL fragment (`SqlLiteral`, `StringJoin`),
so it cannot tell whether the fragment reads a table it should be filtering.
Rather than guess, it returns an error wrapping `plugins.ErrUnpoliceable`. If
you know your raw fragments are safe, opt in:

```go
sd := softdelete.New(softdelete.WithAllowRawSQL())
```

#### Upgrading

Earlier versions only filtered the outermost SELECT and let raw SQL through.
After upgrading:

- Subqueries, CTE bodies and set-operation branches now gain the
  `IS NULL` condition, so the generated SQL changes.
- Queries containing raw SQL now fail with `plugins.ErrUnpoliceable`. Either
  replace the raw fragment with AST nodes or add `WithAllowRawSQL()`.

### OPA (Open Policy Agent)

The `opa` plugin injects access-control conditions based on policies evaluated
at query time. It supports both local policy functions and a remote OPA server.

#### Local policy function

```go
import (
    "github.com/bawdo/gosbee"
    "github.com/bawdo/gosbee/nodes"
    "github.com/bawdo/gosbee/plugins/opa"
)

policy := func(tableName string) ([]nodes.Node, error) {
    if tableName == "secrets" {
        return nil, errors.New("access denied to secrets table")
    }
    if tableName == "users" {
        t := gosbee.NewTable(tableName)
        return []nodes.Node{t.Col("tenant_id").Eq(gosbee.BindParam(42))}, nil
    }
    return nil, nil // no restrictions
}

users := gosbee.NewTable("users")
query := gosbee.NewSelect(users).
    Select(users.Star()).
    Use(opa.New(policy))

// SELECT "users".* FROM "users"
// WHERE "users"."tenant_id" = 42
```

#### Remote OPA server

```go
import (
    "github.com/bawdo/gosbee"
    "github.com/bawdo/gosbee/plugins/opa"
)

opaPlugin := opa.NewFromServer(
    "http://localhost:8181",     // OPA server URL
    "data.authz.allow",         // policy path
)

users := gosbee.NewTable("users")
query := gosbee.NewSelect(users).
    Select(users.Star()).
    Use(opaPlugin)
```

## The Transformer interface

To write your own plugin, implement the `Transformer` interface from the
`plugins` package:

```go
import "github.com/bawdo/gosbee/plugins"

type Transformer interface {
    TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error)
    TransformInsert(stmt *nodes.InsertStatement) (*nodes.InsertStatement, error)
    TransformUpdate(stmt *nodes.UpdateStatement) (*nodes.UpdateStatement, error)
    TransformDelete(stmt *nodes.DeleteStatement) (*nodes.DeleteStatement, error)
}
```

Embed `plugins.BaseTransformer` to get no-op defaults for methods you don't
need:

```go
type AuditLogger struct {
    plugins.BaseTransformer
}

func (a *AuditLogger) TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error) {
    // Add a query comment for audit logging
    core.Comment = "audit: query by service-x"
    return core, nil
}
```

Register it like any other plugin:

```go
query := managers.NewSelectManager(users).
    Use(&AuditLogger{})
```

### Returning an error

If a transformer returns a non-nil error, `ToSQL()` will propagate it and no SQL
is generated. This is useful for policy enforcement:

```go
func (p *MyPolicy) TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error) {
    if containsForbiddenTable(core) {
        return nil, errors.New("access denied")
    }
    return core, nil
}
```

## Next steps

- **[Getting Started](getting-started.md)** — building queries with the managers
  API.
- **[Visitor Dialects](visitors.md)** — switching between PostgreSQL, MySQL, and
  SQLite.
