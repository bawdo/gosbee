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

Upgrading from an earlier version changes the SQL this plugin generates; see
[Upgrading](#upgrading) below.

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

A transformer is called for every SELECT in the query, not just the outermost
one, so this stamps the comment on each subquery, CTE body and set-operation
branch too. That is harmless for a comment. If you want it once, set it on the
manager instead with `query.Comment("audit: query by service-x")`. See
[Writing Plugins](../development/writing-plugins.md) for how the walker calls
your plugin.

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

## Upgrading

Plugins now police every query nested inside the one you build, and refuse
what they cannot see rather than let it through. If you are upgrading from a
version that only transformed the outermost SELECT, expect these changes:

**Every plugin, including your own**

- A transformer now runs on every nested `SelectCore`: subqueries, CTE
  bodies, joined subqueries and each branch of a set operation, inner ones
  first. A plugin that adds a `LIMIT`, for example, now limits every subquery
  as well. Check the core you are handed if that is not what you want.
- A plugin whose `TransformSelect` delegates to `plugins.ApplySelect` must
  also implement `TransformSelectScoped`, or it recurses until the stack
  overflows.
- A transformer that returns a `nil` core with no error now fails the query
  with `plugins: transformer returned nil core`, instead of panicking when the
  SQL is rendered.
- Under a strict plugin (softdelete by default, OPA always), a custom
  `nodes.Node` type the walker does not know is refused with
  `plugins.ErrUnpoliceable`. Under a non-strict plugin it is kept unchanged.
- Under a strict plugin, a `FROM` or `JOIN` target that is not a table, a
  nested query, or an alias over one of those (for example `FROM ("users")` or
  an alias over another alias) is refused with `plugins.ErrUnpoliceable`.
- In a `WITH RECURSIVE` list a CTE body now sees only the CTEs declared before
  it, plus its own name when it is recursive. A reference to a CTE declared
  later is treated as the real table and filtered, as MySQL resolves it. On
  dialects that allow forward references this adds a filter, or an SQL error,
  rather than letting the table through.

**Soft delete**

- Subqueries, CTE bodies and set-operation branches now gain the `IS NULL`
  condition, so the generated SQL changes.
- Queries containing raw SQL (`SqlLiteral`, `StringJoin`) now fail with
  `plugins.ErrUnpoliceable`. Either replace the raw fragment with AST nodes or
  add `WithAllowRawSQL()`.

**OPA**

- Policy conditions are applied inside nested queries too, and each table is
  evaluated once per query.
- A condition for an aliased table (`FROM users AS u`) is now written against
  the alias, so it filters `u` rather than some other `users` row in scope,
  such as the outer row of a correlated subquery or the other side of a
  self-join.
- Queries containing raw SQL fail with `plugins.ErrUnpoliceable`.
- INSERT, UPDATE and DELETE fail with `opa.ErrUnsupportedStatement`; the
  plugin only polices SELECT.
- A masked column used anywhere other than as a plain projection (in a
  `WHERE`, an expression, `ORDER BY` and so on) fails with
  `opa.ErrMaskedColumnUse`.
- A masked projection is now rendered as a bind parameter aliased to the
  column (`$1 AS "ssn"` on PostgreSQL), not an inline quoted literal.

**Calling `TransformSelect` directly**

- `TransformSelect` on the softdelete and OPA plugins, called outside a
  manager, now walks the whole tree the same way a manager does and no longer
  modifies the core you pass in. Use the returned core.

## Next steps

- **[Getting Started](getting-started.md)** — building queries with the managers
  API.
- **[Visitor Dialects](visitors.md)** — switching between PostgreSQL, MySQL, and
  SQLite.
