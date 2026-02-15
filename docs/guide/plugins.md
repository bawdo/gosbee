# Using Plugins

> This guide covers registering and using AST transformer plugins. For an
> introduction to gosbee, see the [Getting Started guide](getting-started.md).

Plugins transform the AST before a visitor renders it into SQL. They implement
the `Transformer` interface and are registered on managers with `Use()`.

## Registering a plugin

Call `Use()` on any manager — SELECT, INSERT, UPDATE, or DELETE:

```go
import "github.com/bawdo/gosbee/plugins/softdelete"

query := managers.NewSelectManager(users).
    Select(users.Star()).
    Use(softdelete.New())
```

Multiple plugins can be chained. They are applied in registration order:

```go
query := managers.NewSelectManager(users).
    Select(users.Star()).
    Use(softdelete.New()).
    Use(myCustomPlugin)
```

Plugins run automatically when you call `ToSQL()` or `ToSQLParams()`. The
original AST is cloned before transformation, so the source manager is never
mutated.

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

The plugin applies to all DML operations:

```go
// SELECT — adds WHERE "users"."deleted_at" IS NULL
managers.NewSelectManager(users).Use(sd)

// UPDATE — adds WHERE "users"."deleted_at" IS NULL
managers.NewUpdateManager(users).Set(users.Col("status"), "inactive").Use(sd)

// DELETE — adds WHERE "users"."deleted_at" IS NULL
managers.NewDeleteManager(users).Use(sd)
```

When a query involves joins, the soft-delete condition is added for each joined
table (unless restricted with `WithTables`).

### OPA (Open Policy Agent)

The `opa` plugin injects access-control conditions based on policies evaluated
at query time. It supports both local policy functions and a remote OPA server.

#### Local policy function

```go
import (
    "github.com/bawdo/gosbee/nodes"
    "github.com/bawdo/gosbee/plugins/opa"
)

policy := func(tableName string) ([]nodes.Node, error) {
    if tableName == "secrets" {
        return nil, errors.New("access denied to secrets table")
    }
    if tableName == "users" {
        t := nodes.NewTable(tableName)
        return []nodes.Node{t.Col("tenant_id").Eq(42)}, nil
    }
    return nil, nil // no restrictions
}

query := managers.NewSelectManager(users).
    Select(users.Star()).
    Use(opa.New(policy))

// SELECT "users".* FROM "users"
// WHERE "users"."tenant_id" = 42
```

#### Remote OPA server

```go
opaPlugin := opa.NewFromServer(
    "http://localhost:8181",     // OPA server URL
    "data.authz.allow",         // policy path
)

query := managers.NewSelectManager(users).
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

If a transformer returns a non-nil error, `ToSQL()` and `ToSQLParams()` will
propagate it and no SQL is generated. This is useful for policy enforcement:

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
