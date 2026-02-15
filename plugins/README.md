# Plugin System

The plugin system provides AST-level middleware for SQL queries. Plugins
transform the AST before SQL generation, enabling cross-cutting concerns like
row filtering, column masking, soft-delete, and multi-tenancy without modifying
query-building logic.

> For library usage documentation, see the [Plugins guide](../docs/guide/plugins.md)
> in the user-facing docs. This README is for developers working on or extending
> the plugin system itself.

## Architecture

Plugins sit between the Manager (query builder) and the Visitor (SQL generator):

```
Manager → [Plugin 1] → [Plugin 2] → ... → Visitor → SQL string
           Transform()   Transform()         Accept()
```

When `ToSQL()` or `ToSQLParams()` is called on any manager (SELECT, INSERT,
UPDATE, DELETE), it:

1. Creates a shallow clone of the statement AST (protecting the original)
2. Passes the clone through each registered transformer in order
3. Hands the transformed AST to the Visitor for SQL generation

Each transformer receives the AST node and returns a (possibly modified) node.
If any transformer returns an error, SQL generation stops and the error
propagates to the caller.

### The Transformer Interface

The entire plugin system is built on a single interface:

```go
// plugins/transformer.go
type Transformer interface {
    TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error)
    TransformInsert(stmt *nodes.InsertStatement) (*nodes.InsertStatement, error)
    TransformUpdate(stmt *nodes.UpdateStatement) (*nodes.UpdateStatement, error)
    TransformDelete(stmt *nodes.DeleteStatement) (*nodes.DeleteStatement, error)
}
```

Embed `plugins.BaseTransformer` to get no-op defaults for methods you don't
need. For example, a soft-delete plugin might only override `TransformSelect`
and `TransformUpdate`, leaving INSERT and DELETE untouched.

The AST nodes expose all parts of their statements for inspection and
modification. See `nodes/select_core.go`, `nodes/insert_statement.go`,
`nodes/update_statement.go`, and `nodes/delete_statement.go` for field details.

### Registration

Plugins are registered on a `SelectManager` via the `Use()` method. Multiple plugins are applied in registration order:

```go
query := managers.NewSelectManager(table)
query.Use(softdelete.New())      // runs first
query.Use(opa.NewFromServer(...)) // runs second
```

### Clone Protection

All managers (`SelectManager`, `InsertManager`, `UpdateManager`,
`DeleteManager`) clone their statement AST before passing it to transformers.
This shallow copies all slice fields (`Projections`, `Wheres`, `Joins`,
`Values`, etc.) so that plugins can append to or replace slices without mutating
the original. The original AST remains untouched, allowing the same query to be
generated multiple times with different results (e.g. if policy inputs change
between calls).

### Helper: CollectTables

The `plugins` package provides `CollectTables()` to extract all table references from a `SelectCore`:

```go
refs := plugins.CollectTables(core) // []TableRef
for _, ref := range refs {
    ref.Name     // underlying table name (e.g. "users")
    ref.Relation // the Node used for column references (preserves aliases)
}
```

This handles `*nodes.Table` and `*nodes.TableAlias` and skips subqueries.

## Built-in Plugins

| Plugin | Package | Status | Description |
|--------|---------|--------|-------------|
| [Soft Delete](softdelete/README.md) | `plugins/softdelete` | **Proof of Concept** | Injects `IS NULL` conditions to filter soft-deleted rows |
| [OPA](opa/README.md) | `plugins/opa` | **Proof of Concept** | Enforces Open Policy Agent policies via row filtering and column masking |

Both plugins demonstrate the plugin architecture but are not production-ready.
Use them as reference implementations when building your own plugins.

## Writing Your Own Plugin

### Step 1: Create the Package

Create a new directory under `plugins/` for your plugin:

```
plugins/
  mytenant/
    mytenant.go
    mytenant_test.go
```

### Step 2: Implement the Transformer Interface

Embed `plugins.BaseTransformer` and override the methods you need:

```go
package mytenant

import (
    "github.com/bawdo/gosbee/nodes"
    "github.com/bawdo/gosbee/plugins"
)

type MultiTenant struct {
    plugins.BaseTransformer // provides no-op defaults
    TenantID int
}

func New(tenantID int) *MultiTenant {
    return &MultiTenant{TenantID: tenantID}
}

func (mt *MultiTenant) TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error) {
    for _, ref := range plugins.CollectTables(core) {
        attr := nodes.NewAttribute(ref.Relation, "tenant_id")
        core.Wheres = append(core.Wheres, attr.Eq(mt.TenantID))
    }
    return core, nil
}

// TransformUpdate, TransformInsert, TransformDelete inherited as no-ops from BaseTransformer
```

Key points:

- Use `plugins.CollectTables(core)` to iterate over all tables in the query (FROM + JOINs)
- Use `ref.Relation` (not a new `nodes.NewTable()`) when creating column references — this preserves table aliases
- Use `ref.Name` when you need to match against a table name for filtering logic
- Append conditions to `core.Wheres` — they are AND'd together automatically
- Return `nil, error` to reject the query entirely (e.g. access denied)
- The core you receive is already a clone — you can safely modify its slices

### Step 3: Write Tests

Test directly against the `SelectCore` and `Visitor` without needing a database:

```go
package mytenant

import (
    "testing"

    "github.com/bawdo/gosbee/nodes"
    "github.com/bawdo/gosbee/visitors"
)

func TestInjectsTenantCondition(t *testing.T) {
    users := nodes.NewTable("users")
    core := &nodes.SelectCore{From: users}

    mt := New(42)
    result, err := mt.Transform(core)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    got := result.Accept(visitors.NewPostgresVisitor())
    expected := `SELECT * FROM "users" WHERE "users"."tenant_id" = 42`
    if got != expected {
        t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
    }
}
```

Test with joins to verify multi-table behavior:

```go
func TestAppliedToJoinedTables(t *testing.T) {
    users := nodes.NewTable("users")
    posts := nodes.NewTable("posts")
    core := &nodes.SelectCore{
        From: users,
        Joins: []*nodes.JoinNode{
            {
                Left:  users,
                Right: posts,
                Type:  nodes.InnerJoin,
                On:    users.Col("id").Eq(posts.Col("user_id")),
            },
        },
    }

    mt := New(42)
    result, err := mt.Transform(core)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    got := result.Accept(visitors.NewPostgresVisitor())
    expected := `SELECT * FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id" WHERE "users"."tenant_id" = 42 AND "posts"."tenant_id" = 42`
    if got != expected {
        t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
    }
}
```

### Step 4: Use It

```go
query := managers.NewSelectManager(users)
query.Use(mytenant.New(42))

sql, err := query.ToSQL(visitors.NewPostgresVisitor())
// SELECT * FROM "users" WHERE "users"."tenant_id" = 42
```

Plugins compose with each other:

```go
query.Use(softdelete.New())
query.Use(mytenant.New(42))
// SELECT * FROM "users"
//   WHERE "users"."deleted_at" IS NULL AND "users"."tenant_id" = 42
```

### Design Guidelines

- **Modify the core directly** — you receive a clone, so appending to `Wheres`, `Projections`, etc. is safe.
- **Use functional options** for configuration (see `softdelete.WithColumn()`, `opa.WithColumnResolver()`).
- **Return errors for hard failures** — e.g. access denied, missing configuration. The error stops SQL generation.
- **Keep transforms idempotent** — `ToSQL()` may be called multiple times on the same manager.
- **Avoid side effects** — don't make network calls in `Transform()` if you can fetch data beforehand and close over it (though the OPA plugin is an intentional exception since policy must be evaluated at query time).
