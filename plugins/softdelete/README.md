# Soft Delete Plugin

**Status: Proof of Concept** — This plugin demonstrates the transformer
architecture but is not production-ready. Use it as a reference implementation
when building your own plugins.

> For general plugin usage, see the [Plugins guide](../../docs/guide/plugins.md).
> For plugin development, see the [Plugin System README](../README.md).

The soft delete plugin automatically injects `IS NULL` conditions into queries,
filtering out soft-deleted rows. It operates as a `Transformer` that appends
`WHERE "table"."deleted_at" IS NULL` for every table referenced in the FROM and
JOIN clauses.

## How It Works

The plugin inspects the AST at SQL generation time and calls
`plugins.CollectTables()` to find all referenced tables (FROM and JOINs). For
each table that passes the filter, it appends an `IS NULL` condition on the
configured column. The condition is AND'd with any existing WHERE clauses.

By default it applies to every table using `deleted_at` as the column name. Both
the column name and the set of tables can be customised via options.

The plugin supports SELECT, UPDATE, and DELETE operations. INSERT is left
untouched (no soft-delete filtering on insertions).

### Configuration Options

| Option | Description |
|---|---|
| `WithColumn(name)` | Set the soft-delete column name (default: `deleted_at`) |
| `WithTables(names...)` | Restrict the plugin to only the named tables |
| `WithTableColumn(table, column)` | Set a per-table column override (also adds the table to the whitelist) |

### Table Aliases

When a table is aliased (e.g. `"users" AS "u"`), the plugin matches by the underlying table name (`users`) but qualifies the column with the alias (`"u"."deleted_at" IS NULL`).

## Examples

### Using the Plugin in Code

```go
package main

import (
    "fmt"

    "github.com/bawdo/gosbee/managers"
    "github.com/bawdo/gosbee/nodes"
    "github.com/bawdo/gosbee/plugins/softdelete"
)

func main() {
    // --- Default: deleted_at IS NULL on all tables ---

    users := nodes.NewTable("users")
    query := managers.NewSelectManager(users)
    query.Use(softdelete.New())

    fmt.Println(query.ToSQL())
    // SELECT * FROM "users" WHERE "users"."deleted_at" IS NULL

    // --- Custom column name ---

    query = managers.NewSelectManager(users)
    query.Use(softdelete.New(softdelete.WithColumn("removed_at")))

    fmt.Println(query.ToSQL())
    // SELECT * FROM "users" WHERE "users"."removed_at" IS NULL

    // --- Restrict to specific tables ---

    posts := nodes.NewTable("posts")
    query = managers.NewSelectManager(users)
    query.InnerJoin(posts).On(users.Col("id").Eq(posts.Col("user_id")))
    query.Use(softdelete.New(softdelete.WithTables("users")))

    fmt.Println(query.ToSQL())
    // SELECT * FROM "users"
    //   INNER JOIN "posts" ON "users"."id" = "posts"."user_id"
    //   WHERE "users"."deleted_at" IS NULL
    // (posts is not filtered)

    // --- Per-table column overrides ---

    query = managers.NewSelectManager(users)
    query.InnerJoin(posts).On(users.Col("id").Eq(posts.Col("user_id")))
    query.Use(softdelete.New(
        softdelete.WithTableColumn("users", "deleted_at"),
        softdelete.WithTableColumn("posts", "removed_at"),
    ))

    fmt.Println(query.ToSQL())
    // SELECT * FROM "users"
    //   INNER JOIN "posts" ON "users"."id" = "posts"."user_id"
    //   WHERE "users"."deleted_at" IS NULL AND "posts"."removed_at" IS NULL
}
```

### Using the REPL

The REPL provides a `plugin` command to enable and configure soft delete interactively.

```
gosbee> table users
  Registered table "users"
gosbee> from users
  Query FROM "users"
gosbee> sql
  SELECT * FROM "users";

gosbee> plugin softdelete
  Soft-delete enabled (column: deleted_at)
gosbee> sql
  SELECT * FROM "users" WHERE "users"."deleted_at" IS NULL;
```

Custom column name:

```
gosbee> plugin softdelete removed_at
  Soft-delete enabled (column: removed_at)
gosbee> sql
  SELECT * FROM "users" WHERE "users"."removed_at" IS NULL;
```

Restrict to specific tables when joining:

```
gosbee> plugin softdelete removed_at on users posts
  Soft-delete enabled (column: removed_at, tables: users, posts)
```

Per-table column overrides:

```
gosbee> plugin softdelete users.deleted_at, posts.removed_at
  Soft-delete enabled (per-table columns)
```

Check active plugins:

```
gosbee> plugins
  Available plugins:
    softdelete    on   (column: deleted_at)
    opa           off
```

Disable the plugin:

```
gosbee> plugin off softdelete
  Soft-delete disabled
```

#### REPL Commands

| Command | Description |
|---|---|
| `plugin softdelete` | Enable with default column (`deleted_at`) on all tables |
| `plugin softdelete <col>` | Enable with a custom column name on all tables |
| `plugin softdelete <col> on <tables...>` | Enable with a custom column on specific tables |
| `plugin softdelete <t.col, ...>` | Enable with per-table column overrides |
| `plugin off softdelete` | Disable the soft delete plugin |
| `plugin off` | Disable all plugins |
| `plugins` | List available plugins and their status |
