# Visitor Dialects

> This guide covers SQL dialect selection and parameterisation. For an
> introduction to gosbee, see the [Getting Started guide](getting-started.md).

Visitors render the gosbee AST into dialect-specific SQL. Three built-in
visitors are provided — one for each supported database.

## Choosing a visitor

```go
import "github.com/bawdo/gosbee/visitors"

// PostgreSQL — double-quoted identifiers, $1/$2 parameters
v := visitors.NewPostgresVisitor()

// MySQL — backtick-quoted identifiers, ? parameters
v := visitors.NewMySQLVisitor()

// SQLite — double-quoted identifiers, ? parameters
v := visitors.NewSQLiteVisitor()
```

Pass the visitor to any manager's `ToSQL` or `ToSQLParams` method:

```go
sql, err := query.ToSQL(v)
```

## Identifier quoting

Each dialect quotes table and column names differently:

| Dialect | Style | Example |
|---------|-------|---------|
| PostgreSQL | Double quotes | `"users"."name"` |
| MySQL | Backticks | `` `users`.`name` `` |
| SQLite | Double quotes | `"users"."name"` |

Quoting is handled automatically — you never need to quote identifiers yourself.

## Parameterised queries

Enable parameterisation by passing `WithParams()` when creating a visitor:

```go
v := visitors.NewPostgresVisitor(visitors.WithParams())

query := managers.NewSelectManager(users).
    Where(users.Col("name").Eq("Alice")).
    Where(users.Col("age").Gt(18))

sql, params, err := query.ToSQLParams(v)
// sql:    SELECT ... WHERE "users"."name" = $1 AND "users"."age" > $2
// params: []any{"Alice", 18}
```

### Placeholder styles

| Dialect | Placeholders |
|---------|-------------|
| PostgreSQL | `$1`, `$2`, `$3` (1-based) |
| MySQL | `?`, `?`, `?` (positional) |
| SQLite | `?`, `?`, `?` (positional) |

### What is parameterised

- Go values (strings, numbers, booleans) are replaced with placeholders and
  collected into the params slice.
- `NULL` is always rendered inline (`IS NULL`, not a parameter).
- `SqlLiteral` values are always rendered inline — they represent trusted SQL
  fragments.
- Node-to-node comparisons (e.g. `col.Eq(otherCol)`) produce no parameters.

### Reusing a visitor

A parameterising visitor accumulates state across calls. Call `Reset()` before
reusing it, or use `ToSQLParams()` which handles this automatically:

```go
// Manual reset
v.Reset()
sql, err := query.ToSQL(v)
params := v.Params()

// Or use the convenience method (recommended)
sql, params, err := query.ToSQLParams(v)
```

## Dialect-specific features

Some SQL features behave differently across dialects. gosbee handles the
differences automatically:

| Feature | PostgreSQL | MySQL | SQLite |
|---------|-----------|-------|--------|
| DISTINCT ON | Supported | Not supported | Not supported |
| NULLS FIRST/LAST | Supported | Emulated | Emulated |
| RETURNING | Supported | Not supported | Supported |
| ON CONFLICT | Supported | Not supported | Supported |
| FOR UPDATE/SHARE | Supported | Supported | Not supported |
| LATERAL JOIN | Supported | Supported | Not supported |
| Window frames | Full support | Full support | Full support |
| CASE-insensitive match | `ILIKE` | `LIKE` (default) | `LIKE` (default) |

## Next steps

- **[Getting Started](getting-started.md)** — building queries with the managers
  API.
- **[Plugins](plugins.md)** — using transformers to modify queries before
  rendering.
