# Visitor Dialects

> This guide covers SQL dialect selection and parameterisation. For an
> introduction to gosbee, see the [Getting Started guide](getting-started.md).

Visitors render the gosbee AST into dialect-specific SQL. Three built-in
visitors are provided — one for each supported database.

## Choosing a visitor

**Simple import style:**

```go
import "github.com/bawdo/gosbee"

// PostgreSQL — double-quoted identifiers, $1/$2 parameters
visitor := gosbee.NewPostgresVisitor()

// MySQL — backtick-quoted identifiers, ? parameters
visitor := gosbee.NewMySQLVisitor()

// SQLite — double-quoted identifiers, ? parameters
visitor := gosbee.NewSQLiteVisitor()
```

**Explicit import style:**

```go
import "github.com/bawdo/gosbee/visitors"

visitor := visitors.NewPostgresVisitor()
visitor := visitors.NewMySQLVisitor()
visitor := visitors.NewSQLiteVisitor()
```

Pass the visitor to any manager's `ToSQL` method:

```go
sql, params, err := query.ToSQL(visitor)
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

Parameterised queries are **enabled by default** for SQL injection protection. Use `BindParam()` to create parameterised values:

```go
import "github.com/bawdo/gosbee"

query := gosbee.NewSelect(users).
    Where(users.Col("name").Eq(gosbee.BindParam("Alice"))).
    Where(users.Col("age").Gt(gosbee.BindParam(18)))

// Parameterisation is enabled by default
visitor := gosbee.NewPostgresVisitor()
sql, params, err := query.ToSQL(visitor)
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

### Disabling parameterisation (Not Recommended)

⚠️ **WARNING**: Disabling parameterisation removes SQL injection protection. Only use for debugging or when all values are trusted. **Production code should NEVER use this option.**

```go
// Disable parameterisation (literals instead of placeholders)
visitor := gosbee.NewPostgresVisitor(gosbee.WithoutParams())
sql, _, err := query.ToSQL(visitor)
// sql:    SELECT ... WHERE "users"."name" = 'Alice' AND "users"."age" > 18
// Note: No $1, $2 placeholders — values are inlined
```

### Reusing a visitor

A parameterising visitor accumulates state across calls. The `ToSQL()` method handles reset automatically:

```go
// ToSQL automatically resets the visitor before rendering
sql, params, err := query.ToSQL(visitor)

// For manual control, call Reset() before rendering
visitor.Reset()
sql := query.Accept(visitor)
params := visitor.Params()
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
