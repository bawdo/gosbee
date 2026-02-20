# Gosbee REPL

An interactive shell for building SQL queries with gosbee and optionally
executing them against a live database.

> **Note:** The REPL is a companion tool for exploring the library and
> prototyping queries. For library usage in your Go projects, see the
> [Getting Started guide](../../docs/guide/getting-started.md).

The REPL provides:

- Interactive query building with all DML operations (SELECT, INSERT, UPDATE, DELETE)
- Live database connectivity (PostgreSQL, MySQL, SQLite)
- Tab completion for commands, tables, and columns
- Plugin support (soft-delete, OPA)
- DOT/Graphviz visualisation of the AST
- Expression evaluation for testing predicates
- Parameterised query output
- Command history and readline editing

## Installation

Install the REPL as a standalone binary:

```bash
go install github.com/bawdo/gosbee/cmd/repl@latest
```

Or run from the repository:

```bash
go run ./cmd/repl
```

On startup, the REPL walks you through configuration:

1. **Engine selection** — choose `postgres`, `mysql`, or `sqlite` (or set `GOSBEE_ENGINE`)
2. **Database connection** — an interactive wizard asks engine-specific questions to build the DSN (or set `DATABASE_URL` to skip)

### PostgreSQL example

```
[Config] Select engine (postgres, mysql, sqlite) [postgres]:
[Config] Engine: postgres
[Config] Connect to a database? (y/N): y
[Config] PostgreSQL connection setup:
[Config]   User [bawdo]:
[Config]   Password:
[Config]   Host [localhost]:
[Config]   Port [5432]:
[Config]   Database [bawdo]: myapp_dev
[Config]   SSL mode (disable/require/verify-full) [disable]:
[Config] DSN: postgres://bawdo@localhost:5432/myapp_dev?sslmode=disable
  Connected to postgres://bawdo@localhost:5432/myapp_dev?sslmode=disable (postgres)
```

### MySQL example

```
[Config] Select engine (postgres, mysql, sqlite) [postgres]: mysql
[Config] Engine: mysql
[Config] Connect to a database? (y/N): y
[Config] MySQL connection setup:
[Config]   User [root]:
[Config]   Password: secret
[Config]   Host [localhost]:
[Config]   Port [3306]:
[Config]   Database: myapp_dev
[Config] DSN: root:****@tcp(localhost:3306)/myapp_dev
  Connected to root:****@tcp(localhost:3306)/myapp_dev (mysql)
```

### SQLite example

```
[Config] Select engine (postgres, mysql, sqlite) [postgres]: sqlite
[Config] Engine: sqlite
[Config] Connect to a database? (y/N): y
[Config] SQLite connection setup:
[Config]   Database path [:memory:]:
[Config] DSN: :memory:
  Connected to :memory: (sqlite)
```

All fields have sensible defaults — press Enter to accept each one. The assembled DSN is displayed (with passwords masked) before connecting.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GOSBEE_ENGINE` | Skip the engine prompt. Values: `postgres`, `mysql`, `sqlite` |
| `DATABASE_URL` | Auto-connect on startup, skipping the connection prompt |

Example:

```bash
DATABASE_URL=postgres://user:pass@localhost:5432/mydb GOSBEE_ENGINE=postgres go run ./cmd/repl
```

## Connecting to a Database

### During Startup

If `DATABASE_URL` is not set, the REPL asks whether you want to connect and walks you through engine-specific questions to build the DSN:

| Engine | Questions | Defaults |
|--------|-----------|----------|
| **postgres** | User, Password, Host, Port, Database, SSL mode | OS user, (none), localhost, 5432, same as user, disable |
| **mysql** | User, Password, Host, Port, Database | root, (none), localhost, 3306, (required) |
| **sqlite** | Database path | :memory: |

Press Enter on any question to accept its default value.

### Within the REPL

The `connect` command works in three modes:

**1. Direct DSN** — provide a DSN as an argument:

```
gosbee> connect :memory:
  Connected to :memory: (sqlite)
```

**2. Bare `connect`** — when no previous connection exists, launches the setup wizard:

```
gosbee> connect
[Config] SQLite connection setup:
[Config]   Database path [:memory:]: /tmp/myapp.db
  DSN: /tmp/myapp.db
  Connected to /tmp/myapp.db (sqlite)
```

**3. Bare `connect` after a previous connection** — offers to reconnect or run setup again:

```
gosbee> disconnect
  Disconnected from /tmp/myapp.db
gosbee> connect
[Config]   Reconnect to /tmp/myapp.db? (y/n/setup) [y]:
  Connected to /tmp/myapp.db (sqlite)
```

Enter `setup` to go through the wizard with new parameters instead:

```
gosbee> disconnect
gosbee> connect
[Config]   Reconnect to /tmp/myapp.db? (y/n/setup) [y]: setup
[Config] SQLite connection setup:
[Config]   Database path [:memory:]: /tmp/other.db
  DSN: /tmp/other.db
  Connected to /tmp/other.db (sqlite)
```

### Switching Engines

The `engine` command changes the SQL dialect used for query generation. It does **not** change the database connection. If you switch engines after connecting, `exec` will warn about the mismatch but still execute against the connected database:

```
gosbee> engine postgres
  Engine set to postgres
gosbee> exec
  Warning: connected to sqlite but engine is set to postgres
  ...
```

To connect to a different database type, disconnect, switch engines, then connect:

```
gosbee> disconnect
gosbee> engine postgres
gosbee> connect
[Config] PostgreSQL connection setup:
[Config]   User [bawdo]:
...
```

## Executing Queries

The `exec` command (alias: `run`) executes the current query against the connected database:

```
gosbee> from users
  Query FROM "users"
gosbee> select users.id, users.name
  Projections set (2 columns)
gosbee> where users.active = true
  WHERE condition added
gosbee> exec
  SELECT "users"."id", "users"."name" FROM "users" WHERE "users"."active" = ?;
  Params: [true]
+----+-------+
| id | name  |
+----+-------+
| 1  | Alice |
| 2  | Bob   |
+----+-------+
(2 rows)
```

Key behaviors:

- **Always parameterized** — `exec` always uses bind parameters for safety, regardless of the `parameterize` toggle. The toggle only affects `sql` display output.
- **NULL display** — NULL values are displayed as `NULL` in the result table.
- **Row limit** — Results are truncated at 1,000 rows.

## Expression Evaluation

The `expr` command evaluates a standalone expression and renders it as SQL without building a full query. This is useful for learning the AST, experimenting with operators, and testing expression syntax across dialects.

```
gosbee> table users
  Registered table "users"
gosbee> expr users.active = true
  "users"."active" = TRUE
gosbee> expr users.age > 18 and users.active = true
  "users"."age" > 18 AND "users"."active" = TRUE
gosbee> expr users.name like '%alice%' or users.name like '%bob%'
  ("users"."name" LIKE '%alice%' OR "users"."name" LIKE '%bob%')
gosbee> expr users.age between 18 and 65
  "users"."age" BETWEEN 18 AND 65
gosbee> expr not users.active = true
  NOT ("users"."active" = TRUE)
```

Expressions support AND/OR combinators with standard SQL precedence (AND binds tighter than OR) and a NOT prefix:

```
gosbee> expr users.a = 1 and users.b = 2 or users.c = 3
  ("users"."a" = 1 AND "users"."b" = 2 OR "users"."c" = 3)
```

Switching engines changes the quoting style:

```
gosbee> engine mysql
  Engine set to mysql
gosbee> expr users.active = true
  `users`.`active` = TRUE
```

When parameterized mode is on, `expr` shows bind parameters and their values:

```
gosbee> parameterize
  Parameterized queries enabled
gosbee> expr users.age > 18
  "users"."age" > $1
  Params: [18]
```

## DOT Export (Graphviz)

The `dot` command exports the current AST as a Graphviz DOT file for visualization:

```
gosbee> table users
  Registered table "users"
gosbee> from users
  Query FROM "users"
gosbee> select users.id, users.name
  Projections set (2 columns)
gosbee> where users.active = true
  WHERE condition added
gosbee> dot /tmp/query.dot
  Wrote DOT to /tmp/query.dot
```

To render the DOT file as an image, use the Graphviz `dot` command:

```bash
dot -Tpng /tmp/query.dot -o /tmp/query.png
```

Other output formats: `-Tsvg`, `-Tpdf`. Install Graphviz via `brew install graphviz` (macOS) or `apt install graphviz` (Linux).

### Color Legend

| Category | Color | Nodes |
|----------|-------|-------|
| Tables | Blue | Table, TableAlias |
| Attributes | Light blue | Attribute, Star |
| Comparisons | Orange | =, !=, >, LIKE, IN, BETWEEN, IS NULL |
| Logical | Yellow | AND, OR, NOT, Grouping, DISTINCT |
| Literals | Grey | Literal values, SqlLiteral |
| Joins | Green | INNER/LEFT/RIGHT/FULL/CROSS JOIN |
| Ordering | Purple | ORDER BY ASC/DESC |
| DML | Red | INSERT/UPDATE/DELETE, Assignment, ON CONFLICT |

### Plugin Provenance

When plugins (softdelete, OPA) are active, nodes they contribute are grouped in a dashed bounding box labeled with the plugin name:

```
gosbee> plugin softdelete
  softdelete enabled (column: deleted_at)
gosbee> dot /tmp/query.dot
  Wrote DOT to /tmp/query.dot
```

The rendered graph will show the softdelete-added `deleted_at IS NULL` condition inside a dashed red cluster labeled "softdelete".

## Readline Support

The REPL uses readline for an interactive editing experience:

- **Line editing** — move within the current line with arrow keys, Ctrl+A/E, etc.
- **Command history** — Up/Down arrows navigate previous commands, persisted across sessions in `~/.gosbee_history`
- **Reverse search** — Ctrl+R searches history interactively
- **Tab completion** — context-aware completion for commands, table names, column references, engines, plugins, and operators
- **Ctrl+C** — cancels the current line without exiting
- **Ctrl+D** — exits the REPL

## Tab Completion

Press Tab to autocomplete based on context:

| Context | Completes |
|---------|-----------|
| Start of line | Command names (`from`, `select`, `where`, `join`, ...) |
| After `from`, `join`, `left join`, etc. | Table names (registered + DB schema) |
| After `select`, `where`, `having`, `group`, `expr` | Column refs (`table.col`, `table.*`) |
| After `order <col>` | Direction (`asc`, `desc`) |
| After `engine` / `set_engine` | Engine names (`postgres`, `mysql`, `sqlite`) |
| After `plugin` | Plugin names (`softdelete`, `off`) |
| After `alias` | Registered table names |
| After a column ref in condition context | Operators (`=`, `!=`, `>`, `like`, `in`, `between`, ...) |

When connected to a database, column names are auto-discovered from the schema. Type `users.` then Tab to see available columns.

## How the REPL Uses gosbee

The REPL is built on top of the public gosbee API — the same API available to
library users. Commands map to manager methods:

| REPL Command | Library Equivalent |
|--------------|-------------------|
| `from users` | `managers.NewSelectManager(users)` |
| `select users.id, users.name` | `m.Select(users.Col("id"), users.Col("name"))` |
| `where users.active = true` | `m.Where(users.Col("active").Eq(true))` |
| `join posts on users.id = posts.user_id` | `m.Join(posts).On(users.Col("id").Eq(posts.Col("user_id")))` |
| `sql` | `m.ToSQL(visitor)` |
| `plugin softdelete` | `m.Use(softdelete.New())` |

The REPL maintains a session state (current table, query, plugins, etc.) and
translates commands into method calls. This makes it a useful learning tool —
you can experiment with commands in the REPL and then translate them directly
into Go code.

For example, this REPL session:

```
gosbee> from users
gosbee> select users.id, users.name
gosbee> where users.active = true
gosbee> plugin softdelete
gosbee> sql
```

Translates to this Go code:

```go
users := nodes.NewTable("users")
m := managers.NewSelectManager(users).
    Select(users.Col("id"), users.Col("name")).
    Where(users.Col("active").Eq(true)).
    Use(softdelete.New())

v := visitors.NewPostgresVisitor()
sql, err := m.ToSQL(v)
```

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| Tab | Auto-complete |
| Up / Down | Navigate history |
| Ctrl+A | Move to start of line |
| Ctrl+E | Move to end of line |
| Ctrl+K | Delete to end of line |
| Ctrl+U | Delete to start of line |
| Ctrl+W | Delete previous word |
| Ctrl+L | Clear screen |
| Ctrl+R | Reverse history search |
| Ctrl+C | Cancel current line |
| Ctrl+D | Exit REPL |

## DML Operations

The REPL supports all four DML operations with dedicated commands.

### SELECT queries

```
gosbee> from users
gosbee> select users.id, users.name
gosbee> where users.active = true
gosbee> order users.created_at desc
gosbee> limit 10
gosbee> sql
```

### INSERT statements

```
gosbee> insert into users
gosbee> columns users.name, users.email
gosbee> values 'Alice', 'alice@example.com'
gosbee> values 'Bob', 'bob@example.com'
gosbee> returning users.id
gosbee> sql
```

With ON CONFLICT (UPSERT):

```
gosbee> insert into users
gosbee> columns users.email, users.name
gosbee> values 'alice@example.com', 'Alice'
gosbee> on conflict users.email do nothing
gosbee> sql
```

### UPDATE statements

```
gosbee> update users
gosbee> set users.status = 'inactive'
gosbee> where users.last_login < '2024-01-01'
gosbee> returning users.id
gosbee> sql
```

### DELETE statements

```
gosbee> delete from users
gosbee> where users.status = 'deleted'
gosbee> returning users.id
gosbee> sql
```

## Advanced Features

### Window Functions

```
gosbee> window w1 partition by users.department order by users.salary desc
gosbee> select users.name, sum(users.salary) over w1
gosbee> sql
```

### Common Table Expressions (CTEs)

```
gosbee> with active_users as (select * from users where users.active = true)
gosbee> from active_users
gosbee> sql
```

### Set Operations

```
gosbee> from users
gosbee> where users.status = 'active'
gosbee> union
gosbee> from users
gosbee> where users.role = 'admin'
gosbee> sql
```

## Plugins

The REPL supports the built-in soft-delete and OPA plugins. Both are marked as
**Proof of Concept** — see [plugins/README.md](../../plugins/README.md) for
details.

### Soft Delete

```
gosbee> plugin softdelete
  softdelete enabled (column: deleted_at)
gosbee> sql
  SELECT * FROM "users" WHERE "users"."deleted_at" IS NULL;
```

Custom column and table restrictions:

```
gosbee> plugin softdelete removed_at on users posts
  softdelete enabled (column: removed_at, tables: users, posts)
```

Per-table columns:

```
gosbee> plugin softdelete users.deleted_at, posts.removed_at
  softdelete enabled (per-table columns)
```

### OPA Integration

Connect to an OPA server for row-level filtering and column masking.

The `opa` command runs an interactive setup wizard. After you enter the server
URL, it queries `/v1/policies` and presents any policies whose package path
contains `include`, `filter`, or `mask` as a numbered list for selection. It
then inspects the selected policy's source to discover table names, which you
can select with a comma-separated list. Both steps fall back to manual text
entry if discovery returns no results.

```
gosbee> opa
  OPA setup:
[Config]   OPA server URL [http://localhost:8181]:
  Searching for policies...
  Found 2 matching policy(s):
    [1] data.policies.filtering.platform.consignment.include
    [2] data.policies.filtering.platform.shipment.filter
[Config]   Select policy (1-2): 1
  Inspecting policy for tables...
  Found 2 table(s):
    [1] consignments
    [2] carriers
[Config]   Select table(s) (e.g. 1,3 or leave blank for none): 1
  Policy requires 2 input(s):
[Config]   user.id: 42
[Config]   user.role: reader
  OPA enabled — policy: data.policies.filtering.platform.consignment.include
  Tables: consignments
gosbee> sql
```

**Fallback — no matching policies found:**

```
gosbee> opa
  OPA setup:
[Config]   OPA server URL [http://localhost:8181]:
  Searching for policies...
  (no matching policies found — enter path manually)
[Config]   Policy path (e.g. data.authz.allow): data.authz.allow
  Inspecting policy for tables...
  (no tables found — enter table manually)
[Config]   Table name (for data discovery): users
  OPA enabled — policy: data.authz.allow
```

After setup, `opa status` shows the active configuration including selected tables:

```
gosbee> opa status
  OPA: on
    Server: http://localhost:8181
    Policy: data.policies.filtering.platform.consignment.include
    Tables: consignments
    Inputs:
      user.id: 42
      user.role: reader
    Masks: none
```

See the [OPA plugin README](../../plugins/opa/README.md) for full details on
server setup, policy syntax, and masking behaviour.

## Full Command Reference

Type `help` in the REPL for the complete command list. Key commands:

### Query Building

| Command | Description |
|---------|-------------|
| `table <name>` | Register a table for use in queries |
| `from <table>` | Set the FROM clause (SELECT) |
| `select <cols...>` | Set projections (SELECT) |
| `where <condition>` | Add a WHERE condition |
| `join <table>` / `left join` / `cross join` | Add a JOIN |
| `on <condition>` | Set the ON clause for the last JOIN |
| `group <cols...>` | Set GROUP BY columns |
| `having <condition>` | Add a HAVING condition |
| `order <col> [asc\|desc]` | Add an ORDER BY clause |
| `limit <n>` | Set LIMIT |
| `offset <n>` | Set OFFSET |
| `distinct [on <cols...>]` | Enable DISTINCT or DISTINCT ON |
| `window <name> partition by <cols> order by <cols>` | Define a named window |

### DML Operations

| Command | Description |
|---------|-------------|
| `insert into <table>` | Start an INSERT statement |
| `columns <cols...>` | Set columns for INSERT |
| `values <vals...>` | Add a row of values (can be called multiple times) |
| `on conflict <cols> do nothing` | Add ON CONFLICT DO NOTHING |
| `on conflict <cols> do update <assignments>` | Add ON CONFLICT DO UPDATE |
| `update <table>` | Start an UPDATE statement |
| `set <col> = <val>` | Add an assignment for UPDATE |
| `delete from <table>` | Start a DELETE statement |
| `returning <cols...>` | Add a RETURNING clause (INSERT/UPDATE/DELETE) |

### Advanced Features

| Command | Description |
|---------|-------------|
| `with <name> as (<query>)` | Add a CTE |
| `union` / `intersect` / `except` | Add a set operation |
| `expr <expression>` | Evaluate a standalone expression |
| `dot <filepath>` | Export the AST as a Graphviz DOT file |

### Database Connection

| Command | Description |
|---------|-------------|
| `connect` | Reconnect to last database or launch setup wizard |
| `connect <dsn>` | Connect directly with a DSN |
| `disconnect` | Close the current database connection |
| `exec` / `run` | Execute the current query against the connected database |
| `engine <name>` | Switch SQL dialect (postgres/mysql/sqlite) |

### Plugins

| Command | Description |
|---------|-------------|
| `plugin softdelete [col]` | Enable soft-delete (default column: `deleted_at`) |
| `plugin softdelete <col> on <tables..>` | Soft-delete for specific tables only |
| `plugin softdelete <t.col, ...>` | Per-table soft-delete columns |
| `opa` | Interactive OPA setup wizard |
| `opa status` / `opa off` / `opa reload` | Manage OPA plugin |
| `plugin off [name]` | Disable one plugin by name, or all if no name given |
| `plugins` | List available plugins and their on/off status |

### Display and State

| Command | Description |
|---------|-------------|
| `sql` | Show the generated SQL |
| `ast` | Show the AST structure (debug view) |
| `parameterize` / `params` | Toggle parameterised query output |
| `reset` | Clear the current query and start fresh |
| `help` | Show all available commands |
| `exit` / `quit` | Exit the REPL |

## Related Documentation

- **[Getting Started Guide](../../docs/guide/getting-started.md)** — library usage
  in Go projects, covering all query types and the public API
- **[Visitors Guide](../../docs/guide/visitors.md)** — SQL dialect selection,
  parameterisation, and identifier quoting
- **[Plugins Guide](../../docs/guide/plugins.md)** — using and writing AST
  transformer plugins
- **[Plugin System README](../../plugins/README.md)** — plugin architecture and
  development guide
- **[OPA Plugin README](../../plugins/opa/README.md)** — Open Policy Agent
  integration details
- **[Soft Delete Plugin README](../../plugins/softdelete/README.md)** —
  soft-delete filtering implementation
