# OPA Plugin

**Status: Proof of Concept** — This plugin demonstrates integration with
external policy engines but is not production-ready. Use it as a reference
implementation when building your own policy-driven plugins.

> For general plugin usage, see the [Plugins guide](../../docs/guide/plugins.md).
> For plugin development, see the [Plugin System README](../README.md).

The OPA plugin integrates [Open Policy Agent](https://www.openpolicyagent.org/)
with gosbee to enforce row-level filtering and column-level masking on SQL
queries. Policies are evaluated externally by an OPA server (or in-process via a
Go callback) and the results are injected into the query AST before SQL
generation.

## How It Works

The plugin operates as a `Transformer` that is applied to the AST before SQL
generation. It does two things:

1. **Row filtering** — Calls the OPA [Compile API](https://www.openpolicyagent.org/docs/latest/rest-api/#compile-api)
   (`POST /v1/compile`) with the table name as an unknown. OPA returns partial
   evaluation residuals — conditions the query must satisfy. These are
   translated into AST nodes and appended to the WHERE clause.

2. **Column masking** — Calls the OPA [Data API](https://www.openpolicyagent.org/docs/latest/rest-api/#data-api)
   (`POST /v1/data/<package>/masks`) to evaluate mask rules. Columns marked for
   masking have their projections replaced with literal values (e.g.
   `'<MASKED>' AS "billed_total"`). When the query uses `SELECT *`, the star is
   expanded into explicit column references using a `ColumnResolver` so that
   individual columns can be masked.

The plugin currently only supports SELECT queries. UPDATE, INSERT, and DELETE
are left untouched.

### Supported Operators

The Compile API response is translated into SQL using these OPA operators:

| OPA Operator   | SQL                          |
|----------------|------------------------------|
| `eq` / `equal` | `=`                          |
| `neq`          | `!=`                         |
| `lt`           | `<`                          |
| `lte`          | `<=`                         |
| `gt`           | `>`                          |
| `gte`          | `>=`                         |
| `startswith`   | `LIKE 'value%'`              |
| `endswith`     | `LIKE '%value'`              |
| `contains`     | `LIKE '%value%'`             |

Multiple queries in the Compile response are AND'd within each query and OR'd across queries, matching OPA's partial evaluation semantics. An empty query set means access denied. A single empty query means unconditional allow (no conditions injected).

### Mask Resolution

Masks are fetched from the Data API by deriving the masks path from the policy path. For example, if the policy path is `data.policies.filtering.ecommerce.orders.include`, the masks are fetched from `policies/filtering/ecommerce/orders/masks`.

The mask response is a nested map of `table -> column -> action`. A `replace` action with a string value means the column is masked. A non-string value (such as an empty object `{}`) means no mask applies — this allows role-based masking where a superadmin sees all columns unmasked.

### Two Modes

- **Server mode** (`NewFromServer`) — Calls a running OPA server. Supports both row filtering and column masking.
- **PolicyFunc mode** (`New`) — Calls a Go function directly for row filtering. No masking support (projections are untouched).

## Examples

### Using the Plugin in Code

```go
package main

import (
    "fmt"

    "github.com/bawdo/gosbee/managers"
    "github.com/bawdo/gosbee/nodes"
    "github.com/bawdo/gosbee/plugins/opa"
)

func main() {
    // --- PolicyFunc mode (in-process, no OPA server) ---

    policy := func(table string) ([]nodes.Node, error) {
        if table == "orders" {
            t := nodes.NewTable(table)
            cond := t.Col("merchant_name").Eq("Koala Commerce Pty Ltd")
            return []nodes.Node{cond}, nil
        }
        return nil, nil
    }

    table := nodes.NewTable("orders")
    query := managers.NewSelectManager(table)
    query.Use(opa.New(policy))

    sql := query.ToSQL()
    fmt.Println(sql)
    // SELECT * FROM "orders" WHERE "orders"."merchant_name" = 'Koala Commerce Pty Ltd'

    // --- Server mode (calls a running OPA server) ---

    input := map[string]any{
        "user": map[string]any{
            "merchant_name": "Koala Commerce Pty Ltd",
            "state":         "NSW",
            "role":          "sales",
        },
    }

    // Column resolver provides schema knowledge for star expansion.
    resolver := func(tableName string) ([]string, error) {
        if tableName == "orders" {
            return []string{"merchant_name", "total_amount", "customer_state"}, nil
        }
        return nil, fmt.Errorf("unknown table %q", tableName)
    }

    table = nodes.NewTable("orders")
    query = managers.NewSelectManager(table)
    query.Use(opa.NewFromServer(
        "http://localhost:8181",
        "data.policies.filtering.ecommerce.orders.include",
        input,
        opa.WithColumnResolver(resolver),
    ))

    sql = query.ToSQL()
    fmt.Println(sql)
    // SELECT "orders"."merchant_name", '<MASKED>' AS "total_amount", "orders"."customer_state"
    // FROM "orders"
    // WHERE "orders"."merchant_name" = 'Koala Commerce Pty Ltd'
    //   AND "orders"."customer_state" = 'NSW'
}
```

### Using the REPL

The REPL provides an interactive `opa` command that connects to an OPA server, auto-discovers required inputs, and applies both row filtering and column masking to queries.

```
gosbee> table orders
  Registered table "orders"
gosbee> from orders
  Query FROM "orders"
gosbee> select orders.merchant_name , orders.total_amount , orders.customer_state
  Projections set (3 columns)
gosbee> limit 10
  LIMIT set to 10
gosbee> run
  SELECT "orders"."merchant_name", "orders"."total_amount", "orders"."customer_state" FROM "orders" LIMIT $1;
  Params: [10]
+---------------------------+--------------+----------------+
| merchant_name             | total_amount | customer_state |
+---------------------------+--------------+----------------+
| Fair Dinkum Books         | 89.95        | NSW            |
| Fair Dinkum Books         | 145.50       | NSW            |
| Billabong Homewares       | 299.00       | VIC            |
| Koala Commerce Pty Ltd    | 54.90        | QLD            |
| Vegemite Ventures         | 199.95       | NSW            |
| Koala Commerce Pty Ltd    | 129.00       | NSW            |
| Wattle & Co Trading       | 78.50        | WA             |
| Fair Dinkum Books         | 234.00       | VIC            |
| Koala Commerce Pty Ltd    | 89.95        | NSW            |
| Billabong Homewares       | 449.00       | SA             |
+---------------------------+--------------+----------------+
(10 rows)
gosbee> opa
  OPA setup:
[Config]   OPA server URL [http://localhost:8181]:
[Config]   Policy path (e.g. data.authz.allow): data.policies.filtering.ecommerce.orders.include
[Config]   Table name (for data discovery): orders
  Policy requires 3 input(s):
[Config]   user.merchant_name: Koala Commerce Pty Ltd
[Config]   user.state: NSW
[Config]   user.role: sales
  OPA enabled — policy: data.policies.filtering.ecommerce.orders.include
gosbee> run
  SELECT "orders"."merchant_name", '<MASKED>' AS "total_amount", "orders"."customer_state" FROM "orders" WHERE "orders"."merchant_name" = $1 AND "orders"."customer_state" = $2 LIMIT $3;
  Params: [Koala Commerce Pty Ltd NSW 10]
+------------------------+--------------+----------------+
| merchant_name          | total_amount | customer_state |
+------------------------+--------------+----------------+
| Koala Commerce Pty Ltd | <MASKED>     | NSW            |
| Koala Commerce Pty Ltd | <MASKED>     | NSW            |
| Koala Commerce Pty Ltd | <MASKED>     | NSW            |
| Koala Commerce Pty Ltd | <MASKED>     | NSW            |
| Koala Commerce Pty Ltd | <MASKED>     | NSW            |
| Koala Commerce Pty Ltd | <MASKED>     | NSW            |
| Koala Commerce Pty Ltd | <MASKED>     | NSW            |
| Koala Commerce Pty Ltd | <MASKED>     | NSW            |
| Koala Commerce Pty Ltd | <MASKED>     | NSW            |
| Koala Commerce Pty Ltd | <MASKED>     | NSW            |
+------------------------+--------------+----------------+
(10 rows)
gosbee> sql
  SELECT "orders"."merchant_name", '<MASKED>' AS "total_amount", "orders"."customer_state" FROM "orders" WHERE "orders"."merchant_name" = 'Koala Commerce Pty Ltd' AND "orders"."customer_state" = 'NSW' LIMIT 10;
gosbee> opa inputs
  Policy requires 3 input(s):
[Config]   user.merchant_name [Koala Commerce Pty Ltd]:
[Config]   user.state [NSW]:
[Config]   user.role [sales]: admin
  OPA inputs updated and reloaded.
gosbee> run
  SELECT "orders"."merchant_name", "orders"."total_amount", "orders"."customer_state" FROM "orders" LIMIT $1;
  Params: [10]
+---------------------------+--------------+----------------+
| merchant_name             | total_amount | customer_state |
+---------------------------+--------------+----------------+
| Fair Dinkum Books         | 89.95        | NSW            |
| Fair Dinkum Books         | 145.50       | NSW            |
| Billabong Homewares       | 299.00       | VIC            |
| Koala Commerce Pty Ltd    | 54.90        | QLD            |
| Vegemite Ventures         | 199.95       | NSW            |
| Koala Commerce Pty Ltd    | 129.00       | NSW            |
| Wattle & Co Trading       | 78.50        | WA             |
| Fair Dinkum Books         | 234.00       | VIC            |
| Koala Commerce Pty Ltd    | 89.95        | NSW            |
| Billabong Homewares       | 449.00       | SA             |
+---------------------------+--------------+----------------+
```

Enabling OPA as a `sales` user injects WHERE conditions that restrict results to the user's merchant and state, and masks the `total_amount` column. Switching the role to `admin` removes both the row filters and the column mask — the policy returns unconditional allow with no masks, so the original query runs unmodified.

#### REPL Commands

| Command                         | Description                                          |
|---------------------------------|------------------------------------------------------|
| `opa`                           | Interactive setup wizard (server URL, policy, inputs) |
| `opa status`                    | Show OPA configuration and active mask count         |
| `opa off`                       | Disable OPA plugin                                   |
| `opa reload`                    | Rebuild plugin with current config                   |
| `opa inputs`                    | Re-discover and set input values                     |
| `opa explain <table>`           | Show how OPA translates to SQL conditions and masks  |
| `opa explain <table> verbose`   | Include raw OPA request/response and translation trace |
| `opa conditions`                | Show OPA-injected conditions on the current query    |
| `opa masks`                     | Show column masks for the current query tables       |
| `opa url <url>`                 | Change OPA server URL                                |
| `opa policy <path>`             | Change OPA policy path                               |
| `opa input <key> <value>`       | Set or update a single input value                   |
| `opa input <key>`               | Remove an input value                                |
