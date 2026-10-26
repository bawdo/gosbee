# Writing Plugins

This page covers what a plugin author needs to know about how gosbee applies
transformers to a query. For the basics of the `Transformer` interface and
registering a plugin, see the [plugins guide](../guide/plugins.md).

## How the walker applies a transformer

A manager does not call your `TransformSelect` on the outermost query alone.
Built-in plugins delegate to `plugins.ApplySelect`, which walks the whole tree
and hands your transformer every `SelectCore` it finds: subqueries, CTE bodies,
joined subqueries (including `LATERAL`) and each branch of a set operation.

```go
func (p *MyPlugin) TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error) {
    return plugins.ApplySelect(p, core)
}
```

The walk gives you three guarantees:

- **Copy on write.** The input tree is never modified. Each core is copied
  before your transformer sees it, so appending to `core.Wheres` is safe, and
  every container node on the path to a changed node is copied too.
- **Post-order.** Inner cores are transformed before the core that contains
  them, so by the time you see an outer core its subqueries are already done.
- **One visit per core.** `JoinNode.Left` and `Attribute.Relation` are not
  walked (neither is rendered), so a subquery is never transformed twice.

`plugins.ApplyNode` is the same walk for any node, such as a
`SetOperationNode`. Only `SelectCore`s are passed to your transformer.

## ScopedTransformer

Implement `ScopedTransformer` when your plugin needs to know what is visible to
the core it is looking at. The walker calls it instead of `TransformSelect`:

```go
type ScopedTransformer interface {
    TransformSelectScoped(core *nodes.SelectCore, scope plugins.Scope) (*nodes.SelectCore, error)
}
```

`Scope` carries:

- `CTENames`: CTEs declared on this core or any enclosing core.
- `Qualifiers`: maps each name a column may be qualified with (a table name or
  an alias) to the underlying table name, for this core and every enclosing
  core. Inner names shadow outer ones.
- `Tables(core)`: the table relations the core references, as
  `plugins.CollectTables` returns them, minus names that refer to a CTE in
  scope and minus derived tables. A name that refers to a CTE is not a table,
  and neither is the alias over a subquery: the walker polices a derived table
  by walking into it.
- `Memo(key, fn)`: caches the result of `fn` for the duration of one walk, so
  work that does not depend on the core (such as fetching masks from a remote
  server) happens once however many cores there are. Errors are not cached.

Derived-table bodies are scoped as SQL scopes them. A non-`LATERAL` body sees
only the enclosing scope. A `LATERAL` body also sees the real tables of the core
it hangs off.

The softdelete plugin is the short example:

```go
func (sd *SoftDelete) TransformSelectScoped(core *nodes.SelectCore, scope plugins.Scope) (*nodes.SelectCore, error) {
    for _, ref := range scope.Tables(core) {
        if sd.appliesTo(ref.Name) {
            attr := nodes.NewAttribute(ref.Relation, sd.columnFor(ref.Name))
            core.Wheres = append(core.Wheres, attr.IsNull())
        }
    }
    return core, nil
}
```

The managers start the walk themselves: `SelectManager` calls
`plugins.ApplySelect` and `SetOperationManager` calls `plugins.ApplyNode`, and
the walker then calls your `TransformSelectScoped` for each core. Your
`TransformSelect` is not on that path. Still keep one that delegates to
`plugins.ApplySelect(p, core)`: the `Transformer` interface requires it, and it
gives anyone calling your plugin directly the same behaviour as the managers.

`TransformSelectScoped` is already running inside a walk, so it must not start
another one (no `ApplySelect` or `ApplyNode` calls from it). Transform only the
core you are handed.

## StrictTransformer

A raw SQL fragment (`SqlLiteral`, `StringJoin`) is opaque to the walker, so a
policy plugin cannot tell what it reads. Implement `StrictTransformer` to have
the walker refuse them:

```go
type StrictTransformer interface {
    RefusesRawSQL() bool
}
```

When `RefusesRawSQL` returns true, the walk fails with an error wrapping
`plugins.ErrUnpoliceable`. The error never includes the raw SQL text. Callers
can test for it with `errors.Is`. The softdelete plugin returns
`!sd.allowRaw`, so `WithAllowRawSQL()` opts out. The OPA plugin always returns
true.

One exemption: the type argument of `CAST(expr AS type)` is stored as a
`SqlLiteral` by `nodes.Cast`, but the SQL visitor validates it as a type name
when rendering, so the walker does not refuse it. The `expr` argument is still
walked.

## Inspecting attributes

`plugins.Attributes(n)` returns every `*nodes.Attribute` under a node,
including those inside function calls and nested queries.
`plugins.ShallowAttributes(n)` does the same without descending into nested
queries (the root itself is still collected when it is a `SelectCore`). Use the
shallow form when you are checking one core at a time under a scoped walk, to
avoid seeing the same attribute at every level.

## Set operations

`SelectManager.Union`, `UnionAll`, `Intersect`, `IntersectAll`, `Except` and
`ExceptAll` return a bare `*nodes.SetOperationNode`. It renders via `Accept` and
runs no transformers. To police every branch, wrap it:

```go
union := managers.NewSetOperationManager(a.Union(b)).Use(sd)
sql, _, err := union.ToSQL(visitors.NewPostgresVisitor())
```

## Known limit: transformers on inner managers are ignored

A manager used as a subquery (for example the `sub` in `users.Col("id").In(sub)`)
contributes its `Core` to the outer tree. Transformers attached to it with `Use`
are ignored. The outer manager's transformers police the whole tree, so attach
your plugins to the outermost manager.
