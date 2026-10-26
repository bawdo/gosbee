# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- Upgraded `golang.org/x/text` to v0.41.0 to fix GO-2026-6629, a panic on
  crafted input that the REPL could reach through `sql.Open`.

## [v0.2.0] - 2026-10-27

This release closes several ways around the soft delete and OPA plugins. It
changes the SQL they generate and refuses some queries that used to pass, so
read [Upgrading](docs/guide/plugins.md#upgrading) before you update.

### Security

- `nodes.Cast` type names are validated before rendering, which closes an SQL
  injection through the CAST type argument.
- OPA no longer inverts a policy when the value comes before the column
  (`5 < input.x`) or when the expression is negated (`not ...`).
- Soft delete and OPA now apply inside subqueries, CTE bodies, joined
  subqueries and set-operation branches. Previously only the outermost SELECT
  was filtered.
- OPA conditions on an aliased table are written against the alias, so a
  correlated subquery or a self-join can no longer read the unfiltered copy.
- OPA masked columns are refused anywhere other than a plain projection, and a
  mask is rendered as a bind parameter rather than an inline literal. This
  closes an injection through mask values on MySQL.
- Strict plugins refuse what they cannot see inside: raw SQL (`SqlLiteral`,
  `StringJoin`), custom node types, and relations that are not a table, a
  nested query or an alias over one (including table functions such as
  `generate_series`).

### Added

- `managers.SetOperationManager`, so UNION, INTERSECT and EXCEPT queries run
  their transformers. Build one with `managers.NewSetOperationManager`.
- `plugins.ScopedTransformer`, `plugins.StrictTransformer`, `plugins.Scope`,
  `plugins.ApplySelect`, `plugins.ApplyNode` and `plugins.ErrUnpoliceable` for
  writing plugins that police nested queries.
- `softdelete.WithAllowRawSQL()` to let raw SQL through soft delete.
- `opa.ErrUnsupportedStatement` and `opa.ErrMaskedColumnUse`.
- Constructors in `nodes` for combinable nodes (logical, grouping, unary,
  `IN` and ordering), so a copied node combines with `.And` and friends
  correctly.
- A guide to writing plugins in `docs/development/writing-plugins.md`.

### Changed

- Every transformer runs on each nested SELECT, inner ones first. A plugin
  that adds a `LIMIT` now limits subqueries too.
- OPA refuses INSERT, UPDATE and DELETE instead of silently doing nothing.
- `TransformSelect` on the soft delete and OPA plugins walks the whole tree
  and no longer modifies the core passed in.
- A transformer returning a `nil` core is now an error.
- In a `WITH RECURSIVE` list, a CTE body sees only the CTEs declared before it,
  plus itself when recursive.
- A bare `SelectManager` used as a join target is rendered in parentheses.
- The REPL renders `sql` and `exec` through one path that runs the plugins.

## [v0.1.0] - 2026-10-26

First tagged release.

### Added

- SQL AST builder in the style of Ruby's Arel: tables, attributes, predicates,
  joins, subqueries, CTEs, set operations, window functions and DML.
- Visitors for PostgreSQL, MySQL and SQLite, plus DOT and formatting visitors.
- Parameterised queries by default.
- Plugin system with soft delete and OPA (row filters and column masks).
- Interactive REPL for building and running queries against live databases.

[v0.2.0]: https://github.com/bawdo/gosbee/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/bawdo/gosbee/releases/tag/v0.1.0
