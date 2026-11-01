# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.3.0-rc.1] - 2026-11-01

### Added

- An OpenSSF Scorecard workflow checks the repository's supply chain hygiene
  weekly and on every push to `main`, publishing the result to scorecard.dev
  and the Security tab. The README carries the Scorecard badge.

- CI lints every script under `scripts/` with a pinned, checksum verified
  shellcheck release.

- Each release attaches the Sigstore bundle of its build provenance
  attestation as `gosbee-repl_<tag>_provenance.intoto.jsonl`, so
  `gh attestation verify --bundle` can check a binary without the attestation
  API.

- Each release attaches CycloneDX SBOMs, one for the module and one for each
  REPL binary, listed in `SHA256SUMS` and so covered by the build provenance
  attestation.

- The release workflow checks that proxy.golang.org serves the tagged commit
  and that sum.golang.org records the version.

- Native Go fuzz targets cover identifier quoting and string escaping for
  PostgreSQL, MySQL and SQLite, the OPA plugin's column masks and the REPL's
  input parsing, each checking a property such as a quoted value never closing
  its quotes early.
  `make fuzz` runs them all, and CI fuzzes every target weekly and on a manual
  run, keeping failing inputs as an artifact for 14 days.

- Testable examples, checked by `go test` and shown on pkg.go.dev, for the
  core builder (select, join, where, insert, update and delete), each dialect
  visitor (PostgreSQL, MySQL and SQLite) and each plugin (soft delete and
  OPA, including column masking of a `SELECT *`).

- SECURITY.md explains how to verify a release: the signed tag on `main`,
  `gh attestation verify` for a binary or SBOM (also offline with the attached
  bundle), the checksums, the SBOMs, and the Go module through
  sum.golang.org.

- A code of conduct (Contributor Covenant 2.1), issue forms for bug reports
  and feature requests, a pull request template and a CODEOWNERS file. Blank
  issues are turned off, and security reports are pointed at private
  reporting.

- `scripts/repo-settings.sh` keeps the repository's GitHub settings in the
  tree, in phases (`metadata`, `rulesets`, `harden`, `show`) that each send
  their whole desired state and can be dry-run with `DRY_RUN=1`. Pull
  requests to `main` must now be up to date and pass CodeQL with no blocking
  alert, and only an admin can create a `v*` tag. The release workflow's
  publish job names a `release` environment, which `harden` limits to `v*`
  tags and `main`. SECURITY.md covers `gh release verify` for
  releases published once immutable releases are on.

### Changed

- The `CI OK` check is decided by an in-repo script, `scripts/ci-result.sh`,
  in place of the third-party `re-actors/alls-green` action. It fails on any
  failed, cancelled, missing or unexpected job, and on a skipped job unless
  that event is permitted to skip it.
- Codecov's project and patch coverage statuses now fail a pull request that
  drops below their targets, instead of being informational.
- REPL release binaries are built without the symbol table and DWARF debug
  data, which makes them about a third smaller.
- Releases check the tag name first, with no token and no checkout, so a
  by-hand run naming any other tag stops before its code runs. They then run
  the whole CI workflow on the tag, then build with a read-only token, then publish from a job that is the only one able to write.
  The build checks the tag is signed and verified by GitHub and smoke tests a
  binary. Publishing goes through a draft, so a release never exists without
  its assets, and a run that fails before publishing is recovered by running
  `Release` again with the tag. A pre-release tag is never marked latest.
- Release notes are no longer rewritten after publication: a published
  release is never changed.
- Coverage is uploaded to Codecov by a separate `Coverage` workflow instead of
  the CI test job.

### Fixed

- `scripts/pre-ci-check.sh --fix gofmt` (and `make pre-ci-fix`) now applies
  the fix. The option was parsed but never reached the script's main
  function, so it was silently ignored.
- `UPDATE ... SET` and `ON CONFLICT ... DO UPDATE SET` write each target
  column bare (`SET "name" = $1`). They wrote it table-qualified
  (`SET "users"."name" = $1`), which PostgreSQL and SQLite reject. The value
  side of an assignment is unchanged. Because the target is now bare, `ToSQL`
  refuses a target column of another table with `managers.ErrSetTargetTable`
  rather than letting it resolve to a column of the table being written.

## [v0.2.1] - 2026-10-27

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

[v0.3.0-rc.1]: https://github.com/bawdo/gosbee/compare/v0.2.1...v0.3.0-rc.1
[v0.2.1]: https://github.com/bawdo/gosbee/compare/v0.2.0...v0.2.1
[v0.2.0]: https://github.com/bawdo/gosbee/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/bawdo/gosbee/releases/tag/v0.1.0
