# Unit 07: examples

Mode: implement
Close: auto
Branch: unit/07-examples
Review lanes: security, correctness, brief

## Items

- Add testable `Example*` functions for the core builder.
- Add examples for each dialect visitor.
- Add examples for each shipped plugin.
- Update CHANGELOG.md.

## Acceptance criteria

- `Example*` functions, each with an `// Output:` block that `go test` checks, cover:
  - the core builder: a select with projection, a join, a where with combined
    predicates, an insert, an update and a delete;
  - each dialect visitor (PostgreSQL, MySQL, SQLite), showing the dialect difference
    in quoting or parameters;
  - each shipped plugin (at least soft delete and OPA, plus any other in `plugins/`).
- Examples live in `_test.go` files in the package they document, named per Go
  convention (`ExampleType_Method`, `ExampleFunc`), so pkg.go.dev attaches them.
- Examples use only the public API; no `internal/` imports and no network. An OPA
  example that needs a policy server uses an in-process stub or a documented fake.
- Every example passes under `go test -race -count=1 -shuffle=on ./...`.
- golangci-lint passes; CHANGELOG.md has an `[Unreleased]` entry.

## Decisions

- CORRECTION FROM UNIT 06: unit 06's fuzz targets do not cover the OPA plugin's star expansion
  branch (`plugins/opa/opa.go` `applyMasks` with a `ColumnResolver`) or its `refuseMaskedUse` check.
  Unit 06 judged the first covered on the claim that it renders through the same
  `maskedProjection`, which nothing fuzzes directly. An example that shows masking with `SELECT *`
  exercises a path no fuzz property holds.
- Read the correction as a reason to add a `SELECT *` masking example
  (`ExampleNewFromServer`) and a refusal example (`ExampleNewFromServer_maskedColumnRefused`, for
  `refuseMaskedUse`). Both fit the existing OPA criterion, so no criterion changed. Note the star
  branch already has a unit test (`TestMaskExpandsStar`); the correction was about fuzzing only.
- Every example file is an external test package (`package <pkg>_test`), so the compiler enforces
  "public API only": an unexported or `internal/` symbol would not resolve.
- Builder examples live in `managers` (where `SelectManager` and friends are defined), not the root
  `gosbee` package, whose types are aliases. Names are `ExampleSelectManager_Select`, `_Join`,
  `_Where`, `ExampleInsertManager_Values`, `ExampleUpdateManager_Set`,
  `ExampleDeleteManager_Where`; `go vet` checks each name resolves.
- Dialect examples render one shared query through each visitor, and use `CaseInsensitiveEq` so
  each dialect shows a difference beyond quoting and placeholders (PostgreSQL `LOWER() = LOWER()`,
  MySQL plain `=`, SQLite `COLLATE NOCASE`).
- The OPA server examples use `httptest.NewServer` as the in-process stub the criterion allows.
  The plugin's HTTP client is unexported, so there is no public seam for a fake transport; adding
  one would widen the public API for a test's sake. Cost: the stub binds a loopback port, which this
  agent sandbox refuses, so those two examples are verified by the operator's run, not in session.
  The PostgreSQL output for `ExampleNewFromServer` was derived by hand from `TestMaskExpandsStar`
  plus parameter order (projections bind before WHERE).
- UPDATE defect found while writing `ExampleUpdateManager_Set`: `baseVisitor.VisitAssignment`
  rendered the SET target through the full visitor, so `SET "users"."name" = $1`, which PostgreSQL
  and SQLite reject. Put to the operator with three options (MySQL example and defer, pin and
  defer, fix here). **Operator's answer: fix it in this unit.** Fix: a column target renders as
  `quoteIdent(attr.Name)`; the value side is unchanged.
- Caller census for that change (a shared contract: assignment SQL output). Commands:
  `grep -rn 'VisitAssignment\|AssignmentNode{\|\.Assignments\|NewAssignment' --include='*.go' . | grep -v _test.go`
  and `grep -rn 'SET "\|SET \`\|DO UPDATE' --include='*_test.go' .`, plus a docs search
  `grep -rn 'SET "[a-z_]*"\."\|SET \`[a-z_]*\`\.\`' --include='*.md' --include='*.go' .`.
  Renderers: `visitors/visitor.go` UPDATE SET (`VisitUpdateStatement`) and ON CONFLICT DO UPDATE SET
  (`VisitOnConflict`), both through `VisitAssignment`; `FormattingVisitor.VisitAssignment`
  delegates to the inner visitor, so it inherits the fix. Not SQL, untouched: `DotVisitor`
  (graph labels), `plugins/walk.go` (AST rewrite), `internal/testutil` stub. Producers:
  `UpdateManager.Set`, `OnConflictContext.DoUpdate`, `cmd/repl/dml_commands.go:123`. Pinned
  expectations: 12 in `visitors/visitors_test.go`, 3 in `cmd/repl/repl_test.go`. The SET grep
  missed `TestVisitAssignment`, which pins an assignment alone; the first green run found it, and
  it was corrected the same way. No docs carried the qualified form.

## Plan

## Outcome

## Not done, and why
