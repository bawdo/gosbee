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
- Review finding (security and correctness lanes, the same one): dropping the qualifier made a
  SET target from another table resolve silently, `NewUpdateManager(users).Set(posts.Col("title"),
  x)` rendering `UPDATE "users" SET "title" = $1`, where it had rendered SQL every engine rejects.
  Reproduced in session. Put to the operator (refuse, document and accept, or defer). **Operator's
  answer: refuse mismatched targets.** Design: the managers check, in `toSQLCore` after the
  transformers run (so what is checked is what renders), each SET target of an UPDATE and each
  DO UPDATE SET target of an INSERT. A target `*nodes.Attribute` passes where its `Relation` is
  nil, or shares the statement table's `nodes.RelationName` (alias or table name) or
  `nodes.TableSourceName` (the underlying table, so `users.Col` against `users AS u` passes);
  anything else fails with an error wrapping the exported `managers.ErrSetTargetTable`. Visitors
  return strings and cannot refuse, so a caller rendering `Statement.Accept` directly bypasses the
  check; that is stated on the error's doc comment.
- Caller census for that change (a shared contract: `UpdateManager.ToSQL` and
  `InsertManager.ToSQL` can now fail on a statement that rendered before). Command:
  `grep -rn 'ToSQL(' cmd/repl/*.go` and
  `grep -rn 'updateQuery.ToSQL\|insertQuery.ToSQL\|\.toSQLCore' --include='*.go' . | grep -v _test.go`.
  Production callers: `cmd/repl/session.go:132` (insert) and `:137` (update), both returning the
  error to the REPL's caller. Other callers are tests and the examples.

## Plan

## Outcome

Close mode as run: auto. Commit grant given once in conversation for the session.

Questions that fired, with the operator's answer:
- Scope (found mid-unit, before the close): UPDATE SET targets render table-qualified, which
  PostgreSQL and SQLite reject. Options offered: MySQL example and defer, pin and defer, fix here.
  **Answer: fix it in this unit.**
- Environment (step 1): the agent sandbox refuses httptest's loopback bind, so the verification
  ran in the operator's shell. Not a gate question: the operator ran the command handed over.
- Review triage scope call (step 6): a SET target from another table now resolves silently.
  Options offered: refuse mismatched targets, document and accept, defer. **Answer: refuse
  mismatched targets.**

What landed:
- Testable examples, each with an `// Output:` block, in external test packages:

  ```
  $ git grep -c '^func Example' -- '*_test.go'
  managers/example_test.go:6
  plugins/opa/example_test.go:3
  plugins/softdelete/example_test.go:2
  visitors/example_test.go:3
  ```

  Builder: `ExampleSelectManager_Select`, `_Join`, `_Where` (AND plus a grouped OR),
  `ExampleInsertManager_Values`, `ExampleUpdateManager_Set`, `ExampleDeleteManager_Where`.
  Dialects: `ExampleNewPostgresVisitor`, `ExampleNewMySQLVisitor`, `ExampleNewSQLiteVisitor`.
  Plugins: softdelete `ExampleNew`, `ExampleWithTableColumn`; opa `ExampleNew` (PolicyFunc),
  `ExampleNewFromServer` (`SELECT *` masked through a `ColumnResolver`, the unit 06 correction's
  path), `ExampleNewFromServer_maskedColumnRefused` (`refuseMaskedUse`, `ErrMaskedColumnUse`).
  `plugins/` ships two plugins (softdelete, opa); the rest of `plugins/` is the transformer
  framework, not a plugin.
- Fix: `baseVisitor.VisitAssignment` writes a column target bare, so UPDATE SET and ON CONFLICT
  DO UPDATE SET are valid on PostgreSQL and SQLite. Pinned expectations corrected, 14 in
  `visitors/visitors_test.go` and 3 in `cmd/repl/repl_test.go`:

  ```
  $ git diff workstream/repo-quality...HEAD -- visitors/visitors_test.go cmd/repl/repl_test.go | grep -c '^-[^-]'
  17
  ```

  A SQLite assertion and `TestVisitUpdateSetTargetUnqualifiedValueQualified` were added.
- CHANGELOG `[Unreleased]`: Added (examples) and Fixed (UPDATE SET).

Proof:
- Each example was first seen failing against a placeholder `// Output:` (visitors, softdelete)
  or a wrong one (`ExampleUpdateManager_Set` before the fix), so `go test` checks the output.
- The fix's tests were seen red before the change. Mutation, through `record-mutation.sh`
  (ledger in the session scratchpad, outside the repository):

  ```
  mutation .../scratchpad/m1.patch
  command  go test -count=1 -run Update\|Assign\|Conflict\|Example ./visitors/ ./managers/ ./cmd/repl/
  diff     visitors/visitor.go: `ok {` -> `ok && false {` in VisitAssignment
  first    --- FAIL: TestVisitInsertOnConflictDoUpdateWithWhere (0.00s)
  verdict  KILLED
  ```

- `ExampleNewFromServer`'s output was derived by hand and first run in the operator's
  verification at `80693c2`, where the `test` gate (`go test -race -count=1 -shuffle=on ./...`)
  passed.

Verification at step 1 (`WOMBAT_BASE_REF=workstream/repo-quality scripts/wombat-verify.sh`, run
by the operator at `80693c2`): tree clean; gates fmt, vet, lint, tidy, changelog, release-scripts,
vuln, zizmor, shellcheck, ci-result, fuzz-script, test all `pass`; none skipped.

Lanes: `select-gates.sh .` printed security, correctness, brief (3 lanes, none skipped, no
`unmatched` line).

Scan: `scan-secrets.sh .` printed `scan clean: 14 files`.

Sweep: `sweep-docs.sh . --base workstream/repo-quality --claim 'SET "users"."name"'` printed 2
candidates; both dismissed, none rewritten or deleted:
- `CHANGELOG.md` (claim): this unit's own Fixed entry, quoting the old output on purpose.
- `CONTRIBUTING.md` (named-file `visitor.go`): names `nodes/visitor.go`, not the changed
  `visitors/visitor.go`; still true.
No correction written into a later unit's brief.

Review (step 5): 3 lanes dispatched (security, correctness, brief), each a fresh subagent in its
own detached worktree, all removed after reporting.

Triage (step 6), counted from the lane reports in this session:
- Security and correctness, the same finding: a SET target from another table resolves silently
  once targets render bare. **Real**; reproduced in session (`UPDATE "users" SET "title" = $1`, no
  error). Scope call put to the operator, answered refuse. Fixed in `f8647d0`:
  `managers.ErrSetTargetTable`, checked in `UpdateManager` and `InsertManager` (DO UPDATE) after
  the transformers run. Tests seen red on their assertions before the check existed.
  Mutations, through `record-mutation.sh` (ledger in the session scratchpad):

  ```
  m2  set_target.go  `if sameRelation(...)` -> `if true || sameRelation(...)`
      first --- FAIL: TestInsertOnConflictDoUpdateTargetFromAnotherTableRefused   KILLED
  m3  insert_manager.go  DO UPDATE check guarded off with `false &&`
      first --- FAIL: TestInsertOnConflictDoUpdateTargetFromAnotherTableRefused   KILLED
  m4  set_target.go  `return src != "" && ...` -> `return false`
      first none   SURVIVED  (invalid mutant: `src` unused, the build failed, no test ran)
  m5  set_target.go  `return src != "" && ...` -> `return src != "" && false`
      first --- FAIL: TestUpdateSetTargetSameTableAccepted   KILLED
  ```

- Brief: CHANGELOG's Fixed entry landed in the examples commit, not the fix commit. **Not acted
  on**: the end state is correct and moving it would rewrite branch history for nothing.
- Every other check the three lanes made came back clean (brief lane: every Item and criterion
  met).

Sweep re-run after the review fix (`sweep-docs.sh . --base workstream/repo-quality --claim 'SET
"users"."name"'`): the same 2 candidates, both still dismissed for the reasons above. The getting
started guide's UPDATE and DO UPDATE snippets (`docs/guide/getting-started.md:380`, `:417`) use
same-table targets and show no output, so they stay true.

For 08: the README and guides carry no example section pointing at pkg.go.dev; whether they should
is 08's call. For 10: `ExampleNewFromServer*` bind a loopback port, as the OPA tests already do.

## Not done, and why

- `examples/before_after_example.go` (a commented-out sketch, not compiled) shows stale API
  (`sm.UsePlugin`, `plugins.NewSoftDeletePlugin`, a two-value `ToSQL`). Outside this unit's items;
  left for the operator to schedule or delete. Carried into unit 08's brief as a
  `CORRECTION FROM UNIT 07:` at step 9.
