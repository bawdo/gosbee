# Workstream: repo-quality

Integration branch: workstream/repo-quality
Next unit: 08

Bring gosbee's GitHub, CI and release practice up to (and past) bawdo/caddy_shack,
bawdo/jellyfish and bawdo/go-blinkstick: a strong OpenSSF Scorecard and readiness for
the OpenSSF Best Practices survey. Solo maintainer: nothing may need a second
contributor. Baseline 2026-11-01: Scorecard API 404 for gosbee (never scored); peers
8.3, 8.0 and 7.6.

Cut by file collision (each workflow file is owned by a contiguous run of units),
with dependency order on top: 03 extends 02's jobs, 05 builds the 02 and 03 plans,
06 needs 04's skip table, 08 documents what 05 ships, 09 needs the final action set,
10 proves it all. Re-cut 2026-11-01 after 04, on the operator's word: 05 inserted,
old 05 to 09 renumbered 06 to 10, and 06 and 09 continue from plan into build.

## Units

| # | Slug | Mode | Branch | Status | Brief |
|---|---|---|---|---|---|
| 01 | scorecard | implement | unit/01-scorecard | done | units/01-scorecard.md |
| 02 | release-pipeline | plan | unit/02-release-pipeline | done | units/02-release-pipeline.md |
| 03 | release-supply-chain | plan | unit/03-release-supply-chain | done | units/03-release-supply-chain.md |
| 04 | ci-gate-and-lint | implement | unit/04-ci-gate-and-lint | done | units/04-ci-gate-and-lint.md |
| 05 | release-build | implement | unit/05-release-build | done | units/05-release-build.md |
| 06 | fuzzing | plan | unit/06-fuzzing | done | units/06-fuzzing.md |
| 07 | examples | implement | unit/07-examples | done | units/07-examples.md |
| 08 | community-docs | implement | unit/08-community-docs | next | units/08-community-docs.md |
| 09 | repo-settings | plan | unit/09-repo-settings | pending | units/09-repo-settings.md |
| 10 | release-proof | implement | unit/10-release-proof | pending | units/10-release-proof.md |

## Handover from unit 07

Unit 07 added 14 `Example*` functions with `// Output:` blocks in external test packages:
managers (select, join, where, insert, update, delete), visitors (one query per dialect:
quoting, placeholders, case-insensitive comparison) and both plugins (softdelete; opa PolicyFunc,
server-mode `SELECT *` masking through a `ColumnResolver`, and the `ErrMaskedColumnUse` refusal,
against an httptest stub). On the operator's word it also fixed UPDATE / ON CONFLICT DO UPDATE SET
targets, which rendered table-qualified (rejected by PostgreSQL and SQLite) and now render bare,
and, from review, `ToSQL` refuses a SET target of another table with `managers.ErrSetTargetTable`.
**Still open from 06, operator to schedule:** the `quoting.EscapeString` backslash doubling.
**For 08:** a correction in its brief (stale `examples/` sketch; UPDATE output changed); from 06,
fuzzing is in CONTRIBUTING (Testing, Fuzzing) and CI fuzz findings are public on failure.
**For 10:** from 06, the fuzz job, `!inputs.ref` on schedule, the `workflow_call` EVENT and the
schedule concurrency group are unproven until a remote run. From 07, the OPA server examples bind a
loopback port, as the OPA tests do; the agent sandbox still refuses local binding, so verification
ran in the operator's shell.
