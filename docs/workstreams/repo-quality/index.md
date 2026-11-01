# Workstream: repo-quality

Integration branch: workstream/repo-quality
Next unit: 07

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
| 07 | examples | implement | unit/07-examples | next | units/07-examples.md |
| 08 | community-docs | implement | unit/08-community-docs | pending | units/08-community-docs.md |
| 09 | repo-settings | plan | unit/09-repo-settings | pending | units/09-repo-settings.md |
| 10 | release-proof | implement | unit/10-release-proof | pending | units/10-release-proof.md |

## Handover from unit 06

Unit 06 built fuzzing. Nine `FuzzXxx` targets (internal/quoting, visitors, plugins/opa, cmd/repl)
check properties by reading SQL back with `internal/sqllex` (ANSI and MySQL; byte-wise, so single
byte and UTF-8 only). `scripts/fuzz.sh` and `make fuzz FUZZTIME=` (10s default, exported, not
spliced) run them all; `scripts/fuzz_test.sh` is the CI lint step and the `fuzz-script` verify gate.
ci.yml `fuzz` runs on schedule (5m per target) and direct dispatch (15s) only, `if: !inputs.ref`, so
never in release.yml's call. The gate's EVENT reads `workflow_call` when `inputs.ref` is set.
`ci-result.sh` lets fuzz skip on pull_request, push and workflow_call. Schedule now has its own
concurrency group. **Open, operator to schedule:** `quoting.EscapeString` doubles `\` for
PostgreSQL/SQLite (value altered, never early close), pinned by the skipped
`TestEscapeStringANSIRoundTripBackslash`, with backslash exemptions in 3 targets to remove with the
fix. **For 07:** a correction in its brief (OPA star expansion is unfuzzed). **For 08:** fuzzing is
in CONTRIBUTING (Testing, Fuzzing); CI fuzz findings are public on failure. **For 10:** the fuzz job,
`!inputs.ref` on schedule, the `workflow_call` EVENT and the schedule concurrency group are unproven
until a remote run.
