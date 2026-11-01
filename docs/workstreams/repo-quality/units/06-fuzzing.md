# Unit 06: fuzzing

Mode: plan
Close: auto
Branch: unit/06-fuzzing
Review lanes: security, correctness, brief

## Items

- Choose fuzz targets and the property each asserts, and record them in the plan.
- Write native Go fuzz targets with seed corpora.
- Add `make fuzz FUZZTIME=...`.
- Add a fuzz job to `ci.yml` and its row in `scripts/ci-result.sh`'s skip table.
- Update CONTRIBUTING.md (testing) and CHANGELOG.md.
- Continue from the plan into the build in this unit, and build every task of it.

## Acceptance criteria

- Native Go fuzz targets (`func FuzzXxx(f *testing.F)`) cover at least:
  - identifier quoting for each dialect (PostgreSQL, MySQL, SQLite);
  - string and literal escaping for each dialect;
  - the OPA plugin's MaskLiteral path;
  - one parser-style entry point (the REPL's input parsing, or the closest equivalent).
- Each target asserts a property, not just "does not panic", where one exists: for
  example a quoted identifier round trips, or escaped output never closes the quote
  early. The property per target is written in the plan.
- Seed corpora are added with `f.Add` and, where useful, under `testdata/fuzz/`. The
  seeds run as ordinary tests in every `go test` run.
- `make fuzz FUZZTIME=<d>` runs every fuzz target in turn (Go fuzzes one target at a
  time), defaulting to a short time.
- The CI fuzz job runs only on `schedule` (5m per target, timeout sized to the target
  count) and `workflow_dispatch` (15s per target), never on pull requests or pushes;
  uploads `testdata/fuzz/` failures as an artifact with 14 day retention; has
  `persist-credentials: false` and SHA-pinned actions.
- `scripts/ci-result.sh` permits `fuzz` to be skipped on `pull_request` and `push`,
  and its test script gains cases proving that, and proving fuzz may not be skipped on
  `schedule` or `workflow_dispatch`.
- **A real defect found by fuzzing is not fixed in this unit.** A vulnerability is
  reported to the operator for a private security advisory; any other defect is
  recorded under Not done with the failing input. The target stays, with the failing
  input either fixed in a separate change or the target skipped with a reason.
- golangci-lint, zizmor and `scripts/wombat-verify.sh` pass; CHANGELOG.md has an
  `[Unreleased]` entry.

## Decisions

- OPERATOR RULING 2026-11-01 (after unit 04 closed): this unit writes its plan and then continues
  into implement in the same unit, so the fuzz targets, `make fuzz` and the CI fuzz job land here
  rather than as a plan nothing builds. Renumbered from 05 when the release-build unit was
  inserted before it.
- Skills: `plan` goes to `superpowers:writing-plans` and `testFirst` to
  `superpowers:test-driven-development`, both installed. `implement` is not configured, so this
  session builds the plan inline. No `jira` block, no `knownTraps`, and no `preflight` key
  (pre-flight printed `none`).
- The brief's "the OPA plugin's MaskLiteral path" names no function in the tree. The mask path is
  `plugins/opa/opa.go` `applyMasks` -> `maskedProjection` -> `nodes.NewBindParam` -> the visitor's
  `VisitBindParam`, which parameterises or renders through `literalToSQL`. The target fuzzes that
  path end to end through `TransformSelect`.
- OPERATOR RULING 2026-11-01, release call: release.yml calls ci.yml, and a called workflow
  inherits the caller's `github.event_name`, so a dispatched release re-run reads as
  `workflow_dispatch`. The fuzz job runs only when ci.yml is triggered directly (`inputs.ref` is
  empty). The gate's EVENT is `workflow_call` when `inputs.ref` is set, and the skip table lets
  fuzz be skipped on `workflow_call` alone besides `pull_request` and `push`. `schedule` and direct
  `workflow_dispatch` may still not skip it.
- OPERATOR RULING 2026-11-01, backslash: `quoting.EscapeString` doubles backslashes for every
  dialect (a documented choice for MySQL). Under PostgreSQL (standard_conforming_strings on) and
  SQLite, a backslash is literal, so in non-parameterised mode the value is altered, though the
  quote is never closed early. Every dialect's literal target asserts the no-early-close property
  under its own lexer, and MySQL also asserts a full round trip. PostgreSQL and SQLite assert a
  round trip only for backslash-free inputs. The backslash case is recorded under Not done, with
  input `\`, and fixed in a separate change.
- Commit grant: the operator granted commits for this unit without asking each time.
- Caller census of the CI gate contract (`JOBS`, `skippable`, the gate's `EVENT:` line, `needs:`),
  run as `grep -rn "ci-result\|EVENT:\|JOBS=\|skippable" -I .`, excluding `.git`, coverage files
  and closed unit briefs. Readers: `scripts/ci-result.sh` (JOBS, skippable, EVENT);
  `scripts/ci-result_test.sh` (its own JOBS copy at line 39; `permitted_skip_passes` seds the
  pull_request arm's exact `echo ""` shape, so it breaks once that arm names fuzz; `ci_yml_gate_shape`
  pins the `EVENT:` line verbatim; the needs and job-list cases); `.github/workflows/ci.yml` (lint
  step, gate comment "today, none", gate env); `scripts/wombat-verify.sh` (gate `ci-result`);
  `scripts/README.md` (ci-result.sh section); `CONTRIBUTING.md:407-411`; `CHANGELOG.md:33`.

## Plan

`docs/workstreams/repo-quality/plans/2026-11-01-fuzzing.md`: 9 targets in 9 tasks, built in this
session inline, task by task.

## Outcome

## Not done, and why
