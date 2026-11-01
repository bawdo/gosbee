# Unit 04: ci-gate-and-lint

Mode: implement
Close: auto
Branch: unit/04-ci-gate-and-lint
Review lanes: security, correctness, brief

## Items

- Replace `re-actors/alls-green` in `ci.yml` with an in-repo `scripts/ci-result.sh`
  (reference: bawdo/caddy_shack `scripts/ci-result.sh` and `ci-result_test.sh`).
- Add a shellcheck step to the CI lint job, and fix `scripts/pre-ci-check.sh`.
- Widen the shellcheck gate in `scripts/wombat-verify.sh` to `scripts/*.sh`.
- Make the Codecov project status gate, at a measured floor.
- Update CONTRIBUTING.md (check list) and CHANGELOG.md.

## Acceptance criteria

- The gate job keeps the check name `CI OK`, so the main ruleset needs no change in
  this unit. It runs `if: always()`, needs every other CI job, and runs
  `scripts/ci-result.sh` with `EVENT` and `NEEDS` passed through `env:` only.
- `scripts/ci-result.sh` holds an explicit per-event table of jobs permitted to be
  skipped (today: none for any event; unit 05 adds fuzz). It fails on a failed or
  cancelled job, on a skip the event does not permit, and on a job missing from
  `NEEDS`. It prints each job's result.
- A test script for `ci-result.sh` covers: all success passes; one failure fails; a
  cancelled job fails; a permitted skip passes; an unpermitted skip fails; a missing
  job fails. Each test is shown to fail against a deliberately broken script
  (mutation) before it is trusted. It runs from `scripts/wombat-verify.sh` and CI.
- `re-actors/alls-green` no longer appears in any workflow.
- The CI lint job downloads a pinned shellcheck release, verifies it by sha256 before
  extracting, prints its version and runs it over `scripts/*.sh`.
- `scripts/pre-ci-check.sh` passes shellcheck with no findings, with behaviour
  unchanged. Any `disable` directive carries a reason.
- `scripts/wombat-verify.sh`'s shellcheck gate runs over `scripts/*.sh`.
- `.codecov.yml` project status is `informational: false` with a target measured from
  current total coverage rounded down to a whole number, and the command and output
  used to measure it are pasted in the Outcome. The patch status gates too, with a
  target that is stated and justified under Decisions.
- CONTRIBUTING.md's "Branch Rules" check list matches the CI as it now is.
- zizmor reports no findings; CHANGELOG.md has an `[Unreleased]` entry.

## Decisions

- Run as `legacy` (`work-items.sh` printed `legacy`: the Items carry no ids), built in this
  session. `skills.plan` is unused (implement unit); `skills.testFirst` is
  `superpowers:test-driven-development`, installed; `skills.implement` is not configured, so the
  build runs directly. Pre-flight printed `preflight none`; no `knownTraps`; no Jira block.
- Caller census before changing the shared contracts (the `CI OK` gate job, the wombat-verify
  shellcheck gate, `.codecov.yml` status, `pre-ci-check.sh`). Command:
  `git grep -n -e 'alls-green' -e 'ci-ok' -e 'CI OK' -e 'pre-ci-check' -e 'shellcheck' -e 'informational'`
  (closed units' briefs and the plans excluded, then `git grep -ln -e 'CI OK' -e 'ci-ok' -- plans/`
  and `grep -n 'ci.yml\|workflow_call\|needs'` over `release.yml` and `scorecard.yml`). Found:
  `ci.yml:134-143` (the gate job itself); `CONTRIBUTING.md:126,129,406` (pre-ci-check usage and
  the check list); `Makefile:86,90` (`make pre-ci` callers of pre-ci-check, arguments unchanged
  here); `scripts/README.md` (pre-ci-check docs); `scripts/wombat-verify.sh:87-93` (the scoped
  shellcheck gate); `.codecov.yml:11,18`; briefs 08 (rename decision, `main` still on alls-green
  until merged). No plan names `CI OK` or `ci-ok`. `release.yml` and `scorecard.yml` do not
  depend on `ci.yml` today; the unbuilt 02 plan (Task 1) makes `ci.yml` callable through
  `workflow_call` and moves Codecov out, so its builder edits the file this unit changes.
- Copied from caddy_shack `scripts/ci-result.sh` and `ci-result_test.sh`. Checked the source for
  known defects: its skip table is a jq array chosen by `case`, `EVENT`/`NEEDS` arrive by env
  only, a non-object `NEEDS` and extra jobs fail. One weakness not carried: its tests have no way
  to exercise a permitted skip once the table permits none, which is gosbee's state today. Here
  the table is a `skippable` function with one `case` arm per event, and the "permitted skip
  passes" test runs a temporary copy of the script with one arm rewritten (and asserts the
  rewrite changed the copy), so the production script keeps no override hook an attacker or a
  workflow could set.
- Table events: `pull_request`, `push`, `schedule`, `workflow_dispatch` (the four `ci.yml`
  triggers) each permit nothing, and any other event permits nothing. Explicit arms rather than
  only a catch-all, so unit 05 edits one named line per event.
- Extra jobs in `NEEDS` fail, reported as `<job>: unexpected` (caddy_shack prints their raw
  result, which reads as a plain skip or success and hides why it failed).
- The skip list is split in jq, not into a bash array: `"${arr[@]}"` on an empty array is unbound
  under `set -u` in bash 3.2 (macOS `/bin/bash`), which the first version hit. The test suite and
  the script were both run under `/bin/bash` 3.2.57 and Homebrew bash 5.3.20.
- The `ci-result_test.sh` cases also pin the gate's shape in `ci.yml` (name `CI OK`,
  `if: always()`, `EVENT`/`NEEDS` through `env:`, `run: scripts/ci-result.sh`), that the gate's
  `needs:` equals `--jobs`, that every `ci.yml` job is in `--jobs`, and that no workflow names
  `alls-green`. CI runs it in the `lint` job; `wombat-verify.sh` runs it as gate `ci-result`
  (skipped with a reason where `jq` is absent).
- Shellcheck pin: v0.11.0, sha256 `8c3be12b…e227198`. Measured, not copied: downloaded the
  release asset and ran `shasum -a 256`, which matched caddy_shack's pin. The version and digest
  sit in the step's `env:` (not workflow-level), so only that step sees them.
- **Divergence from "behaviour unchanged", on the operator's word.** `pre-ci-check.sh` called
  `main` with no arguments (SC2120/SC2119), so `--fix gofmt`, used by `make pre-ci-fix` and
  documented in CONTRIBUTING, never took effect. Operator chose to pass `"$@"`, fixing it (and an
  unknown argument now errors, as the script's own parser always intended). CHANGELOG `Fixed`
  entry added. The other findings are behaviour-preserving: SC2086 quoting, and SC2001
  `echo | sed 's/^/  /'` replaced by `${var//$'\n'/$'\n'  }`, whose output was compared byte for
  byte against the sed form under bash 3.2.57 and 5.3.20 (including an empty line). No
  `disable` directives were needed in `pre-ci-check.sh`. The two in `ci-result_test.sh` (SC2016)
  carry a reason.
- `scripts/README.md` gained `--fix gofmt` usage and a `ci-result.sh` section, since both are
  documentation this unit invalidated or created.

## Plan

## Outcome

## Not done, and why
