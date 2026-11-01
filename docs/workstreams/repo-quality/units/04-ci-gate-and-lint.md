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

## Plan

## Outcome

## Not done, and why
