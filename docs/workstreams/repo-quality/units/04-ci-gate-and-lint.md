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
- Codecov project target 93%: the measured 93.9% with `.codecov.yml`'s ignore list applied (the
  figure the project status judges), rounded down. Not the raw 83.1%, which counts `cmd/repl`.
  Risk named: Codecov scores Go by line, the measurement is by statement, so the first upload may
  differ slightly; 0.9 points of rounding plus the kept 0.5% threshold are the headroom. Measured
  outside the sandbox by the operator, because the tests bind local ports.
- Codecov patch target 80% (was 70%, informational). Justification: a patch's percentage moves in
  large steps (one uncovered line in five is 80%), so holding a patch at the 93% project figure
  would fail most small changes on granularity rather than on testing; the project status is what
  stops the total drifting, and 80% still requires most new lines to be tested. Threshold 0.5%
  kept for both.
- `pre-ci-check.sh`'s "80% from .codecov.yml" comment and CONTRIBUTING's "target: 80%" became
  false, so both now say 80% is a local guide on the raw total. The check's behaviour is
  unchanged.
- `.codecov.yml` checked with Codecov's validator (`curl --data-binary @.codecov.yml
  https://codecov.io/validate`): `Valid!`.
- `scripts/README.md` gained `--fix gofmt` usage and a `ci-result.sh` section, since both are
  documentation this unit invalidated or created.

## Plan

## Outcome

Close mode as run: **auto**. Questions that fired, with the operator's answers:

- Commit permission (unit open, house rules): "Yes, commit freely".
- `pre-ci-check.sh` `main` without `"$@"`, against "behaviour unchanged" (scope): "Pass \"$@\"".
- Coverage and zizmor could not run here: the sandbox blocks local port binding (`httptest`),
  and the auto-mode classifier refused both an unsandboxed `go test` and `uvx zizmor`. The
  operator ran `measure.sh` and the step 1 verification themselves.

### What landed

- `scripts/ci-result.sh` decides `CI OK`; `re-actors/alls-green` is gone from every workflow
  (`ci-result_test.sh` case `no_alls_green`). The gate job keeps the name `CI OK`, `if: always()`,
  `needs: [ lint, test ]`, `EVENT`/`NEEDS` through `env:` only.
- CI `lint` job: shellcheck v0.11.0 by sha256, `--version`, over `scripts/*.sh`; then
  `scripts/ci-result_test.sh`. actionlint clean locally.
- `pre-ci-check.sh` shellcheck clean, no `disable` directives; `--fix gofmt` now reaches `main`.
- `wombat-verify.sh`: shellcheck over `scripts/*.sh`; new `ci-result` gate.
- `.codecov.yml`: project 93% and patch 80%, both `informational: false`.
- CONTRIBUTING Branch Rules check list, scripts/README, CHANGELOG `[Unreleased]` (Added, Changed,
  Fixed).

### Verification (step 1, run by the operator at `7ec5cca`)

`WOMBAT_BASE_REF=workstream/repo-quality scripts/wombat-verify.sh`: tree `clean`; `fmt`, `vet`,
`lint`, `tidy`, `changelog`, `vuln`, `shellcheck`, `ci-result`, `test` pass; `zizmor` skipped,
reason "zizmor not installed (CI runs it)" (run by hand earlier: no findings, below).
Lanes from `select-gates.sh .`: 3 (`security`, `correctness`, `brief`), none skipped.

### Sweep (step 3)

`sweep-docs.sh . --base workstream/repo-quality --claim 're-actors/alls-green'` printed 8
candidates; a second run with `--claim 'target (80%'` printed the same 8 paths.
Acted on: none rewritten in the close, none deleted. The stale sentences had already been
rewritten in the work (`CONTRIBUTING.md` check list and pre-ci table, `scripts/README.md`,
`pre-ci-check.sh` comment). Dismissed: `CHANGELOG.md` (`claim`: its only hit is the new entry
saying alls-green was replaced, which is true); `CONTRIBUTING.md`, `scripts/README.md` (`touched`,
read back, current); `README.md`, `cmd/repl/README.md`, `plugins/README.md`,
`plugins/opa/README.md`, `plugins/softdelete/README.md` (`named-file` on the basename
`README.md` only; `grep -n -i` for coverage, shellcheck, `CI OK`, pre-ci found nothing in them).
CONTRIBUTING's "This script mimics the GitHub Actions workflow" is left: pre-ci-check never ran
every CI step (zizmor, tidy), and shellcheck joins that list without making the sentence false.
Corrections written into later briefs: none (unit 05 adds fuzz to a table that exists as briefed;
unit 08's rename question stands as written).

### What the next units inherit

- Unit 05: add `fuzz` to `JOBS`, to the gate's `needs:`, and to the `skippable` arms of the events
  that may skip it. `ci-result_test.sh` fails until `JOBS`, `needs:` and the `ci.yml` job list
  agree, and its permitted-skip case rewrites the `pull_request) echo "" ;;` line in a copy: if
  05 changes that arm, the test's `sed` must follow (the test fails loudly if it does not).
- The 02 builder: 02's plan makes `ci.yml` callable and moves Codecov out. The gate job now has its
  own checkout, which under `workflow_call` needs the same `ref: inputs.ref` treatment as every
  other checkout in the file.
- Unit 08: `Coverage` stays not required; Codecov's statuses now gate but are not a ruleset check.

### Coverage measurement (run by the operator, outside the sandbox)

```
$ COVERPKG=$(go list ./... | grep -v testutil | tr '\n' ',' | sed 's/,$//')
$ go test -count=1 -coverprofile=coverage.txt -covermode=atomic -coverpkg=$COVERPKG ./...
go test exit: 0
$ go tool cover -func coverage.txt | tail -1   (raw)
total:  (statements)  83.1%
$ go tool cover -func coverage-ignored.txt | tail -1   (.codecov.yml ignores applied:
  cmd/repl, internal/testutil, _test.go, testdata, .pb.go lines removed from the profile)
total:  (statements)  93.9%
```

Project target: 93%.

### zizmor (run by the operator)

`zizmor 1.31.1 --offline .github/workflows`: `No findings to report. Good job! (3 suppressed)`,
exit 0. All three workflows audited.

### Mutation ledger (ci-result.sh)

Each run through `record-mutation.sh . --patch <p> --fail-pattern '^FAIL ' --ledger <scratchpad>/mut/ledger.txt -- scripts/ci-result_test.sh`
against commit `cb3bb0a`. 8 blocks in the ledger, 8 `verdict  KILLED`, measured with `grep -c`.

| Mutation | First failing line | Verdict |
|---|---|---|
| success scored bad | `FAIL all_success_passes_pull_request: expected exit 0` | KILLED |
| failure scored ok | `FAIL one_failure_fails: expected exit 1` | KILLED |
| cancelled scored ok | `FAIL cancelled_job_fails: expected exit 1` | KILLED |
| permitted skip ignored (`elif false`) | `FAIL permitted_skip_passes: expected exit 0` | KILLED |
| any skip ok (table ignored) | `FAIL permitted_skip_is_per_event: expected exit 1` | KILLED |
| push arm permits `lint` | `FAIL unpermitted_skip_fails_push: expected exit 1` | KILLED |
| missing job defaults to success | `FAIL missing_job_fails: expected exit 1` | KILLED |
| extra job scored by its own result | `FAIL extra_job_fails: expected exit 1` | KILLED |

## Not done, and why
