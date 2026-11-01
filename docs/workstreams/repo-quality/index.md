# Workstream: repo-quality

Integration branch: workstream/repo-quality
Next unit: 05

Bring gosbee's GitHub, CI and release practice up to (and past) bawdo/caddy_shack,
bawdo/jellyfish and bawdo/go-blinkstick: a strong OpenSSF Scorecard and readiness for
the OpenSSF Best Practices survey. Solo maintainer: nothing may need a second
contributor. Baseline 2026-11-01: Scorecard API 404 for gosbee (never scored); peers
8.3, 8.0 and 7.6.

Cut by file collision (each workflow file is owned by a contiguous run of units),
with dependency order on top: 03 extends 02's jobs, 05 needs 04's skip table, 07
documents what 02 and 03 ship, 08 needs the final action set, 09 proves it all.

## Units

| # | Slug | Mode | Branch | Status | Brief |
|---|---|---|---|---|---|
| 01 | scorecard | implement | unit/01-scorecard | done | units/01-scorecard.md |
| 02 | release-pipeline | plan | unit/02-release-pipeline | done | units/02-release-pipeline.md |
| 03 | release-supply-chain | plan | unit/03-release-supply-chain | done | units/03-release-supply-chain.md |
| 04 | ci-gate-and-lint | implement | unit/04-ci-gate-and-lint | done | units/04-ci-gate-and-lint.md |
| 05 | fuzzing | plan | unit/05-fuzzing | next | units/05-fuzzing.md |
| 06 | examples | implement | unit/06-examples | pending | units/06-examples.md |
| 07 | community-docs | implement | unit/07-community-docs | pending | units/07-community-docs.md |
| 08 | repo-settings | plan | unit/08-repo-settings | pending | units/08-repo-settings.md |
| 09 | release-proof | implement | unit/09-release-proof | pending | units/09-release-proof.md |

## Handover from unit 04

Unit 04 landed code. `CI OK` is now decided by `scripts/ci-result.sh` (alls-green gone): `JOBS=(lint test)`
plus a `skippable` case table, one arm per event, all empty. **For 05:** add `fuzz` to `JOBS`, to the gate's
`needs:`, and to the arms of the events that may skip it; `scripts/ci-result_test.sh` fails until `JOBS`,
`needs:` and the ci.yml job list agree, and its permitted-skip case `sed`-rewrites the exact line
`        pull_request) echo "" ;;` in a copy (it fails loudly if that line changes). CI lint now shellchecks
`scripts/*.sh` (v0.11.0, sha256 pinned in the step's env, not tracked by Dependabot). Codecov project 93% and
patch 80% now gate (Codecov's `main` read 94.16%). **For the 02 builder:** the gate job has its own checkout,
which needs `ref: inputs.ref` once ci.yml is callable. `pre-ci-check.sh --fix gofmt` works now. Local `go test`
needs port binding the agent sandbox blocks, so verification runs fell to the operator; zizmor is not
installed locally. Still open: the 02/03 re-cut; for 07, pin `--signer-workflow` to `release.yml` and
mention SBOMs; for 08, `Coverage` is not a required check. gpg signing needs commits outside the sandbox.
