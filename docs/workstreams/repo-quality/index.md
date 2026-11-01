# Workstream: repo-quality

Integration branch: workstream/repo-quality
Next unit: 05

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
| 05 | release-build | implement | unit/05-release-build | next | units/05-release-build.md |
| 06 | fuzzing | plan | unit/06-fuzzing | pending | units/06-fuzzing.md |
| 07 | examples | implement | unit/07-examples | pending | units/07-examples.md |
| 08 | community-docs | implement | unit/08-community-docs | pending | units/08-community-docs.md |
| 09 | repo-settings | plan | unit/09-repo-settings | pending | units/09-repo-settings.md |
| 10 | release-proof | implement | unit/10-release-proof | pending | units/10-release-proof.md |

## Handover from unit 04

Unit 04 landed code. `CI OK` is now decided by `scripts/ci-result.sh` (alls-green gone): `JOBS=(lint test)`
plus a `skippable` case table, one arm per event, all empty. **For 06 (fuzzing):** add `fuzz` to `JOBS`, to the gate's
`needs:`, and to the arms of the events that may skip it; `scripts/ci-result_test.sh` fails until `JOBS`,
`needs:` and the ci.yml job list agree, and its permitted-skip case `sed`-rewrites the exact line
`        pull_request) echo "" ;;` in a copy (it fails loudly if that line changes). CI lint now shellchecks
`scripts/*.sh` (v0.11.0, sha256 pinned in the step's env, not tracked by Dependabot). Codecov project 93% and
patch 80% now gate (Codecov's `main` read 94.16%). **For the 02 builder:** the gate job has its own checkout,
which needs `ref: inputs.ref` once ci.yml is callable. `pre-ci-check.sh --fix gofmt` works now. Local `go test`
needs port binding the agent sandbox blocks, so verification runs fell to the operator; zizmor is not
installed locally. Re-cut done (05 builds 02/03); for 08, pin `--signer-workflow` to `release.yml` and
mention SBOMs; for 09, `Coverage` stays not required. gpg signing needs commits outside the sandbox.
