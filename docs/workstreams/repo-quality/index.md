# Workstream: repo-quality

Integration branch: workstream/repo-quality
Next unit: 01

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
| 01 | scorecard | implement | unit/01-scorecard | next | units/01-scorecard.md |
| 02 | release-pipeline | plan | unit/02-release-pipeline | pending | units/02-release-pipeline.md |
| 03 | release-supply-chain | plan | unit/03-release-supply-chain | pending | units/03-release-supply-chain.md |
| 04 | ci-gate-and-lint | implement | unit/04-ci-gate-and-lint | pending | units/04-ci-gate-and-lint.md |
| 05 | fuzzing | plan | unit/05-fuzzing | pending | units/05-fuzzing.md |
| 06 | examples | implement | unit/06-examples | pending | units/06-examples.md |
| 07 | community-docs | implement | unit/07-community-docs | pending | units/07-community-docs.md |
| 08 | repo-settings | plan | unit/08-repo-settings | pending | units/08-repo-settings.md |
| 09 | release-proof | implement | unit/09-release-proof | pending | units/09-release-proof.md |

## Handover from unit 00

Workstream opened. Verification is `scripts/wombat-verify.sh`, which writes the
gitignored record `.claude/wombat-verification`. Its shellcheck gate covers only
`scripts/changelog.sh` and `scripts/wombat-verify.sh`, because `scripts/pre-ci-check.sh`
has outstanding findings; unit 04 fixes them and widens the gate. zizmor is not
installed locally, so that gate records a skip; CI still runs it. Remotes are
`local`: commit only, hand push and pull request commands to the operator.
