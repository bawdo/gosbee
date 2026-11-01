# Workstream: repo-quality

Integration branch: workstream/repo-quality
Next unit: 03

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
| 03 | release-supply-chain | plan | unit/03-release-supply-chain | next | units/03-release-supply-chain.md |
| 04 | ci-gate-and-lint | implement | unit/04-ci-gate-and-lint | pending | units/04-ci-gate-and-lint.md |
| 05 | fuzzing | plan | unit/05-fuzzing | pending | units/05-fuzzing.md |
| 06 | examples | implement | unit/06-examples | pending | units/06-examples.md |
| 07 | community-docs | implement | unit/07-community-docs | pending | units/07-community-docs.md |
| 08 | repo-settings | plan | unit/08-repo-settings | pending | units/08-repo-settings.md |
| 09 | release-proof | implement | unit/09-release-proof | pending | units/09-release-proof.md |

## Handover from unit 02

Unit 02 landed a plan only:
`docs/workstreams/repo-quality/plans/2026-11-01-release-pipeline.md`, six TDD tasks. Plans and
specs now live under this workstream (`planDir`/`specDir` in `.claude/wombat-workstream.json`),
because `/docs/plans` is gitignored. **Open question for the operator: nothing builds this plan.**
02 and 03 are plan units, and 09 assumes the pipeline is on `main`, so a re-cut is due before 03
or 09 opens. The operator chose to decide later. Decided in 02: sync-notes is dropped; Codecov
moves to a new `coverage.yml` (OIDC), so the release chain holds `id-token` only in publish;
`ci.yml` `workflow_call` takes a `ref` input and keys concurrency on it; asset names keep the
v-prefixed tag (`gosbee-repl_<tag>_*`, `SHA256SUMS`, `..._provenance.intoto.jsonl`); building
moves to `scripts/build-release.sh`; pre-09 exercise is local only. For 03: verification docs
should pin `--signer-workflow` to `release.yml`. For 04: an actionlint gate is left to it. For 08:
`Coverage` is not a required check. gpg signing fails inside the agent sandbox, so commit outside it.
