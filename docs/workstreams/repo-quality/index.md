# Workstream: repo-quality

Integration branch: workstream/repo-quality
Next unit: 04

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
| 04 | ci-gate-and-lint | implement | unit/04-ci-gate-and-lint | next | units/04-ci-gate-and-lint.md |
| 05 | fuzzing | plan | unit/05-fuzzing | pending | units/05-fuzzing.md |
| 06 | examples | implement | unit/06-examples | pending | units/06-examples.md |
| 07 | community-docs | implement | unit/07-community-docs | pending | units/07-community-docs.md |
| 08 | repo-settings | plan | unit/08-repo-settings | pending | units/08-repo-settings.md |
| 09 | release-proof | implement | unit/09-release-proof | pending | units/09-release-proof.md |

## Handover from unit 03

Unit 03 landed a plan only:
`docs/workstreams/repo-quality/plans/2026-11-01-release-supply-chain.md`, four TDD tasks, built
on top of the 02 plan (it stops if 02's scripts and jobs are absent). SBOMs: `cyclonedx-gomod`
v1.12.0 via `tools/go.mod`, CycloneDX JSON 1.6, a module SBOM plus one per binary, all named
inside 02's `gosbee-repl_<tag>_*` globs so `SHA256SUMS` (now nine lines) and `subject-checksums`
cover them. The proxy check is `scripts/check-module-proxy.sh`, run by a `proxy` job after publish
with no checkout (the script arrives as a build artifact) against the build job's `commit` output.
**Still open for the operator: the re-cut.** Nothing builds the 02 or 03 plans, and 09 assumes the
pipeline is on `main`. 03 ran as briefed because the question went unanswered at open. **For the
02 builder:** 02's plan lists the provenance bundle twice in `gh release create` (its line 101).
For 07: the verification docs should pin `--signer-workflow` to `release.yml` (passed on from 02),
and mention the SBOMs. Carried from 02: for 04, an actionlint gate is left to it; for 08,
`Coverage` is not a required check. gpg signing fails inside the agent sandbox, so commit outside it.
