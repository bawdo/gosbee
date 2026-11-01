# Workstream: repo-quality

Integration branch: workstream/repo-quality
Next unit: 10

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
| 07 | examples | implement | unit/07-examples | done | units/07-examples.md |
| 08 | community-docs | implement | unit/08-community-docs | done | units/08-community-docs.md |
| 09 | repo-settings | plan | unit/09-repo-settings | done | units/09-repo-settings.md |
| 10 | release-proof | implement | unit/10-release-proof | next | units/10-release-proof.md |

## Handover from unit 09

Unit 09 added `scripts/repo-settings.sh` (`metadata`, `rulesets`, `harden`, `show`; `DRY_RUN=1`
prints each write and its body) with `repo-settings_test.sh` in CI and `wombat-verify.sh`.
**Applied live 2026-11-01:** homepage, topics, auto-merge, Update branch; rulesets
`Protect main` (strict on, `CI OK` + `Analyze (go)`, `code_scanning` CodeQL), `Protect release
tags`, new `Protect release tag creation` (admin-only `v*`). `release.yml` publish job names
`environment: release`; SECURITY.md gained `gh release verify`.
**For 10:** after the operator's merge to `main` (PR must be up to date: strict is now on), run
`DRY_RUN=1 scripts/repo-settings.sh harden` from `main`, get approval, apply, paste `show`.
Check at that dry run: whether GitHub matches subpath actions against `owner/repo@*`, and whether
composite actions (codecov-action) pull nested third-party or tag-pinned actions that the
allowlist or SHA pinning would refuse. Until `harden` runs, the `release` environment has no
deployment rule. `gh` needs the sandbox off (TLS, OSStatus -26276); zizmor runs in it with uv's
cache, tool and Python dirs under `$TMPDIR`; verification still runs outside it.
**Open, operator to schedule:** unchanged from 08 (`verify-tag.sh`, the stale example sketch,
`quoting.EscapeString`).
