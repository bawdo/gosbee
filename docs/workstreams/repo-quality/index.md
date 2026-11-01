# Workstream: repo-quality

Integration branch: workstream/repo-quality
Next unit: 09

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
| 09 | repo-settings | plan | unit/09-repo-settings | next | units/09-repo-settings.md |
| 10 | release-proof | implement | unit/10-release-proof | pending | units/10-release-proof.md |

## Handover from unit 08

Unit 08 added SECURITY.md "Verifying a release" (signed tag on main with `git fetch origin main
tag`, key `661B...82D1` / signing subkey `9954...EE78` read from the real key, `gh attestation
verify` with and without `--bundle`, checksums as integrity only, SBOMs, the module through
sum.golang.org), a Contributor Covenant 2.1 CODE_OF_CONDUCT.md routed to private reporting, issue
forms with blank issues off, a PR template, `CODEOWNERS` `* @bawdo`, README CI and licence badges,
and a CONTRIBUTING refresh with the Best Practices badge note under "Badges".
**For 09:** a correction in its brief: SECURITY.md describes only what `release.yml` ships and
leaves the tag ruleset, publish environment, immutable releases and `gh release verify` to 09;
the CoC and bug form rely on private vulnerability reporting; `scripts/verify-tag.sh` accepts any
GitHub-verified signature, looser than the published subkey.
**Open, operator to schedule:** deleting the stale `examples/before_after_example.go` sketch;
tightening `verify-tag.sh`; still from 06, the `quoting.EscapeString` backslash doubling.
**For 10:** verification again ran in the operator's shell (sandbox blocks Go cache, uv,
vuln.go.dev and loopback binding).
