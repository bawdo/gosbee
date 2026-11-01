# Workstream: repo-quality

Integration branch: workstream/repo-quality
Next unit: 06

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
| 06 | fuzzing | plan | unit/06-fuzzing | next | units/06-fuzzing.md |
| 07 | examples | implement | unit/07-examples | pending | units/07-examples.md |
| 08 | community-docs | implement | unit/08-community-docs | pending | units/08-community-docs.md |
| 09 | repo-settings | plan | unit/09-repo-settings | pending | units/09-repo-settings.md |
| 10 | release-proof | implement | unit/10-release-proof | pending | units/10-release-proof.md |

## Handover from unit 05

Unit 05 built both release plans. `ci.yml` is callable (`workflow_call`, optional `ref`, all 3 checkouts
use it), Codecov moved to `coverage.yml`, and `release.yml` runs `ci` -> read-only `build` -> `publish` (only writer,
only `id-token`) -> `proxy`. Scripts: `verify-tag.sh`, `build-release.sh` (4 binaries, CycloneDX 1.6 SBOMs via
`tools/go.mod` cyclonedx-gomod v1.12.0, `SHA256SUMS` of 9), `publish-release.sh TAG TAG_OBJECT NOTES BUNDLE` and
`--check TAG TAG_OBJECT`, `check-module-proxy.sh`; all tested by `scripts/release-scripts_test.sh` (fake gh/curl/sleep/go),
run by CI lint and the `release-scripts` verify gate. **For 06 (fuzzing):** add `fuzz` to `JOBS`, to the gate's
`needs:` and to ci.yml as before; a new ci.yml checkout must take `ref: ${{ inputs.ref }}` and `persist-credentials: false`.
**For 08:** pin `--signer-workflow` to `release.yml`, not `--source-ref` (a dispatched run's provenance names the branch);
mention SBOMs. **For 09:** an `environment:` on publish with a `v*` tag rule, and a tag ruleset, are what bind
`workflow_dispatch` (only documented so far); `Coverage` stays not required. **For 10:** nothing has run remotely; the
job graph, OIDC, attestation, sparse checkout by sha and `tag_object` output are first proven there; plain tags get
`--latest`, even an older line's patch. zizmor runs locally via `uvx zizmor@1.31.1`; verification needs to run outside
the agent sandbox (Go cache, httptest binds, vuln.go.dev). `coverage.yml` is force-added past `.gitignore`'s `coverage.*`.
