# Unit 05: release-build

Mode: implement
Close: auto
Branch: unit/05-release-build
Review lanes: security, correctness, brief

## Items

- [a] Build every task of `docs/workstreams/repo-quality/plans/2026-11-01-release-pipeline.md`
  (the unit 02 plan, Tasks 1 to 6), with the two builder fixes below.
  Plan: no
  Files: .github/workflows/ci.yml, .github/workflows/coverage.yml, .github/workflows/release.yml, scripts/verify-tag.sh, scripts/build-release.sh, scripts/publish-release.sh, scripts/release-scripts_test.sh, scripts/wombat-verify.sh, CONTRIBUTING.md, CHANGELOG.md
- [b] Build every task of `docs/workstreams/repo-quality/plans/2026-11-01-release-supply-chain.md`
  (the unit 03 plan, Tasks 1 to 4), on top of [a].
  Plan: no
  Files: tools/go.mod, tools/go.sum, scripts/build-release.sh, scripts/check-module-proxy.sh, scripts/release-scripts_test.sh, .github/workflows/release.yml, scripts/wombat-verify.sh, CONTRIBUTING.md, CHANGELOG.md
  After: a

Two builder fixes from the index handovers are part of [a]. First, the `CI OK` gate job (added by
unit 04, after the 02 plan was written) has its own `actions/checkout`: once `ci.yml` is callable
it takes `ref: inputs.ref` like every other checkout in the file, and the 02 plan's "every
checkout uses `inputs.ref`" grep covers it. Second, the 02 plan lists the provenance bundle twice
in `gh release create` (around its line 101: once through the `dist/gosbee-repl_<tag>_*` glob,
once as `<bundle>`); the asset is uploaded once.

## Acceptance criteria

Taken from the two plans' Global Constraints, Review Focus and task tests, not from the closed
02 and 03 briefs, which a unit session does not read. Where a plan says "unit 09" it means
release-proof, now unit 10.

- Every task of both plans has landed, in plan order, with the 03 plan's precondition check
  (02's scripts and `ci`, `build`, `publish` jobs exist) passing before its Task 1.
- Every test case each plan task names exists, was seen to fail before its code (TDD), and
  passes. Each script test suite is shown to fail against a deliberately broken script through
  `record-mutation.sh`, with the ledger cited in the Outcome. The suites run from
  `scripts/wombat-verify.sh` and CI.
- `ci.yml` is callable through `workflow_call` with an optional `ref` input; every checkout in it,
  the `CI OK` gate's included, uses that ref. Codecov upload lives in `coverage.yml`, and no job in
  the release chain except `publish` holds `id-token`. `scripts/ci-result_test.sh` still passes,
  and `JOBS`, the gate's `needs:` and the `ci.yml` job list still agree.
- `release.yml` runs `ci` (calling `ci.yml` with `ref: refs/tags/<tag>`), then a read-only
  `build` (tag verified by `scripts/verify-tag.sh` against
  `^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$`, then `scripts/build-release.sh`), then `publish`
  (the only job that can write: attests, creates a draft, uploads, publishes; refuses a release
  already published before any write; replaces a stale draft), then `proxy` (no checkout, runs
  `scripts/check-module-proxy.sh` from a build artifact against the build job's `commit` output).
- Binaries for `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`, built with
  `CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' ./cmd/repl`, named
  `gosbee-repl_<tag>_<os>_<arch>`. CycloneDX JSON 1.6 SBOMs from `cyclonedx-gomod` v1.12.0 via
  `tools/go.mod`: `gosbee-repl_<tag>_module.cdx.json` and one `.cdx.json` per binary. `SHA256SUMS`
  covers every binary and SBOM, and the provenance attestation covers the same set.
- A pre-release tag (containing `-`) is accepted end to end, gets SBOMs and the proxy check, and
  is never marked latest. A hostile tag or input (`v1.0.0;id`, `v1.0.0$(id)`, `main`) is refused
  before the build job's checkout.
- The proxy check uses exactly `https://proxy.golang.org/github.com/bawdo/gosbee/@v/<tag>.info`
  and `https://sum.golang.org/lookup/github.com/bawdo/gosbee@<tag>`, up to 5 and 2 attempts, 5s
  apart, `curl -fsS --max-time 5`, never retrying a hash mismatch; a moved tag fails at once
  naming both commits.
- Every action pinned by full SHA with a `# vX.Y.Z` comment; every checkout
  `persist-credentials: false`; every job `timeout-minutes`; untrusted values reach `run:` only
  through `env:`.
- zizmor and actionlint report no findings; shellcheck passes on `scripts/*.sh`;
  `scripts/wombat-verify.sh` is green at the tip.
- CONTRIBUTING.md's release process and Branch Rules, and CHANGELOG.md `[Unreleased]`, describe the
  pipeline as built. No tag is pushed and nothing runs remotely: unit 10 proves it.

## Decisions

- OPERATOR RULING 2026-11-01 (after unit 04 closed): nothing built the 02 and 03 plans, and unit 10
  (then 09) assumes the pipeline on `main`. The operator accepted inserting this build unit
  before fuzzing, renumbering fuzzing to release-proof from 05-09 to 06-10. This is a re-cut,
  counted in the closeout. Built next because both plans predate unit 04's `ci.yml` changes, so
  building them soonest keeps the drift smallest.
- Opened 2026-11-01: router current, pre-flight `none` (no checks configured), no `jira` block, no
  `knownTraps` key, no inherited corrections. `work-items.sh` gave two waves (`a`, then `b`), no
  collision (shared paths are ordered by `After: a`), not oversize. Commit grant given once, in
  conversation, for the unit.
- Skills: `testFirst` (superpowers:test-driven-development) handed to each implementer; `plan`
  configured but unused (no `Plan: yes` item); `implement` not configured, and dispatch replaces it.
- Verification runs unsandboxed: in the sandbox the Go build cache is unwritable, httptest binds are
  refused even with `allowLocalBinding` set, and vuln.go.dev is blocked. The baseline at `9a9905d`
  was green on every gate unsandboxed, with zizmor skipped (not installed). zizmor is run ad hoc
  through `uvx zizmor --offline .github/workflows` to meet the zizmor criterion, since the verify
  script skips it locally.
- Mutation ledgers live outside the repository, beside the item worktrees under
  `$TMPDIR/u05-items.suryDu/`; their blocks are copied into `## Outcome`.

## Plan

## Outcome

## Not done, and why
