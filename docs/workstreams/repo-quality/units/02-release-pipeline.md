# Unit 02: release-pipeline

Mode: plan
Close: auto
Branch: unit/02-release-pipeline
Review lanes: security, correctness, brief

## Items

- Give `ci.yml` a `workflow_call` trigger so `release.yml` can run it first.
- Restructure `release.yml` into ci, build and publish jobs (reference:
  bawdo/caddy_shack `release.yml`, `scripts/publish-release.sh`).
- Port `scripts/verify-tag.sh` from caddy_shack and call it from the build job.
- Decide the fate of the `sync-notes` job under immutable releases, and record it.
- Update CONTRIBUTING.md's "Releasing" and "Branch Rules" sections and CHANGELOG.md.

## Acceptance criteria

- `ci.yml` gains `workflow_call` (with an optional `CODECOV_TOKEN` secret only if one
  is still needed; OIDC upload stays the default). Its concurrency group uses a
  literal prefix (`ci-...`), not `github.workflow`, so caller and callee never share a
  group and deadlock. Pull request and push behaviour is unchanged.
- `release.yml` has top-level `permissions: contents: read` and three jobs in order:
  - **ci**: `uses: ./.github/workflows/ci.yml`.
  - **build** (`needs: ci`, read-only token, `timeout-minutes` set): validates the tag
    against `^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$` before checkout; checks out
    `refs/tags/<tag>` fully qualified with `persist-credentials: false` and full
    history; confirms HEAD is the tag's commit and that commit is on `origin/main`;
    runs `scripts/verify-tag.sh` (tag annotated, signed, `verification.verified` true
    from the GitHub API); runs `scripts/changelog.sh check` and writes the notes;
    sets up Go with `cache: false`; runs `go mod verify`; builds the four REPL
    binaries (`linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`,
    `CGO_ENABLED=0`, `-trimpath`, `-ldflags='-s -w'`); smoke tests the native linux
    binary; writes a checksums file; uploads binaries, checksums and notes as
    artifacts with `if-no-files-found: error` and 1 day retention.
  - **publish** (`needs: build`): the only job with `contents: write`,
    `id-token: write`, `attestations: write`. Checks out nothing but the publish
    script (sparse checkout). Downloads the artifacts, verifies them against the
    checksums, attests with `actions/attest-build-provenance` using
    `subject-checksums`, writes the Sigstore bundle as
    `gosbee-repl_<version>_provenance.intoto.jsonl` on a single line (asserted), and
    publishes through the publish script.
- The publish script creates the release as a draft, uploads every asset, then
  publishes it (`--draft=false --latest`, or `--prerelease` for a suffixed tag). A
  stale draft for the tag is deleted and replaced. A published release is never
  modified: no `gh release upload --clobber`, no `gh release edit` of assets.
- `workflow_dispatch` takes a required `tag` input for re-running an existing tag; the
  concurrency group is keyed on the tag.
- Untrusted values (tag names, inputs) reach `run:` only through `env:`, never by
  `${{ }}` interpolation inside a script. zizmor reports no findings.
- `sync-notes`: the decision (keep, drop, or change) is written under Decisions with
  the reason, checked against what GitHub immutable releases actually permit.
- `scripts/verify-tag.sh` and the publish script pass shellcheck. Add both to the
  shellcheck gate list in `scripts/wombat-verify.sh`.
- CONTRIBUTING.md describes the new release flow accurately; CHANGELOG.md has an
  `[Unreleased]` entry.
- The plan states how the workflow is exercised before unit 09 (for example `act`,
  a dry run on a fork, or not at all), and the Outcome says which was done.

## Decisions

## Plan

## Outcome

## Not done, and why
