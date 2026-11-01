# Release Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn gosbee's single-job release into a ci, build, publish pipeline: tested first, built read-only, published by the only job that can write, and never touching a release once it is published.

**Architecture:** `release.yml` calls `ci.yml` through `workflow_call`, then a read-only build job verifies the tag and runs `scripts/build-release.sh`, then a publish job (which checks out only `scripts/publish-release.sh`) attests and publishes through a draft. The Codecov upload moves to its own `coverage.yml`, so no job in the release chain except publish holds `id-token`.

**Tech Stack:** GitHub Actions, bash, gh CLI, jq, Go toolchain (`stable`).

**Spec:** `docs/workstreams/repo-quality/units/02-release-pipeline.md` (Items, Acceptance criteria and Decisions). Reference implementation: `/Users/bawdo/working/projects/caddy_shack` at `35972c1` (`.github/workflows/release.yml`, `scripts/verify-tag.sh`, `scripts/publish-release.sh`, `scripts/release-workflow_test.sh`).

## Global Constraints

- Tag pattern, everywhere a tag is checked: `^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$`. A tag containing `-` is a pre-release.
- Targets: `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`. Built with `CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' ./cmd/repl`.
- Asset names: `gosbee-repl_<tag>_<os>_<arch>`, `SHA256SUMS`, `gosbee-repl_<tag>_provenance.intoto.jsonl`. `<tag>` keeps its `v`.
- Every action pinned by full commit SHA with a `# vX.Y.Z` comment, reusing the SHAs already in `ci.yml` and `release.yml`. New ones: `actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2` and `actions/download-artifact@9000827ccba6bdab643e8b6fd33ac0654aef8333 # v8.0.2` (as caddy_shack pins them).
- Every checkout sets `persist-credentials: false`. Every job sets `timeout-minutes`.
- Untrusted values (tag names, `inputs.*`, `github.ref_name`) reach `run:` only through `env:`. `${{ }}` is allowed in `with:`, `env:`, `concurrency:` and `if:`, never inside a `run:` body.
- `zizmor --offline .github/workflows` (through `uvx zizmor@1.31.1` where zizmor is not installed) reports no findings. `actionlint` reports nothing. `shellcheck` passes on every script touched.
- Scripts: `#!/usr/bin/env bash`, `set -euo pipefail`, a `die()` that prints `<script>: <message>` to stderr and exits 1, four-space indentation (as caddy_shack's are; `changelog.sh` uses tabs and stays as it is).
- Commits: past tense, 60 characters or fewer, gpg signed, no agent attribution, never on `main`. No push.
- Every behaviour change goes through `superpowers:test-driven-development`: write the failing test and see it fail before the code. Workflow YAML has no unit test, so its proof is actionlint and zizmor plus the grep assertions each task names.

### Review lanes every task is held to

The close-out runs three lanes (`security`, `correctness`, `brief`) against the whole diff. Build each task so it answers them:

- **security:** what an attacker or a hostile input could do with this change: secrets, injection, authorisation, and anything it exposes that was not exposed before. Here that means a hostile tag or dispatch input, a token reachable by code that ends up in the release, a moved tag, and a published release being altered.
- **correctness:** whether the code does what it claims under the cases it will actually meet, including the edges, the errors and the concurrent ones. Here that means pre-release tags, a re-run after a part-failed run, a dispatch re-run of an existing tag, and two runs for one tag.
- **brief:** whether the change delivers what the brief specified, criterion by criterion, with each divergence recorded under the brief's Decisions.

## Review Focus

1. **A dispatch re-run tests the tag, not the branch it was dispatched from.** The `ci` job passes `ref: refs/tags/<tag>`, and every checkout in `ci.yml` uses `inputs.ref`. Pinned by Task 1's grep and Task 5's grep.
2. **A pre-release tag (`v0.3.0-rc.1`) is accepted end to end and is never marked latest.** Pinned by `verify_tag_accepts_prerelease` (Task 2), `publish_marks_prerelease` (Task 3) and `build_accepts_prerelease_tag` (Task 4).
3. **A part-failed run is recoverable, and a published release is untouchable.** A stale draft is replaced, a published one is refused before any write, and a bad bundle is refused before the draft is touched. Pinned by Task 3's cases.
4. **A hostile tag or input (`v1.0.0;id`, `v1.0.0$(id)`, `main`) is refused before checkout** and never reaches a shell by interpolation. Pinned by `verify_tag_rejects_bad_name` and `build_rejects_bad_tag`, and by zizmor in Task 5.
5. **Re-running a tag made before this pipeline existed (`v0.2.1`)** fails cleanly at the build job, because that tag carries no `scripts/verify-tag.sh`. Accepted, not fixed. CONTRIBUTING says so in Task 6.

---

### Task 1: ci.yml becomes callable, and Codecov moves to coverage.yml

**Files:**
- Modify: `.github/workflows/ci.yml`
- Create: `.github/workflows/coverage.yml`

**Interfaces:**
- Produces: `ci.yml` `on.workflow_call.inputs.ref` (`type: string`, `required: false`, `default: ''`), described as "Fully qualified ref to test, e.g. refs/tags/v1.2.3; empty tests the event's ref". No `secrets:` block. Task 5 calls it with `ref: refs/tags/<tag>`.

- [ ] **Step 1: Edit ci.yml**
  - Add `workflow_call` with the `ref` input above.
  - Concurrency becomes `group: ci-${{ github.event.pull_request.number || github.ref }}` and keeps `cancel-in-progress: ${{ github.event_name == 'pull_request' }}`. Add a comment saying why the prefix is literal: in a called workflow `github.workflow` names the caller, so caller and callee would share a group and deadlock.
  - Add `ref: ${{ inputs.ref }}` to both checkouts (lint and test).
  - Test job: remove its `permissions:` block (the top-level `contents: read` applies), the `coverage: true` matrix key, the `COVERAGE` env and branch, and the Codecov step.
- [ ] **Step 2: Create coverage.yml**
  - `name: Coverage`. Triggers `push: branches: [main]`, `pull_request`, `workflow_dispatch`. Top-level `permissions: contents: read`.
  - Concurrency `coverage-${{ github.event.pull_request.number || github.ref }}`, cancelling on pull requests only.
  - One job, `Coverage (stable, ubuntu-latest)`, `timeout-minutes: 20`, `permissions: { contents: read, id-token: write }` with the comment `# Codecov upload via OIDC, no token secret`.
  - Steps: checkout, setup-go `stable` with `cache: true`, then `go test -count=1 -shuffle=on -race -coverprofile=coverage.txt -covermode=atomic -coverpkg="$COVERPKG" ./...`, with `COVERPKG` computed exactly as ci.yml computed it. Then the Codecov step moved unchanged (`use_oidc: true`, `fail_ci_if_error: false`).
  - Header comment: why this lives outside ci.yml (release.yml calls ci.yml, and a called workflow cannot be granted `id-token` without the caller granting it too).
- [ ] **Step 3: Verify**
  - `actionlint .github/workflows/ci.yml .github/workflows/coverage.yml` prints nothing.
  - `uvx zizmor@1.31.1 --offline .github/workflows` reports no findings.
  - `grep -c 'ref: \${{ inputs.ref }}' .github/workflows/ci.yml` prints `2`.
  - `grep -n 'id-token' .github/workflows/ci.yml` prints nothing.
  - `grep -n 'github.workflow' .github/workflows/ci.yml` prints nothing.
- [ ] **Step 4: Commit:** `Made CI callable and moved Codecov to its own workflow`

### Task 2: scripts/verify-tag.sh, with the release script test harness

**Files:**
- Create: `scripts/release-scripts_test.sh` (executable)
- Create: `scripts/verify-tag.sh` (executable)

**Interfaces:**
- Produces: `scripts/verify-tag.sh <tag>`. It needs `GITHUB_REPOSITORY` and an authenticated `gh`, and runs from a checkout of the tag. Exit 0 prints `verify-tag: <tag> is annotated and its signature is verified by GitHub`. Every refusal exits 1 with one stderr line.
- Produces: the harness in `release-scripts_test.sh`: a fake `gh` first on `PATH` that logs its arguments, plus the helpers `fake`, `run_gh`, `expect_refusal`, `log_order` and `tagged`, ported from caddy_shack's `release-workflow_test.sh` (lines 1-136) with `caddy_shack` renamed to `gosbee`. It prints `ok <name>` per case and exits 1 on the first failure. Tasks 3 and 4 append to it.

- [ ] **Step 1: Write the harness and the failing cases.** Port caddy_shack's five verify-tag cases verbatim (accepts a valid signed tag, rejects lightweight, rejects a tag moved since checkout, rejects a missing local tag, rejects unverified). Add two:
  - `verify_tag_accepts_prerelease`: as the accepting case, with the fixture tag `v0.1.0-rc.1`. Exit 0.
  - `verify_tag_rejects_bad_name`: for each of `v1.0.0;id`, `v1.0.0$(id)`, `main` and `v1.0`, expect exit 1, stderr exactly `verify-tag: usage: $SCRIPTS/verify-tag.sh vX.Y.Z[-suffix]`, and an empty `gh.log`.
- [ ] **Step 2: Run** `bash scripts/release-scripts_test.sh`. Expected: FAIL on the first case, because the script does not exist.
- [ ] **Step 3: Implement** `scripts/verify-tag.sh`, ported from caddy_shack's, with the global tag pattern and the usage text `vX.Y.Z[-suffix]`. The header comment says what it checks and that GitHub does the signature verification.
- [ ] **Step 4: Run** `bash scripts/release-scripts_test.sh` and `shellcheck scripts/verify-tag.sh scripts/release-scripts_test.sh`. Expected: seven `ok` lines and no shellcheck output.
- [ ] **Step 5: Commit:** `Added verify-tag.sh and the release script tests`

### Task 3: scripts/publish-release.sh

**Files:**
- Create: `scripts/publish-release.sh` (executable)
- Modify: `scripts/release-scripts_test.sh`

**Interfaces:**
- Produces: `scripts/publish-release.sh <tag> <notes-file> <bundle-file>`, run from a directory holding `dist/`. It compacts the bundle to `dist/gosbee-repl_<tag>_provenance.intoto.jsonl` and checks it is one line before any `gh` call. A published release makes it refuse with `publish-release: <tag> is already published`, and a draft is deleted. It then runs `gh release create <tag> --draft --verify-tag --title <tag> --notes-file <notes> [--prerelease] dist/gosbee-repl_<tag>_* dist/SHA256SUMS <bundle>`, then `gh release edit <tag> --draft=false` plus `--latest` for a plain tag (nothing more for a pre-release), then prints the release URL. It never calls `gh release upload`, and never `gh release edit` without `--draft=false`.

- [ ] **Step 1: Write the failing cases.** Port caddy_shack's five publish cases (refuses a published release, replaces a draft, creates when missing, refuses a bad bundle before touching the draft, needs a bundle). Adjust the fixtures to `gosbee-repl_v0.1.0_linux_amd64`, `SHA256SUMS` and `gosbee-repl_v0.1.0_provenance.intoto.jsonl`. Add:
  - `publish_marks_prerelease`: tag `v0.1.0-rc.1`. The log carries `release create v0.1.0-rc.1 --draft --verify-tag` with `--prerelease`, and `release edit v0.1.0-rc.1 --draft=false`, and no line contains `--latest`.
  - `publish_never_mutates_assets`: across every case's log, no line matches `release upload` or `--clobber`.
- [ ] **Step 2: Run** the tests. Expected: FAIL at the first publish case.
- [ ] **Step 3: Implement** `scripts/publish-release.sh`, ported, with the global tag pattern, the gosbee asset names and the pre-release branch. The header comment says a published release is never touched.
- [ ] **Step 4: Run** the tests and `shellcheck scripts/publish-release.sh`. Expected: every case `ok`.
- [ ] **Step 5: Commit:** `Added publish-release.sh, publishing through a draft`

### Task 4: scripts/build-release.sh

**Files:**
- Create: `scripts/build-release.sh` (executable)
- Modify: `scripts/release-scripts_test.sh`

**Interfaces:**
- Produces: `scripts/build-release.sh <tag>`, run from the repository root. It builds the four targets into `dist/gosbee-repl_<tag>_<os>_<arch>` with the global build flags. It smoke tests the host's binary: `printf 'exit\n' | HOME=<tempdir> GOSBEE_ENGINE=sqlite <binary>` must exit 0, with output containing `Engine: sqlite (from GOSBEE_ENGINE)`, otherwise it dies with `build-release: smoke test failed for <name>`. In CI the host binary is the native linux one. It writes `dist/SHA256SUMS` with `sha256sum`, falling back to `shasum -a 256` where `sha256sum` is absent so it runs on macOS. A bad tag dies with the usage line before any build.

- [ ] **Step 1: Write the failing cases**, placed last because each one cross-compiles:
  - `build_rejects_bad_tag`: `v1.0.0;id`. Exit 1, the usage line on stderr, no `dist/`.
  - `build_writes_four_binaries_and_sums`: from a `git worktree add --detach` of HEAD in the test's temp root, run with `v0.0.0-test`. Exit 0. `dist/` holds exactly the four binaries and `SHA256SUMS`. `(cd dist && sha256sum -c SHA256SUMS)` (or `shasum -a 256 -c`) passes. `SHA256SUMS` has 4 lines.
  - `build_accepts_prerelease_tag`: covered by the `v0.0.0-test` name above. Assert the binary name carries `v0.0.0-test`.
- [ ] **Step 2: Run** the tests. Expected: FAIL at `build_rejects_bad_tag`.
- [ ] **Step 3: Implement** `scripts/build-release.sh`.
- [ ] **Step 4: Run** the tests and shellcheck. Expected: every case `ok`. Then record the smoke check's power with `record-mutation.sh`: a patch that changes the expected string to `Engine: postgres` must make `build_writes_four_binaries_and_sums` fail.
- [ ] **Step 5: Commit:** `Added build-release.sh with a smoke test and checksums`

### Task 5: release.yml as ci, build and publish

**Files:**
- Modify: `.github/workflows/release.yml` (rewrite)

**Interfaces:**
- Consumes: `ci.yml` `inputs.ref` (Task 1), `scripts/verify-tag.sh` (Task 2), `scripts/publish-release.sh` (Task 3), `scripts/build-release.sh` (Task 4), `scripts/changelog.sh check` and `scripts/changelog.sh notes <tag> <owner/repo>` (existing).

- [ ] **Step 1: Rewrite release.yml**, modelled on caddy_shack's, with these exact differences and values:
  - Header comment rewritten for gosbee: notes come from the changelog, a published release is never changed (there is no notes sync any more), the publish job is the only writer, and a part-failed run is recovered by dispatching with the same tag.
  - `on:` is `push: tags: ['v*']` and `workflow_dispatch` with a required string input `tag`. There is no branch trigger.
  - Top-level `permissions: contents: read`. Concurrency `release-${{ inputs.tag || github.ref_name }}`, with `cancel-in-progress: false`.
  - `ci`: `uses: ./.github/workflows/ci.yml`, `with: ref: refs/tags/${{ inputs.tag || github.ref_name }}`, no `secrets:` and no `permissions:` block.
  - `build`: `needs: ci`, `permissions: contents: read`, `timeout-minutes: 20`, `env: TAG: ${{ inputs.tag || github.ref_name }}`. Steps in order:
    1. Check the tag name against the global pattern (in bash `[[ =~ ]]`) before checkout.
    2. Checkout with `ref: refs/tags/<tag>`, `fetch-depth: 0` and `persist-credentials: false`.
    3. Assert HEAD is `refs/tags/$TAG^{commit}` and that commit is an ancestor of `origin/main`.
    4. `scripts/verify-tag.sh "$TAG"` with `GH_TOKEN: ${{ github.token }}`.
    5. `scripts/changelog.sh check`.
    6. `scripts/changelog.sh notes "$TAG" "$GITHUB_REPOSITORY" > "$RUNNER_TEMP/notes.md"`, then `cat` it.
    7. setup-go with `go-version: stable` and `cache: false`.
    8. `go mod verify`.
    9. `scripts/build-release.sh "$TAG"`.
    10. Upload `dist/gosbee-repl_*` and `dist/SHA256SUMS` as artifact `dist`, and `${{ runner.temp }}/notes.md` as `release-notes`, each with `if-no-files-found: error` and `retention-days: 1`.
  - `publish`: `needs: build`, `timeout-minutes: 10`, `permissions` `contents: write`, `id-token: write` and `attestations: write`, each with caddy_shack's comment. Steps:
    1. Sparse checkout of `scripts/publish-release.sh` only (`sparse-checkout-cone-mode: false`) at `refs/tags/<tag>`.
    2. Download both artifacts.
    3. `cd dist && sha256sum -c SHA256SUMS`.
    4. `actions/attest-build-provenance` with `id: attest` and `subject-checksums: dist/SHA256SUMS`.
    5. `scripts/publish-release.sh "$TAG" notes/notes.md "$BUNDLE_PATH"`, with `GH_TOKEN`, `GH_REPO: ${{ github.repository }}` and `BUNDLE_PATH: ${{ steps.attest.outputs.bundle-path }}` in `env:`.
- [ ] **Step 2: Verify**
  - `actionlint` prints nothing.
  - zizmor reports no findings.
  - `grep -n 'sync-notes\|--clobber\|release upload' .github/workflows/release.yml` prints nothing.
  - `grep -c 'write' .github/workflows/release.yml` counts only the three publish permissions plus comment lines (read each match).
  - zizmor reports no `template-injection` finding, and a read of every `run:` body in release.yml finds no `${{`.
- [ ] **Step 3: Commit:** `Split the release into ci, build and publish jobs`

### Task 6: Gates, CI step and documentation

**Files:**
- Modify: `scripts/wombat-verify.sh:91` (the shellcheck list), plus a new gate
- Modify: `.github/workflows/ci.yml` (lint job)
- Modify: `CONTRIBUTING.md` ("Branch Rules" and "Releasing")
- Modify: `scripts/README.md`
- Modify: `CHANGELOG.md` (`[Unreleased]`)

- [ ] **Step 1: wombat-verify.sh.** Add `scripts/verify-tag.sh`, `scripts/publish-release.sh`, `scripts/build-release.sh` and `scripts/release-scripts_test.sh` to the shellcheck line. Add `run release-scripts bash scripts/release-scripts_test.sh` after `changelog`. Where `zizmor` is absent but `uvx` is present, run `uvx zizmor@1.31.1 --offline .github/workflows` instead of skipping (the unit 01 handover suggests it, and it is what proves this unit's zizmor criterion). An actionlint gate is left to unit 04 (ci-gate-and-lint).
- [ ] **Step 2: ci.yml lint job.** Add the step `Test the release scripts` running `bash scripts/release-scripts_test.sh`, after `Check CHANGELOG.md`.
- [ ] **Step 3: CONTRIBUTING.md.**
  - Branch Rules: coverage is uploaded by the separate `Coverage` workflow, which is not a required check.
  - Releasing step 3: CI runs first. The build job checks the tag is signed and verified by GitHub, is on `main` and has a changelog section. It builds and smoke tests the binaries and writes `SHA256SUMS`. The publish job attests them, attaches the Sigstore bundle as `gosbee-repl_<tag>_provenance.intoto.jsonl` and publishes through a draft.
  - Replace the "rewrites the notes" paragraph: a published release is never changed, so a changelog fix after release lives in `CHANGELOG.md` only. A part-failed run is recovered from the Actions tab by running `Release` with the tag. Tags made before this pipeline (v0.2.1 and earlier) cannot be re-run that way.
- [ ] **Step 4: scripts/README.md.** One entry per new script, in the file's existing format.
- [ ] **Step 5: CHANGELOG.md `[Unreleased]`.**
  - `### Changed`: releases run CI first, build with a read-only token, publish through a draft from a job that is the only one able to write, and attach the provenance bundle. Release notes are no longer rewritten after publication.
  - `### Added`: the provenance bundle asset. Then run `scripts/changelog.sh check`.
- [ ] **Step 6: Verify.** `WOMBAT_BASE_REF=workstream/repo-quality scripts/wombat-verify.sh`, run outside the agent sandbox. Read every gate line in `.claude/wombat-verification`: each is `pass`, and any `skipped` names its tool.
- [ ] **Step 7: Commit:** `Documented the release pipeline and gated its scripts`
