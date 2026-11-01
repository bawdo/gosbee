# Release Supply Chain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every release attaches CycloneDX SBOMs that its provenance attestation covers, and the release workflow proves that proxy.golang.org serves the tagged commit and sum.golang.org records the version.

**Architecture:** `scripts/build-release.sh` (from the 02 plan) gains an SBOM step, using `cyclonedx-gomod` from `tools/go.mod`, between the smoke test and `SHA256SUMS`, so the SBOMs travel through 02's existing `dist` artifact, checksums and `subject-checksums` attestation unchanged. A new `proxy` job runs after `publish`. It has no checkout and runs `scripts/check-module-proxy.sh`, which reaches it as a build-job artifact.

**Tech Stack:** GitHub Actions, bash, curl, jq, Go toolchain, `github.com/CycloneDX/cyclonedx-gomod` v1.12.0.

**Spec:** `docs/workstreams/repo-quality/units/03-release-supply-chain.md` (Items, Acceptance criteria and Decisions). **Precondition:** every task of `docs/workstreams/repo-quality/plans/2026-11-01-release-pipeline.md` (unit 02) has landed. Before Task 1, check that `scripts/build-release.sh`, `scripts/publish-release.sh` and `scripts/release-scripts_test.sh` exist and that `release.yml` has `ci`, `build` and `publish` jobs. If not, stop: this plan does not apply to the single-job `release.yml`. Reference: go-blinkstick `.github/workflows/release.yml` `proxy` job at `89bfcec` (`/Users/bawdo/working/projects/blinkstick-playground/go-blinkstick`).

## Global Constraints

- Everything in the 02 plan's Global Constraints still holds: tag pattern `^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$`, actions pinned by full SHA with a `# vX.Y.Z` comment, `persist-credentials: false`, `timeout-minutes` on every job, untrusted values reach `run:` only through `env:`, the script conventions (`set -euo pipefail`, `die()` printing `<script>: <message>`, four-space indentation), and the commit rules.
- Module path: `github.com/bawdo/gosbee`. It is all lower case, so no proxy escaping is needed, and the script refuses a path with a capital letter rather than escaping it.
- SBOM tool: `cyclonedx-gomod` v1.12.0, through `tool` in `tools/go.mod`, run as `go -C tools tool cyclonedx-gomod`. No other install, no action, no download at run time.
- SBOM format: CycloneDX JSON, `-output-version 1.6`.
- SBOM assets: `gosbee-repl_<tag>_module.cdx.json` and `gosbee-repl_<tag>_<os>_<arch>.cdx.json` (one per binary). `<tag>` keeps its `v`.
- Proxy URLs, exactly: `https://proxy.golang.org/github.com/bawdo/gosbee/@v/<tag>.info` and `https://sum.golang.org/lookup/github.com/bawdo/gosbee@<tag>`.
- Retry: proxy up to 5 attempts, sumdb up to 2, 5 seconds between attempts, `curl -fsS --max-time 5` on every request. Worst case 60 seconds. A hash mismatch is never retried.
- `zizmor` reports no findings, `actionlint` reports nothing, `shellcheck` passes on every script touched, and every new script is on the shellcheck line in `scripts/wombat-verify.sh`.
- Every behaviour change goes through `superpowers:test-driven-development`: write the failing test and see it fail before the code. Workflow YAML is proved by actionlint, zizmor and the grep assertions each task names.
- Commits: past tense, 60 characters or fewer, gpg signed, no agent attribution, never on `main`. No push.

### Review lanes every task is held to

The close-out runs three lanes (`security`, `correctness`, `brief`) against the whole diff. Build each task so it answers them:

- **security:** what an attacker or a hostile input could do with this change: secrets, injection, authorisation, and anything it exposes that was not exposed before. Here that means a hostile tag or input reaching `curl` or a shell, a job gaining a token or a checkout it does not need, the proxy job running code nobody reviewed, and an SBOM or asset escaping the attestation.
- **correctness:** whether the code does what it claims under the cases it will actually meet, including the edges, the errors and the concurrent ones. Here that means a proxy that has not fetched the version yet, a moved tag, a dispatch re-run, a pre-release tag, malformed JSON, and a sumdb line that only resembles the version.
- **brief:** whether the change delivers what the brief specified, criterion by criterion, with each divergence recorded under the brief's Decisions.

## Review Focus

1. **A dispatch re-run checks the tagged commit, not the dispatching branch's.** The proxy job takes `needs.build.outputs.commit`. Pinned by Task 3's grep that `GITHUB_SHA` is absent from the proxy job.
2. **A tag moved after the proxy fetched it fails at once, naming both commits**, and is not retried, because the proxy never changes a version's commit. Pinned by `proxy_rejects_moved_tag` (Task 2).
3. **A brand-new version the proxy has not fetched yet is retried, and gives up within about a minute.** Pinned by `proxy_retries_then_succeeds`, `proxy_gives_up_after_five` and `proxy_time_is_bounded` (Task 2).
4. **A pre-release tag (`v0.3.0-rc.1`) gets SBOMs, sums and a proxy check like any other**, and a sumdb line for `v0.3.0-rc.1` never satisfies `v0.3.0`. Pinned by `build_writes_binaries_sboms_and_sums` with `v0.0.0-test`, `proxy_accepts_prerelease` and `sumdb_rejects_other_version` (Tasks 1 and 2).
5. **A failed SBOM step fails the build loudly, and never ships binaries without SBOMs or SBOMs missing from `SHA256SUMS`.** Pinned by `build_fails_when_sbom_fails` and the exact-set assertion in `build_writes_binaries_sboms_and_sums` (Task 1).

Accepted, not fixed: a `v2+` tag would need the module path `/v2`, and this check would report it as a proxy miss. gosbee is at v0.

## Notes for the builder

- **The 02 plan lists the provenance bundle twice.** Its Task 3 (line 101) compacts the bundle to `dist/gosbee-repl_<tag>_provenance.intoto.jsonl` and then passes `dist/gosbee-repl_<tag>_*` and `<bundle>` to `gh release create`, so the glob already includes the bundle. Whoever builds 02 should resolve this (write the bundle outside `dist/`, or drop the explicit argument) and record it. This plan's SBOM names sit inside that glob on purpose, and whatever the fix is, it must keep them in it.

---

### Task 1: SBOMs from build-release.sh

**Files:**
- Modify: `tools/go.mod`, `tools/go.sum`
- Modify: `scripts/build-release.sh`
- Modify: `scripts/release-scripts_test.sh`

**Interfaces:**
- Consumes: `scripts/build-release.sh <tag>` and the `build_*` cases from 02's Task 4.
- Produces: after a successful run, `dist/` holds exactly ten files: the four binaries, `gosbee-repl_<tag>_module.cdx.json`, the four `gosbee-repl_<tag>_<os>_<arch>.cdx.json`, and `SHA256SUMS` with nine lines, one per other file, sorted by name. An SBOM failure dies with `build-release: SBOM generation failed for <file name>` before `SHA256SUMS` is written.

- [ ] **Step 1: Add the tool.** `go -C tools get -tool github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@v1.12.0`, then `go -C tools mod tidy`. Check `go -C tools tool cyclonedx-gomod version` names v1.12.0, and run `go -C tools tool govulncheck -C .. ./...` (still clean). Build one binary (`CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o "$PWD/dist/probe" ./cmd/repl`), run `go -C tools tool cyclonedx-gomod bin -json -std -output-version 1.6 "$PWD/dist/probe"` against it, and read the output to learn the name it gives the standard library component for Step 2. Only `bin -std` emits that component, and `mod` never does. Paths are absolute because `go -C tools` runs from `tools/`. Delete the probe afterwards.
- [ ] **Step 2: Write the failing cases** in `release-scripts_test.sh`, alongside the other `build_*` cases (they cross-compile, so they stay last).
  - Rename `build_writes_four_binaries_and_sums` to `build_writes_binaries_sboms_and_sums`. With `v0.0.0-test`, `ls dist` equals exactly the ten names above. `SHA256SUMS` has 9 lines, each of the nine names appears in it once, and `sha256sum -c` (or `shasum -a 256 -c`) passes.
  - `build_module_sbom_describes_module`: with `jq` on `gosbee-repl_v0.0.0-test_module.cdx.json`, `.bomFormat == "CycloneDX"`, `.specVersion == "1.6"`, `.metadata.component.type == "library"` and `.metadata.component.purl` starts with `pkg:golang/github.com/bawdo/gosbee@`. `.components[].purl` contains `pkg:golang/github.com/jackc/pgx/v5@<v>` and `pkg:golang/modernc.org/sqlite@<v>`, with each `<v>` read at test time by `go list -m -f '{{.Version}}' <path>`. At least one component carries a `licenses` entry.
  - `build_binary_sbom_describes_binary`: on `gosbee-repl_v0.0.0-test_linux_amd64.cdx.json`, `.bomFormat == "CycloneDX"`, `.metadata.component.version == "v0.0.0-test"`, `.components[].purl` contains `pkg:golang/github.com/ergochat/readline@<v>` (from `go list -m`), and the standard library component found in Step 1 is present.
  - `build_fails_when_sbom_fails`: put a stub `go` first on `PATH` that exits 3 when its arguments contain `cyclonedx-gomod` and otherwise `exec`s the real `go` (resolved before `PATH` is changed). Expect exit 1, stderr containing `build-release: SBOM generation failed for gosbee-repl_v0.0.0-test_module.cdx.json`, and no `dist/SHA256SUMS`.
- [ ] **Step 3: Run** `bash scripts/release-scripts_test.sh`. Expected: FAIL at `build_writes_binaries_sboms_and_sums` (no SBOM files).
- [ ] **Step 4: Implement `write_sboms <tag>` in `scripts/build-release.sh`**, called after the smoke test and before the checksums. 02's script runs from the repository root, so define `ROOT="$(pwd -P)"` at its top if it has no such variable already. Paths are absolute, because `go -C tools` runs from `tools/`:
  - module: `go -C tools tool cyclonedx-gomod mod -json -licenses -type library -output-version 1.6 -output "$ROOT/dist/gosbee-repl_${tag}_module.cdx.json" "$ROOT"`
  - each binary: `go -C tools tool cyclonedx-gomod bin -json -std -version "$tag" -output-version 1.6 -output "$ROOT/dist/<bin>.cdx.json" "$ROOT/dist/<bin>"`
  - Each call that fails dies with the message in Interfaces. `SHA256SUMS` then covers every file in `dist/` except itself, so a new asset cannot be missed.
- [ ] **Step 5: Run** the tests and `shellcheck scripts/build-release.sh scripts/release-scripts_test.sh`. Expected: every case `ok`, no shellcheck output. Then record the exact-set assertion's power with `record-mutation.sh`: a patch that deletes the module SBOM call must make `build_writes_binaries_sboms_and_sums` fail.
- [ ] **Step 6: Commit:** `Added CycloneDX SBOMs to the release build`

### Task 2: scripts/check-module-proxy.sh

**Files:**
- Create: `scripts/check-module-proxy.sh` (executable)
- Modify: `scripts/release-scripts_test.sh`

**Interfaces:**
- Produces: `scripts/check-module-proxy.sh <module> <tag> <commit>`. It needs `curl` and `jq` and nothing else, and reads no repository file. Arguments are checked before any request: module `^[a-z0-9.-]+(/[a-z0-9._-]+)+$`, tag against the global pattern, commit `^[0-9a-f]{40}$`. Otherwise it dies with `check-module-proxy: usage: check-module-proxy.sh <module> vX.Y.Z[-suffix] <40-hex commit>`.
  - Exit 0 prints `check-module-proxy: proxy.golang.org has <module>@<tag> at <commit>` and then `check-module-proxy: sum.golang.org records <module>@<tag>`.
  - A failure prints exactly one of these on **stdout** and exits 1:
    - `::error::proxy.golang.org has no <module>@<tag> after 5 attempts`
    - `::error::proxy.golang.org returned unreadable JSON for <module>@<tag>`
    - `::error::proxy.golang.org has <module>@<tag> at <hash or "unknown">, the tag points at <commit>`
    - `::error::sum.golang.org has no record of <module>@<tag> after 2 attempts`
  - The sumdb check accepts a line only where field 1 is `<module>`, field 2 is `<tag>` and field 3 starts with `h1:` (exact field compare, never a regex built from the tag).

- [ ] **Step 1: Write the failing cases.** Add a fake `curl` to the harness: it logs its full argument list to `curl.log`, picks a response from a fixture directory by URL, and can be told to fail (exit 22) the first N times for a URL. Add a fake `sleep` that logs its argument and returns at once. Fixtures use module `github.com/bawdo/gosbee`, tag `v0.3.0` and a 40-hex commit `C`. The `.info` fixture is shaped like the live one: `{"Version":"v0.3.0","Time":"...","Origin":{"VCS":"git","URL":"https://github.com/bawdo/gosbee","Hash":"C","Ref":"refs/tags/v0.3.0"}}`. The sumdb fixture holds a record id line, `github.com/bawdo/gosbee v0.3.0 h1:...=`, `github.com/bawdo/gosbee v0.3.0/go.mod h1:...=`, and the tree lines. Cases:
  - `proxy_accepts_matching_commit`: exit 0, exactly the two stdout lines above, and `curl.log` holds exactly the two URLs, in order, each call carrying `-fsS` and `--max-time 5`.
  - `proxy_retries_then_succeeds`: the proxy URL fails twice. Exit 0, 3 proxy calls, and `sleep` logged `5` twice.
  - `proxy_gives_up_after_five`: the proxy URL always fails. Exit 1, stdout is the "after 5 attempts" error, 5 proxy calls, 0 sumdb calls.
  - `proxy_rejects_moved_tag`: `Hash` is another commit `D`. Exit 1, the error names `D` and `C`, 1 proxy call, 0 sumdb calls.
  - `proxy_rejects_missing_origin`: `.info` has no `Origin`. Exit 1, the error says `at unknown`.
  - `proxy_rejects_bad_json`: `.info` body is `not json`. Exit 1, the unreadable JSON error.
  - `sumdb_miss_fails`: the sumdb body has only the `v0.3.0/go.mod` line. Exit 1, the "after 2 attempts" error, 2 sumdb calls.
  - `sumdb_rejects_other_version`: tag `v0.3.0`, and the sumdb body has `v0.3.0-rc.1` lines only. Exit 1.
  - `proxy_accepts_prerelease`: tag `v0.3.0-rc.1` with matching fixtures. Exit 0.
  - `proxy_rejects_bad_args`: each of tag `v0.3.0;id`, tag `v0.3.0$(id)`, module `github.com/Bawdo/gosbee`, commit `abc123`. Exit 1, stderr the usage line, empty `curl.log`.
  - `proxy_time_is_bounded`: the proxy URL fails four times and returns a matching `.info` on its fifth call, and the sumdb URL always fails. The `sleep` arguments sum to `25` (4 proxy waits plus 1 sumdb wait), and the calls number 7. With `--max-time 5` on each, that is a worst case of 60 seconds.
- [ ] **Step 2: Run** `bash scripts/release-scripts_test.sh`. Expected: FAIL at `proxy_accepts_matching_commit` (no script).
- [ ] **Step 3: Implement** `scripts/check-module-proxy.sh`, with a header comment taken from go-blinkstick's: the proxy keeps the first commit it sees for a version for good, a mismatch means the tag moved and nothing can undo it, and the checksum database is what makes the version tamper evident. Use one helper `fetch_until <url> <attempts> <check>` for both endpoints. It retries, sleeping 5 between attempts, until curl succeeds **and** the check function passes on the body. For the proxy, the check is "the body is present" (so a hash mismatch, judged after `fetch_until` returns, is never retried). For sumdb, the check is "the exact record line is present", so a content miss is retried, as `sumdb_miss_fails` expects. `jq -r '.Origin.Hash // empty'` reads the hash, and a `jq` parse failure is the unreadable JSON error.
- [ ] **Step 4: Run** the tests and `shellcheck scripts/check-module-proxy.sh scripts/release-scripts_test.sh`. Expected: every case `ok`, no output from shellcheck.
- [ ] **Step 5: Commit:** `Added a module proxy and checksum database check`

### Task 3: The proxy job in release.yml

**Files:**
- Modify: `.github/workflows/release.yml`

**Interfaces:**
- Consumes: `scripts/check-module-proxy.sh <module> <tag> <commit>` (Task 2). From 02: the build job's step that asserts HEAD is `refs/tags/$TAG^{commit}`, and the `publish` job.
- Produces: build job output `commit`, the 40-hex tagged commit. Artifact `proxy-check` holding `check-module-proxy.sh`.

- [ ] **Step 1: Edit release.yml.**
  - Build job: give the HEAD assertion step `id: tagged`, and have it append `commit=$(git rev-parse HEAD)` to `$GITHUB_OUTPUT` after the assertion passes. Add `outputs: commit: ${{ steps.tagged.outputs.commit }}`. After the `dist` upload, upload `scripts/check-module-proxy.sh` as artifact `proxy-check` with `if-no-files-found: error` and `retention-days: 1`. The `dist` upload's `dist/gosbee-repl_*` path is unchanged, and it now carries the SBOMs.
  - New job `proxy`, named `Check the module proxy`: `needs: [build, publish]`, `runs-on: ubuntu-latest`, `timeout-minutes: 5`, `permissions: contents: read`, no checkout and no setup-go. Steps: download artifact `proxy-check` with `path: proxy-check`, then run `bash proxy-check/check-module-proxy.sh "$MODULE" "$TAG" "$COMMIT"` with `MODULE: github.com/bawdo/gosbee`, `TAG: ${{ inputs.tag || github.ref_name }}` and `COMMIT: ${{ needs.build.outputs.commit }}` in `env:`. It uses `bash` because an artifact does not keep the executable bit.
  - Comment above the job: it runs after publish, so a failure cannot stop the release. A mismatch means the tag moved after the proxy fetched it, which nothing undoes, so the version needs a `retract` and a new tag. It has no checkout, and the script comes from the build job's read-only checkout of the tag.
  - Update the header comment to mention the SBOMs and the proxy check.
- [ ] **Step 2: Verify**
  - `actionlint .github/workflows/release.yml` prints nothing.
  - `uvx zizmor@1.31.1 --offline .github/workflows` (or `zizmor`) reports no findings.
  - The proxy job's block, printed by `awk '/^  proxy:/{f=1;print;next} f&&/^  [a-z-]+:$/{exit} f' .github/workflows/release.yml` (a plain `/start/,/end/` range does not work here, because `  proxy:` also matches the end pattern and closes the range on its own line), runs to the job's last step and has no `actions/checkout`, no `setup-go`, no `GITHUB_SHA`, no `write` and no `${{` inside `run:`.
  - `grep -c 'proxy-check' .github/workflows/release.yml` is at least 3 (upload name, download name, run path), and each match reads right.
- [ ] **Step 3: Commit:** `Checked the module proxy after each release`

### Task 4: Gates and documentation

**Files:**
- Modify: `scripts/wombat-verify.sh` (the shellcheck line)
- Modify: `scripts/README.md`
- Modify: `CONTRIBUTING.md` ("Releasing", around line 443)
- Modify: `CHANGELOG.md` (`[Unreleased]`)

- [ ] **Step 1: wombat-verify.sh.** Add `scripts/check-module-proxy.sh` to the shellcheck line. The new tests already run through 02's `release-scripts` gate, so no gate is added.
- [ ] **Step 2: scripts/README.md.** Add an entry for `check-module-proxy.sh` in the file's existing format, and add the SBOMs to the `build-release.sh` entry.
- [ ] **Step 3: CONTRIBUTING.md Releasing.** The build job also writes a CycloneDX SBOM for the module and one per binary, listed in `SHA256SUMS` and so covered by the provenance attestation. After publish, a job checks that proxy.golang.org serves the tagged commit and sum.golang.org records the version, and says what a failure means (the tag moved: retract and tag again).
- [ ] **Step 4: CHANGELOG.md `[Unreleased]`, `### Added`:** each release attaches CycloneDX SBOMs, for the module and for each REPL binary, listed in `SHA256SUMS` and covered by the provenance attestation. The release workflow checks that proxy.golang.org serves the tagged commit and that sum.golang.org records the version. Then run `scripts/changelog.sh check`.
- [ ] **Step 5: Verify.** `WOMBAT_BASE_REF=workstream/repo-quality scripts/wombat-verify.sh`, run outside the agent sandbox. Read every gate line in `.claude/wombat-verification`: each one is `pass`, and any `skipped` names its tool. `zizmor` must be `pass`, not skipped (it runs through `uvx` after 02's Task 6).
- [ ] **Step 6: Commit:** `Documented the release SBOMs and proxy check`
