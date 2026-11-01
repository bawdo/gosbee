# Unit 10: release-proof

Mode: implement
Close: auto
Branch: unit/10-release-proof
Review lanes: security, correctness, brief

## Items

- Precondition: the operator has merged `workstream/repo-quality` into `main` through
  a pull request, so the new workflows run from `main`.
- Cut a release candidate through the new pipeline, operator pushing the tag.
- Verify it end to end with the commands in SECURITY.md, exactly as written.
- Record the first published Scorecard result.
- Fix whatever the proof shows is wrong, or record it.
- Before the release candidate, apply `scripts/repo-settings.sh harden` (from unit 09) on the
  operator's approval of its dry run, now that `main` no longer runs `re-actors/alls-green`.

## Acceptance criteria

- A signed annotated tag `vX.Y.Z-rc.1` (version chosen from CHANGELOG.md) on a `main`
  commit is pushed by the operator. Commands are handed over; the agent pushes
  nothing (remotes `local`).
- The Release run shows: CI passed first; the tag shape, on-main and signature checks
  passed; the build job held no write token; the release was created as a draft,
  assets uploaded, then published as a pre-release; the proxy and sum.golang.org
  checks passed.
- The release carries the four REPL binaries, the checksums file, the
  `.intoto.jsonl` bundle and the SBOM, and is marked immutable.
- Every verification command in SECURITY.md runs successfully against the rc as
  written. Any command that needed changing is fixed in SECURITY.md in this unit.
- Scorecard publishes a result for gosbee. The per-check results are pasted in the
  Outcome with the command that fetched them. Every check except Code-Review,
  Contributors, Branch-Protection and CII-Best-Practices scores 10, or the shortfall
  is explained under Not done with its cause. Signed-Releases may lag until enough
  releases carry provenance; say so if it does.
- The final closeout states the Scorecard result against the 2026-11-01 baseline
  (gosbee unscored; caddy_shack 8.3, jellyfish 8.0, go-blinkstick 7.6).

## Decisions

- OPERATOR RULING 2026-11-01 (after unit 04 closed): sequence is unit 09 applies `metadata` and
  `rulesets`, the operator merges `workstream/repo-quality` to `main` by PR, then this unit applies
  `harden` before cutting the rc. Renumbered from 09.
- 2026-11-01, precondition not met at open: `origin/main` at `ed10f4e` (#27), the integration
  branch 94 commits ahead and its remote copy 28 behind (at unit 06). The operator pushed it and
  opened PR #28. Branch cut only after that, off `8a5d877`.
- 2026-11-01, PR #28 is blocked by the `CodeQL` check: alert #5, high,
  `actions/cache-poisoning/poisonable-step` at `.github/workflows/ci.yml:92-96`, from the
  `ref: ${{ inputs.ref }}` checkouts, under `workflow_dispatch` and `schedule`. Fixed in this unit
  under the Item "fix whatever the proof shows is wrong", since #28 cannot merge without it.
- Diagnosis, read from github/codeql `actions/ql`: `CachePoisoningViaPoisonableStep.ql` looks at no
  cache step at all (and `setup-go` is not one of its `CacheWritingStep`s), so turning the Go cache
  off would not clear it. A first proposal to do that was withdrawn. The checkout counts as
  untrusted through `ActionsMutableRefCheckout`'s field-name heuristic (`.*(head|branch|ref).*` on a
  simple reference), which `inputs.ref` matches. The real gap behind it: a by-hand `release.yml`
  dispatch can name any existing tag, a non-`v*` one included, and `ci` runs that tag's code in
  main's context before `build` checks the tag name.
- OPERATOR RULING 2026-11-01: (b) move the tag-name check out of `build` into a read-only `tag` job
  that `ci` needs, and rename `ci.yml`'s `workflow_call` input `ref` to `tag` (the bare tag name;
  `ci.yml` builds `refs/tags/<tag>`). The Go cache stays on. Unprovable locally (no CodeQL CLI):
  proof is CodeQL on #28. Fallback if it still fires: dismiss #5 citing (b), operator's call.
- Caller census, `git grep -n -E 'inputs\.ref|inputs\.tag|ref: refs/tags|Check the tag name|workflow_call'`:
  `ci.yml` lines 12-18 (input), 28 and 35 (concurrency comment and group), 48, 140, 182, 227
  (checkouts), 168 and 174 (fuzz comment and `if`), 214 and 232 (gate comment and `EVENT`);
  `release.yml` 58, 66-72 (`ci` job), 88-99 (build's `TAG` and tag-name check), 106, 193, 271;
  `scripts/ci-result.sh` 23-24 (comment); `scripts/ci-result_test.sh` 226, 245, 248, 282 (exact
  strings). `scripts/repo-settings_test.sh`'s fake `ci.yml` uses no input and is left alone. The
  plans `2026-11-01-release-pipeline.md` and `2026-11-01-fuzzing.md` name `inputs.ref` and are left
  as written: they are closed units' plans, a record of what was planned then.
- 2026-11-01, PR #28 squash merged as `cf168eb` (squash is the only method: `metadata` and the
  `Protect main` ruleset both allow nothing else). Its tree equals `0b29ba9`'s (`eeab35d`). Joined
  to the unit branch with a signed merge of `origin/main`, adding no content, so later pull
  requests diff only new work. Done on the unit branch, not the integration branch, because the
  close-out carries it there.
- `harden` dry run from a detached worktree of `origin/main` at `cf168eb`, checked before
  approval: the only subpath action (`github/codeql-action/upload-sarif`) is GitHub owned; the
  composite actions' nested actions (`actions/github-script`, `github/codeql-action/upload-sarif`,
  `actions/attest`) are all GitHub owned and SHA pinned. `ossf/scorecard-action` runs a
  tag-pinned image (`ghcr.io/ossf/scorecard-action:v2.5.0`), which SHA pinning does not police.
  OPERATOR APPROVED; applied.
- Version: `v0.3.0-rc.1`. CHANGELOG.md's Unreleased holds Added, Changed and Fixed since v0.2.1,
  a minor bump on 0.x. `changelog.sh check` needs a section for every `v*` tag and the build job
  needs `changelog.sh notes <tag>`, so Unreleased becomes `[v0.3.0-rc.1] - 2026-11-01` through one
  more pull request to `main`. That pull request also fixes the docs this unit invalidated:
  CONTRIBUTING.md's release stages (the tag job, the tag input) and its, SECURITY.md's and
  CHANGELOG.md's wording of `harden` as still to come.
- 2026-11-01, PR #29 squash merged as `d4de27c` (tree `83c6586`, equal to `8fe9325`'s). The
  operator tagged `d4de27c` and pushed `v0.3.0-rc.1`; GitHub reported the `Protect release tag
  creation` admin bypass, which is that ruleset admitting its one bypass actor.
- OPERATOR RULING 2026-11-01 on the proof's findings: bump `golang.org/x/net` to v0.62.0 in
  `tools/go.mod`; record `GO-2026-5932` under Not done rather than suppress it in an
  `osv-scanner.toml`; add SECURITY.md notes on the gpg trust warning and on reading
  `gh attestation verify`'s exit status.

## Plan

## Outcome

- CodeQL alert #5 fix, `3fbca7e`: tests written first and seen to fail (`FAIL ci_yml_gate_shape:
  EVENT not passed through env`; `FAIL release_yml_tag_job_shape: no tag job in release.yml`).
  Mutations through `record-mutation.sh`, ledger outside the repository, all three KILLED:
  - `ci.yml` lint checkout back on `ref: ${{ inputs.ref }}`: first failing line
    `FAIL ci_yml_called_with_tag: ci.yml still reads inputs.ref`.
  - `release.yml` `ci` job `needs: tag` to `needs: []`: first failing line
    `FAIL release_yml_ci_needs_tag: ci does not need the tag job`.
  - `release.yml` tag regex with its `^` anchor dropped: first failing line
    `FAIL release_yml_tag_check_stops (x-v1.2.3): expected exit 1`.
- Verification at `3fbca7e`, `WOMBAT_BASE_REF=workstream/repo-quality scripts/wombat-verify.sh`, run
  outside the sandbox (the go build cache is not writable in it): tree clean, all 13 gates pass.
  actionlint clean; zizmor 1.31.1 `--offline` "No findings" (1 ignored, 3 suppressed).
- CodeQL on #28 at `0b29ba9`: `CodeQL` pass, no open alert on the PR head. Alert #5 cleared by the
  rename and the tag job; no dismissal.
- `harden` applied 2026-11-01 from `cf168eb`, all ten writes, exit 0. The query suite read
  `default` until GitHub's "CodeQL Setup" run finished (success), then `extended` (updated
  10:10:36Z). `scripts/repo-settings.sh show` after it:

  ```
  description: A Golang SQL Builder
  homepage: https://pkg.go.dev/github.com/bawdo/gosbee
  topics: arel ast go golang mysql postgresql query-builder sql sql-builder sqlite
  merge_methods: squash
  allow_auto_merge: true
  allow_update_branch: true
  delete_branch_on_merge: true
  has_discussions: true
  has_wiki: false
  has_projects: false
  rulesets: Protect main, Protect release tag creation, Protect release tags
  private_vulnerability_reporting: enabled
  allowed_actions: selected
  sha_pinning_required: true
  allowed_action_patterns: codecov/codecov-action@* golangci/golangci-lint-action@* ossf/scorecard-action@* zizmorcore/zizmor-action@*
  default_workflow_permissions: read
  fork_pr_approval: all_external_contributors
  code_scanning_default_setup: configured
  code_scanning_query_suite: default
  immutable_releases: enabled
  release_environment_policies: branch main, tag v*
  ```

  (`query_suite` re-read from `code-scanning/default-setup` after the setup run: `extended`.)
- Release run 41296868668 (push of `v0.3.0-rc.1`), success. Job order and times (UTC):
  `Check the tag name` 10:18:03-10:18:05; CI jobs from 10:18:05, `CI / CI OK` 10:19:25-10:19:29
  (Fuzz skipped, as ci-result.sh permits on `workflow_call`); `Build release` 10:19:31-10:22:27;
  `Publish release` 10:22:31-10:23:03; `Check the module proxy` 10:23:05-10:23:22.
  - Tag job token: `Metadata: read` only; `TAG: v0.3.0-rc.1`. Build token: `Contents: read`,
    `Metadata: read` (no write). Publish token: `Attestations: write`, `Contents: write`,
    `Metadata: read`.
  - Build steps all success, including "Check the tag is on main" and "Check the tag is signed"
    (`verify-tag: v0.3.0-rc.1 is annotated and its signature is verified by GitHub`).
  - Publish: `publish-release: v0.3.0-rc.1 can be published from tag object 9c95bf7...`;
    "Attestation created for 9 subjects" (attestations/62306225); the log names the draft
    (`releases/tag/untagged-9478bada8c52663e7e48`) and then `releases/tag/v0.3.0-rc.1`.
  - Proxy: `proxy.golang.org has github.com/bawdo/gosbee@v0.3.0-rc.1 at d4de27c...`, one 5s curl
    timeout retried, then `sum.golang.org records github.com/bawdo/gosbee@v0.3.0-rc.1`.
- The release, from `gh api repos/bawdo/gosbee/releases/tags/v0.3.0-rc.1`: `draft: false`,
  `prerelease: true`, `immutable: true`, published 10:22:59Z; latest is still `v0.2.1`. Eleven
  assets: the four `gosbee-repl_v0.3.0-rc.1_{darwin,linux}_{amd64,arm64}` binaries, `SHA256SUMS`,
  `gosbee-repl_v0.3.0-rc.1_provenance.intoto.jsonl`, and five CycloneDX SBOMs (module and one per
  binary).
- SECURITY.md's commands, run as written from a fresh clone with `<version>` = `0.3.0-rc.1`:
  - `curl -fsSL https://github.com/bawdo.gpg | gpg --import`: two keys imported, exit 0, into a
    temporary `GNUPGHOME` (a first attempt failed only because that path was too long for
    gpg-agent's socket). The served key carries signing subkey `99548FCC...292EEE78`.
  - `git fetch origin main tag v0.3.0-rc.1`, `git merge-base --is-ancestor v0.3.0-rc.1
    origin/main`, `git rev-parse 'v0.3.0-rc.1^{commit}'` (`d4de27c...`): all exit 0.
  - `git verify-tag v0.3.0-rc.1`: Good signature, subkey `99548FCC...292EEE78`. Run against the
    operator's keyring, since the git-guard hook takes only a plain git call; cross-checked with
    `gpg --verify` of the tag payload against the temporary keyring alone: Good signature, plus
    gpg's "not certified with a trusted signature" warning.
  - `gh attestation verify` online and with `--bundle`, for `darwin_arm64` and `linux_amd64`, and
    online for `module.cdx.json` and `darwin_arm64.cdx.json`: all exit 0. It prints nothing on
    success when not on a terminal; a control with a wrong `--source-digest` exits 1 ("expected
    SourceRepositoryDigest to be cf168eb..., got d4de27c...").
  - `shasum -a 256 -c SHA256SUMS --ignore-missing`: all nine listed files `OK`.
  - `gh release verify v0.3.0-rc.1` "verified", listing all eleven digests; `gh release
    verify-asset` for `darwin_arm64` and `linux_amd64`: "Verification succeeded".
  - `go mod download -json github.com/bawdo/gosbee@v0.3.0-rc.1`: the first try timed out reading
    the zip over IPv6; the retry exit 0, `Origin.Hash` `d4de27c...`, `Ref refs/tags/v0.3.0-rc.1`.
    `go env GOSUMDB GONOSUMDB GOPRIVATE`: `sum.golang.org`, empty, empty.
  - No command needed changing. Two notes added to SECURITY.md: the gpg trust warning is expected,
    and `gh attestation verify` is read by its exit status.
- Scorecard, first published result. Command:
  `curl -fsS https://api.securityscorecards.dev/projects/github.com/bawdo/gosbee`. Date
  2026-11-01T10:16:58Z, commit `d4de27c`, Scorecard v5.6.0, **aggregate 7.5** (baseline: unscored;
  caddy_shack 8.3, jellyfish 8.0, go-blinkstick 7.6). Per check:

  ```
  10  Dependency-Update-Tool  update tool detected
  10  Maintained              30 commit(s) and 0 issue activity found in the last 90 days -- score normalized to 10
   0  Code-Review             Found 0/9 approved changesets -- score normalized to 0
  10  Token-Permissions       GitHub workflow tokens follow principle of least privilege
  -1  Packaging               packaging workflow not detected
  10  Security-Policy         security policy file detected
  10  Binary-Artifacts        no binaries found in the repo
  10  Dangerous-Workflow      no dangerous workflow patterns detected
  10  Pinned-Dependencies     all dependencies are pinned
   2  CII-Best-Practices      badge detected: InProgress
  10  License                 license file detected
   7  Vulnerabilities         3 existing vulnerabilities detected
  10  Fuzzing                 project is fuzzed
   4  Branch-Protection       branch protection is not maximal on development and all release branches
   0  Signed-Releases         Project has not signed or included provenance with any releases.
  10  SAST                    SAST tool is run on all commits
   6  Contributors            project has 2 contributing companies or organizations -- score normalized to 6
   9  CI-Tests                11 out of 12 merged PRs checked by a CI test -- score normalized to 9
  ```

- `golang.org/x/net` v0.61.0 to v0.62.0 in `tools/go.mod` (`go -C tools get`, then `mod tidy`;
  only `x/net` moved), fixing `GO-2026-6860` and `GO-2026-6866`, both pulled in through
  cyclonedx-gomod's go-license-detector.

- Measured counts behind the figures above, run 2026-11-01 at close:

  ```
  $ grep -c -P "\tpass\t" .claude/wombat-verification
  13
  $ gh api repos/bawdo/gosbee/releases/tags/v0.3.0-rc.1 --jq ".assets | length"
  11
  $ shasum -a 256 -c SHA256SUMS --ignore-missing | grep -c ": OK$"
  9
  $ grep -c "^verdict  KILLED" ledger.md
  3
  ```

- Close mode as run: `auto`. Questions put to the operator, each with its cause and answer:
  - Commit grant at open: granted for the session.
  - The CodeQL fix's approach (cause: diagnosis before fix): first "both (a) and (b)", then, after
    the corrected diagnosis, "(b) plus the rename".
  - Squash versus merge commit for #28 (cause: squash is the only method): squash, as the
    settings require.
  - `harden`'s dry run (cause: a write to live settings): approved.
  - The proof's findings: bump `x/net`, record `GO-2026-5932` (option a), add the SECURITY.md notes.
  - The join of `d4de27c` stopped on a conflict in this brief (cause: merge conflict, aborted and
    handed over, not resolved): "proceed" with `git merge -s ours`, which kept this branch's tree,
    since `d4de27c`'s tree equals `8fe9325`'s and `8fe9325` is on this branch. Joined as `8c2e792`.
- Close-out step 1, verification at `8c2e792`, `WOMBAT_BASE_REF=workstream/repo-quality
  scripts/wombat-verify.sh` (outside the sandbox): tree clean; gates run, all pass, none skipped:
  fmt, vet, lint, tidy, changelog, release-scripts, repo-settings, vuln, zizmor, shellcheck,
  ci-result, fuzz-script, test. `select-gates.sh .`: lanes security, correctness, brief; no
  `unmatched` and no `floor` line. `work-item-tree.sh . list`: `none`.
- Step 2, `scan-secrets.sh .`: `scan clean: 15 files`.
- Step 3, `sweep-docs.sh . --base workstream/repo-quality`, run twice, with `--claim` set to the
  CONTRIBUTING.md sentence this unit falsified (CI "called with `refs/tags/<tag>`") and to
  `inputs.ref`. Each printed four candidates:
  - `CHANGELOG.md` touched, `SECURITY.md` touched: this unit's own edits, read back and left.
  - `CONTRIBUTING.md` claim (first run) and touched (second): the old sentence is gone; the claim
    matched the rewritten one, which keeps both fragments. Re-read: one line of the rewrite ran
    past the file's wrap width, rewrapped. No other change.
  - `README.md` named-file `ci.yml`: the CI badge's link to `actions/workflows/ci.yml`, which
    still exists. Dismissed.
  - No later unit exists, so no correction was written into another brief.
- Review lanes, three, from the selector: security, correctness, brief. None skipped.
- Step 6 triage. Six findings across the three lanes (two each), counted from their reports, two
  of them the same wording point. All low or cosmetic.
  - security: the tag regex admits names git refuses as refs (`v1.2.3-a..b`, `.lock`). Not a
    defect: such a ref cannot exist, so the ci checkout fails and nothing is injected. No change.
  - security: on a dispatch from `main`, ci runs a `v*` tag's code in main's cache scope before
    build checks the tag is signed and on `main`. Real, by design; recorded under Not done.
  - brief: CHANGELOG.md's tag-check entry ran to 130 characters. Fixed, rewrapped (`339392a`).
  - brief and correctness: that entry said the tag job runs "with no token"; it gets
    `Metadata: read`. Fixed to "no token scope" (`339392a`), as CONTRIBUTING.md words it. The
    published rc notes are immutable and keep the old wording.
  - correctness: `release_yml_tag_job_shape` did not refuse a job or step `if:` or
    `continue-on-error`, either of which lets a refused name reach ci. Fixed (`339392a`); two
    mutations through `record-mutation.sh`, both KILLED:
    - `continue-on-error: true` on the check step: first failing line
      `FAIL release_yml_tag_job_shape: tag job tolerates a failed check`.
    - `if: github.event_name == 'push'` on the tag job: first failing line
      `FAIL release_yml_tag_job_shape: tag job or its step has a condition`.
  - `knownTraps` is not configured, so no trap file is grown.

## Not done, and why

- Scorecard checks below 10 outside the brief's exempt four (Code-Review, Contributors,
  Branch-Protection, CII-Best-Practices):
  - Signed-Releases 0: the scan at `d4de27c` ran at 10:16:58Z, before the rc was published at
    10:22:59Z, so the only release it saw was v0.2.1, with no provenance. It moves on the next
    scan (the next push to `main`, or the weekly run), and lags after that, because v0.2.1 and
    earlier carry no provenance and Scorecard scores across recent releases.
  - Vulnerabilities 7: three OSV findings, all in `tools/go.mod` and none in the library's
    `go.mod`. `GO-2026-6860` and `GO-2026-6866` (`golang.org/x/net` below v0.62.0) are fixed in
    this unit, which shows on the scan after it reaches `main`. `GO-2026-5932` (the unmaintained
    `golang.org/x/crypto/openpgp` package) affects every x/crypto version and has no fixed
    release. It reaches the tools through cyclonedx-gomod, go-git and ProtonMail's go-crypto,
    which import only `x/crypto/hkdf`. Not suppressed with an `osv-scanner.toml`, on the
    operator's ruling, so it stays until cyclonedx-gomod drops the dependency.
  - CI-Tests 9: 11 of 12 merged pull requests were checked by CI. The twelfth predates this
    workstream; the ratio rises as pull requests land.
  - Packaging -1 (inconclusive, left out of the aggregate): Scorecard recognises known publishing
    actions, and the release publishes through `gh` in `scripts/publish-release.sh`.
- The release's cache-scope trust, from the security lane: on a `release.yml` dispatch from `main`,
  ci runs the named `v*` tag's code, with the Go cache on, in main's cache scope, before build
  checks the tag is signed and on `main`. So any `v*` tag, even an unsigned one at a commit off
  `main`, can write main's Go cache. What stands between that and a hostile write is that only an
  admin can create a `v*` tag (`Protect release tag creation`, bypass: admin role only). Release
  artefacts are not exposed: build compiles with `cache: false`. Closing it would mean moving the
  signature and on-`main` checks ahead of ci, or turning the cache off for called runs; neither
  was asked for, and it is left for the operator to schedule.
