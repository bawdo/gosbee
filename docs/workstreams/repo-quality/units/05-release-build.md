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
  `/private/tmp/claude-501/u05-items.suryDu/`; their blocks are copied into `## Outcome`.
- Item [a] rulings (implementer's, accepted at merge):
  - Fix 2 is done by attaching exactly the files `dist/SHA256SUMS` lists, then `SHA256SUMS`, then
    the bundle. A glob also matches the bundle's own name, so the plan's command uploaded it twice.
    `publish-release.sh` refuses a malformed, duplicate, path-bearing, other-tag or missing entry.
  - The build tests run the working tree's script from a `git archive HEAD` copy rather than the
    plan's `git worktree add`, which builds the committed script, so an uncommitted mutation patch
    would always survive.
  - `-s -w` is checked with `go tool nm`: under `-trimpath`, Go leaves `-ldflags` out of the build
    info that `go version -m` reads.
  - zizmor's `self-repository` audit and actionlint disagree on the reusable call: actionlint rejects
    the `$/` form. Kept `./.github/workflows/ci.yml` with `# zizmor: ignore[self-repository]` and a
    comment giving the reason: a job-level reusable workflow resolves from the caller's commit.
    Revisit once actionlint accepts `$/`.
  - `coverage.yml` is force-added: the existing `.gitignore` rule `coverage.*` matches it.
  - The reusable `ci` job carries no `timeout-minutes` (GitHub forbids it there); every job inside
    `ci.yml` has one.
  - `scripts/README.md` is edited although outside the item's file list, as plan Task 6 asks.
  - Plain tags get `--latest`, as planned. For unit 10: a patch for an older line published after a
    newer release would also be marked latest.
- Item [b] rulings (implementer's, accepted at merge):
  - `cyclonedx-gomod bin` takes goos/goarch for every purl from the running `go env`, so the plan's
    command labelled each binary SBOM with the build host's platform. `build-release.sh` resolves the
    pinned tool with `go -C tools tool -n cyclonedx-gomod` and runs it under each binary's
    GOOS/GOARCH. It is the same pinned tool, with a test that each SBOM names its own target.
  - `cyclonedx-gomod mod` takes the version from a git tag at HEAD and picks `v0.3.0` over
    `v0.3.0-rc.1` on one commit. The build now dies unless the module SBOM's version is the tag.
  - Licences are asserted in `.evidence.licenses`, where the plan's command puts them.
  - `SHA256SUMS` keeps the `gosbee-repl_*` glob, and an exact-set assertion on `dist/` proves
    nothing else is present.
  - Module path check tightened beyond the plan's regex, which accepted `../x/y`. A proxy
    `Origin.Hash` that is not 40 hex is reported as `unknown`, so a hostile response cannot inject
    workflow commands into the log.
  - Sleep and curl are faked on PATH; production code is unchanged.
- The CI lint job gains the step `Test the release scripts` (`bash scripts/release-scripts_test.sh`)
  after `Check CHANGELOG.md`. This is a deliberate change to pull request and push behaviour, one
  more lint step, as 02 plan Task 6 Step 2 asks to be recorded here (raised by the brief lane).
- The local `release-scripts` gate in `scripts/wombat-verify.sh` is skipped, with a reason, where
  jq is missing, like the `ci-result` gate. CI runs the suite unconditionally, so the criterion
  holds there. Locally, a skip is visible in the record and is not a pass (raised by the brief lane).
- Review triage (step 6), decided in this session: the fixes go to a fresh fix subagent on
  `unit/05-release-build-fix`, test first, then merge into the unit branch. Fixed: the tag moving
  between build and publish; an attestation minted before the published-release refusal; every gh
  error read as "no release"; the SBOM tool running before the binaries are checksummed; the test
  power gaps; and the docs on proxy recovery and the dispatch trust model. Not acted on, with reasons
  in `## Outcome`. The `environment:` and tag ruleset that would bind `workflow_dispatch` are
  repository settings: they go to unit 09 (repo-settings) through the handover. That is not a
  re-cut, because unit 09's scope already covers repository settings.

## Plan

## Outcome

Close mode as run: `auto`. Questions that fired: none so far.

What landed: both plans in full, in plan order. Work item `[a]` (the 02 plan) merged at `dd10469`
and item `[b]` (the 03 plan) merged at `93810c1`, both signed, with verification green on every gate
after each merge, zizmor included. The 03 plan's precondition check passed before its Task 1: the
scripts were present, and the `ci`, `build` and `publish` jobs sat at lines 44, 56 and 142 of
`release.yml`. actionlint printed nothing at `93810c1`. Nothing was pushed or tagged, and nothing
ran remotely: unit 10 is the first real run.

```
$ git rev-list --count --no-merges dd10469^1..dd10469^2
6
$ git diff --name-only dd10469^1 dd10469 | wc -l
11
$ git rev-list --count --no-merges 93810c1^1..93810c1^2
4
$ git diff --name-only 93810c1^1 93810c1 | wc -l
9
$ bash scripts/release-scripts_test.sh 2>&1 | grep -c "^ok"
38
$ git diff --stat workstream/repo-quality...1f699ec -- ':!docs/workstreams' | tail -1
14 files changed, 2080 insertions(+), 102 deletions(-)
```

The suite count above is at `1f699ec`, after the review fixes. The diffstat is also taken there,
leaving out the workstream record, so the figure names its sha and the brief's own later edits do
not stale it. The brief lane found the first figure here lagging the tip.

Verification at step 1 (`c93f34f`): every gate ran and passed (fmt, vet, lint, tidy, changelog,
release-scripts, vuln, zizmor, shellcheck, ci-result, test). None was skipped, and `tree` read
clean. The run was unsandboxed, as Decisions says. The lane selector printed security,
correctness and brief, with no `unmatched` line. `scan-secrets.sh` printed `scan clean: 13 files`.

Sweep (`sweep-docs.sh . --base workstream/repo-quality`, no `--claim`: no single sentence was
falsified, since the docs the change made untrue were rewritten inside the items):

```
CHANGELOG.md	touched	CHANGELOG.md
CONTRIBUTING.md	touched	CONTRIBUTING.md
README.md	named-file	README.md
cmd/repl/README.md	named-file	README.md
plugins/README.md	named-file	README.md
plugins/opa/README.md	named-file	README.md
plugins/softdelete/README.md	named-file	README.md
scripts/README.md	touched	scripts/README.md
```

- Acted on: the three `touched` documents, which the items rewrote to describe the pipeline as
  built (CONTRIBUTING.md Branch Rules and Releasing, CHANGELOG.md `[Unreleased]`, and
  scripts/README.md for the four new scripts).
- Dismissed: the five `named-file` READMEs, which match only because `scripts/README.md`
  changed. None mentions release, coverage or a workflow file (`git grep` for `release`,
  `codecov`, `ci.yml`, `sync-notes` and SBOM). The root README's `CI` badge still resolves,
  because `ci.yml` keeps `name: CI`.
- Deleted: none. Corrections written into later briefs: none.

Mutation ledgers (`record-mutation.sh`, fail pattern anchored on the suite's `^FAIL <case>`
marker), as `patch: first failing line`:

```
$ grep -c '^verdict  KILLED' /private/tmp/claude-501/u05-items.suryDu/ledger-a.txt
6
$ grep -c '^verdict  SURVIVED' /private/tmp/claude-501/u05-items.suryDu/ledger-a.txt
0
$ grep -c '^verdict  KILLED' /private/tmp/claude-501/u05-items.suryDu/ledger-b.txt
16
$ grep -c '^verdict  SURVIVED' /private/tmp/claude-501/u05-items.suryDu/ledger-b.txt
1
```

- `[a]`, all KILLED:
  - `build-smoke-postgres`: `FAIL build_writes_four_binaries_and_sums: expected exit 0`
  - `build-no-strip`: `FAIL build_writes_four_binaries_and_sums: darwin_amd64 is not stripped`
  - `verify-tag-unanchored`: `FAIL verify_tag_rejects_bad_name (v1.0.0;id)`
  - `publish-bundle-twice`: `FAIL publish_creates_when_missing: wrong create line` (bundle twice)
  - `publish-prerelease-latest`: `FAIL publish_marks_prerelease`
  - `publish-overwrites-published`: `FAIL publish_stops_on_published_release: expected exit 1`
- `[b]`, every record KILLED except the one SURVIVED, then re-recorded:
  - `proxy-retries-mismatch`: `FAIL proxy_rejects_moved_tag: wrong output`
  - `proxy-six-attempts`: `FAIL proxy_gives_up_after_five: expected 5 proxy calls`
  - `sumdb-three-attempts`: `FAIL sumdb_miss_fails: expected 2 sumdb calls`
  - `sleep-four`: `FAIL proxy_retries_then_succeeds: expected two 5 second sleeps`
  - `moved-tag-undetected`: `FAIL proxy_rejects_moved_tag: expected exit 1`
  - `no-max-time`: `FAIL proxy_accepts_matching_commit: wrong curl calls`
  - `hash-unsanitised`: `FAIL proxy_rejects_hostile_hash: wrong output`
  - `dot-segment-allowed`: `FAIL proxy_rejects_bad_args (../x/y|...)`
  - `sumdb-substring`: SURVIVED under `^FAIL sumdb_rejects_other_version` (first `none`; the suite
    failed earlier, at `sumdb_miss_fails`), then KILLED re-recorded with the broader pattern:
    `FAIL sumdb_miss_fails: expected exit 1`
  - `sumdb-version-prefix`, `sumdb-any-module`: `FAIL sumdb_rejects_other_version`
  - `no-module-sbom`: `FAIL build_writes_binaries_sboms_and_sums: dist/ holds: ...` (no module SBOM)
  - `sboms-after-sums`: `FAIL build_writes_binaries_sboms_and_sums: expected nine checksum lines`
  - `sbom-host-target`: `FAIL build_writes_binaries_sboms_and_sums: the SBOM for darwin_amd64
    describes another target`
  - `no-version-check`: `FAIL build_refuses_module_sbom_of_other_version: expected exit 1`
  - `sbom-failure-ignored`: `FAIL build_fails_when_sbom_fails: no SBOM failure reported`

Review (step 5): 3 lanes, security, correctness and brief, each a fresh subagent in its own
detached worktree, given the diff and the brief. None was skipped, and the selector printed no
`unmatched` line.

Triage (step 6), finding counts read off each lane's report in this session: security 6
(2 medium, 2 low, 2 informational), correctness 4 plus one note, brief 5.

- Fixed, by a fresh fix subagent on `unit/05-release-build-fix`, merged at `1f699ec`:
  - security 1 (medium): a tag moved between build and publish chose the script publish ran, and
    the commit the release attached to. Publish now checks out `needs.build.outputs.commit`. Build
    exports the verified `tag_object`, and `publish-release.sh --check` refuses a moved tag before
    any write. The script re-reads the tag just before `gh release create`.
  - correctness 2: the attestation (a write and a Rekor entry) ran before the published-release
    refusal. The `--check` step runs first.
  - correctness 3: any `gh release view` error read as "no release". Only `release not found` now
    means missing; anything else dies with gh's message.
  - security 3 (low): the SBOM tool ran over the built binaries before they were checksummed. The
    binaries are now hashed first and re-hashed after the SBOMs, and the build dies on a change.
  - correctness 4: tests had less power than they looked. There is a new case for the tool run
    itself failing, and only nm's exact "no symbols" output counts as stripped.
  - correctness 1 and the documentation half of security 2: the recovery documented for a failed
    proxy check was untrue. It is rewritten in CONTRIBUTING.md and the release.yml header, which now
    also cover the dispatch trust model and that a dispatched run's provenance names the branch, not
    the tag. CONTRIBUTING's "cannot be moved" became "must never be", pending a tag ruleset. One
    CHANGELOG entry was reworded to match.
  - brief 5: scripts/README.md wrapped, with a Usage line for the test suite.
- Fixed in this session: brief 2 and brief 3 (decisions recorded above) and brief 4 (the figure
  above).
- brief 1 (no evidence of red runs): the implementers reported red-first runs, quoted in their hand
  backs: for example `FAIL verify_tag_accepts_valid_signed_tag ... exit: 127` before verify-tag.sh
  existed, `FAIL build_writes_binaries_sboms_and_sums` before the SBOM code, and
  `FAIL publish_stops_on_other_gh_error ... HTTP 502` before the fix for correctness 3. Those are
  relayed claims, not measurements of this session. Each task commit carries its test and its code
  together, so test-first order cannot be shown from history. What is measured is power: every
  ledger below, and every mutation in them was killed in the end.
- Not acted on: security 4 (the `ci` job takes the raw dispatch input: a `with:` value, prefixed
  `refs/tags/`, read-only, and git refuses bad refnames); security 5 (release notes are `cat` into
  the log, from reviewed main); security 6 (`v1.0.0-..` passes the regex, but git refuses it at
  checkout); the correctness note that a missing proxy `Origin` reads like a moved tag (a false
  alarm only, after publish). The repository-settings half of security 2 (an `environment:` on
  publish with a tag rule, and a tag ruleset) goes to unit 09 through the handover.
- Review fix commits and the fix mutation ledger:

```
$ git rev-list --count --no-merges 1f699ec^1..1f699ec^2
5
$ git diff --name-only 1f699ec^1 1f699ec | wc -l
7
$ grep -c '^verdict  KILLED' /private/tmp/claude-501/u05-items.suryDu/ledger-fix.txt
7
$ grep -c '^verdict  SURVIVED' /private/tmp/claude-501/u05-items.suryDu/ledger-fix.txt
0
```

  - `f1-no-recheck-before-create`: `FAIL publish_refuses_tag_moved_before_create: expected exit 1`
  - `f1-check-skips-tag`: `FAIL publish_check_accepts_missing_or_draft (missing): made other calls`
  - `f2-check-skips-published`: `FAIL publish_check_accepts_missing_or_draft (missing): made other
    calls: api repos/bawdo/gosbee/git/ref/tags/v0.1.0`
  - `f3-any-error-is-missing`: `FAIL publish_stops_on_other_gh_error: ... HTTP 502: Bad Gateway`
  - `f4-no-recheck-after-sboms`: `FAIL build_fails_when_sbom_alters_a_binary: expected exit 1`
  - `f5a-tool-run-status-ignored`: `FAIL build_fails_when_sbom_tool_run_fails: no SBOM failure
    reported`
  - `f5b-nm-other-error`: `FAIL build_writes_binaries_sboms_and_sums: go tool nm failed on
    linux_amd64`

Step 7 sweep, re-run because the fixes changed `publish-release.sh`'s arguments: the same eight
candidates as at step 3, with no `claim` line for the old usage string. `git grep` finds no caller
left on the old argument order.

## Not done, and why
