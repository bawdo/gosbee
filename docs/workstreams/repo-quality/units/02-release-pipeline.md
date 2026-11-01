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

- Skills: `skills.plan` (`superpowers:writing-plans`) and `skills.testFirst`
  (`superpowers:test-driven-development`) were configured and installed, and the plan was
  written with the first. `skills.implement` is not configured. No `knownTraps` is set, no
  `jira` block, no pre-flight checks (`preflight none`).
- Commit grant: given in conversation for this unit ("Yes, commit freely").
- **The plan lives in `docs/workstreams/repo-quality/plans/`** (operator's choice). The default
  `docs/plans` is gitignored (`.gitignore:56`), so a plan there could never be committed or
  reviewed. `.claude/wombat-workstream.json` now sets `planDir` to that path and `specDir` to
  `docs/workstreams/repo-quality/specs`, and the `docs/plans` ignore rule stays as it is.
- **Codecov moves to its own `coverage.yml`, so the release chain carries no `id-token`
  outside publish.** GitHub validates a called workflow's token permissions statically: a
  nested job that asks for more than the caller grants makes the whole run invalid ("The
  nested job ... is requesting 'id-token: write', but is only allowed 'id-token: none'"),
  and an `if:` on the job does not avoid it. ci.yml's test job asks for `id-token: write`
  for the Codecov OIDC upload, so calling ci.yml as-is would need the release's `ci` job to
  grant `id-token: write`, against the criterion that publish is the only such job. That
  would also let a compromised action in the lint or test jobs mint a Sigstore certificate
  for a tag ref. The operator asked for the most modern, acceptable and secure approach.
  OIDC is still Codecov's recommended token-free upload, and a separate workflow keeps it
  without a secret and without widening release.yml. The cost is one extra Linux test run
  per pull request and push. Rejected: granting `id-token` on the caller (wider token in
  release) and a `CODECOV_TOKEN` secret (a long-lived secret replacing a working OIDC
  setup). `workflow_run` was rejected too, because zizmor flags it as a dangerous trigger.
  Because the secret is gone, ci.yml's `workflow_call` declares no secrets.
- **ci.yml's `workflow_call` takes an optional `ref` input**, passed by release.yml as
  `refs/tags/<tag>` and used by every checkout in ci.yml (empty means the event's own ref,
  so pull request and push are unchanged). Without it a `workflow_dispatch` re-run tests the
  branch it was dispatched from, not the tag, and caddy_shack's `release.yml` has that
  defect. Checked in the source before copying: caddy_shack's verify-tag.sh and
  publish-release.sh accept only `vX.Y.Z`, but this brief allows a pre-release suffix, so
  both regexes widen to the brief's pattern. caddy_shack also always passes `--latest`, so
  the port picks `--prerelease` for a suffixed tag instead.
- **sync-notes is dropped** (operator's choice, 2026-11-01). Checked against GitHub's
  immutable releases docs: "You can still edit the title and release notes of a published
  immutable release", while assets and the tag are locked. So keeping it was allowed, and
  dropping it is policy rather than a constraint: a published release is never changed,
  notes included, and publish stays release.yml's only write job. release.yml loses its
  `push: branches: main, paths: CHANGELOG.md` trigger with it.
- Asset names stay as v0.2.x shipped them: `gosbee-repl_<tag>_<os>_<arch>` and `SHA256SUMS`,
  with the tag keeping its `v`. The bundle follows suit as
  `gosbee-repl_<tag>_provenance.intoto.jsonl`, reading the brief's `<version>` as the tag so
  existing download URLs keep their shape.
- Building moves into `scripts/build-release.sh` (caddy_shack's pattern), so the build, the
  smoke test and the checksums are testable locally and shellchecked. The smoke test pipes
  `exit` into the host binary with `GOSBEE_ENGINE=sqlite`, which I checked by hand to exit 0
  printing `Engine: sqlite (from GOSBEE_ENGINE)`.
- Exercise before unit 09: local only. The script tests use a fake `gh`, plus actionlint,
  zizmor (`uvx zizmor@1.31.1`), shellcheck and the verify gates. `act` was rejected because
  it cannot run OIDC, attestations or a faithful `workflow_call`. A fork dry run was
  rejected because a solo account cannot fork its own repository, and a scratch repository
  would need a push, which `remotes: local` forbids. Unit 09's first tag (an `-rc.1` is
  suggested) is the first real run.
- Caller census, run before planning:
  `git grep -n -E 'ci\.yml|release\.yml|sync-notes|SHA256SUMS|gosbee-repl_|Release. workflow|workflow_call|codecov|Codecov|coverage\.txt|wombat-verify\.sh|verify-tag|publish-release' -- ':!docs/workstreams' ':!docs/plans'`.
  Readers found: `.github/workflows/ci.yml` (OIDC permission, coverage args, upload step),
  `.github/workflows/release.yml` (asset names, SHA256SUMS, attest, create, sync-notes),
  `CONTRIBUTING.md:442-449` (Releasing), `scripts/README.md:63` (Release workflow),
  `scripts/wombat-verify.sh:91` (shellcheck list), `README.md:4` (codecov badge, still valid
  with the upload moved), `scripts/pre-ci-check.sh` (local coverage, independent of CI),
  `.claude/wombat-workstream.json` (`.codecov.yml` lane path, unaffected). The required check
  `CI OK` keeps its name.

## Plan

`docs/workstreams/repo-quality/plans/2026-11-01-release-pipeline.md`

## Outcome

Close mode as run: `auto`. The unit stopped at its plan, as a `plan` unit does, and the brief
does not say to continue into the build.

Landed: the plan at `docs/workstreams/repo-quality/plans/2026-11-01-release-pipeline.md` (six
tasks: callable CI with Codecov split out, verify-tag.sh with the test harness,
publish-release.sh, build-release.sh, the release.yml rewrite, then gates and docs). Also the
Decisions above, and `planDir`/`specDir` in `.claude/wombat-workstream.json`. No workflow,
script or documentation outside the workstream record changed, so nothing the plan describes
is built yet.

Exercise before unit 09, as decided: local only (fake `gh` script tests, actionlint, zizmor,
shellcheck, verify gates). This unit wrote no code, so none of it has run yet. It is the
implementing session's to do.

Questions put to the operator, with answers:
- Codecov OIDC versus "publish is the only job with id-token": "Research the most modern,
  acceptable, and secure approach." The research and the choice (a separate `coverage.yml`)
  are under Decisions.
- sync-notes: "Drop it (Recommended)".
- Commit grant: "Yes, commit freely".
- Plan location, raised because `docs/plans` is gitignored: "Set planDir in workstream
  (Recommended)".
- Signing failed once, inside the agent sandbox (`gpg: can't connect to the gpg-agent:
  Operation not permitted`). It was retried outside the sandbox and signed, with no key
  refresh needed.

Verification at step 1 (`WOMBAT_BASE_REF=workstream/repo-quality scripts/wombat-verify.sh`,
run outside the sandbox), record at `7a34731`:

```
wombat-verification 1
tree	clean
fmt	7a347319601966f0da96237305ebb1b98c5e49ee	pass	0	-
vet	7a347319601966f0da96237305ebb1b98c5e49ee	pass	0	-
lint	7a347319601966f0da96237305ebb1b98c5e49ee	pass	1	-
tidy	7a347319601966f0da96237305ebb1b98c5e49ee	pass	0	-
changelog	7a347319601966f0da96237305ebb1b98c5e49ee	pass	0	-
vuln	7a347319601966f0da96237305ebb1b98c5e49ee	pass	3	-
zizmor	7a347319601966f0da96237305ebb1b98c5e49ee	skipped	0	zizmor not installed (CI runs it)
shellcheck	7a347319601966f0da96237305ebb1b98c5e49ee	pass	0	-
test	7a347319601966f0da96237305ebb1b98c5e49ee	pass	2	-
```

Lanes (`select-gates.sh .`): 1 lane ran, `brief`. `security` and `correctness` were skipped
with "no changed path matched", because this diff touches only the workstream record and
`.claude/wombat-workstream.json`.

Scan (`scan-secrets.sh .`): `scan clean: 11 files`.

Sweep (`sweep-docs.sh . --base workstream/repo-quality`): printed nothing, so 0 candidates. No
claim was passed, because this unit changed no documented behaviour. A manual
`git grep -n "docs/plans\|docs/specs"` found only this brief and
`nodes/rawsql_security_test.go:32`, a pre-existing citation of a local plan that this unit
did not touch. No later brief was corrected.

For unit 03 (release-supply-chain): the release chain grants `id-token` only in publish, and
the verification docs should pin `--signer-workflow` to `release.yml`. For unit 04: ci.yml
gains a `workflow_call` with a `ref` input, a `ci-` concurrency prefix and a release script
test step, and Codecov lives in `coverage.yml`. An actionlint gate is left to unit 04. For
unit 08: `Coverage` is a new workflow but not a required check. For unit 09: the first real
run of the pipeline is the first tag, and an `-rc.1` is suggested.

## Not done, and why

- The build itself: this is a `plan` unit and its brief does not say to continue. The plan
  needs an implementing session. The index has no implement unit for it yet, and that is the
  operator's call at the hand back.
