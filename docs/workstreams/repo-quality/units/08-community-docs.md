# Unit 08: community-docs

Mode: implement
Close: auto
Branch: unit/08-community-docs
Review lanes: security, correctness, brief

## Items

- Add a "Verifying a release" section to SECURITY.md (reference: bawdo/caddy_shack).
- Add CODE_OF_CONDUCT.md.
- Add issue forms and a PR template.
- Add CODEOWNERS.
- Update README.md badges, and add a tracked note for the Best Practices badge.
- Refresh CONTRIBUTING.md and update CHANGELOG.md.

## Acceptance criteria

- SECURITY.md "Verifying a release" matches what units 02 and 03 actually ship (read
  `release.yml` as it stands, not this brief):
  - importing the maintainer's key (`https://github.com/bawdo.gpg`) and the primary
    key fingerprint and signing subkey, read from the real key, not copied;
  - `git verify-tag`, `git merge-base --is-ancestor <tag> origin/main`,
    `git rev-parse '<tag>^{commit}'`;
  - `gh attestation verify` for a REPL binary with `--repo`, `--signer-workflow`,
    `--source-ref refs/tags/<tag>`, `--source-digest`, `--deny-self-hosted-runners`,
    and the `--bundle` variant using the attached `.intoto.jsonl`;
  - checksum verification on Linux (`sha256sum`) and macOS (`shasum -a 256`);
  - verifying the module itself (`go mod download` and sum.golang.org);
  - where the SBOM is and what it does and does not prove;
  - a sentence that an attestation is provenance, not a security guarantee.
- CODE_OF_CONDUCT.md is Contributor Covenant 2.1, with enforcement contact through
  GitHub private reporting or another private channel (no personal email committed
  unless the operator says so).
- `.github/ISSUE_TEMPLATE/bug_report.yml` and `feature_request.yml` are issue forms;
  `config.yml` sets `blank_issues_enabled: false` and links security reports to
  `https://github.com/bawdo/gosbee/security/advisories/new`.
- `.github/PULL_REQUEST_TEMPLATE.md` has a short "What changed" and a checklist:
  `make pre-ci` passes, CHANGELOG.md `[Unreleased]` updated or not user-visible.
- `.github/CODEOWNERS` is `* @bawdo`.
- README.md: the CI badge uses
  `https://github.com/bawdo/gosbee/actions/workflows/ci.yml/badge.svg`; a Licence
  badge is added; the Scorecard badge from unit 01 stays.
- The OpenSSF Best Practices badge is not added (no project id exists yet). A note in
  CONTRIBUTING.md (or a tracked TODO the operator approves) says where it goes once the
  survey is done.
- CONTRIBUTING.md links CODE_OF_CONDUCT.md instead of the one line section, and every
  fact in it (check names, release steps, tooling, `docs/design/` path) matches the
  tree. The `scripts/sweep-docs.sh` candidates are judged and recorded in the Outcome.
- CHANGELOG.md has an `[Unreleased]` entry.

## Decisions

- CORRECTION FROM UNIT 07: `examples/before_after_example.go` (a commented-out sketch, never
  compiled) shows API that no longer exists (`sm.UsePlugin`, `plugins.NewSoftDeletePlugin`, a
  two-value `ToSQL`). Unit 07 added compiled `Example*` functions in `managers`, `visitors`,
  `plugins/softdelete` and `plugins/opa` and left the sketch untouched as outside its items.
  Unit 07 also changed `UPDATE ... SET` output (targets bare, another table's column refused with
  `managers.ErrSetTargetTable`).
- Correction from 07, handled at open: no markdown file links `examples/before_after_example.go`
  (`grep -rn "examples/\|before_after" . --include='*.md'`, workstreams excluded: no hits), and
  the only table-qualified `SET` in the docs (`grep -rn 'SET "[a-z_]*"\."' . --include='*.md'`) is
  the CHANGELOG fix entry itself (line 71 at open, line 81 after this unit's entries), which is correct. Deleting the sketch is outside this
  unit's Items; asked the operator at open, no answer given, so it is left (see Not done).
- Key facts read from the real key, not copied from caddy_shack: `https://github.com/bawdo.gpg`
  fetched and read with `gpg --show-keys` in a throwaway GNUPGHOME. It serves two primary keys
  (ed25519 `8DB1 63FD ... B39F` and rsa2048 `661B 030E ... 82D1`). The release key is settled by
  the v0.2.1 tag's signature packet (`gpg --list-packets`): issuer fpr
  `99548FCCB94BE84541E573AA48234C2C292EEE78`, the [S] subkey of `661B...82D1`, which also matches
  `git config user.signingkey`. SECURITY.md names that primary and subkey and says the URL serves
  more than one key.
- SECURITY.md describes only what `release.yml` ships today. caddy_shack's paragraphs on a tag
  ruleset, immutable releases and `gh release verify` are left out: those are repository settings
  owned by unit 09 and do not exist yet. Releases up to v0.2.1 are named as predating the pipeline.
- The module section names `GOSUMDB=off`, `GONOSUMDB` and `GOPRIVATE`, checked against
  `go help environment` (go1.27.3); a first draft named `GONOSUMCHECK` and `GOFLAGS=-insecure`,
  which are wrong, and was corrected before commit. `Origin.Hash` is what
  `scripts/check-module-proxy.sh` reads from the proxy's `.info`.
- Dispatched-run caveat carried into SECURITY.md from CONTRIBUTING (Releasing): `--source-ref`
  fails for a by-hand run's assets.
- CODE_OF_CONDUCT.md is built from upstream, not caddy_shack's copy: fetched
  `EthicalSource/contributor_covenant` `release/content/version/2/1/code_of_conduct.md`, stripped
  its front matter, and diffed the body against caddy_shack's (identical but for the contact line).
  caddy_shack's contact, "a private report to @bawdo on GitHub", names no real channel (GitHub has
  no direct messages), so gosbee's points at the repository's private reporting form, with a line
  asking the reporter to title it a conduct report. Upstream's American spelling is kept: it is a
  quoted standard text.
- Issue forms are written for a library, not copied: the bug form asks for a minimal program, the
  generated and expected SQL, dialect and plugins (both multi-select), gosbee and Go versions, and
  sends security problems to SECURITY.md. YAML checked with Ruby's parser.
- The "~1096 tests" in README and CONTRIBUTING is stale. Measured with
  `git ls-files '*_test.go' | xargs grep -hE '^func (Test|Example|Fuzz|Benchmark)[A-Za-z0-9_]*\('`:
  1330 Test, 14 Example, 9 Fuzz, 6 Benchmark. The hand-kept figure is removed rather than replaced,
  since nothing keeps it current; README now describes coverage in words.
- CONTRIBUTING facts corrected against the tree: `docs/design/` does not exist (section replaced by
  `docs/plans/` and `docs/workstreams/`); the `Visitor` interface is in `nodes/node.go`, not
  `nodes/visitor.go`; `baseVisitor` is in `visitors/visitor.go`, `DotVisitor` in
  `visitors/dot_visitor.go`, the only `stubVisitor` in `nodes/nodes_test.go`; `go mod tidy` at
  Getting Started replaced by `make deps` (CI fails on a tidy diff, so tidy is not a setup step);
  the workflow's numbering (1 to 6, then 5, 6 again) is renumbered 1 to 9 and uses the Makefile
  targets (each checked against the Makefile); `feature/` branch example dropped; a CHANGELOG step
  and signed commits added; `internal/`, `scripts/` and `tools/` described as they are. The
  8-check pre-CI table was checked against `scripts/pre-ci-check.sh` (eight `check_*` functions).
- Best Practices badge note lives in a new CONTRIBUTING "Badges" section, per the criterion, with
  the badge's form and where it goes; no badge added.
- Em dashes: the two README lines added beside existing em-dash list items use colons instead, per
  the operator's writing rules. Existing em dashes in untouched lines are left alone (out of scope).

## Plan

## Outcome

Close mode as run: auto. Commit grant given in conversation at open, for the session.

Landed, all six Items:

- SECURITY.md "Verifying a release": signed tag on `main` (`git verify-tag`,
  `git merge-base --is-ancestor`, `git rev-parse '<tag>^{commit}'`), the key (`bawdo.gpg`, primary
  `661B...82D1`, signing subkey `9954...EE78`, read from the real key and the v0.2.1 signature),
  `gh attestation verify` with `--repo`, `--signer-workflow`, `--source-ref`, `--source-digest`,
  `--deny-self-hosted-runners` and the `--bundle` variant, `sha256sum` / `shasum -a 256`, the SBOMs
  and what they do not prove, the Go module via `go mod download` and sum.golang.org, and
  "an attestation is provenance, not a security guarantee".
- CODE_OF_CONDUCT.md: Contributor Covenant 2.1 from upstream, contact through private reporting.
- `.github/ISSUE_TEMPLATE/{bug_report,feature_request,config}.yml`, `.github/PULL_REQUEST_TEMPLATE.md`,
  `.github/CODEOWNERS` (`* @bawdo`).
- README: CI badge on `actions/workflows/ci.yml/badge.svg`, licence badge, Scorecard badge kept,
  CoC and security policy linked, stale test count removed.
- CONTRIBUTING: CoC linked, facts corrected (see Decisions), Best Practices badge note in "Badges".
- CHANGELOG `[Unreleased]` entries; `scripts/changelog.sh check` printed
  `CHANGELOG.md is consistent (3 versions)`.

Verification at step 1 (`WOMBAT_BASE_REF=workstream/repo-quality scripts/wombat-verify.sh`, in the
agent sandbox), record at 137fb46, tree clean:

```
fmt pass | vet fail | lint fail | tidy fail | changelog pass | release-scripts fail | vuln fail
zizmor fail | shellcheck pass | ci-result pass | fuzz-script pass | test fail
```

Every fail is the sandbox, not the diff: Go's build cache under `~/Library/Caches/go-build` is not
writable ("operation not permitted"), uv's cache for zizmor is not writable, and vuln.go.dev is not
on the network allowlist; the OPA tests also need loopback binding. This unit changes no Go, no
script and no workflow. The green record that authorises the merge has to come from the operator's
shell at step 7, as for unit 07. No gate was skipped: the script runs every gate every time.

Lanes: 3, selected by `select-gates.sh`: security, correctness, brief. None skipped, no
`unmatched` path.

Secrets scan (`scan-secrets.sh .`): `scan clean: 14 files`.

Sweep (`sweep-docs.sh . --base workstream/repo-quality --claim '~1096 tests'`): 6 candidates, all
`touched` (the PR template, CHANGELOG, CODE_OF_CONDUCT, CONTRIBUTING, README, SECURITY), each
written by this unit and read in full here; none rewritten further, none deleted. The claim matched
nowhere else, and `grep -rn "nodes/visitor.go\|docs/design" . --include='*.md'` (workstreams
excluded) found nothing. No correction written into a later brief: unit 09 (repo-settings) already
owns the tag ruleset, environment and private reporting, which SECURITY.md and CONTRIBUTING name
as unit 09's, unchanged in meaning.

Next unit inherits: unit 09 should know SECURITY.md deliberately omits immutable releases, a tag
ruleset and `gh release verify`; once 09 turns those on, SECURITY.md's "What an attestation proves"
gains the server-side controls (caddy_shack's wording is the reference). CODE_OF_CONDUCT.md and
the bug form both rely on private vulnerability reporting staying enabled.

Review triage (step 6). Three fresh lanes, each in its own detached worktree. Findings counted
from their reports: brief 2, correctness 5, security 6, with brief 1 and correctness 1 the same
defect. Every one was checked on the tree before acting, and all were judged real and fixed:

- Brief 1 / correctness 1: `docs/plans/` is gitignored (`.gitignore:56 /docs/plans`;
  `git ls-files docs` lists development, guide and workstreams only). Removed from the CONTRIBUTING
  tree and heading; plans are described under `docs/workstreams/`.
- Brief 2: the licence badge rendered shields' own "license" label; `?label=licence` added.
- Correctness 2: the node-type checklist named one stub. A grep for `VisitTable(` implementations
  found six; all six are now listed, plus `go build ./... && go vet ./...` to catch a missed one.
- Correctness 3: the "CI runs more than this" summary omitted `go mod tidy -diff` and the test
  matrix; added.
- Correctness 4: CODEOWNERS does not request the author's own review; reworded to "opened by
  someone else".
- Correctness 5: the Decisions line reference CHANGELOG.md:71 moved to 81 after this unit's entries
  (`grep -n` printed `81:`); corrected.
- Security 1: a dispatched run fails `--source-digest` as well as `--source-ref`, and
  `--signer-workflow` alone binds no source; reworded.
- Security 2: `SHA256SUMS` is unsigned and unattested, now said so; macOS `/sbin/sha256sum`
  exits 0 with no file matched (reproduced: `darwin sha256sum exit=0`, while `shasum` printed
  `no file was verified` and exited 1), so macOS readers are told to use `shasum` and to check
  each file is reported `OK`.
- Security 3: gh's own caveat for `--bundle` (cannot detect a renamed or re-registered repository)
  added.
- Security 4: `scripts/verify-tag.sh` accepts any signature GitHub verifies (read at lines 33-38),
  so the doc's pinned subkey is stricter than CI. Left strict (fails closed) and added a rotation
  path (a new key is named in SECURITY.md on `main` first) and an out-of-band cross-check line.
  CI's looser check is not changed: a script change is outside this unit's Items.
- Security 5: `git fetch origin tag` left `origin/main` stale; now `git fetch origin main tag`.
- Security 6: the checksum-database bypass check now says how (`go env GOSUMDB GONOSUMDB GOPRIVATE`,
  which shows `go env -w` values) and that `GOSUMDB` should be `sum.golang.org`.

No `knownTraps` key is configured, so no trap file is grown.

## Not done, and why

- `examples/before_after_example.go`, the stale commented-out sketch named in unit 07's correction,
  is left in the tree. Deleting it is outside this unit's Items; the operator was asked at open and
  has not ruled. It is not linked from any document. Operator to schedule.
