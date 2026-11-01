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
  CHANGELOG.md:71, the fix entry itself, which is correct. Deleting the sketch is outside this
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

## Not done, and why
