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

## Plan

## Outcome

## Not done, and why
