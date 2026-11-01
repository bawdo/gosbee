# Unit 09: repo-settings

Mode: plan
Close: auto
Branch: unit/09-repo-settings
Review lanes: security, correctness, brief

## Items

- Write `scripts/repo-settings.sh` with tests (reference: bawdo/caddy_shack
  `scripts/repo-settings.sh` and `repo-settings_test.sh`).
- Apply, with the operator's approval per phase: immutable releases, admin-only `v*`
  tag creation, `code_scanning` rule, extended CodeQL, strict status checks, Actions
  allowlist, topics, homepage, auto-merge.
- Decide whether the required check is renamed from `CI OK` to `CI result`.
- Update CONTRIBUTING.md and CHANGELOG.md.
- Continue from the plan into the build in this unit: write the script and apply `metadata` and
  `rulesets`; dry-run `harden` for unit 10 to apply.

## Acceptance criteria

- `scripts/repo-settings.sh` has phases at least `metadata`, `rulesets`, `harden` and
  `show`; `REPO` defaults to `bawdo/gosbee`. Every write sends the full desired state
  (PUT or PATCH; rulesets found by name and replaced in place), so any phase re-runs
  safely. Every write prints its method and path first. With `DRY_RUN` set, writes
  print their bodies and are not sent.
- A test script exercises the dry run output of every phase (stubbing `gh`), is shown
  to fail against a broken script (mutation), passes shellcheck, and runs from
  `scripts/wombat-verify.sh` and CI.
- `metadata`: description kept; homepage `https://pkg.go.dev/github.com/bawdo/gosbee`;
  topics `go, golang, sql, sql-builder, query-builder, ast, postgresql, mysql, sqlite,
  arel`; auto-merge allowed; squash merge only; delete branch on merge.
- `rulesets`:
  - main: deletion, non fast forward, required signatures, pull request (squash only,
    0 approvals, thread resolution), required status checks with strict policy on,
    and a `code_scanning` rule (CodeQL, alerts `errors`, security alerts
    `high_or_higher`). No bypass actors.
  - release tags `v*`: deletion, update, non fast forward blocked.
  - release tag creation `v*`: creation blocked, with only the repository admin role
    as a bypass actor.
- `harden`: Actions `allowed_actions: selected`, GitHub-owned allowed, verified
  creators not blanket allowed, and explicit patterns for exactly the third-party
  actions in use, computed from the `uses:` lines in `.github/workflows/` (print the
  list); SHA pinning required; default token read; fork PR approval for all external
  contributors; CodeQL default setup with the `extended` suite; immutable releases on.
- **Applying never breaks what is on `main` now.** Until the integration branch has
  merged, `main` still runs `re-actors/alls-green` and the old release workflow.
  Either the allowlist includes what `main` still uses until that merge, or the
  `harden` phase is applied after it; the choice is recorded under Decisions.
- Each phase is applied only after the operator approves its dry run output, and
  `show` output after applying is pasted in the Outcome.
- The check name decision is recorded; if renamed, `ci.yml`, the ruleset and
  CONTRIBUTING.md change together.
- CONTRIBUTING.md documents `scripts/repo-settings.sh`; CHANGELOG.md has an
  `[Unreleased]` entry.

## Decisions

- OPERATOR RULING 2026-11-01 (after unit 04 closed): this unit writes its plan and then continues
  into implement in the same unit. Renumbered from 08.
- OPERATOR RULING 2026-11-01: the required check keeps the name `CI OK`; no rename, so `ci.yml`,
  the ruleset and CONTRIBUTING need no change for it.
- OPERATOR RULING 2026-11-01: Codecov is not a required status check. Fork PRs cannot mint the
  OIDC token the upload uses, so a required Codecov status could never post and would block
  them; Codecov's own statuses already gate.
- OPERATOR RULING 2026-11-01, the allowlist criterion's choice: `harden` is applied after the
  integration branch merges to `main`, not with `re-actors/alls-green` allowlisted. This unit
  applies `metadata` and `rulesets` and shows `harden`'s dry run; `harden` itself is applied in
  unit 10, after the operator's merge, so the allowlist never names an action already removed.
- CORRECTION FROM UNIT 08: SECURITY.md now carries "Verifying a release" and describes only what
  `release.yml` ships. It names a tag ruleset, a publish `environment:` and private vulnerability
  reporting as this unit's, and omits immutable releases and `gh release verify`; CODE_OF_CONDUCT.md
  and the bug form route reports through private vulnerability reporting. Separately,
  `scripts/verify-tag.sh` accepts any tag signature GitHub verifies, while SECURITY.md tells users to
  accept only signing subkey `9954 8FCC ... EE78`, so CI is looser than the published procedure.

## Plan

## Outcome

## Not done, and why
