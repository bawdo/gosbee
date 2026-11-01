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
- OPERATOR RULING 2026-11-01, on the unit 08 correction: `harden` also sends private
  vulnerability reporting (already on; the PUT keeps it on) and the publish environment. Both are
  written and dry-run here and applied with the rest of `harden` in unit 10.
- OPERATOR RULING 2026-11-01: SECURITY.md is updated in this unit to cover immutable releases and
  `gh release verify`. Tightening `scripts/verify-tag.sh` to the published subkey stays open for
  the operator to schedule.
- Live state read 2026-11-01 (GET only): rulesets "Protect main" (13059268; requires `CI OK` and
  `Analyze (go)`, strict off, squash only) and "Protect release tags" (25916143; deletion, update,
  non fast forward) exist; no branch protection; CodeQL default setup on, `default` suite,
  languages actions and go; private vulnerability reporting on; Actions `allowed_actions: all`
  with SHA pinning on; fork approval `all_external_contributors`; default token read; immutable
  releases off; no environments; topics empty; homepage null; auto-merge off;
  `allow_update_branch` off; `has_discussions` on.
- Ruleset names: the script keeps the existing names "Protect main" and "Protect release tags" so
  both are replaced in place rather than duplicated, and adds "Protect release tag creation".
  Caddy_shack's names (`main`, `release-tags`) would have created two more rulesets beside them.
- Required status checks stay `CI OK` and `Analyze (go)`, both from integration 15368 (GitHub
  Actions). Caddy_shack dropped its CodeQL check for the `code_scanning` rule; keeping `Analyze (go)`
  is the current state and costs nothing, so applying the ruleset removes no check `main` has now.
- `metadata` also sets `allow_update_branch: true`: the strict status check policy makes a pull
  request behind `main` unmergeable, and the button is how a solo maintainer brings it up to date.
  `has_discussions`, `has_wiki` and `has_projects` are sent at their current values (true, false,
  false), so the phase's PATCH states the whole of what it owns and changes nothing it does not.
  The description is sent as its current value, `A Golang SQL Builder`.
- Publish environment `release`: deployment policy custom, admitting tag `v*` **and branch
  `main`**. A tag-only rule would refuse the documented recovery path (dispatching `Release` from
  `main` with an existing tag, where `github.ref` is `refs/heads/main`); admitting `main` alone
  still refuses a dispatch from any other branch, which is the hole CONTRIBUTING describes. No
  required reviewers: solo maintainer. `release.yml`'s publish job names the environment, which
  CONTRIBUTING says this unit adds.
- The live "Protect main" pull request rule carries
  `require_extra_approval_for_unattributed_changes: true`, which the caddy_shack body omits; a PUT
  without it would likely reset it. Found comparing the first `rulesets` dry run with the live
  ruleset. The script sends it as `true`, its current value, and the test pins it.
- Census for `release.yml`'s `environment:`: `grep -rn 'environment' .github scripts
  CONTRIBUTING.md SECURITY.md README.md` found only CONTRIBUTING.md:588-590 ("The workflow names no
  environment yet"); no script parses the publish job.
- Caller census before changing shared contracts: `grep -rn "environment\|repo-settings\|Analyze (go)\|Protect main" --include='*.md' --include='*.yml' --include='*.sh' . | grep -v '^./docs/workstreams/repo-quality/units/0[1-8]'`
  is run at the step that edits each file and its hits recorded in `## Plan`.

## Plan

`docs/workstreams/repo-quality/plans/2026-11-01-repo-settings.md`, written with
`superpowers:writing-plans` (`skills.plan`); built in this session with
`superpowers:test-driven-development` (`skills.testFirst`); `skills.implement` is not configured.
Items carry no ids, so `work-items.sh` was not needed: the unit ran as a legacy list.

## Outcome

Close mode as run: `auto`. Questions that fired, with answers:

- Scope (unit 08 correction): private vulnerability reporting and the publish environment.
  Operator: add both to `harden`.
- SECURITY.md wording and `verify-tag.sh`. Operator: SECURITY.md only.
- Commit grant. Operator: yes, for this unit.
- `metadata` dry run. Operator: apply. `rulesets` dry run. Operator: apply.

Built: `scripts/repo-settings.sh` (phases `metadata`, `rulesets`, `harden`, `show`) and
`scripts/repo-settings_test.sh`, 17 cases, all `ok` (counted from the run's `^ok ` lines),
shellcheck clean; the test runs in CI (`Lint and audit`) and as the `repo-settings` gate in
`scripts/wombat-verify.sh`. `release.yml`'s publish job names `environment: release`. zizmor
1.31.1 offline: no findings (run in the sandbox with uv's cache, tool and Python dirs under
`$TMPDIR`). CONTRIBUTING (Branch Rules, new Repository Settings, Releasing), SECURITY.md,
scripts/README.md and CHANGELOG `[Unreleased]` updated.

Mutations, through `record-mutation.sh` with `--fail-pattern '^FAIL '`, all four KILLED:

- `DRY_RUN` check never true: first failing line `FAIL dry_run_sends_nothing_for_every_phase: sent a write`.
- `Protect main` strict policy false: `FAIL rulesets_creates_when_absent: wrong main ruleset`.
- GitHub-owned actions kept in the allowlist: `FAIL harden_sends_every_setting_in_order: PUT .../selected-actions body fails`.
- A failed list read answered as `[]`: `FAIL rulesets_stops_when_the_list_cannot_be_read: expected exit 1`.

Applied, after approval: `metadata` (PATCH repo, PUT topics, exit 0) and `rulesets` (PUT
13059268, PUT 25916143, POST created `Protect release tag creation` 26305661, exit 0). A dry
re-run of `rulesets` afterwards PUTs all three by id and POSTs nothing. `show` after applying:

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
allowed_actions: all
sha_pinning_required: true
allowed_action_patterns: none
default_workflow_permissions: read
fork_pr_approval: all_external_contributors
code_scanning_default_setup: configured
code_scanning_query_suite: default
immutable_releases: disabled
release_environment_policies: none
```

`harden` dry run for unit 10 (not applied, operator ruling): third-party actions
`codecov/codecov-action@*`, `golangci/golangci-lint-action@*`, `ossf/scorecard-action@*`,
`zizmorcore/zizmor-action@*`; writes, in order: PUT actions/permissions (selected, SHA pinning),
PUT selected-actions (GitHub-owned yes, verified no, those four patterns), PUT
actions/permissions/workflow (read, no PR approval), PUT fork-pr-contributor-approval
(`all_external_contributors`), PATCH code-scanning/default-setup (`extended`, actions and go),
PUT private-vulnerability-reporting, PUT immutable-releases, PUT environments/release (no
reviewers, custom policies), POST policy `v*` (tag), POST policy `main` (branch). Unit 10 must
re-run the dry run from `main` after the merge: the list is read from the workflows checked out.

Close-out verification (step 1), `WOMBAT_BASE_REF=workstream/repo-quality
scripts/wombat-verify.sh`, run outside the sandbox: inside it vet, lint, tidy, release-scripts,
vuln, zizmor and test failed on the Go cache, `vuln.go.dev`, uv and loopback binding. Record at
`3b2e00b`, tree clean, no gate skipped:

```
fmt pass; vet pass; lint pass; tidy pass; changelog pass; release-scripts pass;
repo-settings pass; vuln pass; zizmor pass; shellcheck pass; ci-result pass;
fuzz-script pass; test pass
```

Lanes from `select-gates.sh .`: `security`, `correctness`, `brief` (3, none skipped, no
`unmatched`). `scan-secrets.sh .`: `scan clean: 15 files`.

Sweep, `sweep-docs.sh . --base workstream/repo-quality` with `--claim 'left to the repo-settings
unit'` (and again with the environment sentence), printed 9 candidates each time: 4 `touched`
(CHANGELOG.md, CONTRIBUTING.md, SECURITY.md, scripts/README.md: this unit's own edits, read
back), 5 `named-file` (README.md on `ci.yml`, the CI badge URL; cmd/repl, plugins, plugins/opa,
plugins/softdelete READMEs on `README.md`, links to other READMEs), all dismissed as basename
collisions. `git grep` for `repo-settings unit` and `names no` outside the workstream record finds
nothing. Unit 10's brief was read for criteria this unit made wrong: none; it already applies
`harden` after the operator's merge.

## Not done, and why

- `harden` not applied: operator ruling, applied in unit 10 after the integration branch merges.
- `scripts/verify-tag.sh` still accepts any GitHub-verified signature: operator ruling, left for
  the operator to schedule.
