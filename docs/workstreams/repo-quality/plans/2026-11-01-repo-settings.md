# Repository Settings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this
> plan task by task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `scripts/repo-settings.sh` puts gosbee's GitHub settings under version control, phase by
phase, with a `gh`-stubbed test; this unit applies `metadata` and `rulesets` and dry-runs `harden`.

**Architecture:** One bash script, one `write METHOD PATH [BODY]` function as the only place a write
is sent (prints method and path first; under `DRY_RUN` prints the body and sends nothing). Each
phase sends full desired state; rulesets, environment policies are found by name and replaced in
place. A test script puts a fake `gh` first on `PATH`, logs every call and body, and asserts on
them. Ported from bawdo/caddy_shack `scripts/repo-settings.sh` and `repo-settings_test.sh`, with
the gosbee differences listed under Global Constraints.

**Tech Stack:** bash, `gh api`, `jq`, shellcheck v0.11.0.

**Spec:** `docs/workstreams/repo-quality/units/09-repo-settings.md` (Acceptance criteria and
Decisions; the Decisions win where they refine a criterion).

## Global Constraints

- `REPO` defaults to `bawdo/gosbee`; `R="repos/$REPO"`.
- Phases: `metadata`, `rulesets`, `harden`, `show`. Anything else dies with
  `repo-settings: usage: repo-settings.sh metadata|rulesets|harden|show`.
- Every write prints `METHOD PATH` on its own line before it is sent. `DRY_RUN` set: body printed
  indented four spaces, nothing sent. Reads still run where a phase needs them to choose its writes.
- `metadata` PATCH `$R` body exactly: `description: "A Golang SQL Builder"`,
  `homepage: "https://pkg.go.dev/github.com/bawdo/gosbee"`, `allow_auto_merge: true`,
  `allow_squash_merge: true`, `allow_merge_commit: false`, `allow_rebase_merge: false`,
  `delete_branch_on_merge: true`, `allow_update_branch: true`, `has_discussions: true`,
  `has_wiki: false`, `has_projects: false`. PUT `$R/topics` names, in this order:
  `go golang sql sql-builder query-builder ast postgresql mysql sqlite arel`.
- Ruleset names: `Protect main`, `Protect release tags`, `Protect release tag creation`.
- `Protect main`: target branch, `~DEFAULT_BRANCH`, no bypass actors; rules in order deletion,
  non_fast_forward, required_signatures, pull_request (0 approvals, thread resolution true,
  `allowed_merge_methods: ["squash"]`, other booleans false), required_status_checks (strict true,
  `CI OK` and `Analyze (go)`, each `integration_id: 15368`), code_scanning (CodeQL,
  `alerts_threshold: "errors"`, `security_alerts_threshold: "high_or_higher"`).
- `Protect release tags`: `refs/tags/v*`, no bypass, rules deletion, non_fast_forward, update.
- `Protect release tag creation`: `refs/tags/v*`, rule creation, bypass
  `[{actor_id: 5, actor_type: "RepositoryRole", bypass_mode: "always"}]` (5 is the admin role).
- `rulesets` refuses before writing unless the latest successful push CI run on `main` has a job
  named `CI OK`: `repo-settings: no CI OK job in the latest successful CI run on main`.
- `harden` writes, in order: PUT `$R/actions/permissions`
  (`enabled true, allowed_actions "selected", sha_pinning_required true`); PUT
  `$R/actions/permissions/selected-actions` (`github_owned_allowed true, verified_allowed false,
  patterns_allowed` computed); PUT `$R/actions/permissions/workflow`
  (`default_workflow_permissions "read", can_approve_pull_request_reviews false`); PUT
  `$R/actions/permissions/fork-pr-contributor-approval` (`all_external_contributors`); PATCH
  `$R/code-scanning/default-setup` (`state configured, query_suite extended, languages
  ["actions","go"]`); PUT `$R/private-vulnerability-reporting`; PUT `$R/immutable-releases`; PUT
  `$R/environments/release` (`deployment_branch_policy {protected_branches false,
  custom_branch_policies true}`, `reviewers []`, `wait_timer 0`); then the environment's policies
  `v*` (type tag) and `main` (type branch), each PUT to its id when one by that name and type
  exists, else POST to `$R/environments/release/deployment-branch-policies`, and DELETE for any
  other policy found there.
- Allowlist: `owner/repo@*` for each `uses:` in `.github/workflows/*.y*ml` not under `actions/` or
  `github/`, local `./` skipped, sorted and deduplicated, printed to stdout as
  `third-party actions:` then one per line, before any write. An action not pinned to a 40-hex SHA
  dies: `repo-settings: <file>: <ref> is not pinned to a commit SHA`. Expected list today:
  `codecov/codecov-action@*`, `golangci/golangci-lint-action@*`, `ossf/scorecard-action@*`,
  `zizmorcore/zizmor-action@*`.
- Applying `harden` happens in unit 10 after the operator's merge to `main` (operator ruling).
- House commit rules: subject past tense, no agent reference, at most 60 characters; GPG signed;
  never on `main`; no push, pull or PR (remotes `local`).

## Review lanes every task is held to

From `reviewLanes` and step 5 of the close-out:

- **security:** what an attacker or a hostile input could do with this change: secrets,
  injection, authorisation, and anything it exposes that was not exposed before. Here: workflow
  file contents reaching a shell or a JSON body; any setting that loosens what `main` or `v*` tags
  accept today; any write that bypasses `write`.
- **correctness:** whether the code does what it claims under the cases it will actually meet,
  including the edges, the errors and the concurrent ones. Here: re-runs are idempotent; a failed
  read is never taken as "absent" where that would create a duplicate; `gh` failures stop the run.
- **brief:** does this deliver what the brief specified, criterion by criterion, including the
  Decisions.

## Review Focus

1. A ruleset or environment policy read failing (outage, 403) must stop the phase, not fall through
   to POST and create a duplicate. Test: `rulesets_stops_when_the_list_cannot_be_read`.
2. An unpinned or tag-pinned third-party action must stop `harden` before any write. Test:
   `harden_refuses_an_unpinned_action`.
3. `uses:` with odd spacing, a trailing comment or a subpath (`github/codeql-action/upload-sarif`)
   must still parse to the right `owner/repo@*`. Covered by the fixture in `fake()`.
4. A write rejected by GitHub must exit non-zero with the response on stderr. Test:
   `write_failure_reports_and_stops`.
5. A deployment policy present but not desired (say a stale `release/*`) is deleted, so re-running
   converges. Test: `harden_replaces_environment_policies`.

---

### Task 1: The script and its test

**Files:**
- Create: `scripts/repo-settings.sh`, `scripts/repo-settings_test.sh`

**Interfaces:**
- Produces: phases above; env `REPO`, `DRY_RUN`, `WORKFLOWS_DIR` (default
  `<repo>/.github/workflows`); `show` prints `key: value` lines and exits 1 if a required read
  failed.

- [ ] **Step 1: Write the test first.** Port caddy_shack's fake `gh` and harness (canned
  `runs.json`, `jobs.json`, `rulesets.json`, `policies.json`, `repo.json`, `immutable.json`,
  `gh.log`, `bodies.log`; a `<path>.fail` file makes that read or write exit 1 printing a JSON
  error). Cases, each asserting exit code, `gh.log` and bodies with `jq -e` against the Global
  Constraints values:
  `metadata_patches_settings_and_topics`, `rulesets_needs_ci_ok_job`,
  `rulesets_creates_when_absent`, `rulesets_updates_when_present` (ids 101/202/303 under the gosbee
  names), `rulesets_stops_when_the_list_cannot_be_read`, `harden_sends_every_setting_in_order`,
  `harden_lists_third_party_actions`, `harden_refuses_an_unpinned_action`,
  `harden_needs_a_workflow`, `harden_replaces_environment_policies`,
  `dry_run_sends_nothing_for_every_phase` (metadata, rulesets and harden under `DRY_RUN=1`: no
  non-GET line in `gh.log`, each `METHOD PATH` on stdout followed by an indented body),
  `write_failure_reports_and_stops`, `show_fails_on_read_error`, `show_prints_settings`,
  `unknown_phase_prints_usage`.
- [ ] **Step 2: Run** `bash scripts/repo-settings_test.sh`. Expected: FAIL (script missing).
- [ ] **Step 3: Write `scripts/repo-settings.sh`** from caddy_shack, dropping `pre-public`,
  `public` and `post-public`, adding `metadata`, the gosbee values, `actions/permissions/workflow`,
  private vulnerability reporting and the environment to `harden`, and a `list_or_die PATH` read
  used for rulesets and policies so a failed list stops the phase. `show` adds
  `allow_update_branch`, `default_workflow_permissions`, `environments` and
  `release_environment_policies`.
- [ ] **Step 4: Run** the test and `shellcheck scripts/repo-settings.sh scripts/repo-settings_test.sh`.
  Expected: every case `ok`, shellcheck silent.
- [ ] **Step 5: Mutation, through `record-mutation.sh`**, ledger and patches outside the
  repository, `--fail-pattern '^FAIL '`: (a) `write` sends under `DRY_RUN`; (b) `Protect main`
  strict policy false; (c) the allowlist keeps `actions/*`; (d) a failed ruleset list read treated
  as empty. Each must be killed.
- [ ] **Step 6: Commit** the two files.

### Task 2: Wire the test into verification and CI

**Files:** Modify `scripts/wombat-verify.sh` (a `repo-settings` gate beside `release-scripts`,
skipped with a reason when `jq` is absent), `.github/workflows/ci.yml` (`Lint and audit` step
`Test the repository settings script`, `run: bash scripts/repo-settings_test.sh`).

- [ ] **Step 1:** add both; run zizmor offline over the workflows and shellcheck over `scripts/*.sh`.
- [ ] **Step 2: Commit.**

### Task 3: Publish environment in `release.yml`

**Files:** Modify `.github/workflows/release.yml` (publish job: `environment: release`).

- [ ] **Step 1:** census `grep -rn 'environment' .github scripts CONTRIBUTING.md SECURITY.md`, hits
  into the brief's `## Plan`. Add the line; check `scripts/release-scripts_test.sh` and zizmor
  still pass.
- [ ] **Step 2: Commit.**

### Task 4: Documentation

**Files:** Modify `CONTRIBUTING.md` (new `### Repository Settings` under Branch Rules documenting
the script, its phases and `DRY_RUN`; Branch Rules gains the strict up-to-date rule, the CodeQL
results rule and admin-only `v*` tag creation; Releasing replaces the "left to the repo-settings
unit" paragraphs with what the script sets, saying `harden` is applied after the workstream merges);
`SECURITY.md` (immutable releases and `gh release verify <tag>` in "Verifying a release", the
environment's tag rule, stated as in effect once `harden` is applied); `CHANGELOG.md`
(`[Unreleased]` Added entry); `scripts/README.md` (a section for the script).

- [ ] **Step 1:** edit; run `scripts/changelog.sh check`.
- [ ] **Step 2: Commit.**

### Task 5: Apply, with the operator's approval per phase

- [ ] **Step 1:** `DRY_RUN=1 scripts/repo-settings.sh metadata`; operator approves; apply.
- [ ] **Step 2:** `DRY_RUN=1 scripts/repo-settings.sh rulesets`; operator approves; apply.
- [ ] **Step 3:** `DRY_RUN=1 scripts/repo-settings.sh harden`, output into the brief for unit 10.
- [ ] **Step 4:** `scripts/repo-settings.sh show`, output pasted into `## Outcome`; commit.
