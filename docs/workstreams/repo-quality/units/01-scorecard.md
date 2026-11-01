# Unit 01: scorecard

Mode: implement
Close: auto
Branch: unit/01-scorecard
Review lanes: security, correctness, brief

## Items

- Add `.github/workflows/scorecard.yml`, ported from bawdo/caddy_shack's.
- Add the OpenSSF Scorecard badge to README.md.
- Add a CHANGELOG.md `[Unreleased]` entry.

## Acceptance criteria

- `.github/workflows/scorecard.yml` triggers on push to `main`, a weekly schedule and
  `branch_protection_rule`.
- Top-level `permissions: read-all`; the analysis job alone has
  `security-events: write` and `id-token: write`.
- Every `uses:` is pinned to a full commit SHA with a version comment, using the same
  SHAs already used elsewhere in this repo where the action is shared
  (`actions/checkout`), and SHAs matching caddy_shack for `ossf/scorecard-action`,
  `actions/upload-artifact` and `github/codeql-action/upload-sarif`.
- Checkout uses `persist-credentials: false`. The job has a `timeout-minutes`.
- `ossf/scorecard-action` runs with `results_format: sarif` and `publish_results: true`;
  the SARIF is uploaded as an artifact (short retention) and to code scanning.
- zizmor reports no findings on the new workflow (run zizmor locally if available, or
  state that CI's zizmor step is the check).
- README.md carries the Scorecard badge,
  `https://api.scorecard.dev/projects/github.com/bawdo/gosbee/badge`, linking to
  `https://scorecard.dev/viewer/?uri=github.com/bawdo/gosbee`, placed with the
  existing badges.
- `scripts/changelog.sh check` passes with the new `[Unreleased]` entry.
- Known limit, stated in the Outcome: the score only appears once this workflow runs
  on `main`, which happens when the integration branch merges.

## Decisions

- Caller census: none needed. The unit adds a new workflow file and touches no shared
  contract; README badges and the CHANGELOG `[Unreleased]` section are read by
  `scripts/changelog.sh check` only, which runs at verification.
- Copied `.github/workflows/scorecard.yml` from bawdo/caddy_shack unchanged.
  `diff ../caddy_shack/.github/workflows/scorecard.yml .github/workflows/scorecard.yml`
  prints nothing. Source defects checked: actionlint is clean, and zizmor 1.31.1 (the
  version CI pins) run through `uvx zizmor@1.31.1 --offline` reports no findings at
  the default persona. The `actions/checkout` SHA (`3d3c42e…`, v7.0.1) is the one
  `ci.yml` and `release.yml` already use.
- `uvx zizmor@1.31.1 --offline --persona auditor .github/workflows/scorecard.yml` reports
  three findings that the default persona suppresses (`(3 suppressed)`):
  `excessive-permissions` (top-level `read-all`), `undocumented-permissions` and
  `concurrency-limits`. These are left as they are. CI runs the default persona, the brief
  mandates `permissions: read-all` (the pattern Scorecard's own template uses), and
  keeping the file identical to caddy_shack's is part of this branch's R&D hypothesis.
- `skills.testFirst` does not apply here: there is no behaviour change in Go code with a
  test to write first. `skills.implement` is not configured. `skills.plan` does not apply
  to an implement unit.
- Badge placed after GoDoc, using the same label as caddy_shack ("OpenSSF Scorecard").
  The CHANGELOG entry goes under a new `### Added` heading, ahead of `### Changed`, in Keep
  a Changelog order.

## Plan

## Outcome

Close mode as run: auto. Questions that fired before step 4: the R&D tracking question
(answered "tracked"), commit permission (granted for this unit) and the R&D opening
statement (committed as drafted). None came from a merge or review gate.

Landed:

- `.github/workflows/scorecard.yml`, byte identical to bawdo/caddy_shack's.
  `diff ../caddy_shack/.github/workflows/scorecard.yml .github/workflows/scorecard.yml`
  printed nothing. It triggers on `push` to `main`, a weekly cron (`30 1 * * 1`) and
  `branch_protection_rule`. Top level `permissions: read-all`; job `analysis` alone gets
  `security-events: write` and `id-token: write`; `timeout-minutes: 10`; checkout uses
  `persist-credentials: false`; scorecard-action v2.5.0 runs with `results_format: sarif`
  and `publish_results: true`; the SARIF is uploaded as an artifact (`retention-days: 5`)
  and to code scanning via codeql-action/upload-sarif v4.39.1. Every `uses:` is pinned to
  a full SHA with a version comment.
- README.md Scorecard badge, after the GoDoc badge.
- CHANGELOG.md `[Unreleased]` `### Added` entry. `scripts/changelog.sh check` printed
  `CHANGELOG.md is consistent (3 versions)`.

Verification (step 1). The first run, inside the agent sandbox, failed vet, lint, tidy,
vuln and test, all on sandbox denials: the Go build cache under `~/Library/Caches`,
`httptest` port binding and `vuln.go.dev`. It was re-run outside the sandbox, with a
wrapper on PATH, kept outside the repository, that runs `uvx zizmor@1.31.1` (CI's pinned
version), so the zizmor gate ran instead of skipping. The record:

```
$ cat .claude/wombat-verification
wombat-verification 1
tree	clean
fmt	fbeea0c33502c595b1fdad755c9ab8ba823f3892	pass	0	-
vet	fbeea0c33502c595b1fdad755c9ab8ba823f3892	pass	0	-
lint	fbeea0c33502c595b1fdad755c9ab8ba823f3892	pass	1	-
tidy	fbeea0c33502c595b1fdad755c9ab8ba823f3892	pass	0	-
changelog	fbeea0c33502c595b1fdad755c9ab8ba823f3892	pass	0	-
vuln	fbeea0c33502c595b1fdad755c9ab8ba823f3892	pass	2	-
zizmor	fbeea0c33502c595b1fdad755c9ab8ba823f3892	pass	5	-
shellcheck	fbeea0c33502c595b1fdad755c9ab8ba823f3892	pass	0	-
test	fbeea0c33502c595b1fdad755c9ab8ba823f3892	pass	2	-
```

Every gate ran; none was skipped. actionlint (not a project gate) was also clean on the
new file.

Review lanes selected (`select-gates.sh .`): security, correctness, brief. All three
configured lanes ran; none was skipped.

Scan: `scan-secrets.sh .` printed `scan clean: 10 files`.

Sweep: `sweep-docs.sh . --base workstream/repo-quality` (no `--claim`, since this unit
made no documented sentence untrue) printed two candidates, `CHANGELOG.md touched` and
`README.md touched`. Both are this unit's own intended edits, so both were dismissed with
no further change. A `git grep -ni scorecard -- '*.md'` across the rest of the docs found
only the index and briefs 07 and 09, and both briefs assume what this unit shipped. No
document was rewritten or deleted, and no later brief got a correction.

Review triage (step 6). Lanes run: security, correctness and brief, each a fresh
subagent in its own detached worktree. None was skipped.

- Security, `scorecard.yml:13` (low): top-level `read-all` is broader than `ci.yml`'s
  `contents: read`, and a future job without its own block would inherit it. Not changed:
  the brief mandates `read-all`, the only job overrides it, and the Scorecard template
  uses it.
- Security, `scorecard.yml:21-22` (info): no `contents: read`, so checkout and the API
  reads would fail if the repository went private. Not acted on: the repository is public.
- Brief, Decisions (nit): the checkout SHA is shared with `ci.yml` too, not only
  `release.yml`. Fixed.
- Brief, Decisions (nit): the three-findings figure had no command beside it. Fixed, with
  the auditor-persona command.
- Correctness, `scorecard.yml:7` (info): `branch_protection_rule` fires only for classic
  branch protection, not repository rulesets. Not changed here. Carried in the handover
  for unit 08.
- Both the security and correctness lanes reported that the pinned SHAs match their tags.
  This session measured that again rather than relaying it, with
  `git ls-remote https://github.com/<owner>/<action> 'refs/tags/<tag>*'`: scorecard-action
  `v2.5.0^{}` resolves to `fa7117db…`, upload-artifact `v7.0.2` to `cf430e03…`,
  codeql-action `v4.39.1^{}` to `9b325408…`, and checkout `v7.0.1` to `3d3c42e5…`. All
  four match the version comments.

Known limit: the score only appears once this workflow runs on `main`, which happens
when the integration branch merges. Until then the README badge will not resolve to a
score. Unit 09 records the first published result.

Next unit inherits: nothing new to build on. Unit 08 (repo-settings) needs to know that
Scorecard's `Branch-Protection` check reads branch protection, and this workflow re-runs
on `branch_protection_rule` events.

## Not done, and why
