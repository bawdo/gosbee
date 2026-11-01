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

## Plan

## Outcome

## Not done, and why
