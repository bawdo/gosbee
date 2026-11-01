# Workstream closeout: repo-quality

Opened 2026-11-01, closed 2026-11-01, 10 units.

## Fit

The workstream set out to bring gosbee's GitHub, CI and release practice up to and past
bawdo/caddy_shack, bawdo/jellyfish and bawdo/go-blinkstick: a strong OpenSSF Scorecard and
readiness for the OpenSSF Best Practices survey, with nothing needing a second contributor.

It fits, with the score still catching up. gosbee went from unscored (Scorecard API 404 on
2026-11-01) to a published **7.5** at `d4de27c` (Scorecard v5.6.0, 2026-11-01T10:16:58Z), against
peers caddy_shack 8.3, jellyfish 8.0 and go-blinkstick 7.6. That puts it above go-blinkstick and
below the other two. Of the 18 checks, 10 score 10; the four that need a second contributor or the
Best Practices survey (Code-Review 0, Contributors 6, Branch-Protection 4, CII-Best-Practices 2)
were out of reach by design. The remaining gap is lag, not missing work: Signed-Releases scored 0
from a scan taken 6 minutes before the first provenance-carrying release was published, and two of
the three Vulnerabilities findings are fixed on the integration branch but not yet on `main`.
Per check, with the command, in unit 10's brief, `## Outcome`.

## Proven

- A release pipeline that holds under a real tag. `v0.3.0-rc.1` (run 41296868668) checked the tag
  name, ran CI on the tag, built with a read-only token, published through a draft as an immutable
  pre-release with build provenance and SBOMs, and confirmed proxy.golang.org and sum.golang.org.
  Every verification command in SECURITY.md passed against it, as written, from a fresh clone.
- A solo maintainer can hold that pipeline without a second person: admin-only `v*` tag creation,
  rulesets on `main` and on release tags, a `release` environment limited to `v*` tags and `main`,
  an Actions allowlist with SHA pinning, and a read-only default token, all kept in the tree by
  `scripts/repo-settings.sh` and applied from `main`.
- CodeQL's Actions queries earn their place: alert #5 (cache poisoning) pointed at a real gap,
  where a by-hand release dispatch could run any existing tag's code, even outside `v*`, in main's
  cache scope before the tag name was checked. Unit 10 closed it.

## Disproven

- That turning the Go cache off would clear CodeQL's `actions/cache-poisoning/poisonable-step`.
  Unit 10's first proposal; the query source showed it looks at no cache step at all and keys on a
  field-name heuristic (`.*(head|branch|ref).*`), so the proposal was withdrawn before any code.
- That the integration branch could be merged to `main` with a merge commit. The repository's own
  settings, made by unit 09, allow squash only, so `main` and the integration branch were joined by
  hand after each squash (`287de66`, and `8c2e792`, which needed `-s ours` on the operator's word).

## Divergence

- The re-cut after unit 04: unit 05 inserted, old 05 to 09 renumbered 06 to 10, and 06 and 09
  continued from plan into build (see Re-cut units).
- Unit 10 fixed the pipeline before proving it. PR #28 was blocked by CodeQL, so the tag job and
  the `ref` to `tag` input rename landed first, followed by a second PR (#29) for the rc's
  CHANGELOG section and the docs it invalidated, and a third set of changes (the x/net bump and
  SECURITY.md notes) that is on the integration branch and not yet on `main`.
- Claims about the tree carried here because no unit follows:
  - `quoting.EscapeString` doubles `\` for PostgreSQL and SQLite in non-parameterised mode (unit
    06); pinned by a skipped test, operator to schedule.
  - `examples/before_after_example.go` is a stale, uncompiled sketch (units 07, 08); operator to
    schedule or delete.
  - `scripts/verify-tag.sh` accepts any signature GitHub verifies, looser than SECURITY.md's pinned
    subkey (units 08, 09); operator to schedule.
  - Scorecard below 10 outside the exempt four (unit 10): Signed-Releases (scan lag),
    Vulnerabilities (`GO-2026-5932`, no fix exists; the two x/net findings await `main`), CI-Tests
    9 (one early PR), Packaging -1 (inconclusive).
  - On a release dispatch from `main`, ci runs a `v*` tag's code in main's cache scope before
    build checks the tag is signed and on `main`; only admin-only tag creation stands in front of
    it (unit 10's security lane). Release artefacts are not exposed.

## Not done notes

14 notes across the ten briefs, counted from each brief's `## Not done, and why` bullets:
01 none, 02 one, 03 one, 04 none, 05 none, 06 three, 07 one, 08 two, 09 two, 10 two.

- 02, the release pipeline build needed an implementing session: a scope decision, answered by the
  re-cut that inserted unit 05.
- 03, the re-cut not yet made: a decision, answered by the same re-cut.
- 06, `quoting.EscapeString`'s backslash doubling: a claim about the tree; carried in the index's
  handover from 06 onward, and into Divergence above.
- 06, no fuzz finding produced: a statement of result; nothing to carry.
- 06, the OPA star expansion and `refuseMaskedUse` not fuzzed: a claim about the tree; carried into
  07's brief as `CORRECTION FROM UNIT 06:`.
- 07, the stale example sketch: a claim about the tree; carried into 08's brief as
  `CORRECTION FROM UNIT 07:`.
- 08, the sketch left in the tree: operator to rule; into Divergence above.
- 08, `verify-tag.sh` looser than SECURITY.md: a claim about the tree; carried into 09's brief as
  `CORRECTION FROM UNIT 08:`.
- 09, `harden` not applied: an operator ruling, done in unit 10.
- 09, `verify-tag.sh`: an operator ruling, left to schedule; into Divergence above.
- 10, the Scorecard checks below 10: claims about the tree and the score; into Divergence above.
- 10, the release's cache-scope trust: a claim about the tree; into Divergence above.

Every claim about the tree went somewhere: three as corrections into a following brief, the rest
into Divergence. None travelled nowhere.

## Re-cut units

4 units changed scope after `/wombat-workstream:open` wrote them, all in the one re-cut of
2026-11-01 after unit 04, on the operator's word (index header; unit 10's `## Decisions`):

- 05 release-build: inserted to build what 02 and 03 planned.
- 06 fuzzing: continued from plan into build.
- 09 repo-settings: continued from plan into build.
- 10 release-proof: gained the item to apply `harden` before the rc.

Old units 05 to 09 were renumbered 06 to 10; renumbering alone is not counted.

## Review coverage

Read from each brief's record of the lanes that ran, not its header (every header lists all three):

| Unit | Lanes run |
|---|---|
| 01 | security, correctness, brief |
| 02 | brief (security and correctness not selected: a plan unit's docs only) |
| 03 | brief (as 02) |
| 04 | security, correctness, brief |
| 05 | security, correctness, brief |
| 06 | security, correctness, brief |
| 07 | security, correctness, brief |
| 08 | security, correctness, brief |
| 09 | security, correctness, brief |
| 10 | security, correctness, brief |

## Reachability

No user-facing application surface was built: gosbee is a library and its REPL is unchanged by
this workstream. The surfaces built are for maintainers and verifiers, and each is linked:

- The Scorecard result: the README's Scorecard badge.
- Release verification: SECURITY.md's "Verifying a release", linked from the README ("Security
  Policy: reporting a vulnerability and verifying...") and from the GitHub Security tab.
- The release procedure: CONTRIBUTING.md's Releasing section, linked from the README.
- The repository settings: `scripts/repo-settings.sh`, documented in CONTRIBUTING.md and
  `scripts/README.md`.
- Issue forms, the PR template, CODEOWNERS and the code of conduct: GitHub links each from the
  issue and pull request flows.

## Gate selection

Not every unit ran every gate, so the rules did discriminate:

- Lanes: 02 and 03 ran `brief` alone; the other eight ran all three (Review coverage above).
- Verification gates: `zizmor` was skipped with the reason "zizmor not installed (CI runs it)" in
  units 02 to 05; unit 05 also records `release-scripts` as skipped with a reason where its build
  cannot run locally. Units 08, 09 and 10 ran every gate, 12 then 13 as `repo-settings` was added,
  with none skipped.
- Unit 01 ran every gate; none was skipped.

## Gate activations

`count-gate-log.sh .` as first run read `/Users/bawdo/.local/state/wombat-workstream/logs`, last
written 2026-10-01, before this workstream: it found no row for this repository (10031 legacy rows,
unattributable). The hooks write to the plugin data directory. Run against that:

```
$ CLAUDE_PLUGIN_DATA=/Users/bawdo/.claude/plugins/data/wombat-workstream-wombat-marketplace count-gate-log.sh .
gate log read from /Users/bawdo/.claude/plugins/data/wombat-workstream-wombat-marketplace/logs
commit gate log read from /Users/bawdo/.claude/plugins/data/wombat-jira-wombat-marketplace/logs
     8 verify silent
```

| Gate | Outcome | Count | False |
|---|---|---|---|
| verify | silent | 8 | 0 |

The 8 rows are the merges of units 01 to 05 and 08 to 10 into the integration branch, each "test
census saw no test or source path". Units 06 and 07 changed test files, so their merges wrote no
`silent` row. No row is false: each records a merge that happened, as described. That the
default log directory is stale, and the counter reads it unless told otherwise, is a finding.

Questions an automatic close put to the operator, from the briefs' `## Outcome`: every unit ran
`Close: auto`. None was answered with a plain no. Several were answered with an option other than
the one offered first (04: pass `"$@"`; 07: refuse mismatched SET targets; 10: (b) plus the rename,
not the first proposal), and two were left unanswered (03: the re-cut, later made after 04; 08:
deleting the example sketch). The load-bearing ones were those that changed the work, not those
that refused it.

## Documentation swept

Per unit, from the briefs:

| Unit | Candidates | Rewritten at close | Deleted | Dismissed | Corrections written |
|---|---|---|---|---|---|
| 01 | 2 | 0 | 0 | 2 (own edits) | 0 |
| 02 | 0 | 0 | 0 | 0 | 0 |
| 03 | 0 | 0 | 0 | 0 | 0 |
| 04 | 8 | 0 (rewritten in the work) | 0 | 8 | 0 |
| 05 | 8 | 0 (3 rewritten in the work) | 0 | 8 | 0 |
| 06 | 8 | 0 | 0 | 8 | 1 (into 07) |
| 07 | 2 | 0 | 0 | 2 | 1 (into 08) |
| 08 | 6 | 0 | 0 | 6 (own edits) | 1 (into 09) |
| 09 | 9 | 0 | 0 | 9 | 0 |
| 10 | 4 | 1 (CONTRIBUTING.md rewrap) | 0 | 3 | 0 |

Final sweep, `sweep-docs.sh . --base ed10f4e` (`main` as the workstream found it; today's `main`
already carries the squashed work, so it would ask almost nothing): 11 candidates. 7 `touched`
(the PR template, CHANGELOG, CODE_OF_CONDUCT, CONTRIBUTING, README, SECURITY, scripts/README), each
judged at the close of the unit that edited it; 4 `named-file` on `README.md` (cmd/repl, plugins,
plugins/opa, plugins/softdelete), basename collisions, dismissed. The four plans under `plans/`
served closed units and are kept as the record of what was planned; their mentions of `inputs.ref`
describe the plan as written. Nothing rewritten or deleted.

## Tickets

The project has no `jira` block configured, so no Jira behaviour ran at all: no master ticket, no
unit tickets, no transitions, no failed writes.

## Deferred tickets

No `DEFERRED:` line in any brief, so no ticket was raised for deferred work. The work left undone
is the operator-scheduled set in Divergence above (`quoting.EscapeString`, the example sketch,
`verify-tag.sh`, the release cache-scope trust, the remaining Scorecard gaps); it lives in prose
here and in the briefs, not in a backlog.

## Record cost

Characters and lines a session had to read, by `wc -m` and `wc -l`:

- Unit 01, at open (`b0c1bb4`): index plus brief, 3853 characters, 80 lines. The router of the
  time is gitignored and cannot be recovered, so it is not counted.
- Unit 10, at open (`8a5d877`): index plus brief, plus `HANDOFF.md` (546 characters, 14 lines),
  6052 characters, 113 lines.

The difference is 2199 characters and 33 lines, of which the router accounts for 546 and 14. The
growth is the index's handover section, which carries what each closed unit left the next.
