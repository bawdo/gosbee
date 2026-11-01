# Unit 06: fuzzing

Mode: plan
Close: auto
Branch: unit/06-fuzzing
Review lanes: security, correctness, brief

## Items

- Choose fuzz targets and the property each asserts, and record them in the plan.
- Write native Go fuzz targets with seed corpora.
- Add `make fuzz FUZZTIME=...`.
- Add a fuzz job to `ci.yml` and its row in `scripts/ci-result.sh`'s skip table.
- Update CONTRIBUTING.md (testing) and CHANGELOG.md.
- Continue from the plan into the build in this unit, and build every task of it.

## Acceptance criteria

- Native Go fuzz targets (`func FuzzXxx(f *testing.F)`) cover at least:
  - identifier quoting for each dialect (PostgreSQL, MySQL, SQLite);
  - string and literal escaping for each dialect;
  - the OPA plugin's MaskLiteral path;
  - one parser-style entry point (the REPL's input parsing, or the closest equivalent).
- Each target asserts a property, not just "does not panic", where one exists: for
  example a quoted identifier round trips, or escaped output never closes the quote
  early. The property per target is written in the plan.
- Seed corpora are added with `f.Add` and, where useful, under `testdata/fuzz/`. The
  seeds run as ordinary tests in every `go test` run.
- `make fuzz FUZZTIME=<d>` runs every fuzz target in turn (Go fuzzes one target at a
  time), defaulting to a short time.
- The CI fuzz job runs only on `schedule` (5m per target, timeout sized to the target
  count) and `workflow_dispatch` (15s per target), never on pull requests or pushes;
  uploads `testdata/fuzz/` failures as an artifact with 14 day retention; has
  `persist-credentials: false` and SHA-pinned actions.
- `scripts/ci-result.sh` permits `fuzz` to be skipped on `pull_request` and `push`,
  and its test script gains cases proving that, and proving fuzz may not be skipped on
  `schedule` or `workflow_dispatch`.
- **A real defect found by fuzzing is not fixed in this unit.** A vulnerability is
  reported to the operator for a private security advisory; any other defect is
  recorded under Not done with the failing input. The target stays, with the failing
  input either fixed in a separate change or the target skipped with a reason.
- golangci-lint, zizmor and `scripts/wombat-verify.sh` pass; CHANGELOG.md has an
  `[Unreleased]` entry.

## Decisions

- OPERATOR RULING 2026-11-01 (after unit 04 closed): this unit writes its plan and then continues
  into implement in the same unit, so the fuzz targets, `make fuzz` and the CI fuzz job land here
  rather than as a plan nothing builds. Renumbered from 05 when the release-build unit was
  inserted before it.
- Skills: `plan` goes to `superpowers:writing-plans` and `testFirst` to
  `superpowers:test-driven-development`, both installed. `implement` is not configured, so this
  session builds the plan inline. No `jira` block, no `knownTraps`, and no `preflight` key
  (pre-flight printed `none`).
- The brief's "the OPA plugin's MaskLiteral path" names no function in the tree. The mask path is
  `plugins/opa/opa.go` `applyMasks` -> `maskedProjection` -> `nodes.NewBindParam` -> the visitor's
  `VisitBindParam`, which parameterises or renders through `literalToSQL`. The target fuzzes that
  path end to end through `TransformSelect`.
- OPERATOR RULING 2026-11-01, release call: release.yml calls ci.yml, and a called workflow
  inherits the caller's `github.event_name`, so a dispatched release re-run reads as
  `workflow_dispatch`. The fuzz job runs only when ci.yml is triggered directly (`inputs.ref` is
  empty). The gate's EVENT is `workflow_call` when `inputs.ref` is set, and the skip table lets
  fuzz be skipped on `workflow_call` alone besides `pull_request` and `push`. `schedule` and direct
  `workflow_dispatch` may still not skip it.
- OPERATOR RULING 2026-11-01, backslash: `quoting.EscapeString` doubles backslashes for every
  dialect (a documented choice for MySQL). Under PostgreSQL (standard_conforming_strings on) and
  SQLite, a backslash is literal, so in non-parameterised mode the value is altered, though the
  quote is never closed early. Every dialect's literal target asserts the no-early-close property
  under its own lexer, and MySQL also asserts a full round trip. PostgreSQL and SQLite assert a
  round trip only for backslash-free inputs. The backslash case is recorded under Not done, with
  input `\`, and fixed in a separate change.
- Commit grant: the operator granted commits for this unit without asking each time.
- Caller census of the CI gate contract (`JOBS`, `skippable`, the gate's `EVENT:` line, `needs:`),
  run as `grep -rn "ci-result\|EVENT:\|JOBS=\|skippable" -I .`, excluding `.git`, coverage files
  and closed unit briefs. Readers: `scripts/ci-result.sh` (JOBS, skippable, EVENT);
  `scripts/ci-result_test.sh` (its own JOBS copy at line 39; `permitted_skip_passes` seds the
  pull_request arm's exact `echo ""` shape, so it breaks once that arm names fuzz; `ci_yml_gate_shape`
  pins the `EVENT:` line verbatim; the needs and job-list cases); `.github/workflows/ci.yml` (lint
  step, gate comment "today, none", gate env); `scripts/wombat-verify.sh` (gate `ci-result`);
  `scripts/README.md` (ci-result.sh section); `CONTRIBUTING.md:407-411`; `CHANGELOG.md:33`.

## Plan

`docs/workstreams/repo-quality/plans/2026-11-01-fuzzing.md`: 9 targets in 9 tasks, built in this
session inline, task by task.

## Outcome

Close mode as run: auto. Questions that fired, each with its cause and the operator's answer
(asked at open, before the plan):

- Release call (ci.yml called by release.yml inherits `workflow_dispatch`): "Skip it in calls".
- Backslash escaping property for PostgreSQL and SQLite: "Safety + record defect".
- Commit grant: "Yes, for this unit".

Questions at step 6 triage (no gate command named, so none went through `record-answer.sh`):

- MySQL multibyte client charset (GBK/SJIS/BIG5) defeating EscapeString in non-params mode, from
  the security lane. It is pre-existing, and quoting.go's SECURITY comment already documents it:
  "Docs caveat only". No advisory. The reproducer stays out of the committed record.
- Scheduled runs sharing the `ci-refs/heads/main` concurrency group with pushes, now for 45-60m:
  "Fix in this unit".

### What landed

- `internal/sqllex`: a read-back lexer (ANSI, MySQL) the properties are checked against.
- Fuzz targets, every one asserting a property (table in the plan), with seeds through `f.Add`
  only, so no `testdata/fuzz/` files are committed:
  `FuzzDoubleQuote`, `FuzzBacktick`, `FuzzEscapeString`, `FuzzEscapeLikePattern`
  (`internal/quoting`); `FuzzIdentifierQuoting`, `FuzzStringLiteral` (`visitors`, all three
  dialects, with and without params); `FuzzMaskedProjection` (`plugins/opa`); `FuzzTokenize`,
  `FuzzParseValue` (`cmd/repl`). `TestEscapeStringANSIRoundTripBackslash` skips with the defect.

  ```
  $ go test -list ^Fuzz ./... | grep -c ^Fuzz
  9
  ```
- `scripts/fuzz.sh` (carries on past a failure, fatal on a listing failure or a malformed
  FUZZTIME), `make fuzz FUZZTIME=...` (default 10s), `scripts/fuzz_test.sh` (fake go), run by the CI
  lint job and as the `fuzz-script` verify gate.

  ```
  $ bash scripts/fuzz_test.sh | grep -c "^ok "
  25
  ```
- ci.yml `fuzz` job: `if: !inputs.ref && (schedule || workflow_dispatch)`, FUZZTIME 5m on schedule
  and 15s on dispatch, `timeout-minutes: 60`, `persist-credentials: false`, `ref: inputs.ref`,
  SHA-pinned actions, `fuzz-failures` artifact of `**/testdata/fuzz/**` on failure with 14 day
  retention. The gate needs `[ lint, test, fuzz ]`, and its EVENT is `workflow_call` when
  `inputs.ref` is set. `ci-result.sh` lets `fuzz` be skipped on `pull_request`, `push` and
  `workflow_call` only.

  ```
  $ scripts/ci-result_test.sh | grep -c "^ok "
  34
  $ git show workstream/repo-quality:scripts/ci-result_test.sh | grep -c 'echo "ok '
  16
  ```
  (The second counts echo lines in the old script, several of which sit in loops, so it is not a
  case count. The run count before this unit was not measured.)
- CONTRIBUTING.md (Testing gets a Fuzzing subsection; Branch Rules lists the Fuzz job and its
  skip), CHANGELOG.md `[Unreleased]` Added, scripts/README.md (a `fuzz.sh` section).
- Each target was fuzzed locally for 15 to 30s, one `go test -fuzz` run per target in this
  session, and then all of them together:

  ```
  $ make fuzz FUZZTIME=2s | grep -E '^(==>|Failed|Every)'
  ==> github.com/bawdo/gosbee/cmd/repl FuzzTokenize
  ==> github.com/bawdo/gosbee/cmd/repl FuzzParseValue
  ==> github.com/bawdo/gosbee/internal/quoting FuzzDoubleQuote
  ==> github.com/bawdo/gosbee/internal/quoting FuzzBacktick
  ==> github.com/bawdo/gosbee/internal/quoting FuzzEscapeString
  ==> github.com/bawdo/gosbee/internal/quoting FuzzEscapeLikePattern
  ==> github.com/bawdo/gosbee/plugins/opa FuzzMaskedProjection
  ==> github.com/bawdo/gosbee/visitors FuzzIdentifierQuoting
  ==> github.com/bawdo/gosbee/visitors FuzzStringLiteral
  Every fuzz target passed.
  ```

  No run found anything.

### Mutation ledger

Each run went through `record-mutation.sh . --patch <p> --fail-pattern <runner marker> --ledger
<scratchpad>/mut/ledger.txt -- <command>`, with the patches and the ledger in the session
scratchpad. The table is condensed from the ledger: patch, test, `first` and `verdict` per run.
Runs 17 to 22 come from the review fixes.

```
q-doublequote     FuzzDoubleQuote         first --- FAIL: FuzzDoubleQuote          KILLED
q-escapestring    FuzzEscapeString        first --- FAIL: FuzzEscapeString         KILLED
q-like            FuzzEscapeLikePattern   first --- FAIL: FuzzEscapeLikePattern    KILLED
v-mysqlident      FuzzIdentifierQuoting   first --- FAIL: FuzzIdentifierQuoting    KILLED
v-noescape        FuzzStringLiteral       first none                               SURVIVED
v-noescape2       FuzzStringLiteral       first --- FAIL: FuzzStringLiteral        KILLED
o-rawmask         FuzzMaskedProjection    first --- FAIL: FuzzMaskedProjection     KILLED
r-tokjoin         FuzzTokenize            first --- FAIL: FuzzTokenize             KILLED
r-tokjoin         FuzzParseValue          first none                               SURVIVED
r-parseval        FuzzParseValue          first --- FAIL: FuzzParseValue           KILLED
f-stopfirst       fuzz_test.sh            first FAIL continues_after_failure       KILLED
f-listfail        fuzz_test.sh            first FAIL list_failure_fails            KILLED
c-schedskip       ci-result_test.sh       first FAIL fuzz_skip_refused_schedule    KILLED
c-nocall          ci-result_test.sh       first FAIL fuzz_skip_permitted_workflow_call KILLED
c-ifpr            ci-result_test.sh       first FAIL ci_yml_fuzz_job_shape         KILLED
c-timeout         ci-result_test.sh       first FAIL ci_yml_fuzz_timeout_sized     KILLED
t-uploadif        ci-result_test.sh       first FAIL ci_yml_fuzz_job_shape         KILLED
t-concurrency     ci-result_test.sh       first FAIL ci_yml_schedule_own_concurrency_group KILLED
t-zero            fuzz_test.sh            first FAIL bad_fuzztime_refused_[0s]     KILLED
t-splice          fuzz_test.sh            first FAIL make_fuzz_value_is_not_shell  KILLED
t-mysqldq         TestLex                 first none                               SURVIVED
t-mysqldq2        TestLex                 first --- FAIL: TestLex/mysql_double_quoted_string_escapes KILLED
$ grep -c '^verdict' ledger.txt; grep -c '^verdict  KILLED' ledger.txt; grep -c '^verdict  SURVIVED' ledger.txt
22
19
3
```

The two survivors: `v-noescape` removed the only use of `quoting` in visitor.go, so the mutant did
not compile and no test ran. It was replaced by `v-noescape2` (`EscapeLikePattern` in place of
`EscapeString`), which compiles and was killed. `r-tokjoin` against `FuzzParseValue` is a plan
error: `parseValue` takes a token and never calls `tokenize`, so that target cannot see the
mutation. `FuzzTokenize` kills it. `t-mysqldq` dropped `"` from the string case but not from the
`Other` stop set, so the lexer looped forever on a zero-length token and the run hung. That is a
broken mutant. `t-mysqldq2` drops it from both, which restores the pre-fix behaviour, and was
killed.

### Verification (step 1)

`WOMBAT_BASE_REF=workstream/repo-quality scripts/wombat-verify.sh`, run outside the sandbox (the Go
build cache, httptest binds and vuln.go.dev are blocked inside it). The first run failed `lint`:
`internal/sqllex/sqllex.go:57:3: QF1002: could use tagged switch on ch (staticcheck)`, which was
fixed in 82cf556. The rerun at 82cf556 had tree `clean`, and every gate passed: fmt, vet, lint,
tidy, changelog, release-scripts, vuln, zizmor, shellcheck, ci-result, fuzz-script, test. None was
skipped. Lane selector: `security`, `correctness`, `brief`, with no `unmatched` and no `floor`.

### Scan and sweep

- `scan-secrets.sh .`: `scan clean: 14 files`.
- `sweep-docs.sh . --base workstream/repo-quality --claim 'today it permits none'`, and again with
  `--claim '(today, none)'`: the same 8 candidates each time, and no `claim` hit. That's because the
  CONTRIBUTING and ci.yml sentences had already been rewritten in this diff. `CHANGELOG.md`,
  `CONTRIBUTING.md` and `scripts/README.md` are `touched` (this unit's own edits). `README.md`,
  `cmd/repl/README.md`, `plugins/README.md`, `plugins/opa/README.md` and
  `plugins/softdelete/README.md` are `named-file` on the basename `README.md` alone, because
  `scripts/README.md` changed. All five were read and dismissed: none mentions make targets, CI
  jobs, scripts or fuzzing. No later unit's brief was corrected.

### Review lanes (step 5) and triage (step 6)

There were 3 lanes, security, correctness and brief, each a fresh subagent in its own detached
worktree, given the diff and this brief. The selector skipped none. Counted by reading each report:
the brief lane raised 6 findings, security 5 and correctness 7, so 18 in all. Several overlap.

Fixed, in f294245:

- The `ci-result.sh` usage example in scripts/README.md failed, because `fuzz` is now expected.
  It now passes `fuzz` (brief lane).
- CONTRIBUTING and CHANGELOG overstated the property: not every target reads SQL back, and the
  backslash case is exempt. Both are reworded (brief lane). CONTRIBUTING also said a security
  finding goes privately through SECURITY.md, which a public scheduled run cannot honour. It now
  says CI findings are public on failure (security lane).
- MySQL multibyte client charsets (GBK, SJIS) are outside what a byte-wise oracle can see. Per the
  operator, this is a docs caveat in CONTRIBUTING and the sqllex doc, with no advisory, because
  quoting.go's own SECURITY comment already documents the class. No reproducer is in the record
  (security lane).
- The scheduled run shared `ci-refs/heads/main` with pushes for up to an hour. Per the operator,
  the schedule now has its own concurrency group, pinned by `ci_yml_schedule_own_concurrency_group`
  (brief, security and correctness lanes).
- The upload ran only on `if: failure()`, so a timed out (cancelled) job lost its inputs. It is now
  `failure() || cancelled()`, pinned (correctness lane).
- `FUZZTIME` `0s`, `0x` and `00m` passed the check but `go test` refuses them. They are now
  refused with exit 2, and the message says "positive duration". Valid but unusual forms such as
  `.5s` and `1µs` stay refused: the check is a deliberate subset (correctness lane).
- `make fuzz` spliced `FUZZTIME` and `GO` into the recipe unvalidated. Both are now exported
  target-specific variables, so the value reaches fuzz.sh's check as data, pinned by
  `make_fuzz_value_is_not_shell` (security and correctness lanes).
- The sqllex MySQL dialect did not read `"..."` as a string, which MySQL's default sql_mode does.
  It does now. The test name that contradicted its assertion is fixed, and the doc says where
  `\x` decoding departs from MySQL (correctness lane).
- `FuzzIdentifierQuoting` checked only the identifier list; it now checks the whole token stream.
  `FuzzMaskedProjection` also checks its token-kind shape (security lane).
- `ci_yml_fuzz_job_shape` did not pin FUZZTIME 5m/15s or the upload condition; it pins both now.
  The timeout target count excludes `tools/` and accepts any receiver name (correctness lane).
- In this brief, the "verbatim" ledger claim and the unevidenced local-fuzz claim are corrected
  above (brief lane).

Judged not to need a change, with reasons:

- MySQL `\x` decoding versus MySQL's real `\%`, `\_` and `\n`: exact for what EscapeString
  emits today, and FuzzEscapeString's ANSI read would catch a MySQL-only escaper drifting. It is
  now documented in sqllex (correctness lane).
- The star expansion and `refuseMaskedUse` branches of the OPA plugin are not fuzzed. The brief
  asks for the mask literal path, and star expansion renders through the same `maskedProjection`.
  This is recorded under Not done (security lane).
- The backslash defect has no owner beyond this record. It is carried in the index handover for
  the operator to schedule (brief lane).

### What the next units inherit

- 08 (community-docs): fuzzing exists, is documented in CONTRIBUTING (Testing, Fuzzing), and
  security findings route through SECURITY.md. That is worth naming for the Best Practices survey's
  dynamic analysis criteria.
- 09 (repo-settings): `Fuzz` is a job under `CI OK`, not a separate required check. The gate
  reports `workflow_call` for release runs.
- 10 (release-proof): the first remote run of the fuzz job, the `!inputs.ref` condition in a
  scheduled run, and the gate's `workflow_call` EVENT in a release run are all unproven until then.

## Not done, and why

- Known defect, not fixed here (operator ruling, as the brief requires): `quoting.EscapeString`
  doubles `\` for PostgreSQL and SQLite in non-parameterised mode, so a literal backslash comes
  back doubled. Failing input: `\` (`'\\'` reads back as `\\` under ANSI rules). It is not
  injection: the literal never closes early under either lexer, which `FuzzEscapeString`,
  `FuzzStringLiteral` and `FuzzMaskedProjection` assert for single-byte and UTF-8 connections. It is pinned by the skipped
  `TestEscapeStringANSIRoundTripBackslash`. A fix wants a per-dialect escaper (in a separate
  change), and once it lands the three targets' backslash exemption and the skip are removed.
- No fuzz finding was produced in the local runs, so nothing is reported for a security advisory.
  The security lane's multibyte charset point went to the operator, who ruled a docs caveat, not
  an advisory.
- The OPA plugin's star expansion branch and its `refuseMaskedUse` check are not fuzzed. The first
  renders through the same `maskedProjection` the target covers, and the second is an
  authorisation property, outside the brief's criteria.
