# Unit 10: release-proof

Mode: implement
Close: auto
Branch: unit/10-release-proof
Review lanes: security, correctness, brief

## Items

- Precondition: the operator has merged `workstream/repo-quality` into `main` through
  a pull request, so the new workflows run from `main`.
- Cut a release candidate through the new pipeline, operator pushing the tag.
- Verify it end to end with the commands in SECURITY.md, exactly as written.
- Record the first published Scorecard result.
- Fix whatever the proof shows is wrong, or record it.
- Before the release candidate, apply `scripts/repo-settings.sh harden` (from unit 09) on the
  operator's approval of its dry run, now that `main` no longer runs `re-actors/alls-green`.

## Acceptance criteria

- A signed annotated tag `vX.Y.Z-rc.1` (version chosen from CHANGELOG.md) on a `main`
  commit is pushed by the operator. Commands are handed over; the agent pushes
  nothing (remotes `local`).
- The Release run shows: CI passed first; the tag shape, on-main and signature checks
  passed; the build job held no write token; the release was created as a draft,
  assets uploaded, then published as a pre-release; the proxy and sum.golang.org
  checks passed.
- The release carries the four REPL binaries, the checksums file, the
  `.intoto.jsonl` bundle and the SBOM, and is marked immutable.
- Every verification command in SECURITY.md runs successfully against the rc as
  written. Any command that needed changing is fixed in SECURITY.md in this unit.
- Scorecard publishes a result for gosbee. The per-check results are pasted in the
  Outcome with the command that fetched them. Every check except Code-Review,
  Contributors, Branch-Protection and CII-Best-Practices scores 10, or the shortfall
  is explained under Not done with its cause. Signed-Releases may lag until enough
  releases carry provenance; say so if it does.
- The final closeout states the Scorecard result against the 2026-11-01 baseline
  (gosbee unscored; caddy_shack 8.3, jellyfish 8.0, go-blinkstick 7.6).

## Decisions

- OPERATOR RULING 2026-11-01 (after unit 04 closed): sequence is unit 09 applies `metadata` and
  `rulesets`, the operator merges `workstream/repo-quality` to `main` by PR, then this unit applies
  `harden` before cutting the rc. Renumbered from 09.
- 2026-11-01, precondition not met at open: `origin/main` at `ed10f4e` (#27), the integration
  branch 94 commits ahead and its remote copy 28 behind (at unit 06). The operator pushed it and
  opened PR #28. Branch cut only after that, off `8a5d877`.
- 2026-11-01, PR #28 is blocked by the `CodeQL` check: alert #5, high,
  `actions/cache-poisoning/poisonable-step` at `.github/workflows/ci.yml:92-96`, from the
  `ref: ${{ inputs.ref }}` checkouts, under `workflow_dispatch` and `schedule`. Fixed in this unit
  under the Item "fix whatever the proof shows is wrong", since #28 cannot merge without it.
- Diagnosis, read from github/codeql `actions/ql`: `CachePoisoningViaPoisonableStep.ql` looks at no
  cache step at all (and `setup-go` is not one of its `CacheWritingStep`s), so turning the Go cache
  off would not clear it. A first proposal to do that was withdrawn. The checkout counts as
  untrusted through `ActionsMutableRefCheckout`'s field-name heuristic (`.*(head|branch|ref).*` on a
  simple reference), which `inputs.ref` matches. The real gap behind it: a by-hand `release.yml`
  dispatch can name any existing tag, a non-`v*` one included, and `ci` runs that tag's code in
  main's context before `build` checks the tag name.
- OPERATOR RULING 2026-11-01: (b) move the tag-name check out of `build` into a read-only `tag` job
  that `ci` needs, and rename `ci.yml`'s `workflow_call` input `ref` to `tag` (the bare tag name;
  `ci.yml` builds `refs/tags/<tag>`). The Go cache stays on. Unprovable locally (no CodeQL CLI):
  proof is CodeQL on #28. Fallback if it still fires: dismiss #5 citing (b), operator's call.
- Caller census, `git grep -n -E 'inputs\.ref|inputs\.tag|ref: refs/tags|Check the tag name|workflow_call'`:
  `ci.yml` lines 12-18 (input), 28 and 35 (concurrency comment and group), 48, 140, 182, 227
  (checkouts), 168 and 174 (fuzz comment and `if`), 214 and 232 (gate comment and `EVENT`);
  `release.yml` 58, 66-72 (`ci` job), 88-99 (build's `TAG` and tag-name check), 106, 193, 271;
  `scripts/ci-result.sh` 23-24 (comment); `scripts/ci-result_test.sh` 226, 245, 248, 282 (exact
  strings). `scripts/repo-settings_test.sh`'s fake `ci.yml` uses no input and is left alone. The
  plans `2026-11-01-release-pipeline.md` and `2026-11-01-fuzzing.md` name `inputs.ref` and are left
  as written: they are closed units' plans, a record of what was planned then.

## Plan

## Outcome

- CodeQL alert #5 fix, `3fbca7e`: tests written first and seen to fail (`FAIL ci_yml_gate_shape:
  EVENT not passed through env`; `FAIL release_yml_tag_job_shape: no tag job in release.yml`).
  Mutations through `record-mutation.sh`, ledger outside the repository, all three KILLED:
  - `ci.yml` lint checkout back on `ref: ${{ inputs.ref }}`: first failing line
    `FAIL ci_yml_called_with_tag: ci.yml still reads inputs.ref`.
  - `release.yml` `ci` job `needs: tag` to `needs: []`: first failing line
    `FAIL release_yml_ci_needs_tag: ci does not need the tag job`.
  - `release.yml` tag regex with its `^` anchor dropped: first failing line
    `FAIL release_yml_tag_check_stops (x-v1.2.3): expected exit 1`.
- Verification at `3fbca7e`, `WOMBAT_BASE_REF=workstream/repo-quality scripts/wombat-verify.sh`, run
  outside the sandbox (the go build cache is not writable in it): tree clean, all 13 gates pass.
  actionlint clean; zizmor 1.31.1 `--offline` "No findings" (1 ignored, 3 suppressed).
- Not yet proved: that CodeQL clears alert #5. Only CodeQL on #28 can say.

## Not done, and why
