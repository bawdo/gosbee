# Unit 09: release-proof

Mode: implement
Close: auto
Branch: unit/09-release-proof
Review lanes: security, correctness, brief

## Items

- Precondition: the operator has merged `workstream/repo-quality` into `main` through
  a pull request, so the new workflows run from `main`.
- Cut a release candidate through the new pipeline, operator pushing the tag.
- Verify it end to end with the commands in SECURITY.md, exactly as written.
- Record the first published Scorecard result.
- Fix whatever the proof shows is wrong, or record it.

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

## Plan

## Outcome

## Not done, and why
