# Unit 03: release-supply-chain

Mode: plan
Close: auto
Branch: unit/03-release-supply-chain
Review lanes: security, correctness, brief

## Items

- Add a module proxy and checksum database check to `release.yml` (reference:
  bawdo/go-blinkstick `release.yml`, `proxy` job).
- Generate an SBOM per release, attach it and cover it by the attestation.
- Choose the SBOM tool and format, and record why.
- Update CHANGELOG.md.

## Acceptance criteria

- A job after publish (no checkout, no write permission, `timeout-minutes` set)
  requests `https://proxy.golang.org/github.com/bawdo/gosbee/@v/<tag>.info` with a
  bounded retry (no more than about a minute in total), checks `.Origin.Hash` equals
  the tagged commit, and checks `https://sum.golang.org/lookup/github.com/bawdo/gosbee@<tag>`
  records the version. A mismatch or a miss fails the run with an `::error::` naming
  the cause.
- Each release attaches an SBOM in SPDX JSON or CycloneDX JSON describing the module
  and its dependencies (and the REPL binaries where the tool supports it).
- The SBOM tool is pinned either through `tools/go.mod` (so Dependabot keeps it
  current) or as a SHA-pinned action. No unpinned download.
- The SBOM is generated in the build job (read-only token), listed in the checksums
  file, and therefore covered by the `subject-checksums` attestation.
- The tool and format choice, and the reason, are written under Decisions.
- zizmor reports no findings; any new script passes shellcheck and is added to the
  shellcheck gate list in `scripts/wombat-verify.sh`.
- CHANGELOG.md has an `[Unreleased]` entry.

## Decisions

- **Re-cut.** Unit 02's handover said a re-cut is due before 03 or 09 opens. At open I put three
  options to the operator. They answered the commit question and not that one, so 03 runs as
  briefed: a plan that sits on top of the 02 plan. The re-cut (an implement unit that builds the
  02 and 03 plans before 09) is still open and goes back to the operator at the hand back.
- **The plan builds on 02's plan, not on today's `release.yml`.** Every task assumes 02's six tasks
  have landed: `scripts/build-release.sh`, `scripts/publish-release.sh`,
  `scripts/release-scripts_test.sh`, and the ci, build and publish jobs.
- **SBOM tool: `cyclonedx-gomod` v1.12.0, through a `tool` directive in `tools/go.mod`. Format:
  CycloneDX JSON, spec 1.6.** Why: it reads Go's own module graph (`mod`) and a binary's embedded
  build info (`bin`), so it describes both the module and the exact modules linked into each REPL
  binary. Build info survives `-s -w`. It is pinned by `tools/go.sum`, and Dependabot already
  watches `/tools`, so it stays current without a new mechanism. It downloads nothing at run time.
  Flags checked against the v1.12.0 source: `mod` has `-json -licenses -type -output-version
  -output`, and `bin` has `-json -std -version`. Its module wants `go 1.26.0`, which matches
  `tools/go.mod`. Rejected: syft through `anchore/sbom-action` (the action downloads a syft release
  at run time, pinned by version rather than by hash, and it catalogues by filesystem heuristics
  rather than Go's module graph); `spdx-sbom-generator` (unmaintained); a hand-rolled
  `go version -m` dump (not a standard format). The brief allows either format, and this tool
  writes CycloneDX only.
- **SBOM assets.** `gosbee-repl_<tag>_module.cdx.json` (module and dependencies, with licences)
  and one `gosbee-repl_<tag>_<os>_<arch>.cdx.json` per binary (including the standard library). Both
  names sit inside 02's `gosbee-repl_<tag>_*` globs, so the artifact upload and
  `publish-release.sh` take them unchanged. `SHA256SUMS` grows from four lines to nine, so
  `subject-checksums` attests them.
- **`actions/attest-sbom` not used.** The criterion is coverage by the `subject-checksums`
  attestation, and a second attestation adds an action and another `id-token` use for no criterion.
- **Proxy check as a script, delivered by artifact.** `scripts/check-module-proxy.sh` holds the
  logic, so it gets fake `curl` tests and shellcheck. The criterion says the job has no checkout, so
  the build job (read-only token, tag checkout) uploads the script as artifact `proxy-check` and the
  proxy job downloads it and runs it with `bash`. Inline YAML was rejected because it cannot be tested.
- **The proxy job checks the build job's `commit` output, not `GITHUB_SHA`.** On a dispatch re-run,
  `GITHUB_SHA` is the dispatching branch's commit.
- **Retry budget.** Proxy: up to 5 attempts, 5 seconds apart. Sumdb: up to 2. Every `curl` uses
  `--max-time 5`. Worst case is 60 seconds across both endpoints (45 + 15), and the job's
  `timeout-minutes` is 5. A hash mismatch is not retried, because the proxy never changes a
  version's commit.
- **Proxy job permissions: `contents: read`**, as go-blinkstick sets it. It writes nothing.
- **Caller census** (before any edit, for the dist and `SHA256SUMS` contract):
  `grep -n 'SHA256SUMS\|gosbee-repl_\|dist/\|subject-' docs/workstreams/repo-quality/plans/2026-11-01-release-pipeline.md .github/workflows/*.yml scripts/* CONTRIBUTING.md README.md`.
  Readers: 02 plan lines 17, 101, 118-122, 152, 155-157, 180; `release.yml` lines 71-89 (rewritten
  by 02); `CONTRIBUTING.md:443`. 02's `build_writes_four_binaries_and_sums` (4 lines) is amended
  by this plan's Task 1.
- **A defect found in 02's plan, not fixed here.** Line 101 compacts the bundle to
  `dist/gosbee-repl_<tag>_provenance.intoto.jsonl` and then passes both `dist/gosbee-repl_<tag>_*`
  and `<bundle>` to `gh release create`, so the bundle is listed twice. That is a closed unit's
  plan, so it goes to the operator and the builder rather than being edited here. This plan names it
  under "Notes for the builder".
- **The handover's `--signer-workflow` note.** 02 asked that 03's verification docs pin
  `--signer-workflow` to `release.yml`, but none of 03's Items or criteria writes verification
  docs, and unit 07 documents what 02 and 03 ship. Left to 07 and carried forward in the handover.
- **Reference.** go-blinkstick's `proxy` job at `89bfcec`, read from
  `/Users/bawdo/working/projects/blinkstick-playground/go-blinkstick`. The live shapes were
  confirmed against v0.2.1: `.info` carries `Origin.Hash` `57b8d43...`, which equals
  `v0.2.1^{commit}`, and the sumdb lookup prints `github.com/bawdo/gosbee v0.2.1 h1:...`.

## Plan

`docs/workstreams/repo-quality/plans/2026-11-01-release-supply-chain.md`

## Outcome

Close mode as run: `auto`. Plan only, as the mode says. The plan's handoff to execution was not
taken.

Landed: `docs/workstreams/repo-quality/plans/2026-11-01-release-supply-chain.md`.

```
$ grep -c '^### Task' docs/workstreams/repo-quality/plans/2026-11-01-release-supply-chain.md
4
$ grep -c '^- \[ \]' docs/workstreams/repo-quality/plans/2026-11-01-release-supply-chain.md
20
```

Task 1 adds SBOMs to `build-release.sh`, Task 2 adds `check-module-proxy.sh`, Task 3 adds the
`proxy` job, and Task 4 covers the gates and docs. Every brief criterion maps to a task: the proxy
job (Tasks 2 and 3), the SBOM asset and its pinning (Task 1), generation in the build job, listing
in `SHA256SUMS` and the attestation (Task 1), the tool choice (Decisions), zizmor, shellcheck and
the gate list (Tasks 3 and 4), and CHANGELOG (Task 4).

Verification at step 1, `WOMBAT_BASE_REF=workstream/repo-quality scripts/wombat-verify.sh` (run
outside the sandbox), recorded at `839eb43`: fmt, vet, lint, tidy, changelog, vuln, shellcheck and
test `pass`. zizmor `skipped`, reason "zizmor not installed (CI runs it)". Tree `clean`.

Lane selection, `select-gates.sh .`: `brief` runs. `security` and `correctness` were skipped, both
with the reason "no changed path matched". This unit changed only Markdown under
`docs/workstreams/`.

Scan, `scan-secrets.sh .`: `scan clean: 12 files`.

Sweep, `sweep-docs.sh . --base workstream/repo-quality`: printed no candidates. No `--claim` was
passed, because a plan falsifies no document. Nothing was rewritten, deleted or dismissed, and no
later brief was corrected.

Questions that fired: at open, the re-cut question. The operator answered the commit question
("You may commit - follow all rules.") but not the re-cut one, so the unit ran as briefed. Standing
commit grant given in conversation.

Inherited by whoever builds this: the 02 plan's double-listed bundle (see Decisions), and the
`--signer-workflow` verification docs, now left to unit 07.

## Not done, and why

- The re-cut that 02's handover called due before 03 or 09 opens. It is not this unit's to make,
  and the operator has not ruled on it yet.
