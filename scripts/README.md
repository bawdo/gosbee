# Development Scripts

This directory contains scripts to assist with development and testing.

## pre-ci-check.sh

**Purpose**: Validates your changes locally before pushing to CI, predicting the probability of GitHub Actions success.

**Usage**:
```bash
./scripts/pre-ci-check.sh              # run every check
./scripts/pre-ci-check.sh --fix gofmt  # apply gofmt -s -w first, then check
```

**What it checks**:
1. ✓ Go version compatibility
2. ✓ Dependency download
3. ✓ Code formatting (gofmt -s)
4. ✓ All tests with race detector
5. ✓ Linting (golangci-lint)
6. ✓ Test coverage (with target validation)
7. ✓ Build success

**Output**:
- Detailed report with pass/fail for each check
- CI success probability percentage
- Colour-coded summary
- Detailed logs saved to `coverage/pre-ci-*.txt`

**When to run**:
- Before committing significant changes
- Before creating a pull request
- When you want to ensure CI will pass
- To quickly validate your development environment

**Exit codes**:
- `0`: All checks passed (100% CI success probability)
- `1`: One or more checks failed

**Example output**:
```
╔════════════════════════════════════════════════════════════════════╗
║                     Pre-CI Validation Script                      ║
║                  Mimicking GitHub Actions Workflow                ║
╚════════════════════════════════════════════════════════════════════╝

Check Results:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ✓ Go Version
  ✓ Dependencies
  ✓ Code Formatting
  ✓ Tests
  ✓ Linting
  ✓ Coverage Generation
  ✓ Build

CI Success Probability: 100% 🎉
✓ All checks passed! CI should succeed.
```

## changelog.sh

**Purpose**: Reads `CHANGELOG.md` so GitHub releases are built from it. Used by
the `Changelog` CI check and the `Release` workflow.

**Usage**:
```bash
./scripts/changelog.sh check                 # validate headings, dates, links and tag coverage
./scripts/changelog.sh versions              # list released versions, newest first
./scripts/changelog.sh notes v0.2.0          # print the release notes for a version
./scripts/changelog.sh notes v0.2.0 bawdo/gosbee   # same, with relative links made absolute
```

See [Releasing](../CONTRIBUTING.md#releasing) for the full release process.

## ci-result.sh

**Purpose**: Decides the `CI OK` check, the single check the `main` ruleset
requires. The gate job in `.github/workflows/ci.yml` runs it after every other
job, whatever their outcome.

**Usage**:
```bash
EVENT=pull_request NEEDS='{"lint":{"result":"success"},"test":{"result":"success"}}' \
  ./scripts/ci-result.sh
./scripts/ci-result.sh --jobs   # list the jobs it expects
```

It prints each job's result and fails on a failed, cancelled, missing or
unexpected job, and on a skipped job unless the `skippable` table for that
event permits it. Adding a job to `ci.yml` means adding it to `JOBS` and to the
gate's `needs:` too; `ci-result_test.sh` fails until both agree. It needs `jq`.

**Tests**: `./scripts/ci-result_test.sh`, run by CI and by `wombat-verify.sh`.

## fuzz.sh

**Purpose**: Runs every native Go fuzz target in the module, one after
another, since `go test` fuzzes one target per run. `make fuzz` and the CI
`fuzz` job both call it.

**Usage**:
```bash
./scripts/fuzz.sh                 # 10s per target
FUZZTIME=1m ./scripts/fuzz.sh     # a Go duration, or an iteration count such as 500x
./scripts/fuzz.sh --list          # list "<package> <target>", fuzz nothing
```

A failing target does not stop the run: every target is fuzzed, the failures
are listed at the end and the exit is 1. A package that cannot be listed, or a
malformed `FUZZTIME`, stops it before anything is fuzzed. `go test` writes each
failing input to `<package>/testdata/fuzz/<target>/`.

**Tests**: `./scripts/fuzz_test.sh` (a fake `go`), run by CI and by
`wombat-verify.sh`.

## verify-tag.sh

**Purpose**: Checks a release tag is annotated and that GitHub verified its
signature. The `Release` workflow's build job runs it before building.

**Usage**:
```bash
GITHUB_REPOSITORY=bawdo/gosbee ./scripts/verify-tag.sh v0.3.0
```

Run it from a checkout of the tag, with an authenticated `gh`. It refuses a
tag name that is not `vX.Y.Z` or `vX.Y.Z-suffix` before any call, a
lightweight tag, a tag whose object on GitHub differs from the local one (moved
since checkout), and a signature GitHub did not verify. No key is imported:
GitHub does the verification and the script reads its verdict.

**Tests**: `./scripts/release-scripts_test.sh`.

## build-release.sh

**Purpose**: Builds the REPL release binaries for a tag. The `Release`
workflow's build job runs it.

**Usage**:
```bash
./scripts/build-release.sh v0.3.0   # from the repository root, with no dist/
```

It builds `dist/gosbee-repl_<tag>_<os>_<arch>` for Linux and macOS on amd64
and arm64 with `CGO_ENABLED=0 go build -trimpath -ldflags='-s -w'`, smoke
tests the binary for the host (it must start on the sqlite engine and exit
cleanly), writes CycloneDX JSON 1.6 SBOMs and then `dist/SHA256SUMS` listing
every binary and SBOM. The SBOMs come from `cyclonedx-gomod`, pinned in
`tools/go.mod`: `dist/gosbee-repl_<tag>_module.cdx.json` for the module (its
version, read from the tag at HEAD, must be `<tag>`) and
`dist/gosbee-repl_<tag>_<os>_<arch>.cdx.json` for each binary. The SBOM tool
is third-party code that runs after the binaries exist, so they are
checksummed before it runs, and a binary changed after it fails the build,
named. It refuses a malformed tag and an existing `dist/` before building
anything, and a failed SBOM stops it before `SHA256SUMS` is written. It needs
`jq`.

**Tests**: `./scripts/release-scripts_test.sh`.

## publish-release.sh

**Purpose**: Publishes the GitHub release for a tag, or checks that it can be
published. The `Release` workflow's publish job runs it, and it is the only
script that writes to a release.

**Usage**:
```bash
GH_REPO=bawdo/gosbee ./scripts/publish-release.sh --check v0.3.0 "$TAG_OBJECT"
GH_REPO=bawdo/gosbee ./scripts/publish-release.sh v0.3.0 "$TAG_OBJECT" notes.md bundle.json
```

`TAG_OBJECT` is the sha of the annotated tag object the build verified
(`git rev-parse refs/tags/v0.3.0` in its checkout). Both forms refuse a
release already published and a tag on GitHub that no longer points at
`TAG_OBJECT`, naming both shas, so a tag moved since the build is never
released. `--check` only reads: the publish job runs it before its first
write, the attestation. Only `gh`'s own "release not found" counts as no
release: any other `gh` failure stops it with `gh`'s message.

Publishing runs from a directory holding `dist/`. It attaches exactly the
files `dist/SHA256SUMS` lists, `SHA256SUMS` itself and the provenance bundle
(compacted to `dist/gosbee-repl_<tag>_provenance.intoto.jsonl`), each once.
Every input is checked before any `gh` call. A leftover draft is replaced,
and the tag is read once more just before the release is created. The
release is created as a draft with its assets, then published. A tag with a
suffix is a pre-release and is never marked latest.

**Tests**: `./scripts/release-scripts_test.sh`.

## check-module-proxy.sh

**Purpose**: Checks that proxy.golang.org serves a released version at its
tagged commit and that sum.golang.org records it. The `Release` workflow's
proxy job runs it after publishing.

**Usage**:
```bash
./scripts/check-module-proxy.sh github.com/bawdo/gosbee v0.3.0 "$(git rev-parse 'v0.3.0^{commit}')"
```

It refuses a module path that is not lower case, a malformed tag and anything
but a 40-hex commit before any request. It asks
`https://proxy.golang.org/<module>/@v/<tag>.info` up to 5 times and
`https://sum.golang.org/lookup/<module>@<tag>` up to 2, 5 seconds apart, with
`curl -fsS --max-time 5`, so it gives up within about a minute. A commit
mismatch is never retried: it means the tag moved after the proxy fetched it,
which needs a `retract` and a new tag. A failure is one `::error::` line on
stdout. It needs `curl` and `jq`.

**Tests**: `./scripts/release-scripts_test.sh`.

## release-scripts_test.sh

**Purpose**: Tests `verify-tag.sh`, `publish-release.sh`,
`check-module-proxy.sh` and `build-release.sh`.

**Usage**:
```bash
./scripts/release-scripts_test.sh
```

Every `gh` and `curl` call goes to a fake, so it needs no network or token.
The build cases cross-compile the four binaries and write their SBOMs, so they
run last. It prints `ok <case>` per case and stops at the first failure, on a
line starting `FAIL`. Run by CI and by `wombat-verify.sh`. It needs `jq` and
Go.
