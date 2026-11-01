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

## Plan

## Outcome

## Not done, and why
