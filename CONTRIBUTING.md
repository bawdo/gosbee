# Contributing to gosbee

Thank you for your interest in contributing to gosbee! We welcome contributions
of all kinds — bug reports, documentation improvements, new features, and plugin
implementations.

This project aims to provide a robust, idiomatic Go implementation of the AST
query builder pattern pioneered by Ruby's Arel. Whether you're fixing a typo or
adding a major feature, your contribution helps make SQL query building in Go
more expressive and powerful.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [How Can I Contribute?](#how-can-i-contribute)
- [Development Workflow](#development-workflow)
- [Project Structure](#project-structure)
- [Writing Plugins](#writing-plugins)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Documentation](#documentation)
- [Pull Request Process](#pull-request-process)
- [Releasing](#releasing)
- [Badges](#badges)

## Code of Conduct

This project follows the [Contributor Covenant 2.1](CODE_OF_CONDUCT.md). By
taking part you agree to uphold it. Report unacceptable behaviour privately, as
it describes.

## Getting Started

1. **Fork and clone the repository:**
   ```bash
   git clone https://github.com/YOUR_USERNAME/gosbee.git
   cd gosbee
   ```

2. **Download dependencies:**
   ```bash
   make deps        # go mod download
   ```
   `make help` lists every target.

3. **Run the test suite to verify everything works:**
   ```bash
   go test ./...
   ```

   All tests should pass. If they don't, please open an issue.

4. **Try the REPL to get a feel for the library:**
   ```bash
   go run ./cmd/repl
   ```

## How Can I Contribute?

### Reporting Bugs

Found a bug? Open an issue with the **Bug report** form, which asks for a
minimal program or REPL session, the SQL generated and the SQL you expected,
the dialect, any plugins, and your gosbee and Go versions.

A security problem (SQL injection, or a way around the soft delete or OPA
plugins) is never a public issue: report it privately through
[SECURITY.md](SECURITY.md).

### Suggesting Features

Have an idea for a new feature? Open an issue with the **Feature request**
form: describe the problem you want solved, and if you can, the Go you would
like to write or the SQL you want generated.

### Improving Documentation

Documentation improvements are always welcome! This includes:
- Fixing typos or clarifying explanations
- Adding examples to the guides
- Writing tutorials or blog posts
- Improving code comments

See the [Documentation](#documentation) section below for where different docs live.

### Writing Code

Code contributions might include:
- Bug fixes
- New SQL features (window functions, CTEs, etc.)
- New dialect support (Oracle, SQL Server, etc.)
- Plugin implementations
- Performance improvements
- REPL enhancements

## Development Workflow

1. **Create a branch** for your work:
   ```bash
   git checkout -b your-change
   ```

2. **Make your changes** following the [Coding Standards](#coding-standards).

3. **Write or update tests** to cover your changes.

4. **Run the quick checks** while you work:
   ```bash
   make test        # go test ./...
   make check       # gofmt -s check, go vet, golangci-lint, race tests
   make fmt         # rewrite formatting with gofmt -s
   ```

5. **Check test coverage** (optional):
   ```bash
   make coverage       # writes coverage/coverage.out and prints the summary
   make coverage-html  # opens the HTML report in a browser
   ```

6. **Run the pre-CI validation** before pushing:
   ```bash
   make pre-ci         # ./scripts/pre-ci-check.sh
   make pre-ci-fix     # auto-fix gofmt formatting first, then run every check
   ```

   This script mimics the GitHub Actions workflow and runs 8 checks:

   | # | Check | Tool |
   |---|-------|------|
   | 1 | Go version compatibility | `go` |
   | 2 | Dependency download | `go mod download` |
   | 3 | Code formatting | `gofmt -s` (auto-fixable via `--fix gofmt`) |
   | 4 | Tests with race detector | `go test -race` |
   | 5 | Linting | `golangci-lint` |
   | 6 | Test coverage (local guide: 80% raw; Codecov gates on `.codecov.yml`) | `go test -coverprofile` |
   | 7 | Build | `go build` |
   | 8 | Vulnerability scan | `govulncheck` |

   It generates a CI readiness report showing which checks passed or failed,
   the overall success probability, and coverage trend (delta from the
   previous run). Detailed logs are saved to `coverage/pre-ci-*.txt`. CI runs
   more than this, including `go mod tidy -diff`, shellcheck, the script
   tests, the changelog check, zizmor and tests on macOS and the oldest
   supported Go: see [Branch Rules](#branch-rules) for the full list.

   **Dependencies**

   The following tools must be installed for all checks to run. `govulncheck`
   needs no install: it is pinned in `tools/go.mod` and run with `go tool`.

   | Tool | Install |
   |------|---------|
   | `golangci-lint` | `brew install golangci-lint` |
   | `bc` | Pre-installed on macOS and Linux |

7. **Update `CHANGELOG.md`** under `[Unreleased]` for any user-visible change.

8. **Commit your changes**, signed, with clear commit messages in past tense:
   ```
   Added support for LATERAL JOIN in PostgreSQL visitor

   - Implemented VisitLateralJoin in baseVisitor
   - Added Lateral bool field to JoinNode
   - Updated PostgreSQL tests to cover LATERAL JOIN syntax
   ```

9. **Push to your fork** and open a pull request.

## Project Structure

```
gosbee/
├── nodes/              # AST node types (Table, Attribute, predicates, etc.)
├── managers/           # High-level DSL (SelectManager, InsertManager, etc.)
├── visitors/           # SQL dialect generators (PostgreSQL, MySQL, SQLite)
├── plugins/            # AST transformer plugins
│   ├── softdelete/     # Soft-delete filtering (proof of concept)
│   └── opa/            # OPA policy integration (proof of concept)
├── cmd/
│   └── repl/           # Interactive REPL
├── internal/           # Private helpers (quoting, sqllex, testutil)
├── docs/
│   ├── guide/          # User-facing documentation
│   ├── development/    # Contributor documentation
│   └── workstreams/    # Multi-unit work records, with their specs and plans
├── scripts/            # CI, release and pre-CI scripts, with their tests
└── tools/              # Pinned Go tools (govulncheck, cyclonedx-gomod)
```

### Key Architecture Documents

- **[Architecture Overview](docs/development/architecture.md)** — core concepts,
  visitor pattern, outer dispatch
- **[Plugin System](plugins/README.md)** — transformer architecture and writing
  plugins
- **[Writing Plugins](docs/development/writing-plugins.md)** — detailed plugin
  development guide

## Writing Plugins

Plugins are AST transformers that run before SQL generation. They're a powerful
way to add cross-cutting concerns like access control, soft-delete filtering, or
multi-tenancy without modifying query-building code.

**Quick start:**
1. Create a new directory under `plugins/`
2. Implement the `Transformer` interface (4 methods: TransformSelect,
   TransformInsert, TransformUpdate, TransformDelete)
3. Embed `plugins.BaseTransformer` for no-op defaults
4. Write tests using the AST and visitors directly

**Example:**
```go
package mytenant

import (
    "github.com/bawdo/gosbee/nodes"
    "github.com/bawdo/gosbee/plugins"
)

type MultiTenant struct {
    plugins.BaseTransformer
    TenantID int
}

func (mt *MultiTenant) TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error) {
    for _, ref := range plugins.CollectTables(core) {
        attr := nodes.NewAttribute(ref.Relation, "tenant_id")
        core.Wheres = append(core.Wheres, attr.Eq(mt.TenantID))
    }
    return core, nil
}
```

For a complete guide, see:
- **[Plugin System README](plugins/README.md)** — architecture and full examples
- **[Writing Plugins](docs/development/writing-plugins.md)** — detailed guide
- **[Soft Delete Plugin](plugins/softdelete/README.md)** — reference implementation
- **[OPA Plugin](plugins/opa/README.md)** — advanced reference implementation

## Coding Standards

### Go Style

- Follow standard Go conventions ([Effective Go](https://golang.org/doc/effective_go.html))
- Use `gofmt` for formatting (enforced by `go fmt ./...`)
- Run `go vet ./...` before committing
- Use Australian English in documentation and comments ;-)

### Naming Conventions

- **Exported types/functions:** Use clear, descriptive names (e.g.
  `SelectManager`, `NewPostgresVisitor`)
- **Internal helpers:** Keep concise but meaningful (e.g. `cloneCore`,
  `collectTables`)
- **Test functions:** Use `TestFunctionName_Scenario` (e.g.
  `TestSelectManager_Where_MultipleConditions`)

### AST Node Rules

- All AST nodes must implement the `nodes.Node` interface
- Nodes should be immutable where possible (return new nodes instead of
  modifying)
- Use struct embedding for shared behaviour (e.g. `Predications`, `Combinable`,
  `Arithmetics`)
- Set the `self` field when embedding to enable method chaining

### Visitor Pattern

- The `Visitor` interface lives in `nodes/node.go` to avoid circular
  dependencies
- Use the **outer-dispatch pattern**: `baseVisitor.outer` enables virtual method
  resolution for dialect-specific overrides
- When adding a new node type, update:
  - the `Visitor` interface in `nodes/node.go`
  - `baseVisitor` in `visitors/visitor.go`, with a default implementation
  - `DotVisitor` in `visitors/dot_visitor.go`, for AST visualisation
  - `FormattingVisitor` in `visitors/formatting.go`
  - the plugin AST `rewriter` in `plugins/walk.go`
  - `StubVisitor` and `StubParamVisitor` in `internal/testutil/stub_visitor.go`
  - the `stubVisitor` in `nodes/nodes_test.go`

  `go build ./... && go vet ./...` names any implementation still missing the
  new method.

## Testing

We aim for comprehensive test coverage. When adding new features or fixing bugs:

1. **Write tests first** (TDD approach) or alongside your implementation
2. **Test at the right level:**
   - AST node tests: verify node construction and behaviour
   - Manager tests: verify fluent API and query building
   - Visitor tests: verify SQL generation for each dialect
   - Plugin tests: verify AST transformation logic
3. **Use table-driven tests** for multiple scenarios:
   ```go
   tests := []struct {
       name     string
       input    Node
       expected string
   }{
       {"equality", col.Eq(42), `"users"."age" = 42`},
       {"greater than", col.Gt(18), `"users"."age" > 18`},
   }
   ```
4. **Keep tests focused:** one concept per test function
5. **Use descriptive test names** that explain what's being tested

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests for a specific package
go test ./nodes

# Run tests with coverage
go test -cover ./...

# Run a specific test
go test -run TestSelectManager_Where ./managers
```

### Examples

Testable examples (`func ExampleXxx()` with an `// Output:` block) live in
external test packages beside the code: `managers`, `visitors`,
`plugins/softdelete` and `plugins/opa`. `go test` checks their output and
pkg.go.dev shows them, so a new public pattern earns one.

### Fuzzing

Native Go fuzz targets (`func FuzzXxx(f *testing.F)`) cover identifier quoting
and string escaping for each dialect (`internal/quoting`, `visitors`), the OPA
plugin's column mask path (`plugins/opa`), and the REPL's input parsing
(`cmd/repl`). Each one checks a property, not only that nothing panics. The
quoting, visitor and OPA targets read the rendered SQL back with
`internal/sqllex` under the dialect's own quoting rules: an identifier or value
must come back as one token, never closing its quotes early, and must decode to
what went in (except a backslash in a PostgreSQL or SQLite literal, below). The
REPL targets check that tokenising keeps every byte and that a quoted value
parses back to itself.

`internal/sqllex` reads bytes, so these properties hold for single-byte and
UTF-8 connections only. A MySQL connection with a multibyte client charset such
as GBK or SJIS is outside them, which is one more reason to keep the default
parameterised mode.

```bash
make fuzz                  # every target in turn, 10s each
make fuzz FUZZTIME=1m      # longer; FUZZTIME also takes an iteration count, 500x
./scripts/fuzz.sh --list   # list the targets
```

The seeds added with `f.Add` run as ordinary tests in every `go test` run.
When fuzzing finds a failing input, `go test` writes it to
`<package>/testdata/fuzz/<Target>/`. Commit that file alongside the fix, so it
stays as a regression seed. CI fuzzes every target weekly (5m each) and on a
manual run of the CI workflow (15s each), never on a pull request, and keeps
any failing inputs for 14 days as the `fuzz-failures` artifact. The CI logs and
artifacts are public, so a finding there is disclosed the moment the run
fails: fix one that could be a security issue straight away, and report it
through [SECURITY.md](SECURITY.md). A security finding from a local run is
reported privately through SECURITY.md, not in a public issue.

`TestEscapeStringANSIRoundTripBackslash` is skipped on purpose: it records a
known defect, where string escaping doubles a backslash for PostgreSQL and
SQLite, for which a backslash is an ordinary character.

## Documentation

Documentation lives in multiple places depending on the audience:

### User-Facing Guides (`docs/guide/`)

For developers **using** the library in their projects:
- **[Getting Started](docs/guide/getting-started.md)** — installation, core
  concepts, API overview
- **[Visitor Dialects](docs/guide/visitors.md)** — SQL dialect selection and
  parameterisation
- **[Using Plugins](docs/guide/plugins.md)** — registering and using plugins

### Development Documentation (`docs/development/`)

For **contributors** working on the library itself:
- **[Architecture](docs/development/architecture.md)** — core design patterns
- **[Writing Plugins](docs/development/writing-plugins.md)** — plugin development
  guide

### Plugin Documentation (`plugins/`)

Each plugin has its own README with:
- How it works
- Configuration options
- Code examples
- REPL usage

### REPL Documentation (`cmd/repl/`)

- **[REPL README](cmd/repl/README.md)** — interactive shell usage and commands

### Workstreams (`docs/workstreams/`)

The records of larger work cut into several units, each with its briefs and,
where a unit earned them, its specs and plans.

### When to Update Documentation

- **User guides:** When adding new public API features or changing behaviour
- **Plugin READMEs:** When adding plugin options or changing behaviour
- **Architecture docs:** When making significant structural changes
- **Code comments:** When the code isn't self-evident
- **Examples:** When demonstrating a new pattern or feature (see
  [Examples](#examples))

## Pull Request Process

1. **Ensure all tests pass** and linters are happy
2. **Update documentation** if you've changed behaviour or added features
3. **Fill in the pull request template**, and write a clear description
   that explains:
   - What problem you're solving
   - How you've solved it
   - Any breaking changes (if applicable)
   - Links to related issues
4. **Keep PRs focused:** one feature or fix per PR when possible
5. **Respond to feedback** — reviews are collaborative, not adversarial
6. **Be patient**: maintainers may take time to review

`.github/CODEOWNERS` requests a review from the maintainer on every pull
request opened by someone else.

### PR Title Format

Use clear, descriptive titles in past tense:
- `Added LATERAL JOIN support for PostgreSQL`
- `Fixed parameter indexing in MySQL visitor`
- `Updated getting-started guide with window function examples`

### Breaking Changes

If your change breaks backwards compatibility:
1. Mark it clearly in the PR description
2. Explain why it's necessary
3. Provide migration guidance if possible

### Branch Rules

`main` only changes through pull requests. Direct pushes, force pushes and
deleting `main` are all blocked. Pull requests are squash merged, so GitHub
signs the resulting commit (a rebase merge cannot be signed). The pull request
title becomes the commit subject, so write it in the past tense. Auto-merge is
allowed, and the branch is deleted once merged. A pull request must be up to
date with `main` before it merges (use **Update branch**), and can merge once
these checks pass:

- `CI OK`, which passes only when every job in the CI workflow passed.
  `scripts/ci-result.sh` decides this from each job's result: a failed,
  cancelled or missing job fails it, and so does a skipped job unless the
  script's per-event table permits that skip (only `Fuzz`, which runs on the
  weekly schedule and a manual dispatch alone). The jobs:
  - `Lint and audit`: golangci-lint, shellcheck over `scripts/*.sh` (a pinned
    release, checked by sha256), the `ci-result.sh`, `fuzz.sh` and
    `repo-settings.sh` tests, `go mod tidy -diff`, the changelog check, the
    release script tests
    (`scripts/release-scripts_test.sh`, which cover the build with its SBOMs,
    publishing and the module proxy check), govulncheck and a zizmor audit of
    the workflows
  - `Test`: Linux on the oldest supported Go (`oldstable`) and the current one
    (`stable`), both with the race detector, plus macOS on `stable`.
  - `Fuzz`: every fuzz target in turn (see [Fuzzing](#fuzzing)). Skipped on
    pull requests, pushes and release runs.
- `Analyze (go)` (CodeQL), and CodeQL must report no alert at `error` severity
  and no security alert at `high` or above.

Coverage is measured and uploaded to Codecov by the separate `Coverage`
workflow (`coverage.yml`), which is not a required check. It lives outside CI
because the `Release` workflow calls CI, and keeping the Codecov OIDC token out
of CI means no job in a release except publish holds `id-token`. Codecov's
project and patch statuses gate on the coverage targets in `.codecov.yml`.

CI also runs on every push to `main` and weekly, so a newly published
vulnerability fails the build without waiting for a pull request.

Every commit must be signed (GPG, SSH or S/MIME) and every conversation on
the pull request resolved. No approving review is required. Pull requests from
forks need a maintainer to approve their CI run before it starts.

### Repository Settings

These rules, and the rest of the repository's GitHub settings, are kept in
`scripts/repo-settings.sh` rather than set by hand, so they can be read,
reviewed and re-applied. It runs one phase at a time, as a repository admin
with `gh` logged in:

```bash
DRY_RUN=1 scripts/repo-settings.sh rulesets   # print each write and its body; send nothing
scripts/repo-settings.sh rulesets             # apply
scripts/repo-settings.sh show                 # print the current settings
```

- `metadata`: description, homepage, topics, squash merge only, auto-merge,
  **Update branch**, and deleting the branch on merge.
- `rulesets`: `Protect main` (the branch rules above, with no bypass),
  `Protect release tags` (a `v*` tag can never be moved or deleted) and
  `Protect release tag creation` (only an admin can create a `v*` tag). It
  refuses to run until a successful CI run on `main` has a `CI OK` job.
- `harden`: Actions limited to GitHub's own and the third-party actions the
  workflows use (it prints the list, read from their `uses:` lines, and
  refuses an action not pinned to a commit SHA), SHA pinning required, a
  read-only default token, fork approval for all external contributors,
  CodeQL default setup with the `extended` suite, private vulnerability
  reporting, immutable releases, and the `release` environment that the
  publish job runs in, deployable only from `v*` tags and `main`.

Every write sends the whole desired state (rulesets and environment policies
are found by name and replaced in place), so any phase can be re-run, and
each prints its method and path before it is sent. `REPO` names another
repository (default `bawdo/gosbee`). The `harden` allowlist is computed from
the workflows as they are checked out, so run it from an up to date `main`,
and run it again after a pull request adds a third-party action, or that
action is refused. `scripts/repo-settings_test.sh` checks every phase against
a fake `gh`; CI runs it.

## Releasing

Releases are built from [CHANGELOG.md](CHANGELOG.md), so the changelog is the
only place release notes are written.

1. In a pull request, add a section for the new version at the top of
   `CHANGELOG.md`, following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/):
   ```markdown
   ## [v0.3.0] - 2026-11-15
   ...
   [v0.3.0]: https://github.com/bawdo/gosbee/compare/v0.2.0...v0.3.0
   ```
   The `Changelog` check (`scripts/changelog.sh check`) fails the pull request
   if a heading is malformed, a version is missing its date or link reference,
   or an existing tag has no section.
2. Merge it, then tag the merged commit on `main` and push the tag:
   ```bash
   git pull
   git tag -s v0.3.0 -m v0.3.0
   git push origin v0.3.0
   ```
3. The `Release` workflow runs in five stages:
   - **Tag**, with no token scope and no checkout, refuses a tag name that is
     not `vX.Y.Z` or `vX.Y.Z-suffix`, so no other tag's code is ever run.
   - **CI** then runs on the tag itself (the whole CI workflow, called with
     the tag name, which it checks out as `refs/tags/<tag>`).
   - **Build**, with a read-only token, checks the tag is annotated and its signature verified by GitHub
     (`scripts/verify-tag.sh`), is on `main` and has a changelog section. It
     builds the REPL binaries for Linux and macOS on amd64 and arm64
     (`scripts/build-release.sh`), smoke tests the Linux amd64 one and writes
     a CycloneDX SBOM for the module (`gosbee-repl_<tag>_module.cdx.json`) and
     one per binary (`gosbee-repl_<tag>_<os>_<arch>.cdx.json`), with
     `cyclonedx-gomod` pinned in `tools/go.mod`. `SHA256SUMS` lists every
     binary and SBOM, so the provenance attestation covers the SBOMs too. A
     failed SBOM fails the build.
   - **Publish**, the only job that can write, checks out
     `scripts/publish-release.sh` from the commit the build job built, never
     the tag by name. Before its first write (the attestation, which also
     makes a public transparency log entry) it checks, read only, that the
     release is not already published and that the tag on GitHub still
     points at the tag object the build verified
     (`scripts/publish-release.sh --check`). A tag moved during the build is
     refused there, naming both tag objects. It then checks the binaries
     against `SHA256SUMS`, attests their build provenance and publishes
     through a draft (`scripts/publish-release.sh`, which reads the tag once
     more just before it creates the release): the draft is created with
     every asset attached, then published. Any `gh` failure but "release not
     found" stops it rather than being read as no release. The
     attestation's Sigstore bundle is attached as
     `gosbee-repl_<tag>_provenance.intoto.jsonl`, so
     `gh attestation verify --bundle` can check a binary without the
     attestation API.
   - **Proxy**, after publish and with no checkout, checks that
     proxy.golang.org serves the version at the tagged commit and that
     sum.golang.org records it (`scripts/check-module-proxy.sh`, retrying for
     about a minute while the proxy fetches a new version). It runs after
     publish, so it reports a problem rather than stopping the release. A
     commit mismatch means the tag moved after the proxy first fetched it,
     which nothing undoes: add a `retract` for the version to `go.mod` and
     release a new tag.

   The release notes are that version's changelog section. A tag with a
   suffix (`v0.3.0-rc.1`) is published as a pre-release and is never marked
   latest, and gets the same SBOMs and proxy check as any other.

A published release is never changed, its notes included, so a changelog fix
made after release lives in `CHANGELOG.md` only. Release tags (`v*`) must never
be moved or deleted once pushed, and the `Protect release tags` ruleset
refuses both (see [Repository Settings](#repository-settings)). Publish's
checks also refuse a tag moved during a run.

If a release run fails before the release is published, fix the cause and
recover it from the Actions tab by running the `Release` workflow from `main`
with the existing tag: a leftover draft is replaced and the release published.
That does not help once the release is published: publish's check refuses it
("already published") before anything is written, and the proxy job, which
needs publish to succeed, is skipped. To repeat a failed proxy check:

- use "Re-run failed jobs" on the original run. When publish succeeded, that
  re-runs the proxy job alone, but only while the run's `proxy-check` artifact
  lasts (1 day);
- otherwise run the check by hand, from a checkout of the tag:
  ```bash
  scripts/check-module-proxy.sh github.com/bawdo/gosbee v0.3.0 "$(git rev-parse 'v0.3.0^{commit}')"
  ```

Tags made before this pipeline existed (`v0.2.1` and earlier) cannot be re-run
by hand, because they do not carry `scripts/verify-tag.sh`, so the build job
fails.

A run started by hand (`workflow_dispatch`) uses `release.yml`, and the
`ci.yml` it calls, as they are on the branch the dispatcher picks, not as on
`main`. The tag name, signed tag and on-`main` checks therefore bind only when
the run is dispatched from `main`: a branch carrying an edited `release.yml`
can drop them, and anyone who can push a branch can dispatch from it. Dispatch
only from `main`. A dispatched run's provenance attestation also records the
dispatched ref and that branch's commit as its source, not the tag, so
`gh attestation verify --source-ref refs/tags/<tag>` fails for its assets,
while `--signer-workflow bawdo/gosbee/.github/workflows/release.yml` still
holds. The publish job names the `release` environment, which
`scripts/repo-settings.sh harden` limits to `v*` tags and `main`, so
`release.yml` as `main` has it, dispatched from any other branch, cannot
publish; `main` is admitted so the recovery run above still works. Neither
binds a branch that edits `release.yml` itself, which can drop the
`environment:` line, so who has write access remains the boundary. With
`Protect release tag creation`, only an admin can push the tag that starts a
release.

Preview a release's notes locally with `scripts/changelog.sh notes v0.3.0`.

## Badges

The README carries CI, Codecov, Go reference, OpenSSF Scorecard and licence
badges. The OpenSSF Best Practices badge is not there yet: it needs a project
id, which exists only once the survey at
<https://www.bestpractices.dev/en/projects/new> is started. When it is, add the
badge to the README after the Scorecard badge, in the form
`[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/<id>/badge)](https://www.bestpractices.dev/projects/<id>)`,
and remove this paragraph.

## Questions?

Not sure about something? Open an issue! We'd rather you ask than struggle in
silence. Common questions:
- "Is this feature in scope?"
- "Where should this code live?"
- "How do I test this?"
- "What's the right approach for X?"

We're here to help.

## Recognition

Contributors are recognised in the project's commit history. Thank you for
helping make gosbee better!

---

## Related Resources

- **[README](README.md)** — project overview and quick start
- **[Getting Started Guide](docs/guide/getting-started.md)** — comprehensive API
  documentation
- **[Plugin System](plugins/README.md)** — transformer architecture
- **[REPL Guide](cmd/repl/README.md)** — interactive shell
