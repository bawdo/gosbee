# Unit 07: examples

Mode: implement
Close: auto
Branch: unit/07-examples
Review lanes: security, correctness, brief

## Items

- Add testable `Example*` functions for the core builder.
- Add examples for each dialect visitor.
- Add examples for each shipped plugin.
- Update CHANGELOG.md.

## Acceptance criteria

- `Example*` functions, each with an `// Output:` block that `go test` checks, cover:
  - the core builder: a select with projection, a join, a where with combined
    predicates, an insert, an update and a delete;
  - each dialect visitor (PostgreSQL, MySQL, SQLite), showing the dialect difference
    in quoting or parameters;
  - each shipped plugin (at least soft delete and OPA, plus any other in `plugins/`).
- Examples live in `_test.go` files in the package they document, named per Go
  convention (`ExampleType_Method`, `ExampleFunc`), so pkg.go.dev attaches them.
- Examples use only the public API; no `internal/` imports and no network. An OPA
  example that needs a policy server uses an in-process stub or a documented fake.
- Every example passes under `go test -race -count=1 -shuffle=on ./...`.
- golangci-lint passes; CHANGELOG.md has an `[Unreleased]` entry.

## Decisions

## Plan

## Outcome

## Not done, and why
