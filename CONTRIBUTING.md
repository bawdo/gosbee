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

## Code of Conduct

Be respectful, constructive, and collaborative. We're all here to build
something useful together.

## Getting Started

1. **Fork and clone the repository:**
   ```bash
   git clone https://github.com/YOUR_USERNAME/gosbee.git
   cd gosbee
   ```

2. **Install dependencies:**
   ```bash
   go mod tidy
   ```

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

Found a bug? Open an issue with:
- A clear, descriptive title
- Steps to reproduce the problem
- Expected vs. actual behaviour
- Go version and operating system
- Example code if possible

### Suggesting Features

Have an idea for a new feature? Open an issue with:
- A clear description of the feature
- Why it would be useful
- Example usage (code or pseudocode)
- Any implementation ideas (optional)

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
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following the [Coding Standards](#coding-standards)

3. **Write or update tests** to cover your changes

4. **Run the test suite:**
   ```bash
   go test ./...
   ```

5. **Check test coverage** (optional):
   ```bash
   # Generate coverage report in the coverage/ directory
   go test -coverprofile=coverage/coverage.out ./...

   # View coverage in terminal
   go tool cover -func=coverage/coverage.out

   # Or open an HTML coverage report in your browser
   go tool cover -html=coverage/coverage.out
   ```

6. **Pre-CI validation** (recommended before pushing):
   ```bash
   # Run all CI checks locally and get a success probability report
   ./scripts/pre-ci-check.sh
   ```

   This script mimics the GitHub Actions workflow and checks:
   - Code formatting (gofmt -s)
   - All tests with race detector
   - Linting (golangci-lint)
   - Test coverage
   - Build success

   It generates a detailed report showing which checks passed/failed and the probability of CI succeeding.

5. **Run linters:**
   ```bash
   go vet ./...
   go fmt ./...
   ```

6. **Commit your changes** with clear commit messages in past tense:
   ```
   Added support for LATERAL JOIN in PostgreSQL visitor

   - Implemented VisitLateralJoin in baseVisitor
   - Added Lateral bool field to JoinNode
   - Updated PostgreSQL tests to cover LATERAL JOIN syntax
   ```

7. **Push to your fork** and open a pull request

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
├── docs/
│   ├── guide/          # User-facing documentation
│   ├── development/    # Contributor documentation
│   ├── design/         # Design decisions and architecture
│   └── plans/          # Implementation plans and specs
└── internal/           # Private helpers (not exported)
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

- The `Visitor` interface lives in `nodes/` to avoid circular dependencies
- Use the **outer-dispatch pattern**: `baseVisitor.outer` enables virtual method
  resolution for dialect-specific overrides
- When adding a new node type, update:
  - `Visitor` interface in `nodes/visitor.go`
  - `baseVisitor` with a default implementation
  - `DotVisitor` for AST visualisation
  - All `stubVisitor` implementations in test files

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

Current test count: **~1096 tests** across the entire codebase.

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

### Design Documents (`docs/design/`)

Architecture decisions, feature specifications, and design rationale.

### When to Update Documentation

- **User guides:** When adding new public API features or changing behaviour
- **Plugin READMEs:** When adding plugin options or changing behaviour
- **Architecture docs:** When making significant structural changes
- **Code comments:** When the code isn't self-evident
- **Examples:** When demonstrating a new pattern or feature

## Pull Request Process

1. **Ensure all tests pass** and linters are happy
2. **Update documentation** if you've changed behaviour or added features
3. **Write a clear PR description** that explains:
   - What problem you're solving
   - How you've solved it
   - Any breaking changes (if applicable)
   - Links to related issues
4. **Keep PRs focused:** one feature or fix per PR when possible
5. **Respond to feedback** — reviews are collaborative, not adversarial
6. **Be patient** — maintainers may take time to review

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
