# Development Scripts

This directory contains scripts to assist with development and testing.

## pre-ci-check.sh

**Purpose**: Validates your changes locally before pushing to CI, predicting the probability of GitHub Actions success.

**Usage**:
```bash
./scripts/pre-ci-check.sh
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
