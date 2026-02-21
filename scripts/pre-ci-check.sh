#!/usr/bin/env bash
#
# Pre-CI Validation Script
# Mimics GitHub Actions CI workflow to predict build success probability
#
# Usage: ./scripts/pre-ci-check.sh [--fix gofmt]
#
# Options:
#   --fix gofmt   Auto-fix formatting issues with gofmt -s -w before checking

set -e
set -o pipefail

# Colours for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Colour

# Results tracking
declare -a RESULTS
declare -a FAILURES
TOTAL_CHECKS=0
PASSED_CHECKS=0
COVERAGE=""
COVERAGE_DELTA=""
FORMAT_FAILED=false

# Fix mode flags
FIX_GOFMT=false

# Helper functions
print_header() {
    echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
}

print_step() {
    echo -e "${YELLOW}▶${NC} $1"
}

record_result() {
    local name="$1"
    local status="$2"
    local details="$3"

    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))

    if [ "$status" = "PASS" ]; then
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
        RESULTS+=("${GREEN}✓${NC} $name")
    else
        RESULTS+=("${RED}✗${NC} $name")
        FAILURES+=("  - $name: $details")
    fi
}

# Clean up old artefacts
cleanup() {
    print_step "Cleaning up old test artefacts..."
    rm -f coverage/pre-ci-*.out coverage/pre-ci-*.txt
    mkdir -p coverage
}

# Check 1: Go version
check_go_version() {
    print_header "Check 1: Go Version"
    print_step "Verifying Go installation..."

    if ! command -v go &> /dev/null; then
        record_result "Go Version" "FAIL" "Go not installed"
        echo -e "${RED}✗ Go is not installed${NC}"
        return 1
    fi

    GO_VERSION=$(go version | awk '{print $3}')
    echo "Detected: $GO_VERSION"

    # CI uses Go 1.25.x and 1.24.x
    if [[ "$GO_VERSION" =~ go1\.(25|24)\. ]]; then
        record_result "Go Version" "PASS" ""
        echo -e "${GREEN}✓ Go version compatible with CI${NC}"
    else
        record_result "Go Version" "PASS" "Warning: Using $GO_VERSION (CI uses 1.25.x/1.24.x)"
        echo -e "${YELLOW}⚠ Warning: CI uses Go 1.25.x and 1.24.x, you have $GO_VERSION${NC}"
    fi
}

# Check 2: Dependencies
check_dependencies() {
    print_header "Check 2: Dependencies"
    print_step "Downloading dependencies..."

    if go mod download 2>&1 | tee coverage/pre-ci-deps.txt; then
        record_result "Dependencies" "PASS" ""
        echo -e "${GREEN}✓ Dependencies downloaded successfully${NC}"
    else
        record_result "Dependencies" "FAIL" "Failed to download dependencies"
        echo -e "${RED}✗ Failed to download dependencies${NC}"
        return 1
    fi
}

# Check 3: gofmt formatting
check_formatting() {
    print_header "Check 3: Code Formatting (gofmt -s)"

    if [ "$FIX_GOFMT" = true ]; then
        print_step "Auto-fixing formatting with gofmt -s -w ..."
        FIXED=$(gofmt -s -l . 2>&1 | grep -v '^\.git' | grep -v '^\.obsidian' || true)
        if [ -n "$FIXED" ]; then
            gofmt -s -w .
            echo -e "${YELLOW}⚠ Fixed formatting in:${NC}"
            echo "$FIXED" | sed 's/^/  /'
        fi
    fi

    print_step "Checking code formatting..."

    # Run gofmt -s and capture any files that need formatting
    UNFORMATTED=$(gofmt -s -l . 2>&1 | grep -v '^\.git' | grep -v '^\.obsidian' || true)

    if [ -z "$UNFORMATTED" ]; then
        record_result "Code Formatting" "PASS" ""
        echo -e "${GREEN}✓ All files are properly formatted${NC}"
    else
        FORMAT_FAILED=true
        record_result "Code Formatting" "FAIL" "Files need formatting"
        echo -e "${RED}✗ The following files need formatting:${NC}"
        echo "$UNFORMATTED" | sed 's/^/  /'
        return 1
    fi
}

# Check 4: Tests with race detector
check_tests() {
    print_header "Check 4: Tests (with race detector)"
    print_step "Running test suite..."

    # On macOS/Linux, run with race detector like CI does on Linux
    if [[ "$OSTYPE" == "darwin"* ]] || [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if go test -race -v ./... 2>&1 | tee coverage/pre-ci-tests.txt; then
            record_result "Tests" "PASS" ""
            echo -e "${GREEN}✓ All tests passed${NC}"
        else
            record_result "Tests" "FAIL" "Test failures detected"
            echo -e "${RED}✗ Tests failed${NC}"
            return 1
        fi
    else
        # Fallback for other platforms
        if go test -v ./... 2>&1 | tee coverage/pre-ci-tests.txt; then
            record_result "Tests" "PASS" ""
            echo -e "${GREEN}✓ All tests passed${NC}"
        else
            record_result "Tests" "FAIL" "Test failures detected"
            echo -e "${RED}✗ Tests failed${NC}"
            return 1
        fi
    fi
}

# Check 5: Linting
check_linting() {
    print_header "Check 5: Linting (golangci-lint)"
    print_step "Running golangci-lint..."

    if ! command -v golangci-lint &> /dev/null; then
        record_result "Linting" "FAIL" "golangci-lint not installed"
        echo -e "${RED}✗ golangci-lint is not installed${NC}"
        echo -e "${YELLOW}Install: brew install golangci-lint${NC}"
        return 1
    fi

    # Run with same timeout as CI
    if golangci-lint run --timeout=5m 2>&1 | tee coverage/pre-ci-lint.txt; then
        record_result "Linting" "PASS" ""
        echo -e "${GREEN}✓ No linting issues found${NC}"
    else
        record_result "Linting" "FAIL" "Linting issues detected"
        echo -e "${RED}✗ Linting failed${NC}"
        return 1
    fi
}

# Check 6: Coverage
check_coverage() {
    print_header "Check 6: Test Coverage"
    print_step "Generating coverage report..."

    # Mimic CI coverage command
    COVERPKG=$(go list ./... | grep -v testutil | tr '\n' ',' | sed 's/,$//')

    # Load previous coverage if available
    local PREV_COVERAGE=""
    local COVERAGE_STORE="coverage/.last-coverage"
    if [ -f "$COVERAGE_STORE" ]; then
        PREV_COVERAGE=$(cat "$COVERAGE_STORE")
    fi

    if go test -race -coverprofile=coverage/pre-ci-coverage.out -covermode=atomic -coverpkg="$COVERPKG" ./... 2>&1 | tee coverage/pre-ci-coverage.txt; then
        # Calculate coverage percentage
        COVERAGE=$(go tool cover -func=coverage/pre-ci-coverage.out | grep total | awk '{print $3}' | sed 's/%//')

        # Persist for next run
        echo "$COVERAGE" > "$COVERAGE_STORE"

        # Compute delta from previous run
        if [ -n "$PREV_COVERAGE" ]; then
            COVERAGE_DELTA=$(echo "scale=1; $COVERAGE - $PREV_COVERAGE" | bc)
        fi

        record_result "Coverage Generation" "PASS" ""
        echo -e "${GREEN}✓ Coverage report generated${NC}"
        echo -e "  Total coverage: ${BLUE}${COVERAGE}%${NC}"

        # Check against target (80% from .codecov.yml)
        if (( $(echo "$COVERAGE >= 80" | bc -l) )); then
            echo -e "  ${GREEN}✓ Meets coverage target (80%)${NC}"
        else
            echo -e "  ${YELLOW}⚠ Below coverage target (80%)${NC}"
        fi
    else
        record_result "Coverage Generation" "FAIL" "Coverage generation failed"
        echo -e "${RED}✗ Coverage generation failed${NC}"
        return 1
    fi
}

# Check 7: Build
check_build() {
    print_header "Check 7: Build"
    print_step "Building REPL binary..."

    # Build for current platform
    if go build -o coverage/pre-ci-repl ./cmd/repl 2>&1 | tee coverage/pre-ci-build.txt; then
        record_result "Build" "PASS" ""
        echo -e "${GREEN}✓ Build successful${NC}"
        rm -f coverage/pre-ci-repl
    else
        record_result "Build" "FAIL" "Build failed"
        echo -e "${RED}✗ Build failed${NC}"
        return 1
    fi
}

# Check 8: Vulnerability scan
check_vulnerability_scan() {
    print_header "Check 8: Vulnerability Scan (govulncheck)"
    print_step "Running govulncheck..."

    if ! command -v govulncheck &> /dev/null; then
        record_result "Vulnerability Scan" "FAIL" "govulncheck not installed"
        echo -e "${RED}✗ govulncheck is not installed${NC}"
        echo -e "${YELLOW}Install: go install golang.org/x/vuln/cmd/govulncheck@latest${NC}"
        return 1
    fi

    if govulncheck ./... 2>&1 | tee coverage/pre-ci-vuln.txt; then
        record_result "Vulnerability Scan" "PASS" ""
        echo -e "${GREEN}✓ No vulnerabilities found${NC}"
    else
        record_result "Vulnerability Scan" "FAIL" "Vulnerabilities detected"
        echo -e "${RED}✗ Vulnerability scan failed${NC}"
        return 1
    fi
}

# Generate summary report
generate_report() {
    print_header "CI Readiness Report"

    echo "Check Results:"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    for result in "${RESULTS[@]}"; do
        echo -e "  $result"
    done

    echo ""
    echo "Summary:"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "  Total checks: $TOTAL_CHECKS"
    echo -e "  Passed: ${GREEN}$PASSED_CHECKS${NC}"
    echo -e "  Failed: ${RED}$((TOTAL_CHECKS - PASSED_CHECKS))${NC}"
    if [ -n "$COVERAGE" ]; then
        if [ -n "$COVERAGE_DELTA" ]; then
            local DELTA_DISPLAY
            if (( $(echo "$COVERAGE_DELTA > 0" | bc -l) )); then
                DELTA_DISPLAY="${GREEN}(+${COVERAGE_DELTA}%)${NC}"
            elif (( $(echo "$COVERAGE_DELTA < 0" | bc -l) )); then
                DELTA_DISPLAY="${RED}(${COVERAGE_DELTA}%)${NC}"
            else
                DELTA_DISPLAY="${NC}(no change)"
            fi
            echo -e "  Coverage: ${BLUE}${COVERAGE}%${NC} ${DELTA_DISPLAY}"
        else
            echo -e "  Coverage: ${BLUE}${COVERAGE}%${NC} (first run)"
        fi
    fi

    # Calculate success probability
    SUCCESS_RATE=$(echo "scale=0; ($PASSED_CHECKS * 100) / $TOTAL_CHECKS" | bc)

    echo ""
    if [ $SUCCESS_RATE -eq 100 ]; then
        echo -e "CI Success Probability: ${GREEN}${SUCCESS_RATE}%${NC} 🎉"
        echo -e "${GREEN}✓ All checks passed! CI should succeed.${NC}"
    elif [ $SUCCESS_RATE -ge 80 ]; then
        echo -e "CI Success Probability: ${YELLOW}${SUCCESS_RATE}%${NC} ⚠"
        echo -e "${YELLOW}⚠ Some checks failed. CI may succeed but could have issues.${NC}"
    else
        echo -e "CI Success Probability: ${RED}${SUCCESS_RATE}%${NC} ✗"
        echo -e "${RED}✗ Multiple checks failed. CI will likely fail.${NC}"
    fi

    if [ ${#FAILURES[@]} -gt 0 ]; then
        echo ""
        echo "Failures:"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        for failure in "${FAILURES[@]}"; do
            echo -e "${RED}$failure${NC}"
        done
    fi

    if [ "$FORMAT_FAILED" = true ]; then
        echo ""
        echo -e "${YELLOW}Tip: re-run with --fix gofmt to auto-fix formatting:${NC}"
        echo -e "  ${BLUE}./scripts/pre-ci-check.sh --fix gofmt${NC}"
    fi

    echo ""
    echo "Detailed logs saved in: coverage/pre-ci-*.txt  (checks 1–8)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

# Main execution
main() {
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --fix)
                case "$2" in
                    gofmt) FIX_GOFMT=true; shift 2 ;;
                    *) echo -e "${RED}Unknown --fix target: $2${NC}"; echo "Usage: $0 [--fix gofmt]"; exit 1 ;;
                esac
                ;;
            *) echo -e "${RED}Unknown option: $1${NC}"; echo "Usage: $0 [--fix gofmt]"; exit 1 ;;
        esac
    done

    echo -e "${BLUE}"
    echo "╔════════════════════════════════════════════════════════════════════╗"
    if [ "$FIX_GOFMT" = true ]; then
    echo "║              Pre-CI Validation Script  [--fix gofmt]              ║"
    else
    echo "║                     Pre-CI Validation Script                      ║"
    fi
    echo "║                  Mimicking GitHub Actions Workflow                ║"
    echo "╚════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"

    cleanup

    # Run all checks (continue even if some fail)
    check_go_version || true
    check_dependencies || true
    check_formatting || true
    check_tests || true
    check_linting || true
    check_coverage || true
    check_build || true
    check_vulnerability_scan || true

    # Generate final report
    generate_report

    # Exit with appropriate code
    if [ $PASSED_CHECKS -eq $TOTAL_CHECKS ]; then
        exit 0
    else
        exit 1
    fi
}

# Run main function
main
