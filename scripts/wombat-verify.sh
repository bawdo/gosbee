#!/usr/bin/env bash
#
# Runs the local verification gates and writes the wombat-workstream
# verification record (.claude/wombat-verification, gitignored) in one write.
#
# Usage: scripts/wombat-verify.sh
#
# WOMBAT_BASE_REF is accepted for the workstream plugin's contract but not
# used: every gate runs on every invocation, so there is no change-aware rule
# for a base to inform.
#
# Record format (tab separated after the first two lines):
#   wombat-verification 1
#   tree<TAB>clean | dirty <porcelain summary>
#   <gate><TAB><sha><TAB>pass|fail|skipped<TAB><seconds><TAB><reason>
#
# A gate whose tool is not installed is recorded as skipped with that reason,
# never as a pass. Exits non-zero when any gate fails.

set -uo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1

RECORD=".claude/wombat-verification"
SHA="$(git rev-parse HEAD)"
LOGDIR="$(mktemp -d "${TMPDIR:-/tmp}/wombat-verify.XXXXXX")"
LINES="$LOGDIR/lines"
: > "$LINES"
failed=0

# record GATE OUTCOME SECONDS REASON
record() {
    printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$SHA" "$2" "$3" "$4" >> "$LINES"
}

# run GATE COMMAND...: runs the command, records pass or fail with timing.
run() {
    local gate="$1" start end log="$LOGDIR/$1.log"
    shift
    printf '==> %s\n' "$gate"
    start=$(date +%s)
    if "$@" > "$log" 2>&1; then
        end=$(date +%s)
        record "$gate" pass "$((end - start))" "-"
    else
        end=$(date +%s)
        record "$gate" fail "$((end - start))" "see output above"
        failed=1
        cat "$log"
    fi
}

# skip GATE REASON
skip() {
    printf '==> %s (skipped: %s)\n' "$1" "$2"
    record "$1" skipped 0 "$2"
}

# shellcheck disable=SC2329 # invoked indirectly through run
gofmt_check() {
    local out
    out="$(gofmt -s -l .)"
    [ -z "$out" ] || { printf 'needs gofmt -s -w:\n%s\n' "$out"; return 1; }
}

# shellcheck disable=SC2329 # invoked indirectly through run
tidy_check() {
    go mod tidy -diff && go -C tools mod tidy -diff
}

run fmt gofmt_check
run vet go vet ./...
if command -v golangci-lint > /dev/null 2>&1; then
    run lint golangci-lint run --timeout=5m
else
    skip lint "golangci-lint not installed"
fi
run tidy tidy_check
run changelog scripts/changelog.sh check
if command -v jq > /dev/null 2>&1; then
    run release-scripts bash scripts/release-scripts_test.sh
else
    skip release-scripts "jq not installed (CI runs it)"
fi
run vuln go -C tools tool govulncheck -C .. ./...
if command -v zizmor > /dev/null 2>&1; then
    run zizmor zizmor --offline .github/workflows
elif command -v uvx > /dev/null 2>&1; then
    # The version CI's zizmor-action runs.
    run zizmor uvx zizmor@1.31.1 --offline .github/workflows
else
    skip zizmor "zizmor and uvx not installed (CI runs it)"
fi
if command -v shellcheck > /dev/null 2>&1; then
    run shellcheck shellcheck scripts/*.sh
else
    skip shellcheck "shellcheck not installed"
fi
if command -v jq > /dev/null 2>&1; then
    run ci-result scripts/ci-result_test.sh
else
    skip ci-result "jq not installed (CI runs it)"
fi
run test go test -race -count=1 -shuffle=on ./...

# The tree state is taken after the gates, so a gate that rewrote a file
# shows up as dirty rather than being verified against a sha it differs from.
porcelain="$(git status --porcelain)"
if [ -z "$porcelain" ]; then
    tree="clean"
else
    tree="dirty $(printf '%s' "$porcelain" | wc -l | tr -d ' ') path(s): $(printf '%s' "$porcelain" | head -5 | tr '\n' ' ')"
fi

mkdir -p "$(dirname "$RECORD")"
{
    printf 'wombat-verification 1\n'
    printf 'tree\t%s\n' "$tree"
    cat "$LINES"
} > "$LOGDIR/record"
mv "$LOGDIR/record" "$RECORD"
rm -rf "$LOGDIR"

printf '\n'
cat "$RECORD"
exit "$failed"
