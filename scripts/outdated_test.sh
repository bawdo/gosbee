#!/usr/bin/env bash
#
# Tests for scripts/outdated.sh, against a fake go and a fake curl that
# answer from fixtures, in a scratch project. No network, no terminal, so the
# upgrade prompt is never reached. Prints "ok <name>" per case and exits
# non-zero on the first failure, with a line starting "FAIL".
#
# Usage: scripts/outdated_test.sh

set -euo pipefail

DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$DIR/outdated.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/outdated_test.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

OUT=""
CODE=0

fail() {
    echo "FAIL $1: $2" >&2
    echo "  exit: $CODE" >&2
    echo "  output:" >&2
    while IFS= read -r line; do echo "    $line"; done <<<"$OUT" >&2
    exit 1
}

OLD="2020-01-01T00:00:00.123Z"
FRESH="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
export OLD FRESH

mkdir -p "$TMP/bin" "$TMP/proj/tools" "$TMP/proj/.github/workflows"

# The fake go. "list -m -u -json all" prints $FAKE_ALL (a file), "-C dir list
# ... tool" prints two tools from one module and one from another,
# "list -m -json <mod>@latest" a release per module, "go@latest" FAKE_GO_LATEST.
cat > "$TMP/bin/go" <<'EOF'
#!/usr/bin/env bash
case "$*" in
    "list -m -u -json all")
        [ -z "${FAKE_LIST_FAIL:-}" ] || { echo "go: proxy unreachable" >&2; exit 1; }
        cat "$FAKE_ALL" ;;
    "-C "*" list -f "*" tool")
        [ -z "${FAKE_TOOLS_FAIL:-}" ] || { echo "go: tools broken" >&2; exit 1; }
        printf 'golang.org/x/vuln/cmd/govulncheck\tgolang.org/x/vuln\tv1.9.0\n'
        printf 'golang.org/x/vuln/cmd/vulnreport\tgolang.org/x/vuln\tv1.9.0\n'
        printf 'github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod\tgithub.com/CycloneDX/cyclonedx-gomod\tv1.12.0\n' ;;
    "list -m -json github.com/golangci/golangci-lint/v2@latest")
        printf '{"Version":"v2.16.0","Time":"%s"}\n' "$OLD" ;;
    "list -m -json golang.org/x/vuln@latest")
        printf '{"Version":"v1.9.0","Time":"%s"}\n' "$OLD" ;;
    "list -m -json github.com/CycloneDX/cyclonedx-gomod@latest")
        printf '{"Version":"v1.13.0","Time":"%s"}\n' "$FRESH" ;;
    "list -m -f {{.Version}} go@latest")
        echo "${FAKE_GO_LATEST-1.27.4}" ;;
    *) echo "fake go: unexpected: $*" >&2; exit 2 ;;
esac
EOF

cat > "$TMP/bin/curl" <<'EOF'
#!/usr/bin/env bash
[ -z "${FAKE_CURL_FAIL:-}" ] || exit 22
printf '{"tag_name":"v1.32.0","published_at":"%s"}\n' "$OLD"
EOF
chmod +x "$TMP/bin/go" "$TMP/bin/curl"

# Modules with updates, a fresh one, an indirect one, a deprecated one and a
# retracted one. The main module and an up to date module have no Update.
cat > "$TMP/all.json" <<EOF
{"Path":"example.com/proj","Main":true}
{"Path":"github.com/ergochat/readline","Version":"v0.1.3","Update":{"Version":"v0.1.4","Time":"$OLD"}}
{"Path":"github.com/go-sql-driver/mysql","Version":"v1.10.1","Update":{"Version":"v1.11.0","Time":"$FRESH"}}
{"Path":"github.com/jackc/pgx/v5","Version":"v5.12.0","Deprecated":"use v6"}
{"Path":"modernc.org/sqlite","Version":"v1.62.0","Retracted":["broken build"]}
{"Path":"github.com/google/uuid","Version":"v1.6.0","Indirect":true,"Update":{"Version":"v1.7.0","Time":"$OLD"}}
EOF

cat > "$TMP/current.json" <<EOF
{"Path":"example.com/proj","Main":true}
{"Path":"github.com/ergochat/readline","Version":"v0.1.4"}
EOF

CI_FIXTURE='name: CI
jobs:
  lint:
    steps:
      - name: Run golangci-lint
        uses: golangci/golangci-lint-action@0000000000000000000000000000000000000000 # v9.3.0
        with:
          version: v2.15.0
      - name: Audit workflows with zizmor
        uses: zizmorcore/zizmor-action@0000000000000000000000000000000000000000 # v0.8.0
        with:
          version: 1.31.1'

# reset: restores the scratch project to its default state.
reset() {
    printf 'module example.com/proj\n\ngo 1.26.0\n' > "$TMP/proj/go.mod"
    printf 'module example.com/proj/tools\n' > "$TMP/proj/tools/go.mod"
    printf '%s\n' "$CI_FIXTURE" > "$TMP/proj/.github/workflows/ci.yml"
}

# run [VAR=VALUE...]: runs outdated.sh in the scratch project with the fakes.
run() {
    CODE=0
    OUT=$(cd "$TMP/proj" && env PATH="$TMP/bin:$PATH" GO="$TMP/bin/go" \
        FAKE_ALL="$TMP/all.json" "$@" bash "$SCRIPT" 2>&1 < /dev/null) || CODE=$?
}

expect_code() {
    [ "$CODE" -eq "$2" ] || fail "$1" "expected exit $2"
}

# has NAME PATTERN: the output contains a line matching the extended regex.
has() {
    grep -qE -- "$2" <<<"$OUT" || fail "$1" "no line matching: $2"
}

hasnt() {
    ! grep -qE -- "$2" <<<"$OUT" || fail "$1" "unexpected line matching: $2"
}

reset

t=lists_direct_updates_with_age
run
expect_code $t 0
has $t '^  github.com/ergochat/readline +v0\.1\.3 +v0\.1\.4 +[0-9]+d$'
has $t '^  github.com/go-sql-driver/mysql +v1\.10\.1 +v1\.11\.0 +0d, too fresh$'
hasnt $t 'google/uuid +v1'
echo "ok $t"

t=warns_on_deprecated_and_retracted
has $t '^  ! github.com/jackc/pgx/v5 v5\.12\.0: deprecated: use v6$'
has $t '^  ! modernc.org/sqlite v1\.62\.0: retracted: broken build$'
echo "ok $t"

t=counts_indirect_updates
has $t '^  1 indirect module\(s\) also have updates'
echo "ok $t"

t=no_prompt_without_terminal
has $t '^Not on a terminal, so not offering upgrades\.$'
hasnt $t 'Upgrade\?'
echo "ok $t"

t=golangci_lint_pin_reported_with_line
has $t '^  golangci-lint +v2\.15\.0 +-> v2\.16\.0 +[0-9]+d$'
has $t '^ +pinned in \.github/workflows/ci\.yml:8$'
echo "ok $t"

t=zizmor_pin_reported_without_v
has $t '^  zizmor +1\.31\.1 +-> 1\.32\.0 +[0-9]+d$'
has $t '^ +pinned in \.github/workflows/ci\.yml:12$'
echo "ok $t"

t=tools_module_one_line_per_module
has $t '^  govulncheck +v1\.9\.0 +up to date$'
hasnt $t 'vulnreport'
has $t '^  cyclonedx-gomod +v1\.12\.0 +-> v1\.13\.0 +0d, too fresh$'
has $t '^ +pinned in tools/go\.mod \(github\.com/CycloneDX/cyclonedx-gomod\)$'
echo "ok $t"

t=go_floor_matches_oldstable
has $t '^  Go +1\.26\.0 +up to date \(oldstable 1\.26, latest 1\.27\.4\)$'
echo "ok $t"

t=go_floor_older_than_oldstable
run FAKE_GO_LATEST=1.28.0
expect_code $t 0
has $t '^  Go +1\.26\.0 +-> 1\.27 +oldstable \(latest 1\.28\.0\)$'
has $t "go\.mod's floor is older than oldstable, so CI no longer tests it\.$"
echo "ok $t"

t=go_floor_newer_than_oldstable
run FAKE_GO_LATEST=1.26.3
expect_code $t 0
has $t '^  Go +1\.26\.0 +-> 1\.25 +oldstable'
has $t "go\.mod's floor is newer than oldstable"
echo "ok $t"

t=go_short_directive_accepted
printf 'module example.com/proj\n\ngo 1.26\n' > "$TMP/proj/go.mod"
run
expect_code $t 0
has $t '^  Go +1\.26 +up to date'
reset
echo "ok $t"

t=go_latest_lookup_failure
run FAKE_GO_LATEST=
expect_code $t 0
has $t '^  Go +could not compare \(go\.mod says "1\.26\.0", latest ""\)$'
echo "ok $t"

t=zizmor_lookup_failure
run FAKE_CURL_FAIL=1
expect_code $t 0
has $t '^  zizmor +1\.31\.1 +latest version lookup failed$'
echo "ok $t"

t=pin_missing_from_ci_file
printf 'name: CI\njobs: {}\n' > "$TMP/proj/.github/workflows/ci.yml"
run
expect_code $t 0
has $t '^  golangci-lint +pin not found in \.github/workflows/ci\.yml$'
has $t '^  zizmor +pin not found in \.github/workflows/ci\.yml$'
reset
echo "ok $t"

t=missing_ci_file_still_checks_tools
rm "$TMP/proj/.github/workflows/ci.yml"
run
expect_code $t 0
has $t 'ci\.yml not found, skipping its pins\.$'
has $t '^  govulncheck '
has $t '^  Go '
reset
echo "ok $t"

t=tools_list_failure_reported
run FAKE_TOOLS_FAIL=1
expect_code $t 0
has $t 'could not list the tools in tools/go\.mod:$'
has $t '^    go: tools broken$'
echo "ok $t"

t=all_up_to_date
run FAKE_ALL="$TMP/current.json"
expect_code $t 0
has $t '^  All direct modules are up to date\.$'
hasnt $t 'Not on a terminal'
hasnt $t 'indirect module'
echo "ok $t"

t=go_list_failure_stops
run FAKE_LIST_FAIL=1
expect_code $t 1
has $t 'outdated: go list failed:$'
has $t '^go: proxy unreachable$'
hasnt $t 'Tool pins'
echo "ok $t"

for bad in abc -1 1.5 ''; do
    t="bad_cooldown_refused_[$bad]"
    run "COOLDOWN_DAYS=$bad"
    if [ -z "$bad" ]; then
        # Empty falls back to the default, like an unset variable.
        expect_code "$t" 0
    else
        expect_code "$t" 1
        has "$t" 'COOLDOWN_DAYS must be a whole number of days'
    fi
    echo "ok $t"
done

t=cooldown_zero_clears_too_fresh
run COOLDOWN_DAYS=0
expect_code $t 0
hasnt $t 'too fresh'
echo "ok $t"

# make outdated runs in the real repository, so it reads the real ci.yml and
# go.mod but still answers from the fakes.
t=make_outdated_passes_go
CODE=0
OUT=$(env PATH="$TMP/bin:$PATH" FAKE_ALL="$TMP/all.json" \
    make -s -C "$DIR/.." outdated GO="$TMP/bin/go" 2>&1 < /dev/null) || CODE=$?
expect_code $t 0
has $t '^  github.com/ergochat/readline +v0\.1\.3 +v0\.1\.4'
has $t '^  golangci-lint '
echo "ok $t"

echo "all outdated tests passed"
