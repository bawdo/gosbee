#!/usr/bin/env bash
#
# Reports outdated dependencies and offers to upgrade them.
#
#   Go modules   Direct requirements in go.mod with a newer release in the
#                same major version, plus any that are deprecated or whose
#                current version is retracted. Indirect modules are counted
#                only: they follow along when go mod tidy runs.
#   Tool pins    Report only, as this script never edits the files they live
#                in:
#                  golangci-lint and zizmor, pinned as "version:" inputs in
#                  the CI workflow (Dependabot bumps the action SHAs, not
#                  these inputs);
#                  every tool in tools/go.mod's tool directive, such as
#                  govulncheck (Dependabot bumps these too);
#                  the go directive in go.mod against oldstable, the floor
#                  CI's test matrix runs.
#
# Releases younger than COOLDOWN_DAYS (default 7, matching dependabot.yml)
# are flagged "too fresh" and left out of "all", but can still be picked.
#
# On a terminal it then asks: [a]ll, [p]ick or [N]one. An upgrade runs
# go get, go mod tidy, go build and go test, then shows the go.mod diff.
# Nothing is committed. Without a terminal it prints the report and exits.
#
# GO (default go) is the go binary. CI_FILE, TOOLS_DIR and ZIZMOR_API exist
# so the tests can point the script elsewhere.
#
# Usage: scripts/outdated.sh    (or: make outdated)

set -euo pipefail

GO="${GO:-go}"
COOLDOWN_DAYS="${COOLDOWN_DAYS:-7}"
CI_FILE="${CI_FILE:-.github/workflows/ci.yml}"
TOOLS_DIR="${TOOLS_DIR:-tools}"
ZIZMOR_API="${ZIZMOR_API:-https://api.github.com/repos/zizmorcore/zizmor/releases/latest}"

if ! [[ "$COOLDOWN_DAYS" =~ ^[0-9]+$ ]]; then
    echo "outdated: COOLDOWN_DAYS must be a whole number of days" >&2
    exit 1
fi

if [ -t 1 ]; then
    BOLD=$'\033[1m' DIM=$'\033[2m' RED=$'\033[31m' GREEN=$'\033[32m'
    YELLOW=$'\033[33m' CYAN=$'\033[36m' NC=$'\033[0m'
else
    BOLD='' DIM='' RED='' GREEN='' YELLOW='' CYAN='' NC=''
fi

for bin in "$GO" jq; do
    command -v "$bin" > /dev/null || { echo "outdated: $bin is not installed" >&2; exit 1; }
done

tmp=$(mktemp -d "${TMPDIR:-/tmp}/outdated.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

header() { printf '\n%s%s%s\n' "$BOLD" "$1" "$NC"; }

# jq filter: whole days since an RFC 3339 time, or -1 when there is none.
# Fractional seconds are stripped because fromdateiso8601 rejects them.
AGE='if . then ((now - (sub("\\.[0-9]+"; "") | fromdateiso8601)) / 86400 | floor) else -1 end'

# Prints the age column, marking releases inside the cooldown.
age_label() {
    local days=$1
    if [ "$days" -lt 0 ]; then
        printf '%s' "age unknown"
    elif [ "$days" -lt "$COOLDOWN_DAYS" ]; then
        printf '%s%dd, too fresh%s' "$YELLOW" "$days" "$NC"
    else
        printf '%dd' "$days"
    fi
}

# ---------------------------------------------------------------- Go modules

echo "Checking Go modules against the module proxy..."
if ! "$GO" list -m -u -json all > "$tmp/all.json" 2> "$tmp/all.err"; then
    echo "${RED}outdated: go list failed:${NC}" >&2
    cat "$tmp/all.err" >&2
    exit 1
fi

# Direct modules with an update: path, current, latest, age in days.
jq -r "
    select(.Main != true and (.Indirect | not) and .Update)
    | [.Path, .Version, .Update.Version, (.Update.Time | $AGE)] | @tsv
" "$tmp/all.json" > "$tmp/direct.tsv"

# Direct modules that are deprecated or sit on a retracted version, one line
# per warning (read collapses empty tab-separated fields, so none are emitted).
jq -r '
    select(.Main != true and (.Indirect | not))
    | (if .Deprecated then [.Path, .Version, "deprecated: " + .Deprecated] else empty end),
      (if .Retracted then [.Path, .Version, "retracted: " + (.Retracted | join("; "))] else empty end)
    | @tsv
' "$tmp/all.json" > "$tmp/warn.tsv"

indirect_count=$(jq -s '[.[] | select(.Main != true and .Indirect and .Update)] | length' "$tmp/all.json")

header "Go modules (direct)"
if [ -s "$tmp/direct.tsv" ]; then
    printf '  %s%-40s %-14s %-14s %s%s\n' "$DIM" "MODULE" "CURRENT" "LATEST" "AGE" "$NC"
    while IFS=$'\t' read -r path cur latest days; do
        printf '  %-40s %-14s %s%-14s%s %s\n' \
            "$path" "$cur" "$GREEN" "$latest" "$NC" "$(age_label "$days")"
    done < "$tmp/direct.tsv"
else
    echo "  ${GREEN}All direct modules are up to date.${NC}"
fi

if [ -s "$tmp/warn.tsv" ]; then
    echo
    while IFS=$'\t' read -r path cur msg; do
        printf '  %s!%s %s %s: %s\n' "$RED" "$NC" "$path" "$cur" "$msg"
    done < "$tmp/warn.tsv"
fi

if [ "$indirect_count" -gt 0 ]; then
    printf '\n  %s%d indirect module(s) also have updates; go mod tidy moves those along.%s\n' \
        "$DIM" "$indirect_count" "$NC"
fi

# ---------------------------------------------------------------- Tool pins

# Prints "<version>\t<age-days>" for a module's latest release, or nothing.
latest_of() {
    "$GO" list -m -json "$1@latest" 2> /dev/null | jq -r "[.Version, (.Time | $AGE)] | @tsv" 2> /dev/null || true
}

# Prints "<version>\t<age-days>" for zizmor's latest GitHub release, or
# nothing. Unauthenticated, so it can be rate limited.
latest_zizmor() {
    command -v curl > /dev/null || return 0
    curl -fsSL --max-time 10 -H 'Accept: application/vnd.github+json' "$ZIZMOR_API" 2> /dev/null \
        | jq -r "[(.tag_name | ltrimstr(\"v\")), (.published_at | $AGE)] | @tsv" 2> /dev/null || true
}

# Prints the line number and value of the first "version:" input after the
# "uses:" line of the given action, as "<line>:<version>", or nothing.
action_version() {
    awk -v action="$1@" 'index($0, action) { found = 1 }
        found && /^[[:space:]]*version:/ { print NR ":" $2; exit }' "$CI_FILE"
}

# Prints one tool line: name, pinned version, where it is pinned, latest.
tool_line() {
    local name=$1 pinned=$2 where=$3 latest=$4 days=$5
    if [ -z "$pinned" ]; then
        printf '  %-16s %spin not found in %s%s\n' "$name" "$YELLOW" "$where" "$NC"
    elif [ -z "$latest" ]; then
        printf '  %-16s %-10s %slatest version lookup failed%s\n' "$name" "$pinned" "$YELLOW" "$NC"
    elif [ "$pinned" = "$latest" ]; then
        printf '  %-16s %-10s %sup to date%s\n' "$name" "$pinned" "$GREEN" "$NC"
    else
        printf '  %-16s %-10s -> %s%-10s%s %s\n' "$name" "$pinned" \
            "$GREEN" "$latest" "$NC" "$(age_label "$days")"
        printf '  %-16s %spinned in %s%s\n' "" "$CYAN" "$where" "$NC"
    fi
}

header "Tool pins (report only)"

if [ ! -f "$CI_FILE" ]; then
    echo "  ${YELLOW}$CI_FILE not found, skipping its pins.${NC}"
else
    lint_ref=$(action_version golangci/golangci-lint-action)
    lint_pin=${lint_ref#*:}
    lint_latest='' lint_days=''
    IFS=$'\t' read -r lint_latest lint_days <<< "$(latest_of github.com/golangci/golangci-lint/v2)" || true
    tool_line golangci-lint "$lint_pin" "$CI_FILE${lint_ref:+:${lint_ref%%:*}}" "$lint_latest" "${lint_days:--1}"

    # The pin is a bare version ("1.31.1"); the release tag carries a "v".
    ziz_ref=$(action_version zizmorcore/zizmor-action)
    ziz_pin=${ziz_ref#*:}
    ziz_latest='' ziz_days=''
    IFS=$'\t' read -r ziz_latest ziz_days <<< "$(latest_zizmor)" || true
    ziz_where="$CI_FILE${ziz_ref:+:${ziz_ref%%:*}}"
    grep -q 'zizmor@' scripts/wombat-verify.sh 2> /dev/null && ziz_where="$ziz_where, scripts/wombat-verify.sh"
    tool_line zizmor "$ziz_pin" "$ziz_where" "$ziz_latest" "${ziz_days:--1}"
fi

# Tools in tools/go.mod: one line per module, however many tools it provides.
if [ ! -f "$TOOLS_DIR/go.mod" ]; then
    echo "  ${YELLOW}$TOOLS_DIR/go.mod not found, skipping its tools.${NC}"
elif ! "$GO" -C "$TOOLS_DIR" list -f '{{.ImportPath}}	{{.Module.Path}}	{{.Module.Version}}' tool \
    > "$tmp/tools.tsv" 2> "$tmp/tools.err"; then
    echo "  ${YELLOW}could not list the tools in $TOOLS_DIR/go.mod:${NC}"
    sed 's/^/    /' "$tmp/tools.err"
else
    sort -t $'\t' -k2,2 -u "$tmp/tools.tsv" > "$tmp/tool-mods.tsv"
    while IFS=$'\t' read -r pkg mod ver; do
        t_latest='' t_days=''
        IFS=$'\t' read -r t_latest t_days <<< "$(latest_of "$mod")" || true
        tool_line "${pkg##*/}" "$ver" "$TOOLS_DIR/go.mod ($mod)" "$t_latest" "${t_days:--1}"
    done < "$tmp/tool-mods.tsv"
fi

# Go: CI's test matrix runs stable and oldstable, and treats oldstable as the
# floor go.mod declares. Compare go.mod's major.minor with oldstable's.
go_latest=$("$GO" list -m -f '{{.Version}}' go@latest 2> /dev/null || true)
go_directive=$(awk '$1 == "go" { print $2; exit }' go.mod)
if ! [[ "$go_directive" =~ ^1\.([0-9]+)(\.[0-9]+)?$ ]]; then
    floor_minor=''
else
    floor_minor=${BASH_REMATCH[1]}
fi
if [ -z "$floor_minor" ] || ! [[ "$go_latest" =~ ^1\.([0-9]+)(\.|$) ]] || [ "${BASH_REMATCH[1]}" -lt 1 ]; then
    printf '  %-16s %scould not compare (go.mod says "%s", latest "%s")%s\n' \
        "Go" "$YELLOW" "$go_directive" "$go_latest" "$NC"
else
    oldstable_minor=$((BASH_REMATCH[1] - 1))
    oldstable="1.$oldstable_minor"
    if [ "$floor_minor" -eq "$oldstable_minor" ]; then
        printf '  %-16s %-10s %sup to date (oldstable %s, latest %s)%s\n' \
            "Go" "$go_directive" "$GREEN" "$oldstable" "$go_latest" "$NC"
    else
        if [ "$floor_minor" -lt "$oldstable_minor" ]; then
            msg="go.mod's floor is older than oldstable, so CI no longer tests it"
        else
            msg="go.mod's floor is newer than oldstable, so CI's oldstable job runs a newer toolchain"
        fi
        printf '  %-16s %-10s -> %s%-10s%s oldstable (latest %s)\n' \
            "Go" "$go_directive" "$GREEN" "$oldstable" "$NC" "$go_latest"
        printf '  %-16s %s%s.%s\n' "" "$YELLOW" "$msg" "$NC"
        printf '  %-16s %sthe go directive in go.mod is the minimum supported version; change it deliberately.%s\n' \
            "" "$DIM" "$NC"
    fi
fi

# ---------------------------------------------------------------- Upgrade

echo
if [ ! -s "$tmp/direct.tsv" ]; then
    exit 0
fi

if [ ! -t 0 ] || [ ! -t 1 ]; then
    echo "Not on a terminal, so not offering upgrades."
    exit 0
fi

if git rev-parse --is-inside-work-tree > /dev/null 2>&1 \
    && ! { git diff --quiet -- go.mod go.sum && git diff --cached --quiet -- go.mod go.sum; }; then
    echo "${YELLOW}go.mod or go.sum has uncommitted changes. Commit or stash them first, so the"
    echo "upgrade diff is only the upgrade.${NC}"
    exit 0
fi

eligible=$(awk -F'\t' -v c="$COOLDOWN_DAYS" '$4 >= c' "$tmp/direct.tsv" | wc -l | tr -d ' ')
read -r -p "Upgrade? [a]ll ($eligible past the cooldown), [p]ick, [N]one: " choice || choice=""

: > "$tmp/chosen"
case "$choice" in
    a | A)
        awk -F'\t' -v c="$COOLDOWN_DAYS" '$4 >= c { print $1 "@" $3 }' "$tmp/direct.tsv" > "$tmp/chosen"
        ;;
    p | P)
        # Read answers from the terminal: stdin of this loop is the module list.
        while IFS=$'\t' read -r path cur latest days; do
            note=""
            [ "$days" -lt "$COOLDOWN_DAYS" ] && note=" ${YELLOW}($(age_label "$days"))${NC}"
            read -r -p "  $path $cur -> $latest$note? [y/N] " yn < /dev/tty || yn=""
            case "$yn" in y | Y) echo "$path@$latest" >> "$tmp/chosen" ;; esac
        done < "$tmp/direct.tsv"
        ;;
esac

if [ ! -s "$tmp/chosen" ]; then
    echo "Nothing upgraded."
    exit 0
fi

# Runs one step, and on failure says how to back out and stops.
step() {
    printf '%s==>%s %s\n' "$CYAN" "$NC" "$*"
    if ! "$@"; then
        echo "${RED}Failed: $*${NC}"
        echo "The changes are left in place. To back out: git checkout -- go.mod go.sum"
        exit 1
    fi
}

modules=()
while IFS= read -r m; do modules+=("$m"); done < "$tmp/chosen"

step "$GO" get "${modules[@]}"
step "$GO" mod tidy
step "$GO" build ./...
step "$GO" test ./...

header "go.mod changes"
git --no-pager diff --stat -- go.mod go.sum
git --no-pager diff -- go.mod
echo
echo "${GREEN}Upgraded ${#modules[@]} module(s). Build and tests pass. Nothing has been committed.${NC}"
