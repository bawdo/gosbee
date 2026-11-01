#!/usr/bin/env bash
#
# Runs every native Go fuzz target in the module, one after another (go test
# fuzzes one target per invocation), for FUZZTIME each.
#
#   scripts/fuzz.sh            fuzz every target
#   scripts/fuzz.sh --list     list "<package> <target>" lines, fuzz nothing
#
# FUZZTIME (default 10s) is a positive Go duration (30s, 1m30s) or iteration
# count (500x). GO (default go) is the go binary. Both come in through the
# environment only. A failing target does not stop the run: every target is
# fuzzed, the failures are listed at the end and the exit is 1. go test
# writes a failing input to <package dir>/testdata/fuzz/<target>/.

set -euo pipefail

GO="${GO:-go}"
FUZZTIME="${FUZZTIME-10s}"

die() {
    echo "fuzz: $*" >&2
    exit "${2:-1}"
}

list=0
case "${1:-}" in
    "") ;;
    --list) list=1 ;;
    *) echo "fuzz: unknown argument '$1' (usage: fuzz.sh [--list])" >&2; exit 2 ;;
esac

# A subset of what go test accepts, and never zero, which go test refuses.
duration='^([0-9]+x|([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+)$'
if [ "$list" -eq 0 ] && { ! [[ "$FUZZTIME" =~ $duration ]] || ! [[ "$FUZZTIME" =~ [1-9] ]]; }; then
    echo "fuzz: FUZZTIME must be a positive duration (30s, 1m30s) or count (500x), got '$FUZZTIME'" >&2
    exit 2
fi

# Each listing is captured before it is read, so a package that fails to
# build stops the run instead of silently dropping its targets.
pkgs=$("$GO" list ./...) || die "could not list packages"
targets=""
for pkg in $pkgs; do
    listed=$("$GO" test -list '^Fuzz' "$pkg") || die "could not list fuzz targets in $pkg"
    for name in $(grep -E '^Fuzz[A-Za-z0-9_]*$' <<<"$listed" || true); do
        targets+="$pkg $name"$'\n'
    done
done
[ -n "$targets" ] || die "no fuzz targets found"

if [ "$list" -eq 1 ]; then
    printf '%s' "$targets"
    exit 0
fi

failed=""
while read -r pkg name; do
    echo "==> $pkg $name"
    if ! "$GO" test -run '^$' -fuzz "^${name}\$" -fuzztime "$FUZZTIME" "$pkg"; then
        failed+="  $pkg $name"$'\n'
    fi
done <<<"${targets%$'\n'}"

if [ -n "$failed" ]; then
    echo "Failed:"
    printf '%s' "$failed"
    exit 1
fi
echo "Every fuzz target passed."
