#!/usr/bin/env bash
#
# CI result gate: fails unless every expected job passed.
#
#   EVENT=<github.event_name> NEEDS=<toJSON(needs)> scripts/ci-result.sh
#   scripts/ci-result.sh --jobs     list the expected job names, one per line
#
# A job passes on "success". A "skipped" result passes only where the event's
# row in the skippable table below permits that job. A failed or cancelled
# job fails, a job missing from NEEDS fails, and a job in NEEDS that this
# script does not expect fails too. Each job's result is printed.
#
# EVENT and NEEDS come in through the environment only, never interpolated
# into a run: line, so a crafted job output cannot become shell.

set -euo pipefail

# Every job in ci.yml the CI OK gate needs, in file order.
JOBS=(lint test)

# The jobs each event may skip, space separated. Today no event may skip
# anything. scripts/ci-result_test.sh rewrites the pull_request arm in a copy,
# so keep its exact shape.
skippable() {
    case "$1" in
        pull_request) echo "" ;;
        push) echo "" ;;
        schedule) echo "" ;;
        workflow_dispatch) echo "" ;;
        *) echo "" ;;
    esac
}

die() {
    echo "ci-result: $*" >&2
    exit 1
}

if [ "${1:-}" = "--jobs" ]; then
    printf '%s\n' "${JOBS[@]}"
    exit 0
fi

[ -n "${EVENT:-}" ] || die "EVENT is not set"
[ -n "${NEEDS:-}" ] || die "NEEDS is not set"

may_skip=$(skippable "$EVENT")
all=$(printf '%s\n' "${JOBS[@]}" | jq -R . | jq -sc .)
# Split in jq, not into a bash array: an empty array is unbound under set -u
# in bash 3.2, the macOS system bash.
skip=$(jq -nc --arg s "$may_skip" '$s | split(" ") | map(select(length > 0))')

echo "Event: $EVENT (may skip: ${may_skip:-nothing})"

report=$(jq -r --argjson all "$all" --argjson skip "$skip" '
    if type != "object" then "bad\t  job results are not an object"
    else . as $needs | ($all + keys | unique)[]
        | . as $job
        | (if ($all | any(. == $job)) | not then "unexpected"
           else ($needs[$job].result // "missing") end) as $r
        | (if $r == "success" then "ok"
           elif $r == "skipped" and ($skip | any(. == $job)) then "ok"
           else "bad" end) + "\t  \($job): \($r)"
    end' <<<"$NEEDS")

printf '%s\n' "$report" | cut -f2-
bad=$(printf '%s\n' "$report" | grep '^bad' | cut -f2- || true)

if [ -n "$bad" ]; then
    echo "Not passed:"
    echo "$bad"
    exit 1
fi
echo "Every expected job passed."
