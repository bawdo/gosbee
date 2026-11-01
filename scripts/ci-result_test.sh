#!/usr/bin/env bash
#
# Tests for scripts/ci-result.sh. Each case builds a NEEDS object, runs the
# script for an event and asserts on stdout and exit code.
# Prints "ok <name>" per case and exits non-zero on the first failure, with a
# line starting "FAIL".
#
# Usage: scripts/ci-result_test.sh

set -euo pipefail

DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$DIR/ci-result.sh"
WORKFLOW="$DIR/../.github/workflows/ci.yml"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/ci-result_test.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

OUT=""
ERR=""
CODE=0

fail() {
    echo "FAIL $1: $2" >&2
    echo "  exit: $CODE" >&2
    echo "  stdout: $OUT" >&2
    echo "  stderr: $ERR" >&2
    exit 1
}

# needs JOB=RESULT...: prints a NEEDS object with those jobs.
needs() {
    local json="" pair
    for pair in "$@"; do
        json+="${json:+,}\"${pair%%=*}\":{\"result\":\"${pair#*=}\",\"outputs\":{}}"
    done
    echo "{$json}"
}

JOBS=(lint test)

# all_success [JOB=RESULT...]: every expected job as success, with overrides.
# An override naming a job outside JOBS is added as an extra job.
all_success() {
    local job over pairs=()
    for job in "${JOBS[@]}"; do
        for over in "$@"; do
            if [ "${over%%=*}" = "$job" ]; then
                pairs+=("$over")
                continue 2
            fi
        done
        pairs+=("$job=success")
    done
    for over in "$@"; do
        case " ${JOBS[*]} " in *" ${over%%=*} "*) ;; *) pairs+=("$over") ;; esac
    done
    needs "${pairs[@]}"
}

# run_script SCRIPT EVENT JSON: runs a script, capturing OUT, ERR and CODE.
run_script() {
    CODE=0
    OUT=$(EVENT="$2" NEEDS="$3" bash "$1" 2>"$TMP/err") || CODE=$?
    ERR=$(cat "$TMP/err")
}

# run EVENT JSON: runs ci-result.sh.
run() {
    run_script "$SCRIPT" "$1" "$2"
}

expect_code() {
    [ "$CODE" -eq "$2" ] || fail "$1" "expected exit $2"
}

expect_line() {
    printf '%s\n' "$OUT" | grep -qxF -- "$2" || fail "$1" "missing line: $2"
}

# The six cases the unit 04 brief names, run for every ci.yml trigger.
for event in pull_request push schedule workflow_dispatch; do
    t="all_success_passes_$event"
    run "$event" "$(all_success)"
    expect_code "$t" 0
    expect_line "$t" "  lint: success"
    expect_line "$t" "  test: success"
    [ "${OUT##*$'\n'}" = "Every expected job passed." ] || fail "$t" "last line wrong"
    echo "ok $t"
done

t=one_failure_fails
run pull_request "$(all_success test=failure)"
expect_code $t 1
expect_line $t "  test: failure"
echo "ok $t"

t=cancelled_job_fails
run push "$(all_success lint=cancelled)"
expect_code $t 1
expect_line $t "  lint: cancelled"
echo "ok $t"

# No event permits a skip today, so a permitted skip is tested against a copy
# of the script whose pull_request arm permits "test". The copy must differ
# from the original, or the rewrite silently missed and the case proves nothing.
t=permitted_skip_passes
sed 's/^        pull_request) echo "" ;;$/        pull_request) echo "test" ;;/' \
    "$SCRIPT" > "$TMP/ci-result.sh"
! cmp -s "$SCRIPT" "$TMP/ci-result.sh" || fail $t "could not rewrite the pull_request arm"
run_script "$TMP/ci-result.sh" pull_request "$(all_success test=skipped)"
expect_code $t 0
expect_line $t "  test: skipped"
echo "ok $t"

t=permitted_skip_is_per_event
run_script "$TMP/ci-result.sh" push "$(all_success test=skipped)"
expect_code $t 1
expect_line $t "  test: skipped"
echo "ok $t"

for event in pull_request push schedule workflow_dispatch merge_group; do
    t="unpermitted_skip_fails_$event"
    run "$event" "$(all_success lint=skipped)"
    expect_code "$t" 1
    expect_line "$t" "Not passed:"
    expect_line "$t" "  lint: skipped"
    echo "ok $t"
done

t=missing_job_fails
run pull_request "$(needs lint=success)"
expect_code $t 1
expect_line $t "  test: missing"
echo "ok $t"

t=extra_job_fails
run pull_request "$(all_success fuzz=success)"
expect_code $t 1
expect_line $t "  fuzz: unexpected"
echo "ok $t"

t=needs_not_an_object_fails
run pull_request '[]'
expect_code $t 1
expect_line $t "  job results are not an object"
echo "ok $t"

t=unset_event_fails
CODE=0
OUT=$(NEEDS="$(all_success)" bash "$SCRIPT" 2>"$TMP/err") || CODE=$?
ERR=$(cat "$TMP/err")
expect_code $t 1
[ "$ERR" = "ci-result: EVENT is not set" ] || fail $t "wrong error"
echo "ok $t"

t=unset_needs_fails
CODE=0
OUT=$(EVENT=push bash "$SCRIPT" 2>"$TMP/err") || CODE=$?
ERR=$(cat "$TMP/err")
expect_code $t 1
[ "$ERR" = "ci-result: NEEDS is not set" ] || fail $t "wrong error"
echo "ok $t"

t=jobs_lists_expected
CODE=0
OUT=$(bash "$SCRIPT" --jobs 2>&1) || CODE=$?
expect_code $t 0
[ "$OUT" = "$(printf '%s\n' "${JOBS[@]}")" ] || fail $t "wrong job list"
echo "ok $t"

# The gate job's needs must name exactly the jobs the script expects, so a job
# added to one without the other fails here.
t=ci_yml_needs_matches_jobs
CODE=0
expected=$(bash "$SCRIPT" --jobs | paste -sd, - | sed 's/,/, /g')
actual=$(sed -n '/^  ci-ok:/,$p' "$WORKFLOW" | sed -n 's/^    needs: \[ *\(.*[^ ]\) *\]$/\1/p')
[ "[$actual]" = "[$expected]" ] || fail $t "needs is [$actual], script expects [$expected]"
echo "ok $t"

# Every job ci.yml defines, bar the gate itself, must be one the script expects,
# so a job added to ci.yml alone is not invisible to the gate.
t=ci_yml_jobs_match_gate
CODE=0
expected=$(bash "$SCRIPT" --jobs)
actual=$(sed -n '/^jobs:/,$p' "$WORKFLOW" | sed -n 's/^  \([A-Za-z_][A-Za-z0-9_-]*\):[[:space:]]*\(#.*\)\{0,1\}$/\1/p' | grep -vx ci-ok)
[ "$actual" = "$expected" ] || fail $t "ci.yml defines [$(paste -sd, - <<<"$actual")], script expects [$(paste -sd, - <<<"$expected")]"
echo "ok $t"

# The gate keeps the check name the main ruleset requires, runs whatever the
# jobs it needs did, and hands EVENT and NEEDS to the script through env only.
t=ci_yml_gate_shape
CODE=0
gate=$(sed -n '/^  ci-ok:/,$p' "$WORKFLOW")
grep -qxF '    name: CI OK' <<<"$gate" || fail $t "gate is not named CI OK"
grep -qxF '    if: always()' <<<"$gate" || fail $t "gate does not run if: always()"
# shellcheck disable=SC2016 # the ${{ }} is GitHub expression text, matched literally
grep -qxF '          EVENT: ${{ github.event_name }}' <<<"$gate" || fail $t "EVENT not passed through env"
# shellcheck disable=SC2016 # the ${{ }} is GitHub expression text, matched literally
grep -qxF '          NEEDS: ${{ toJSON(needs) }}' <<<"$gate" || fail $t "NEEDS not passed through env"
grep -qxF '        run: scripts/ci-result.sh' <<<"$gate" || fail $t "gate does not run scripts/ci-result.sh"
echo "ok $t"

t=no_alls_green
CODE=0
! grep -rn 'alls-green' "$DIR/../.github/workflows" || fail $t "re-actors/alls-green still in a workflow"
echo "ok $t"

echo "all ci-result tests passed"
