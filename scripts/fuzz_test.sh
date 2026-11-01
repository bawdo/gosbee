#!/usr/bin/env bash
#
# Tests for scripts/fuzz.sh, against a fake go that logs its arguments.
# Prints "ok <name>" per case and exits non-zero on the first failure, with a
# line starting "FAIL".
#
# Usage: scripts/fuzz_test.sh

set -euo pipefail

DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$DIR/fuzz.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/fuzz_test.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

OUT=""
CODE=0

fail() {
    echo "FAIL $1: $2" >&2
    echo "  exit: $CODE" >&2
    echo "  output: $OUT" >&2
    echo "  go log:" >&2
    sed 's/^/    /' "$TMP/go.log" >&2 2>/dev/null || true
    exit 1
}

# The fake go: "list ./..." prints two packages, "test -list" prints that
# package's targets and an ok line, "test ... -fuzz ^Name$" logs and fails
# when Name is in FAKE_FAIL. Every call is logged, one line of argv each.
cat > "$TMP/go" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_LOG"
case "$1 $2" in
    "list ./...")
        [ -z "${FAKE_GO_LIST_FAIL:-}" ] || { echo "go: no go.mod" >&2; exit 1; }
        printf '%s\n' ${FAKE_PKGS-example.com/a example.com/b}
        exit 0 ;;
esac
if [ "$1" = test ] && [ "$2" = -list ]; then
    if [ "$4" = "${FAKE_LIST_FAIL:-}" ]; then
        echo "$4: build failed" >&2
        exit 1
    fi
    case "$4" in
        example.com/a) printf 'FuzzOne\nFuzzTwo\n' ;;
        example.com/b) printf 'FuzzThree\n' ;;
    esac
    echo "ok  	$4	0.01s"
    exit 0
fi
for arg in "$@"; do
    case "$arg" in
        ^Fuzz*) name="${arg#^}"; name="${name%\$}"
            case " ${FAKE_FAIL:-} " in *" $name "*) echo "--- FAIL: $name"; exit 1 ;; esac ;;
    esac
done
echo PASS
EOF
chmod +x "$TMP/go"

# run [VAR=VALUE...] [-- ARGS...]: runs fuzz.sh with the fake go and a fresh log.
run() {
    : > "$TMP/go.log"
    CODE=0
    local envs=() args=()
    while [ $# -gt 0 ]; do
        if [ "$1" = -- ]; then shift; args=("$@"); break; fi
        envs+=("$1"); shift
    done
    OUT=$(env GO="$TMP/go" FAKE_LOG="$TMP/go.log" ${envs[@]+"${envs[@]}"} \
        bash "$SCRIPT" ${args[@]+"${args[@]}"} 2>&1) || CODE=$?
}

# fuzz_calls: the logged -fuzz invocations, one per line.
fuzz_calls() {
    grep -- '-fuzz ' "$TMP/go.log" || true
}

expect_code() {
    [ "$CODE" -eq "$2" ] || fail "$1" "expected exit $2"
}

t=runs_every_target_in_order
run
expect_code $t 0
expected="test -run ^$ -fuzz ^FuzzOne$ -fuzztime 10s example.com/a
test -run ^$ -fuzz ^FuzzTwo$ -fuzztime 10s example.com/a
test -run ^$ -fuzz ^FuzzThree$ -fuzztime 10s example.com/b"
[ "$(fuzz_calls)" = "$expected" ] || fail $t "wrong calls"
grep -qxF '==> example.com/b FuzzThree' <<<"$OUT" || fail $t "no ==> line for FuzzThree"
echo "ok $t"

t=fuzztime_passed_through
run FUZZTIME=5m
expect_code $t 0
[ "$(fuzz_calls | grep -c -- '-fuzztime 5m ')" -eq 3 ] || fail $t "FUZZTIME not passed to every target"
echo "ok $t"

t=continues_after_failure
run FAKE_FAIL=FuzzTwo
expect_code $t 1
[ "$(fuzz_calls | wc -l | tr -d ' ')" -eq 3 ] || fail $t "stopped before the last target"
grep -qxF 'Failed:' <<<"$OUT" || fail $t "no Failed: line"
grep -qxF '  example.com/a FuzzTwo' <<<"$OUT" || fail $t "failed target not listed"
! grep -qxF '  example.com/a FuzzOne' <<<"$OUT" || fail $t "passing target listed as failed"
echo "ok $t"

t=list_failure_fails
run FAKE_LIST_FAIL=example.com/b
expect_code $t 1
[ -z "$(fuzz_calls)" ] || fail $t "fuzzed with a package unlisted"
grep -qF 'could not list fuzz targets in example.com/b' <<<"$OUT" || fail $t "wrong message"
echo "ok $t"

t=package_list_failure_fails
run FAKE_GO_LIST_FAIL=1
expect_code $t 1
grep -qF 'could not list packages' <<<"$OUT" || fail $t "wrong message"
echo "ok $t"

t=no_targets_fails
run FAKE_PKGS=example.com/none
expect_code $t 1
grep -qF 'no fuzz targets found' <<<"$OUT" || fail $t "wrong message"
echo "ok $t"

for bad in '5 m' abc '' '10' '-1s' '1s;id'; do
    t="bad_fuzztime_refused_[$bad]"
    run "FUZZTIME=$bad"
    expect_code "$t" 2
    grep -qF "FUZZTIME must be a Go duration or Nx, got '$bad'" <<<"$OUT" || fail "$t" "wrong message"
    [ ! -s "$TMP/go.log" ] || fail "$t" "go was called"
    echo "ok $t"
done

for good in 1m30s 100x 1.5s 250ms; do
    t="good_fuzztime_accepted_$good"
    run "FUZZTIME=$good"
    expect_code "$t" 0
    echo "ok $t"
done

t=list_mode_runs_nothing
run -- --list
expect_code $t 0
[ "$OUT" = "example.com/a FuzzOne
example.com/a FuzzTwo
example.com/b FuzzThree" ] || fail $t "wrong list"
[ -z "$(fuzz_calls)" ] || fail $t "a target was fuzzed"
echo "ok $t"

t=unknown_argument_refused
run -- --bogus
expect_code $t 2
[ -z "$(fuzz_calls)" ] || fail $t "a target was fuzzed"
echo "ok $t"

t=make_fuzz_calls_script
CODE=0
OUT=$(make -C "$DIR/.." -n fuzz FUZZTIME=7s 2>&1) || CODE=$?
expect_code $t 0
grep -qE '^FUZZTIME=7s GO=[^ ]+ \./scripts/fuzz\.sh$' <<<"$OUT" || fail $t "make fuzz does not run fuzz.sh with FUZZTIME"
echo "ok $t"

t=make_fuzz_default_10s
CODE=0
OUT=$(make -C "$DIR/.." -n fuzz 2>&1) || CODE=$?
expect_code $t 0
grep -qE '^FUZZTIME=10s ' <<<"$OUT" || fail $t "default FUZZTIME is not 10s"
echo "ok $t"

echo "all fuzz tests passed"
