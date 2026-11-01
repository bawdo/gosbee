#!/usr/bin/env bash
#
# Tests for scripts/repo-settings.sh. Every gh call goes to a fake gh put
# first on PATH, which logs its arguments, copies any --input body to a
# second log and answers reads from canned files, so nothing here touches
# the network or changes a real repository. Prints "ok <name>" per case and
# exits non-zero on the first failure, with a line starting "FAIL".
#
# Usage: scripts/repo-settings_test.sh

set -euo pipefail

SCRIPTS="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(mktemp -d "${TMPDIR:-/tmp}/repo-settings_test.XXXXXX")"
trap 'rm -rf "$ROOT"' EXIT

R=repos/bawdo/gosbee
P="$R/environments/release/deployment-branch-policies"

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

# The fake gh. FAKE_GH_DIR holds the canned answers and the logs:
#   runs.json      what "run list" prints
#   jobs.json      body of GET .../actions/runs/ID/jobs
#   rulesets.json  body of GET .../rulesets
#   policies.json  body of GET .../deployment-branch-policies, when the file
#                  exists; otherwise that read answers 404, as it does
#                  before the environment exists
#   immutable.json body of GET .../immutable-releases, when the file exists
#                  (otherwise 404)
#   repo.json      body of GET repos/OWNER/NAME, when the file exists
#                  (otherwise 404)
#   fail           "METHOD PATH" lines; a call matching one prints a GitHub
#                  style error body on stdout and exits 1
#   gh.log         one line per call, the arguments joined by spaces
#   bodies.log     one line per write: "METHOD PATH <compact json>", or just
#                  "METHOD PATH" for a write without a body
mkdir -p "$ROOT/bin"
cat > "$ROOT/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "$*" >> "$FAKE_GH_DIR/gh.log"
jq_expr=""
input=""
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
    case "${args[$i]}" in
        --jq) jq_expr="${args[$((i + 1))]}" ;;
        --input) input="${args[$((i + 1))]}" ;;
    esac
done
answer() {
    if [ -n "$jq_expr" ]; then jq -r "$jq_expr" "$1"; else cat "$1"; fi
}
case "$1 $2" in
    "run list") answer "$FAKE_GH_DIR/runs.json"; exit 0 ;;
    "api -X")
        method="$3"
        path="$4"
        ;;
    "api "*)
        method=GET
        path="$2"
        ;;
    *)
        echo "fake gh: unexpected call: $*" >&2
        exit 2
        ;;
esac
if [ -f "$FAKE_GH_DIR/fail" ] && grep -qxF "$method $path" "$FAKE_GH_DIR/fail"; then
    echo '{"message":"Validation Failed","errors":["fake rejection"]}'
    exit 1
fi
if [ "$method" != GET ]; then
    if [ -n "$input" ]; then
        echo "$method $path $(jq -c . "$input")" >> "$FAKE_GH_DIR/bodies.log"
    else
        echo "$method $path" >> "$FAKE_GH_DIR/bodies.log"
    fi
    echo '{}'
    exit 0
fi
# A missing file answers as GitHub does for a missing resource: the error
# body on stdout, exit 1.
need() {
    if [ ! -f "$1" ]; then
        echo '{"message":"Not Found","status":"404"}'
        exit 1
    fi
    answer "$1"
}
case "$path" in
    */actions/runs/*/jobs) answer "$FAKE_GH_DIR/jobs.json" ;;
    */rulesets) answer "$FAKE_GH_DIR/rulesets.json" ;;
    */deployment-branch-policies) need "$FAKE_GH_DIR/policies.json" ;;
    */immutable-releases) need "$FAKE_GH_DIR/immutable.json" ;;
    repos/*/*/*) exit 1 ;;
    repos/*/*) need "$FAKE_GH_DIR/repo.json" ;;
    *)
        echo "fake gh: unexpected read: $*" >&2
        exit 2
        ;;
esac
EOF
chmod +x "$ROOT/bin/gh"

JOBS='{"jobs":[{"name":"Lint and audit"},{"name":"Test (stable, ubuntu-latest)"},{"name":"Fuzz"},{"name":"CI OK"}]}'

# fake NAME: a fresh directory of canned answers (a successful CI run with
# the CI jobs, no rulesets, no environment) and empty logs, plus a workflows
# directory using GitHub-owned actions, a subpath of one, a local reusable
# workflow and two third-party actions (one twice, oddly spaced). Prints its
# path.
fake() {
    local dir="$ROOT/$1"
    mkdir -p "$dir/workflows"
    : > "$dir/gh.log"
    : > "$dir/bodies.log"
    echo '[{"databaseId":42}]' > "$dir/runs.json"
    echo "$JOBS" > "$dir/jobs.json"
    echo '[]' > "$dir/rulesets.json"
    cat > "$dir/workflows/ci.yml" <<'EOF'
jobs:
  a:
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: golangci/golangci-lint-action@ba0d7d2ec06a0ea1cb5fa41b2e4a3ab91d21278a # v9.3.0
      - uses: codecov/codecov-action@303a32d7a59b442fa8d48b6a1cc6825c09c847a5 # v7.1.1
EOF
    cat > "$dir/workflows/release.yaml" <<'EOF'
jobs:
  ci:
    uses: ./.github/workflows/ci.yml
  b:
    steps:
      - uses: github/codeql-action/upload-sarif@9b3254085494b55a0f7c4c2b3d2d52cc712ce370 # v4.39.1
      -   uses:   codecov/codecov-action@303a32d7a59b442fa8d48b6a1cc6825c09c847a5
EOF
    echo "$dir"
}

# run DIR [VAR=VALUE...] PHASE: runs repo-settings.sh PHASE with the fake gh
# first on PATH, the fake workflows directory and any extra environment,
# capturing OUT, ERR and CODE.
run() {
    local dir="$1"
    shift
    CODE=0
    OUT=$(env -u DRY_RUN -u REPO PATH="$ROOT/bin:$PATH" FAKE_GH_DIR="$dir" \
        WORKFLOWS_DIR="$dir/workflows" \
        "${@:1:$#-1}" bash "$SCRIPTS/repo-settings.sh" "${!#}" 2>"$dir/.stderr") || CODE=$?
    ERR=$(cat "$dir/.stderr")
}

# body NAME DIR METHOD PATH JQ: asserts the first body sent with METHOD to
# PATH satisfies the jq filter JQ.
body() {
    local json
    json=$(awk -v m="$3" -v p="$4" '$1 == m && $2 == p { $1 = ""; $2 = ""; print; exit }' "$2/bodies.log")
    [ -n "$json" ] || fail "$1" "no $3 $4 body: $(cat "$2/bodies.log")"
    jq -e "$5" <<<"$json" >/dev/null || fail "$1" "$3 $4 body fails $5: $json"
}

# writes DIR: the "METHOD PATH" of every write sent, in order.
writes() {
    cut -d' ' -f1,2 "$1/bodies.log"
}

# no_writes NAME DIR: asserts nothing but GETs reached gh.
no_writes() {
    [ ! -s "$2/bodies.log" ] || fail "$1" "sent a write: $(cat "$2/bodies.log")"
    ! grep -q -- '^api -X' "$2/gh.log" || fail "$1" "ran a write: $(cat "$2/gh.log")"
}

name=metadata_patches_settings_and_topics
dir=$(fake "$name")
run "$dir" metadata
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
[ "$(writes "$dir")" = "PATCH $R
PUT $R/topics" ] || fail "$name" "wrong writes: $(cat "$dir/bodies.log")"
body "$name" "$dir" PATCH "$R" '. == {
    description: "A Golang SQL Builder",
    homepage: "https://pkg.go.dev/github.com/bawdo/gosbee",
    allow_auto_merge: true,
    allow_squash_merge: true,
    allow_merge_commit: false,
    allow_rebase_merge: false,
    delete_branch_on_merge: true,
    allow_update_branch: true,
    has_discussions: true,
    has_wiki: false,
    has_projects: false}'
body "$name" "$dir" PUT "$R/topics" '.names == ["go","golang","sql","sql-builder","query-builder","ast","postgresql","mysql","sqlite","arel"]'
grep -qx "PATCH $R" <<<"$OUT" || fail "$name" "PATCH not printed before sending"
echo "ok $name"

name=rulesets_needs_ci_ok_job
dir=$(fake "$name")
jq '.jobs |= map(select(.name != "CI OK"))' <<<"$JOBS" > "$dir/jobs.json"
run "$dir" rulesets
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
[ "$ERR" = "repo-settings: no CI OK job in the latest successful CI run on main" ] || fail "$name" "wrong refusal"
no_writes "$name" "$dir"
echo "ok $name"

name=rulesets_needs_a_ci_run
dir=$(fake "$name")
echo '[]' > "$dir/runs.json"
run "$dir" rulesets
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
[ "$ERR" = "repo-settings: no successful CI run on main" ] || fail "$name" "wrong refusal"
no_writes "$name" "$dir"
echo "ok $name"

name=rulesets_creates_when_absent
dir=$(fake "$name")
run "$dir" rulesets
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
grep -q -- "^run list --repo bawdo/gosbee --workflow ci.yml --branch main --event push --status success " "$dir/gh.log" \
    || fail "$name" "run list not filtered to successful pushes on main: $(cat "$dir/gh.log")"
[ "$(writes "$dir")" = "POST $R/rulesets
POST $R/rulesets
POST $R/rulesets" ] || fail "$name" "expected three POSTs: $(cat "$dir/bodies.log")"
main_body=$(sed -n 1p "$dir/bodies.log" | cut -d' ' -f3-)
jq -e '.name == "Protect main" and .target == "branch" and .enforcement == "active"
    and .conditions.ref_name == {"include":["~DEFAULT_BRANCH"],"exclude":[]}
    and .bypass_actors == []
    and ([.rules[].type] == ["deletion","non_fast_forward","required_signatures","pull_request","required_status_checks","code_scanning"])
    and (.rules[3].parameters == {
        "required_approving_review_count":0,
        "dismiss_stale_reviews_on_push":false,
        "require_code_owner_review":false,
        "require_last_push_approval":false,
        "required_review_thread_resolution":true,
        "require_extra_approval_for_unattributed_changes":true,
        "allowed_merge_methods":["squash"]})
    and (.rules[4].parameters.strict_required_status_checks_policy == true)
    and (.rules[4].parameters.required_status_checks == [
        {"context":"CI OK","integration_id":15368},
        {"context":"Analyze (go)","integration_id":15368}])
    and (.rules[5].parameters.code_scanning_tools == [
        {"tool":"CodeQL","security_alerts_threshold":"high_or_higher","alerts_threshold":"errors"}])' \
    <<<"$main_body" >/dev/null || fail "$name" "wrong main ruleset: $main_body"
tag_body=$(sed -n 2p "$dir/bodies.log" | cut -d' ' -f3-)
jq -e '.name == "Protect release tags" and .target == "tag" and .enforcement == "active"
    and .bypass_actors == []
    and .conditions.ref_name == {"include":["refs/tags/v*"],"exclude":[]}
    and ([.rules[].type] == ["deletion","non_fast_forward","update"])' <<<"$tag_body" >/dev/null \
    || fail "$name" "wrong tag ruleset: $tag_body"
create_body=$(sed -n 3p "$dir/bodies.log" | cut -d' ' -f3-)
jq -e '.name == "Protect release tag creation" and .target == "tag" and .enforcement == "active"
    and .bypass_actors == [{"actor_id":5,"actor_type":"RepositoryRole","bypass_mode":"always"}]
    and .conditions.ref_name == {"include":["refs/tags/v*"],"exclude":[]}
    and .rules == [{"type":"creation"}]' <<<"$create_body" >/dev/null \
    || fail "$name" "wrong tag creation ruleset: $create_body"
echo "ok $name"

name=rulesets_updates_when_present
dir=$(fake "$name")
echo '[{"id":101,"name":"Protect main","target":"branch"},{"id":202,"name":"Protect release tags","target":"tag"},{"id":303,"name":"Protect release tag creation","target":"tag"},{"id":404,"name":"Something else","target":"branch"}]' > "$dir/rulesets.json"
run "$dir" rulesets
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
[ "$(writes "$dir")" = "PUT $R/rulesets/101
PUT $R/rulesets/202
PUT $R/rulesets/303" ] || fail "$name" "not replaced in place: $(cat "$dir/bodies.log")"
body "$name" "$dir" PUT "$R/rulesets/101" '.name == "Protect main"'
body "$name" "$dir" PUT "$R/rulesets/202" '.name == "Protect release tags"'
body "$name" "$dir" PUT "$R/rulesets/303" '.name == "Protect release tag creation"'
echo "ok $name"

name=rulesets_stops_when_the_list_cannot_be_read
dir=$(fake "$name")
echo "GET $R/rulesets" > "$dir/fail"
run "$dir" rulesets
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
grep -qx "repo-settings: cannot read $R/rulesets" <<<"$ERR" || fail "$name" "no read error"
no_writes "$name" "$dir"
echo "ok $name"

name=harden_sends_every_setting_in_order
dir=$(fake "$name")
run "$dir" harden
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
expected="PUT $R/actions/permissions
PUT $R/actions/permissions/selected-actions
PUT $R/actions/permissions/workflow
PUT $R/actions/permissions/fork-pr-contributor-approval
PATCH $R/code-scanning/default-setup
PUT $R/private-vulnerability-reporting
PUT $R/immutable-releases
PUT $R/environments/release
POST $P
POST $P"
[ "$(writes "$dir")" = "$expected" ] || fail "$name" "wrong writes: $(cat "$dir/bodies.log")"
body "$name" "$dir" PUT "$R/actions/permissions" '. == {"enabled":true,"allowed_actions":"selected","sha_pinning_required":true}'
body "$name" "$dir" PUT "$R/actions/permissions/selected-actions" '. == {
    "github_owned_allowed":true,
    "verified_allowed":false,
    "patterns_allowed":["codecov/codecov-action@*","golangci/golangci-lint-action@*"]}'
body "$name" "$dir" PUT "$R/actions/permissions/workflow" '. == {"default_workflow_permissions":"read","can_approve_pull_request_reviews":false}'
body "$name" "$dir" PUT "$R/actions/permissions/fork-pr-contributor-approval" '. == {"approval_policy":"all_external_contributors"}'
body "$name" "$dir" PATCH "$R/code-scanning/default-setup" '. == {"state":"configured","query_suite":"extended","languages":["actions","go"]}'
body "$name" "$dir" PUT "$R/environments/release" '. == {
    "wait_timer":0,
    "reviewers":[],
    "deployment_branch_policy":{"protected_branches":false,"custom_branch_policies":true}}'
policies=$(grep "^POST $P " "$dir/bodies.log" | cut -d' ' -f3- | jq -sc .)
[ "$policies" = '[{"name":"v*","type":"tag"},{"name":"main","type":"branch"}]' ] \
    || fail "$name" "wrong environment policies: $policies"
echo "ok $name"

name=harden_lists_third_party_actions
dir=$(fake "$name")
run "$dir" DRY_RUN=1 harden
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
listed=$(sed -n '/^third-party actions:$/,/^[A-Z]/p' <<<"$OUT" | sed '1d;$d')
[ "$listed" = "  codecov/codecov-action@*
  golangci/golangci-lint-action@*" ] || fail "$name" "wrong list: $listed"
[ "$(grep -n '^third-party actions:$' <<<"$OUT" | cut -d: -f1)" -lt "$(grep -n '^PUT ' <<<"$OUT" | head -1 | cut -d: -f1)" ] \
    || fail "$name" "list not printed before the first write"
echo "ok $name"

name=harden_replaces_environment_policies
dir=$(fake "$name")
echo '{"total_count":2,"branch_policies":[{"id":7,"name":"v*","type":"tag"},{"id":9,"name":"release/*","type":"branch"}]}' > "$dir/policies.json"
run "$dir" harden
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
[ "$(writes "$dir" | grep "$P")" = "PUT $P/7
POST $P
DELETE $P/9" ] || fail "$name" "policies not converged: $(cat "$dir/bodies.log")"
body "$name" "$dir" PUT "$P/7" '. == {"name":"v*"}'
body "$name" "$dir" POST "$P" '. == {"name":"main","type":"branch"}'
echo "ok $name"

name=harden_matches_policies_on_type_as_well_as_name
dir=$(fake "$name")
echo '{"total_count":2,"branch_policies":[{"id":7,"name":"v*","type":"tag"},{"id":5,"name":"main","type":"tag"}]}' > "$dir/policies.json"
run "$dir" harden
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
[ "$(writes "$dir" | grep "$P")" = "PUT $P/7
POST $P
DELETE $P/5" ] || fail "$name" "a tag policy named main was taken for the branch policy: $(cat "$dir/bodies.log")"
body "$name" "$dir" POST "$P" '. == {"name":"main","type":"branch"}'
echo "ok $name"

name=harden_refuses_a_glob_in_an_action_name
n=0
for bad in 'evil/*' '*/*' 'evil/act?on' 'evil/[ab]'; do
    n=$((n + 1))
    dir=$(fake "$name-$n")
    echo "      - uses: $bad@303a32d7a59b442fa8d48b6a1cc6825c09c847a5" >> "$dir/workflows/ci.yml"
    run "$dir" harden
    [ "$CODE" -eq 1 ] || fail "$name" "$bad: expected exit 1"
    [ "$ERR" = "repo-settings: ci.yml: $bad@303a32d7a59b442fa8d48b6a1cc6825c09c847a5 is not an owner/repo action name" ] \
        || fail "$name" "$bad: wrong refusal"
    no_writes "$name" "$dir"
done
echo "ok $name"

name=harden_stops_when_policies_cannot_be_read
dir=$(fake "$name")
echo '{"total_count":0,"branch_policies":[]}' > "$dir/policies.json"
echo "GET $P" > "$dir/fail"
run "$dir" harden
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
grep -qx "repo-settings: cannot read $P" <<<"$ERR" || fail "$name" "no read error"
! grep -q "^POST $P" "$dir/bodies.log" || fail "$name" "created a policy after a failed read"
echo "ok $name"

name=harden_refuses_an_unpinned_action
dir=$(fake "$name")
echo '      - uses: ossf/scorecard-action@v2.5.0' >> "$dir/workflows/ci.yml"
run "$dir" harden
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
[ "$ERR" = "repo-settings: ci.yml: ossf/scorecard-action@v2.5.0 is not pinned to a commit SHA" ] || fail "$name" "wrong refusal"
no_writes "$name" "$dir"
echo "ok $name"

name=harden_needs_a_workflow
dir=$(fake "$name")
rm "$dir/workflows"/*
run "$dir" harden
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
[ "$ERR" = "repo-settings: no workflows found in $dir/workflows" ] || fail "$name" "wrong refusal"
no_writes "$name" "$dir"
echo "ok $name"

name=dry_run_sends_nothing_for_every_phase
for phase in metadata rulesets harden; do
    dir=$(fake "$name-$phase")
    run "$dir" DRY_RUN=1 "$phase"
    [ "$CODE" -eq 0 ] || fail "$name" "$phase: expected exit 0"
    no_writes "$name" "$dir"
    grep -qE '^(PUT|PATCH|POST) repos/bawdo/gosbee' <<<"$OUT" || fail "$name" "$phase: no write printed"
    # Every printed write that carries a body is followed by it, indented.
    first=$(grep -nE '^(PUT|PATCH|POST) ' <<<"$OUT" | head -1 | cut -d: -f1)
    sed -n "$((first + 1))p" <<<"$OUT" | grep -q '^    ' || fail "$name" "$phase: body not printed after the write"
done
echo "ok $name"

name=write_failure_reports_and_stops
dir=$(fake "$name")
echo "PATCH $R" > "$dir/fail"
run "$dir" metadata
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
grep -qF 'fake rejection' <<<"$ERR" || fail "$name" "GitHub's response not on stderr"
grep -qx "repo-settings: PATCH $R failed (exit 1)" <<<"$ERR" || fail "$name" "no failure line"
! grep -q "PUT $R/topics" "$dir/gh.log" || fail "$name" "carried on after a failed write"
echo "ok $name"

name=show_fails_on_read_error
dir=$(fake "$name")
run "$dir" show
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
grep -qx "repo-settings: cannot read $R" <<<"$ERR" || fail "$name" "no read error for $R"
! grep -q "cannot read" <<<"$OUT" || fail "$name" "read error on stdout"
grep -qx "allowed_actions: unknown" <<<"$OUT" || fail "$name" "no allowed_actions line"
grep -qx "immutable_releases: disabled" <<<"$OUT" || fail "$name" "no immutable_releases line"
grep -qx "release_environment_policies: none" <<<"$OUT" || fail "$name" "no policies line"
echo "ok $name"

name=show_prints_settings
dir=$(fake "$name")
cat > "$dir/repo.json" <<'EOF'
{"description":"d","homepage":"h","delete_branch_on_merge":true,"allow_update_branch":true,
 "has_wiki":false,"has_projects":false,"has_discussions":true,
 "allow_merge_commit":false,"allow_squash_merge":true,"allow_rebase_merge":false,"allow_auto_merge":true,
 "topics":["go","sql"]}
EOF
echo '{"enabled":true,"enforced_by_owner":false}' > "$dir/immutable.json"
echo '[{"id":1,"name":"Protect main"},{"id":2,"name":"Protect release tags"}]' > "$dir/rulesets.json"
echo '{"total_count":2,"branch_policies":[{"id":7,"name":"v*","type":"tag"},{"id":8,"name":"main","type":"branch"}]}' > "$dir/policies.json"
run "$dir" show
! grep -q "cannot read $R\$" <<<"$ERR" || fail "$name" "could not read $R"
grep -qx "homepage: h" <<<"$OUT" || fail "$name" "no homepage line"
grep -qx "merge_methods: squash" <<<"$OUT" || fail "$name" "wrong merge_methods line"
grep -qx "allow_auto_merge: true" <<<"$OUT" || fail "$name" "no allow_auto_merge line"
grep -qx "allow_update_branch: true" <<<"$OUT" || fail "$name" "no allow_update_branch line"
grep -qx "topics: go sql" <<<"$OUT" || fail "$name" "no topics line"
grep -qx "rulesets: Protect main, Protect release tags" <<<"$OUT" || fail "$name" "no rulesets line"
grep -qxF "release_environment_policies: tag v*, branch main" <<<"$OUT" || fail "$name" "no policies line"
grep -qx "immutable_releases: enabled" <<<"$OUT" || fail "$name" "no immutable_releases line"
echo "ok $name"

name=unknown_phase_prints_usage
dir=$(fake "$name")
run "$dir" pre-public
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
[ "$ERR" = "repo-settings: usage: repo-settings.sh metadata|rulesets|harden|show" ] || fail "$name" "wrong usage: $ERR"
no_writes "$name" "$dir"
echo "ok $name"
