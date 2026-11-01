#!/usr/bin/env bash
#
# Applies gosbee's GitHub repository settings, one phase at a time:
#
#   scripts/repo-settings.sh metadata   description, homepage, topics, merge
#                                       methods (squash only), auto-merge,
#                                       branch update and deletion on merge
#   scripts/repo-settings.sh rulesets   the Protect main, Protect release
#                                       tags and Protect release tag creation
#                                       rulesets: main requires CI OK and
#                                       Analyze (go), up to date, and CodeQL
#                                       results, with no bypass actors; only
#                                       admins can create v* tags
#   scripts/repo-settings.sh harden     Actions allowlist and SHA pinning,
#                                       read-only default token, fork
#                                       approval, CodeQL extended, private
#                                       vulnerability reporting, immutable
#                                       releases, the release environment
#   scripts/repo-settings.sh show       prints the current settings
#
# REPO (default bawdo/gosbee) names the repository. Every write sends the
# full desired state (PATCH or PUT; rulesets and environment policies are
# found by name and replaced in place), so any phase can be re-run. Each
# write prints its method and path before it runs. With DRY_RUN set, writes
# are printed with their bodies and not sent; reads still run where a phase
# needs them to work out its writes. WORKFLOWS_DIR (default .github/workflows
# in this script's repository) is where harden reads the actions in use.

set -euo pipefail

REPO="${REPO:-bawdo/gosbee}"
R="repos/$REPO"
ENVIRONMENT=release
P="$R/environments/$ENVIRONMENT/deployment-branch-policies"
WORKFLOWS_DIR="${WORKFLOWS_DIR:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/.github/workflows}"

die() {
    echo "repo-settings: $*" >&2
    exit 1
}

WORK="$(mktemp -d "${TMPDIR:-/tmp}/repo-settings.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

# write METHOD PATH [BODY]: prints "METHOD PATH" and sends it, with BODY (a
# JSON string) as the request body when given. Under DRY_RUN it prints the
# body too and sends nothing. This is the only place a write is sent.
write() {
    local method="$1" path="$2" body="${3:-}" file
    echo "$method $path"
    if [[ -n "$body" ]]; then
        file="$WORK/body.json"
        jq . <<<"$body" > "$file"
    fi
    if [[ -n "${DRY_RUN:-}" ]]; then
        [[ -n "$body" ]] && sed 's/^/    /' "$file"
        return 0
    fi
    local args=(api -X "$method" "$path") out code=0
    [[ -n "$body" ]] && args+=(--input "$file")
    # On failure gh prints the response body (with GitHub's errors array
    # naming any rejected field) on stdout, so keep it for the stderr report.
    out=$(gh "${args[@]}") || code=$?
    if [[ $code -ne 0 ]]; then
        [[ -n "$out" ]] && echo "$out" >&2
        die "$method $path failed (exit $code)"
    fi
}

# list PATH JQ: prints GET PATH filtered by JQ, or dies. A phase that chooses
# between PUT and POST from a list must never read a failed list as empty,
# or it creates a duplicate. With ABSENT_OK set, a 404 prints "[]" instead.
list() {
    local out
    if out=$(gh api "$1" 2>/dev/null); then
        jq -c "$2" <<<"$out"
    elif [[ -n "${ABSENT_OK:-}" ]] && jq -e '.status == "404"' <<<"$out" >/dev/null 2>&1; then
        echo '[]'
    else
        [[ -n "$out" ]] && echo "$out" >&2
        die "cannot read $1"
    fi
}

metadata() {
    write PATCH "$R" "$(jq -n '{
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
        has_projects: false
    }')"
    write PUT "$R/topics" "$(jq -n '{names: ["go", "golang", "sql", "sql-builder",
        "query-builder", "ast", "postgresql", "mysql", "sqlite", "arel"]}')"
}

# ruleset NAME BODY: replaces the ruleset called NAME when it exists,
# otherwise creates it.
ruleset() {
    local id
    id=$(list "$R/rulesets" '.' | jq -r --arg n "$1" '[.[] | select(.name == $n) | .id][0] // empty')
    if [[ -n "$id" ]]; then
        write PUT "$R/rulesets/$id" "$2"
    else
        write POST "$R/rulesets" "$2"
    fi
}

rulesets() {
    local run_id jobs
    run_id=$(gh run list --repo "$REPO" --workflow ci.yml --branch main --event push --status success --limit 1 \
        --json databaseId --jq '.[0].databaseId // empty')
    [[ -n "$run_id" ]] || die "no successful CI run on main"
    jobs=$(gh api "$R/actions/runs/$run_id/jobs" --jq '.jobs[].name')
    grep -qxF 'CI OK' <<<"$jobs" || die "no CI OK job in the latest successful CI run on main"
    # The list is read up front so a failed read stops the phase before its
    # first write rather than between two rulesets.
    list "$R/rulesets" '.' >/dev/null

    # CI OK fails unless every other CI job passed; Analyze (go) is CodeQL
    # default setup. integration_id 15368 is the GitHub Actions app, so only
    # Actions can report either check.
    ruleset "Protect main" "$(jq -n '{
        name: "Protect main",
        target: "branch",
        enforcement: "active",
        bypass_actors: [],
        conditions: {ref_name: {include: ["~DEFAULT_BRANCH"], exclude: []}},
        rules: [
            {type: "deletion"},
            {type: "non_fast_forward"},
            {type: "required_signatures"},
            {type: "pull_request", parameters: {
                required_approving_review_count: 0,
                dismiss_stale_reviews_on_push: false,
                require_code_owner_review: false,
                require_last_push_approval: false,
                required_review_thread_resolution: true,
                allowed_merge_methods: ["squash"]
            }},
            {type: "required_status_checks", parameters: {
                strict_required_status_checks_policy: true,
                required_status_checks: [
                    {context: "CI OK", integration_id: 15368},
                    {context: "Analyze (go)", integration_id: 15368}
                ]
            }},
            {type: "code_scanning", parameters: {
                code_scanning_tools: [{
                    tool: "CodeQL",
                    security_alerts_threshold: "high_or_higher",
                    alerts_threshold: "errors"
                }]
            }}
        ]
    }')"
    ruleset "Protect release tags" "$(jq -n '{
        name: "Protect release tags",
        target: "tag",
        enforcement: "active",
        bypass_actors: [],
        conditions: {ref_name: {include: ["refs/tags/v*"], exclude: []}},
        rules: [{type: "deletion"}, {type: "non_fast_forward"}, {type: "update"}]
    }')"
    # Only the admin role (RepositoryRole 5) can create release tags, so
    # write access alone cannot start a release. It is a separate ruleset so
    # that update and deletion above keep no bypass at all.
    ruleset "Protect release tag creation" "$(jq -n '{
        name: "Protect release tag creation",
        target: "tag",
        enforcement: "active",
        bypass_actors: [{actor_id: 5, actor_type: "RepositoryRole", bypass_mode: "always"}],
        conditions: {ref_name: {include: ["refs/tags/v*"], exclude: []}},
        rules: [{type: "creation"}]
    }')"
}

# third_party_actions: prints "owner/repo@*" for every action the workflows
# use that GitHub does not own (actions/* and github/*), one per line, sorted
# and deduplicated. Local reusable workflows (./...) are not actions. Dies if
# there are no workflows or an action is not pinned to a 40-hex commit SHA,
# since SHA pinning makes GitHub refuse such a workflow anyway.
third_party_actions() {
    local files file ref name
    files=$(find "$WORKFLOWS_DIR" -maxdepth 1 \( -name '*.yml' -o -name '*.yaml' \) 2>/dev/null | sort)
    [[ -n "$files" ]] || die "no workflows found in $WORKFLOWS_DIR"
    while IFS= read -r file; do
        while IFS= read -r ref; do
            [[ "$ref" == ./* ]] && continue
            [[ "$ref" =~ ^([^@]+)@([0-9a-f]{40})$ ]] \
                || die "$(basename "$file"): $ref is not pinned to a commit SHA"
            name="${BASH_REMATCH[1]}"
            case "$name" in
                actions/* | github/*) continue ;;
            esac
            # owner/repo only: a path inside the repository shares its entry.
            echo "$(cut -d/ -f1,2 <<<"$name")@*"
        done < <(sed -nE 's/^[[:space:]]*-?[[:space:]]*uses:[[:space:]]*([^[:space:]#]+).*/\1/p' "$file")
    done <<<"$files" | sort -u
}

# environment: the release environment, deployable from v* tags and from
# main. main is admitted because the documented recovery path dispatches
# Release from main with an existing tag, where the ref is refs/heads/main;
# a dispatch from any other branch is refused. Policies not in that list are
# deleted, so a re-run converges.
environment() {
    write PUT "$R/environments/$ENVIRONMENT" "$(jq -n '{
        wait_timer: 0,
        reviewers: [],
        deployment_branch_policy: {protected_branches: false, custom_branch_policies: true}
    }')"
    local current want type name id
    # Under DRY_RUN the environment may not exist yet, and GitHub answers 404.
    current=$(ABSENT_OK=1 list "$P" '[.branch_policies[]? | {id, name, type}]')
    for want in "tag v*" "branch main"; do
        type="${want%% *}"
        name="${want#* }"
        id=$(jq -r --arg n "$name" --arg t "$type" \
            '[.[] | select(.name == $n and .type == $t) | .id][0] // empty' <<<"$current")
        if [[ -n "$id" ]]; then
            write PUT "$P/$id" "$(jq -n --arg n "$name" '{name: $n}')"
        else
            write POST "$P" "$(jq -n --arg n "$name" --arg t "$type" '{name: $n, type: $t}')"
        fi
    done
    while IFS= read -r id; do
        [[ -n "$id" ]] && write DELETE "$P/$id"
    done < <(jq -r '.[] | select((.type == "tag" and .name == "v*") or (.type == "branch" and .name == "main") | not) | .id' <<<"$current")
}

harden() {
    local patterns line
    patterns=$(third_party_actions)
    echo "third-party actions:"
    while IFS= read -r line; do echo "  $line"; done <<<"$patterns"
    # allowed_actions must be "selected" before the selection can be set.
    write PUT "$R/actions/permissions" '{"enabled":true,"allowed_actions":"selected","sha_pinning_required":true}'
    write PUT "$R/actions/permissions/selected-actions" "$(jq -n --arg p "$patterns" '{
        github_owned_allowed: true,
        verified_allowed: false,
        patterns_allowed: ($p | split("\n") | map(select(. != "")))
    }')"
    write PUT "$R/actions/permissions/workflow" '{"default_workflow_permissions":"read","can_approve_pull_request_reviews":false}'
    write PUT "$R/actions/permissions/fork-pr-contributor-approval" '{"approval_policy":"all_external_contributors"}'
    write PATCH "$R/code-scanning/default-setup" '{"state":"configured","query_suite":"extended","languages":["actions","go"]}'
    write PUT "$R/private-vulnerability-reporting"
    # Once published, a release's tag and assets cannot change.
    # publish-release.sh creates a draft, attaches the assets, then
    # publishes, which fits.
    write PUT "$R/immutable-releases"
    environment
}

# read_api PATH JQ: prints GET PATH filtered by JQ. When the read fails it
# says so on stderr and returns 1 (gh prints the error body on stdout, so it
# is discarded).
read_api() {
    local out
    if out=$(gh api "$1" --jq "$2" 2>/dev/null); then
        echo "$out"
    else
        echo "repo-settings: cannot read $1" >&2
        return 1
    fi
}

# get PATH JQ FALLBACK: prints GET PATH filtered by JQ, or FALLBACK when the
# read fails, for reads where a failure means the feature is off or absent.
get() {
    local out
    if out=$(gh api "$1" --jq "$2" 2>/dev/null); then echo "$out"; else echo "$3"; fi
}

# show: prints what it can read, then exits 1 if a required read failed.
show() {
    local failed=0 value
    read_api "$R" '
        "description: \(.description)",
        "homepage: \(.homepage)",
        "topics: \(.topics | join(" "))",
        "merge_methods: \([["merge", .allow_merge_commit], ["squash", .allow_squash_merge], ["rebase", .allow_rebase_merge]] | map(select(.[1]) | .[0]) | join(" "))",
        "allow_auto_merge: \(.allow_auto_merge)",
        "allow_update_branch: \(.allow_update_branch)",
        "delete_branch_on_merge: \(.delete_branch_on_merge)",
        "has_discussions: \(.has_discussions)",
        "has_wiki: \(.has_wiki)",
        "has_projects: \(.has_projects)"' \
        || failed=1
    value=$(read_api "$R/rulesets" '[.[].name] | join(", ")') || { value=unavailable; failed=1; }
    echo "rulesets: $value"
    local onoff='if .enabled then "enabled" else "disabled" end'
    echo "private_vulnerability_reporting: $(get "$R/private-vulnerability-reporting" "$onoff" unknown)"
    echo "allowed_actions: $(get "$R/actions/permissions" '.allowed_actions' unknown)"
    echo "sha_pinning_required: $(get "$R/actions/permissions" '.sha_pinning_required' unknown)"
    echo "allowed_action_patterns: $(get "$R/actions/permissions/selected-actions" '.patterns_allowed | join(" ")' none)"
    echo "default_workflow_permissions: $(get "$R/actions/permissions/workflow" '.default_workflow_permissions' unknown)"
    echo "fork_pr_approval: $(get "$R/actions/permissions/fork-pr-contributor-approval" '.approval_policy' unknown)"
    echo "code_scanning_default_setup: $(get "$R/code-scanning/default-setup" '.state' unknown)"
    echo "code_scanning_query_suite: $(get "$R/code-scanning/default-setup" '.query_suite' unknown)"
    # GET answers 200 with enabled false when off, so both fall back to
    # disabled.
    echo "immutable_releases: $(get "$R/immutable-releases" "$onoff" disabled)"
    echo "release_environment_policies: $(get "$P" \
        '[.branch_policies[] | "\(.type) \(.name)"] | if length == 0 then "none" else join(", ") end' none)"
    return "$failed"
}

case "${1:-}" in
    metadata) metadata ;;
    rulesets) rulesets ;;
    harden) harden ;;
    show) show ;;
    *) die "usage: repo-settings.sh metadata|rulesets|harden|show" ;;
esac
