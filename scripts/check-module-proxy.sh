#!/usr/bin/env bash
#
# Checks that proxy.golang.org serves a released module version at the
# commit its tag points at, and that sum.golang.org records the version.
#
#   scripts/check-module-proxy.sh <module> vX.Y.Z[-suffix] <40-hex commit>
#
# The proxy keeps the first commit it sees for a version, for good. If that
# is not the tagged commit, the tag was moved after the proxy fetched it, and
# nothing can undo that: the version needs a retract and a new tag. The
# checksum database is what makes the version tamper evident, since every go
# command checks downloads against it.
#
# A version the proxy has not fetched yet is retried, up to 5 attempts, and
# a missing sumdb record up to 2, 5 seconds apart, each request limited to
# 5 seconds: about a minute at worst. A commit mismatch is never retried.
# Failures print one ::error:: line on stdout and exit 1. Needs curl and jq,
# reads no repository file, and checks its arguments before any request.

set -euo pipefail

die() {
    echo "check-module-proxy: $*" >&2
    exit 1
}

# fail MESSAGE: the one failure line, as a GitHub annotation.
fail() {
    echo "::error::$*"
    exit 1
}

usage="usage: check-module-proxy.sh <module> vX.Y.Z[-suffix] <40-hex commit>"
[ "$#" -eq 3 ] || die "$usage"
module="$1"
tag="$2"
commit="$3"
# Lower case only, so the module path needs no proxy escaping.
[[ "$module" =~ ^[a-z0-9.-]+(/[a-z0-9._-]+)+$ ]] || die "$usage"
# No element starts or ends with a dot (as Go requires), so no "." or ".."
# segment reaches a URL.
[[ "/$module/" != *"/."* && "/$module/" != *"./"* ]] || die "$usage"
[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || die "$usage"
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || die "$usage"

BODY=""

# fetch_until URL ATTEMPTS CHECK: fetches URL until curl succeeds and CHECK
# passes on the body, up to ATTEMPTS times, 5 seconds apart. Leaves the body
# in BODY and returns 1 when every attempt fails.
fetch_until() {
    local url="$1" attempts="$2" check="$3" attempt
    for ((attempt = 1; attempt <= attempts; attempt++)); do
        if BODY=$(curl -fsS --max-time 5 "$url") && "$check"; then
            return 0
        fi
        if [ "$attempt" -lt "$attempts" ]; then
            sleep 5
        fi
    done
    return 1
}

body_present() {
    [ -n "$BODY" ]
}

# The record line: module, version and an h1: hash, compared field by field,
# so neither a pre-release of the version nor its go.mod line passes.
sumdb_records_version() {
    awk -v m="$module" -v t="$tag" '$1 == m && $2 == t && substr($3, 1, 3) == "h1:" { found = 1 } END { exit !found }' \
        <<<"$BODY"
}

fetch_until "https://proxy.golang.org/$module/@v/$tag.info" 5 body_present \
    || fail "proxy.golang.org has no $module@$tag after 5 attempts"
hash=$(jq -r '.Origin.Hash // empty' <<<"$BODY" 2>/dev/null) \
    || fail "proxy.golang.org returned unreadable JSON for $module@$tag"
# The hash reaches the job log, so anything but a commit hash is "unknown".
[[ "$hash" =~ ^[0-9a-f]{40}$ ]] || hash=unknown
[ "$hash" = "$commit" ] || fail "proxy.golang.org has $module@$tag at $hash, the tag points at $commit"
echo "check-module-proxy: proxy.golang.org has $module@$tag at $commit"

fetch_until "https://sum.golang.org/lookup/$module@$tag" 2 sumdb_records_version \
    || fail "sum.golang.org has no record of $module@$tag after 2 attempts"
echo "check-module-proxy: sum.golang.org records $module@$tag"
