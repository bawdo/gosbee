#!/usr/bin/env bash
#
# Checks that a release tag on GitHub is annotated and that GitHub verified
# its signature against the keys on the tagger's account.
#
#   scripts/verify-tag.sh vX.Y.Z[-suffix]
#
# Run from a checkout of the tag. Needs GITHUB_REPOSITORY (owner/repo) and an
# authenticated gh. No key is imported here: GitHub does the verification and
# this reads its verdict. The tag object on GitHub must be the one checked out
# locally, so a tag moved after checkout is caught rather than vouching for
# code that was never built. A malformed or hostile tag name is refused before
# any gh or git call.

set -euo pipefail

die() {
    echo "verify-tag: $*" >&2
    exit 1
}

tag="${1:-}"
[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || die "usage: $0 vX.Y.Z[-suffix]"
repo="${GITHUB_REPOSITORY:?verify-tag: GITHUB_REPOSITORY is not set}"

ref=$(gh api "repos/$repo/git/ref/tags/$tag" --jq '.object.type + " " + .object.sha') \
    || die "could not read tag $tag from $repo"
read -r type sha <<<"$ref"
[ "$type" = "tag" ] || die "$tag is not an annotated tag"

local_sha=$(git rev-parse -q --verify "refs/tags/$tag") || die "$tag is not a local tag"
[ "$sha" = "$local_sha" ] || die "$tag on GitHub is $sha, not the checked-out tag object $local_sha"

verdict=$(gh api "repos/$repo/git/tags/$sha" --jq '"\(.verification.verified) \(.verification.reason)"') \
    || die "could not read the tag object for $tag"
read -r verified reason <<<"$verdict"
if [ "$verified" != "true" ] || [ "$reason" != "valid" ]; then
    die "$tag signature not verified by GitHub ($reason)"
fi
echo "verify-tag: $tag is annotated and its signature is verified by GitHub"
