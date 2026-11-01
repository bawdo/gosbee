#!/usr/bin/env bash
#
# Publishes the GitHub release for a tag with the assets in dist/.
#
#   scripts/publish-release.sh vX.Y.Z[-suffix] NOTES_FILE BUNDLE_FILE
#
# Run from a directory holding dist/. The assets attached are exactly the
# files dist/SHA256SUMS lists (the set that was checksummed and attested),
# then SHA256SUMS itself, then the bundle, each once. Every SHA256SUMS line
# must name an existing gosbee-repl_<tag>_ file directly in dist/, so nothing
# outside dist/, of another tag or unlisted is ever attached.
#
# BUNDLE_FILE is the Sigstore bundle of the build provenance attestation. It
# is compacted onto one line, so it is a valid JSON lines file (what
# gh attestation verify --bundle reads), and attached as
# dist/gosbee-repl_<tag>_provenance.intoto.jsonl. Every input is checked
# before any gh call, so a bad one leaves a stale draft alone.
#
# A published release is never touched: finding one stops the script before
# any write. Only gh's own "release not found" means there is no release, so
# any other gh failure stops it too. A draft is left over from a failed run
# and is replaced. The release is created as a draft with its assets and
# only then published, so it never exists without them. A tag with a suffix
# is a pre-release and is never marked latest. Needs GH_REPO or
# GITHUB_REPOSITORY (owner/repo, for gh) and an authenticated gh.

set -euo pipefail

die() {
    echo "publish-release: $*" >&2
    exit 1
}

tag="${1:-}"
notes="${2:-}"
bundle_in="${3:-}"
if ! [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || [ -z "$notes" ] || [ -z "$bundle_in" ]; then
    die "usage: $0 vX.Y.Z[-suffix] NOTES_FILE BUNDLE_FILE"
fi
[ -f "$notes" ] || die "NOTES_FILE $notes is not a file"

prefix="gosbee-repl_${tag}_"
bundle_name="${prefix}provenance.intoto.jsonl"
bundle="dist/$bundle_name"

[ -f dist/SHA256SUMS ] || die "dist/SHA256SUMS is not a file"
assets=()
n=0
while IFS= read -r line || [ -n "$line" ]; do
    n=$((n + 1))
    [[ "$line" =~ ^[0-9a-f]{64}\ [\ *](.+)$ ]] || die "SHA256SUMS line $n is not <sha256>  <name>"
    file="${BASH_REMATCH[1]}"
    if [[ "$file" != "$prefix"* ]] || ! [[ "$file" =~ ^[0-9A-Za-z._-]+$ ]] \
        || [ "$file" = "$bundle_name" ] || [ ! -f "dist/$file" ]; then
        die "SHA256SUMS lists $file, which is not a $tag asset in dist/"
    fi
    for seen in "${assets[@]+"${assets[@]}"}"; do
        [ "$seen" != "dist/$file" ] || die "SHA256SUMS lists $file twice"
    done
    assets+=("dist/$file")
done < dist/SHA256SUMS
[ "${#assets[@]}" -gt 0 ] || die "SHA256SUMS lists no assets"

if ! jq -c . "$bundle_in" > "$bundle.tmp" 2>/dev/null \
    || [ "$(wc -l < "$bundle.tmp" | tr -d ' ')" != 1 ]; then
    rm -f "$bundle.tmp"
    die "BUNDLE_FILE is not a single JSON document"
fi
mv "$bundle.tmp" "$bundle"

flags=()
latest=(--latest)
case "$tag" in
    *-*)
        flags=(--prerelease)
        latest=()
        ;;
esac

# Only gh's own "release not found" means there is no release; any other
# failure (an outage, a rate limit, a bad token) stops here, with gh's message.
if ! draft=$(gh release view "$tag" --json isDraft --jq .isDraft 2>&1); then
    [ "$draft" = "release not found" ] || die "could not read the release for $tag: $draft"
    draft=missing
fi
case "$draft" in
    missing) ;;
    false) die "$tag is already published" ;;
    true)
        echo "publish-release: replacing the draft release for $tag" >&2
        gh release delete "$tag" --yes
        ;;
    *) die "could not read the release for $tag: unexpected answer $draft" ;;
esac

gh release create "$tag" --draft --verify-tag --title "$tag" --notes-file "$notes" \
    "${flags[@]+"${flags[@]}"}" "${assets[@]}" dist/SHA256SUMS "$bundle"
gh release edit "$tag" --draft=false "${latest[@]+"${latest[@]}"}"
gh release view "$tag" --json url --jq .url
