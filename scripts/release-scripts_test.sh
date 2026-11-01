#!/usr/bin/env bash
#
# Tests for the scripts release.yml runs: verify-tag.sh, publish-release.sh,
# check-module-proxy.sh and build-release.sh. Every gh and curl call goes to
# a fake put first on PATH, which logs its arguments and answers from canned
# files (and sleep to one that returns at once), so nothing here touches the
# network. Prints "ok <name>" per case and exits non-zero on the
# first failure, with a line starting "FAIL". The build cases cross-compile
# four binaries and write their SBOMs, so they run last and take a while.
#
# Usage: scripts/release-scripts_test.sh

set -euo pipefail

SCRIPTS="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd -- "$SCRIPTS/.." && pwd)"
ROOT="$(mktemp -d "${TMPDIR:-/tmp}/release-scripts_test.XXXXXX")"
trap 'rm -rf "$ROOT"' EXIT

# Fixture commits and tags must not depend on a global signing setting.
G=(git -c commit.gpgsign=false -c tag.gpgSign=false -c user.name=Test -c user.email=test@example.invalid)

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

# The fake gh. FAKE_GH_DIR holds the canned answers and the call log:
#   ref.json      body of GET repos/OWNER/REPO/git/ref/tags/TAG
#   ref.next.json when present, replaces ref.json once ref.json has been
#                 read, standing in for a tag moved between two reads
#   ref.fail      when present, the ref read prints it to stderr and exits 1
#   tag.json      body of GET repos/OWNER/REPO/git/tags/SHA
#   is_draft      what "release view --json isDraft" prints; when absent the
#                 release does not exist and the view exits 1. Create, edit
#                 and delete keep it in step, as GitHub would.
#   view.fail     when present, "release view" prints it to stderr and exits
#                 1, as gh does on an API error other than a missing release
#   gh.log        one line per call, the arguments joined by spaces
mkdir -p "$ROOT/bin"
cat > "$ROOT/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "$*" >> "$FAKE_GH_DIR/gh.log"
jq_expr=""
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
    [ "${args[$i]}" = "--jq" ] && jq_expr="${args[$((i + 1))]}"
done
answer() {
    if [ -n "$jq_expr" ]; then jq -r "$jq_expr" "$1"; else cat "$1"; fi
}
case "$1 $2" in
    "api "*/git/ref/tags/*)
        if [ -f "$FAKE_GH_DIR/ref.fail" ]; then
            cat "$FAKE_GH_DIR/ref.fail" >&2
            exit 1
        fi
        answer "$FAKE_GH_DIR/ref.json"
        if [ -f "$FAKE_GH_DIR/ref.next.json" ]; then
            mv "$FAKE_GH_DIR/ref.next.json" "$FAKE_GH_DIR/ref.json"
        fi
        ;;
    "api "*/git/tags/*) answer "$FAKE_GH_DIR/tag.json" ;;
    "release view")
        if [ -f "$FAKE_GH_DIR/view.fail" ]; then
            cat "$FAKE_GH_DIR/view.fail" >&2
            exit 1
        fi
        if [ ! -f "$FAKE_GH_DIR/is_draft" ]; then
            echo "release not found" >&2
            exit 1
        fi
        case "$*" in
            *isDraft*) cat "$FAKE_GH_DIR/is_draft" ;;
            *url*) echo "https://github.com/bawdo/gosbee/releases/tag/$3" ;;
        esac
        ;;
    "release create") echo true > "$FAKE_GH_DIR/is_draft" ;;
    "release edit")
        case "$*" in *--draft=false*) echo false > "$FAKE_GH_DIR/is_draft" ;; esac
        ;;
    "release delete") rm -f "$FAKE_GH_DIR/is_draft" ;;
    *)
        echo "fake gh: unexpected call: $*" >&2
        exit 2
        ;;
esac
EOF
chmod +x "$ROOT/bin/gh"

# The tag object the build job verified, which publish-release.sh is given.
OBJ=7a90000123456789abcdef0123456789abcdef03
# Another tag object, for a tag moved since the build.
MOVED=3a40000123456789abcdef0123456789abcdef04

# fake NAME [TAG]: makes a fresh directory for one case's canned answers, with
# a dist/ holding a stand-in binary for TAG (default v0.1.0), a SHA256SUMS
# listing it, a notes.md, a bundle.json (a two-line pretty JSON object) and a
# ref.json in which TAG points at the tag object OBJ, and prints its path.
fake() {
    local dir="$ROOT/$1" tag="${2:-v0.1.0}"
    mkdir -p "$dir/dist"
    : > "$dir/gh.log"
    echo "binary" > "$dir/dist/gosbee-repl_${tag}_linux_amd64"
    printf '%s  %s\n' 0000000000000000000000000000000000000000000000000000000000000000 \
        "gosbee-repl_${tag}_linux_amd64" > "$dir/dist/SHA256SUMS"
    printf '{\n  "mediaType": "bundle"\n}\n' > "$dir/bundle.json"
    echo "notes" > "$dir/notes.md"
    printf '{"object":{"type":"tag","sha":"%s"}}\n' "$OBJ" > "$dir/ref.json"
    echo "$dir"
}

# run_gh DIR SCRIPT ARGS...: runs SCRIPT from DIR with the fake gh first on
# PATH, capturing OUT, ERR and CODE.
run_gh() {
    local dir="$1" script="$2"
    shift 2
    CODE=0
    OUT=$(cd "$dir" && PATH="$ROOT/bin:$PATH" FAKE_GH_DIR="$dir" GITHUB_REPOSITORY=bawdo/gosbee \
        GIT_CEILING_DIRECTORIES="$ROOT" \
        bash "$SCRIPTS/$script" "$@" 2>"$dir/.stderr") || CODE=$?
    ERR=$(cat "$dir/.stderr")
}

# expect_refusal NAME MESSAGE: asserts exit 1 and the exact stderr line.
expect_refusal() {
    [ "$CODE" -eq 1 ] || fail "$1" "expected exit 1"
    [ "$ERR" = "$2" ] || fail "$1" "expected stderr: $2"
}

# log_order NAME DIR PATTERN...: asserts each PATTERN matches a line of the
# fake gh log, later patterns on later lines.
log_order() {
    local name="$1" dir="$2" line=0 found pattern
    shift 2
    for pattern in "$@"; do
        found=$(awk -v start="$line" -v pat="$pattern" 'NR > start && index($0, pat) { print NR; exit }' "$dir/gh.log")
        [ -n "$found" ] || fail "$name" "gh log has no \"$pattern\" after line $line: $(cat "$dir/gh.log")"
        line=$found
    done
}

VALID_TAG='{"verification":{"verified":true,"reason":"valid"}}'

# tagged NAME [TAG]: a fake directory that is also a git repository with an
# annotated (unsigned) tag TAG (default v0.1.0), standing in for the
# checked-out tag. Prints its path.
tagged() {
    local dir tag="${2:-v0.1.0}"
    dir=$(fake "$1" "$tag")
    git init -q -b main "$dir"
    "${G[@]}" -C "$dir" commit -q --allow-empty -m "Initial"
    "${G[@]}" -C "$dir" tag -a "$tag" -m "$tag"
    echo "$dir"
}

# remote_ref DIR TYPE SHA: what GitHub answers for the ref of the tag.
remote_ref() {
    printf '{"object":{"type":"%s","sha":"%s"}}\n' "$2" "$3" > "$1/ref.json"
}

# The accepting case is also the one where the remote tag object matches the
# local one.
name=verify_tag_accepts_valid_signed_tag
dir=$(tagged "$name")
sha=$(git -C "$dir" rev-parse refs/tags/v0.1.0)
remote_ref "$dir" tag "$sha"
echo "$VALID_TAG" > "$dir/tag.json"
run_gh "$dir" verify-tag.sh v0.1.0
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
[ "$OUT" = "verify-tag: v0.1.0 is annotated and its signature is verified by GitHub" ] || fail "$name" "wrong output"
log_order "$name" "$dir" "api repos/bawdo/gosbee/git/ref/tags/v0.1.0" "api repos/bawdo/gosbee/git/tags/$sha"
echo "ok $name"

name=verify_tag_accepts_prerelease
dir=$(tagged "$name" v0.1.0-rc.1)
sha=$(git -C "$dir" rev-parse refs/tags/v0.1.0-rc.1)
remote_ref "$dir" tag "$sha"
echo "$VALID_TAG" > "$dir/tag.json"
run_gh "$dir" verify-tag.sh v0.1.0-rc.1
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
[ "$OUT" = "verify-tag: v0.1.0-rc.1 is annotated and its signature is verified by GitHub" ] || fail "$name" "wrong output"
log_order "$name" "$dir" "api repos/bawdo/gosbee/git/ref/tags/v0.1.0-rc.1" "api repos/bawdo/gosbee/git/tags/$sha"
echo "ok $name"

name=verify_tag_rejects_lightweight_tag
dir=$(tagged "$name")
remote_ref "$dir" commit "$(git -C "$dir" rev-parse HEAD)"
echo "$VALID_TAG" > "$dir/tag.json"
run_gh "$dir" verify-tag.sh v0.1.0
expect_refusal "$name" "verify-tag: v0.1.0 is not an annotated tag"
echo "ok $name"

name=verify_tag_rejects_tag_moved_since_checkout
dir=$(tagged "$name")
sha=$(git -C "$dir" rev-parse refs/tags/v0.1.0)
remote_ref "$dir" tag 0123456789abcdef0123456789abcdef01234567
echo "$VALID_TAG" > "$dir/tag.json"
run_gh "$dir" verify-tag.sh v0.1.0
expect_refusal "$name" "verify-tag: v0.1.0 on GitHub is 0123456789abcdef0123456789abcdef01234567, not the checked-out tag object $sha"
! grep -q "git/tags/" "$dir/gh.log" || fail "$name" "read the verification of the wrong tag object"
echo "ok $name"

name=verify_tag_rejects_missing_local_tag
dir=$(fake "$name")
git init -q -b main "$dir"
remote_ref "$dir" tag 0123456789abcdef0123456789abcdef01234567
echo "$VALID_TAG" > "$dir/tag.json"
run_gh "$dir" verify-tag.sh v0.1.0
expect_refusal "$name" "verify-tag: v0.1.0 is not a local tag"
echo "ok $name"

name=verify_tag_rejects_unverified
dir=$(tagged "$name")
remote_ref "$dir" tag "$(git -C "$dir" rev-parse refs/tags/v0.1.0)"
echo '{"verification":{"verified":false,"reason":"unsigned"}}' > "$dir/tag.json"
run_gh "$dir" verify-tag.sh v0.1.0
expect_refusal "$name" "verify-tag: v0.1.0 signature not verified by GitHub (unsigned)"
echo "ok $name"

# A hostile or malformed name is refused before any gh call. The fixture
# answers would accept the tag, so only the name check can refuse it.
name=verify_tag_rejects_bad_name
# shellcheck disable=SC2016 # the $(id) is a literal hostile name, never expanded
for bad in 'v1.0.0;id' 'v1.0.0$(id)' main v1.0 '' $'v1.0.0\nid' 'v1.0.0 ' '-v1.0.0'; do
    dir=$(tagged "$name")
    remote_ref "$dir" tag "$(git -C "$dir" rev-parse refs/tags/v0.1.0)"
    echo "$VALID_TAG" > "$dir/tag.json"
    run_gh "$dir" verify-tag.sh "$bad"
    expect_refusal "$name ($bad)" "verify-tag: usage: $SCRIPTS/verify-tag.sh vX.Y.Z[-suffix]"
    [ ! -s "$dir/gh.log" ] || fail "$name ($bad)" "called gh: $(cat "$dir/gh.log")"
    rm -rf "$dir"
done
echo "ok $name"

# The release create line of a case's gh log.
create_line() {
    grep '^release create ' "$1/gh.log"
}

PUBLISH_USAGE="publish-release: usage: $SCRIPTS/publish-release.sh vX.Y.Z[-suffix] TAG_OBJECT NOTES_FILE BUNDLE_FILE, or --check vX.Y.Z[-suffix] TAG_OBJECT"

name=publish_stops_on_published_release
dir=$(fake "$name")
echo false > "$dir/is_draft"
run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/notes.md" "$dir/bundle.json"
expect_refusal "$name" "publish-release: v0.1.0 is already published"
[ "$(cat "$dir/gh.log")" = "release view v0.1.0 --json isDraft --jq .isDraft" ] \
    || fail "$name" "made a call after finding the release published: $(cat "$dir/gh.log")"
[ "$(cat "$dir/is_draft")" = false ] || fail "$name" "the release changed"
echo "ok $name"

# Only gh's own "release not found" means there is no release. Any other
# failure (an outage, a rate limit, a bad token) stops the script before any
# write, with gh's message, rather than going on to create a release.
name=publish_stops_on_other_gh_error
dir=$(fake "$name")
echo false > "$dir/is_draft"
echo "HTTP 502: Bad Gateway (https://api.github.com/repos/bawdo/gosbee/releases/tags/v0.1.0)" > "$dir/view.fail"
run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/notes.md" "$dir/bundle.json"
expect_refusal "$name" "publish-release: could not read the release for v0.1.0: HTTP 502: Bad Gateway (https://api.github.com/repos/bawdo/gosbee/releases/tags/v0.1.0)"
[ "$(cat "$dir/gh.log")" = "release view v0.1.0 --json isDraft --jq .isDraft" ] \
    || fail "$name" "made a call after the failed read: $(cat "$dir/gh.log")"
echo "ok $name"

name=publish_replaces_draft
dir=$(fake "$name")
echo true > "$dir/is_draft"
run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/notes.md" "$dir/bundle.json"
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
log_order "$name" "$dir" "release delete v0.1.0 --yes" "release create v0.1.0 --draft --verify-tag" \
    "release edit v0.1.0 --draft=false --latest"
[ "$OUT" = "https://github.com/bawdo/gosbee/releases/tag/v0.1.0" ] || fail "$name" "did not print the release URL"
echo "ok $name"

name=publish_creates_when_missing
dir=$(fake "$name")
run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/notes.md" "$dir/bundle.json"
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
! grep -q "release delete" "$dir/gh.log" || fail "$name" "deleted a release"
log_order "$name" "$dir" "release view v0.1.0 --json isDraft" "release create v0.1.0" \
    "release edit v0.1.0 --draft=false --latest" "release view v0.1.0 --json url"
[ "$(create_line "$dir")" = "release create v0.1.0 --draft --verify-tag --title v0.1.0 --notes-file $dir/notes.md dist/gosbee-repl_v0.1.0_linux_amd64 dist/SHA256SUMS dist/gosbee-repl_v0.1.0_provenance.intoto.jsonl" ] \
    || fail "$name" "wrong create line: $(create_line "$dir")"
# The tag is read again on the call just before the create, so a tag moved
# since the build is caught as late as it can be.
[ "$(grep -B1 '^release create ' "$dir/gh.log" | head -1)" = "api repos/bawdo/gosbee/git/ref/tags/v0.1.0 --jq .object.type + \" \" + .object.sha" ] \
    || fail "$name" "the tag was not read just before the create: $(cat "$dir/gh.log")"
bundle="$dir/dist/gosbee-repl_v0.1.0_provenance.intoto.jsonl"
[ "$(wc -l < "$bundle" | tr -d ' ')" = 1 ] || fail "$name" "bundle is not one line"
[ "$(cat "$bundle")" = "$(jq -c . "$dir/bundle.json")" ] || fail "$name" "bundle is not the compacted input"
echo "ok $name"

# Every file SHA256SUMS lists is attached once, with SHA256SUMS and the
# bundle once each, even though the bundle's name matches the binaries'
# prefix. A file in dist/ that SHA256SUMS does not list (so was neither
# checksummed nor attested) is not attached.
name=publish_attaches_each_asset_once
dir=$(fake "$name")
: > "$dir/dist/SHA256SUMS"
for target in linux_amd64 linux_arm64 darwin_amd64 darwin_arm64; do
    echo "$target" > "$dir/dist/gosbee-repl_v0.1.0_$target"
    printf '%s  %s\n' 0000000000000000000000000000000000000000000000000000000000000000 \
        "gosbee-repl_v0.1.0_$target" >> "$dir/dist/SHA256SUMS"
done
echo stray > "$dir/dist/gosbee-repl_v0.1.0_stray"
run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/notes.md" "$dir/bundle.json"
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
line=$(create_line "$dir")
for asset in gosbee-repl_v0.1.0_linux_amd64 gosbee-repl_v0.1.0_linux_arm64 gosbee-repl_v0.1.0_darwin_amd64 \
    gosbee-repl_v0.1.0_darwin_arm64 SHA256SUMS gosbee-repl_v0.1.0_provenance.intoto.jsonl; do
    [ "$(tr ' ' '\n' <<<"$line" | grep -cxF "dist/$asset")" = 1 ] || fail "$name" "dist/$asset not attached exactly once: $line"
done
[ "$(tr ' ' '\n' <<<"$line" | grep -c '^dist/')" = 6 ] || fail "$name" "expected six assets: $line"
echo "ok $name"

name=publish_refuses_bad_bundle_before_touching_draft
dir=$(fake "$name")
echo true > "$dir/is_draft"
echo "not json" > "$dir/bundle.json"
run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/notes.md" "$dir/bundle.json"
expect_refusal "$name" "publish-release: BUNDLE_FILE is not a single JSON document"
[ ! -s "$dir/gh.log" ] || fail "$name" "called gh: $(cat "$dir/gh.log")"
[ "$(cat "$dir/is_draft")" = true ] || fail "$name" "the draft is gone"
[ ! -e "$dir/dist/gosbee-repl_v0.1.0_provenance.intoto.jsonl" ] || fail "$name" "left a bundle in dist/"
echo "ok $name"

name=publish_needs_a_bundle
dir=$(fake "$name")
run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/notes.md"
expect_refusal "$name" "$PUBLISH_USAGE"
[ ! -s "$dir/gh.log" ] || fail "$name" "called gh"
echo "ok $name"

name=publish_needs_the_notes_file
dir=$(fake "$name")
run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/missing.md" "$dir/bundle.json"
expect_refusal "$name" "publish-release: NOTES_FILE $dir/missing.md is not a file"
[ ! -s "$dir/gh.log" ] || fail "$name" "called gh"
echo "ok $name"

name=publish_rejects_bad_tag
# shellcheck disable=SC2016 # the $(id) is a literal hostile name, never expanded
for bad in 'v1.0.0;id' 'v1.0.0$(id)' main v1.0; do
    dir=$(fake "$name")
    run_gh "$dir" publish-release.sh "$bad" "$OBJ" "$dir/notes.md" "$dir/bundle.json"
    expect_refusal "$name ($bad)" "$PUBLISH_USAGE"
    [ ! -s "$dir/gh.log" ] || fail "$name ($bad)" "called gh"
    rm -rf "$dir"
done
echo "ok $name"

# SHA256SUMS comes from the build job's artifact. A line naming anything but
# an existing gosbee-repl_<tag>_ file directly in dist/ is refused before any
# gh call, so it can neither attach an outside file nor one of another tag.
name=publish_refuses_bad_checksums_entry
for entry in "../notes.md" "gosbee-repl_v0.2.0_linux_amd64" "gosbee-repl_v0.1.0_missing" \
    "gosbee-repl_v0.1.0_a/../../notes.md" "-gosbee-repl_v0.1.0_x" "gosbee-repl_v0.1.0_provenance.intoto.jsonl"; do
    dir=$(fake "$name")
    printf '%s  %s\n' 0000000000000000000000000000000000000000000000000000000000000000 "$entry" \
        >> "$dir/dist/SHA256SUMS"
    run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/notes.md" "$dir/bundle.json"
    expect_refusal "$name ($entry)" "publish-release: SHA256SUMS lists $entry, which is not a v0.1.0 asset in dist/"
    [ ! -s "$dir/gh.log" ] || fail "$name ($entry)" "called gh"
    rm -rf "$dir"
done
dir=$(fake "$name-empty")
: > "$dir/dist/SHA256SUMS"
run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/notes.md" "$dir/bundle.json"
expect_refusal "$name (empty)" "publish-release: SHA256SUMS lists no assets"
[ ! -s "$dir/gh.log" ] || fail "$name (empty)" "called gh"
dir=$(fake "$name-duplicate")
cat "$dir/dist/SHA256SUMS" "$dir/dist/SHA256SUMS" > "$dir/sums" && mv "$dir/sums" "$dir/dist/SHA256SUMS"
run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/notes.md" "$dir/bundle.json"
expect_refusal "$name (duplicate)" "publish-release: SHA256SUMS lists gosbee-repl_v0.1.0_linux_amd64 twice"
[ ! -s "$dir/gh.log" ] || fail "$name (duplicate)" "called gh"
dir=$(fake "$name-malformed")
echo "not-a-checksum  gosbee-repl_v0.1.0_linux_amd64" > "$dir/dist/SHA256SUMS"
run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/notes.md" "$dir/bundle.json"
expect_refusal "$name (malformed)" "publish-release: SHA256SUMS line 1 is not <sha256>  <name>"
[ ! -s "$dir/gh.log" ] || fail "$name (malformed)" "called gh"
echo "ok $name"

name=publish_marks_prerelease
dir=$(fake "$name" v0.1.0-rc.1)
run_gh "$dir" publish-release.sh v0.1.0-rc.1 "$OBJ" "$dir/notes.md" "$dir/bundle.json"
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
[ "$(create_line "$dir")" = "release create v0.1.0-rc.1 --draft --verify-tag --title v0.1.0-rc.1 --notes-file $dir/notes.md --prerelease dist/gosbee-repl_v0.1.0-rc.1_linux_amd64 dist/SHA256SUMS dist/gosbee-repl_v0.1.0-rc.1_provenance.intoto.jsonl" ] \
    || fail "$name" "wrong create line: $(create_line "$dir")"
grep -qx "release edit v0.1.0-rc.1 --draft=false" "$dir/gh.log" || fail "$name" "not published: $(cat "$dir/gh.log")"
! grep -q -- "--latest" "$dir/gh.log" || fail "$name" "a pre-release was marked latest"
echo "ok $name"

# A tag moved after the build (here between the script's first read and the
# one just before the create) is refused, naming both tag objects, and no
# release is created for it.
name=publish_refuses_tag_moved_before_create
dir=$(fake "$name")
echo true > "$dir/is_draft"
printf '{"object":{"type":"tag","sha":"%s"}}\n' "$MOVED" > "$dir/ref.next.json"
run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/notes.md" "$dir/bundle.json"
expect_refusal "$name" "publish-release: replacing the draft release for v0.1.0
publish-release: v0.1.0 on GitHub points at tag $MOVED, not the built tag object $OBJ"
! grep -qE '^release (create|edit)' "$dir/gh.log" || fail "$name" "released a moved tag: $(cat "$dir/gh.log")"
echo "ok $name"

name=publish_refuses_moved_tag_before_any_write
dir=$(fake "$name")
echo true > "$dir/is_draft"
printf '{"object":{"type":"tag","sha":"%s"}}\n' "$MOVED" > "$dir/ref.json"
run_gh "$dir" publish-release.sh v0.1.0 "$OBJ" "$dir/notes.md" "$dir/bundle.json"
expect_refusal "$name" "publish-release: v0.1.0 on GitHub points at tag $MOVED, not the built tag object $OBJ"
! grep -qE '^release (create|edit|delete)' "$dir/gh.log" || fail "$name" "wrote: $(cat "$dir/gh.log")"
[ "$(cat "$dir/is_draft")" = true ] || fail "$name" "the draft is gone"
echo "ok $name"

# --check is the publish job's read-only check before its first write (the
# attestation): the release must not be published and the tag must still
# point at the tag object the build verified.
name=publish_check_accepts_missing_or_draft
for state in missing true; do
    dir=$(fake "$name-$state")
    [ "$state" = missing ] || echo "$state" > "$dir/is_draft"
    run_gh "$dir" publish-release.sh --check v0.1.0 "$OBJ"
    [ "$CODE" -eq 0 ] || fail "$name ($state)" "expected exit 0"
    [ "$OUT" = "publish-release: v0.1.0 can be published from tag object $OBJ" ] || fail "$name ($state)" "wrong output"
    [ "$(cat "$dir/gh.log")" = "release view v0.1.0 --json isDraft --jq .isDraft
api repos/bawdo/gosbee/git/ref/tags/v0.1.0 --jq .object.type + \" \" + .object.sha" ] || fail "$name ($state)" "made other calls: $(cat "$dir/gh.log")"
done
[ "$(cat "$dir/is_draft")" = true ] || fail "$name" "the draft changed"
echo "ok $name"

name=publish_check_refuses_published
dir=$(fake "$name")
echo false > "$dir/is_draft"
run_gh "$dir" publish-release.sh --check v0.1.0 "$OBJ"
expect_refusal "$name" "publish-release: v0.1.0 is already published"
[ "$(cat "$dir/gh.log")" = "release view v0.1.0 --json isDraft --jq .isDraft" ] \
    || fail "$name" "made a call after finding the release published: $(cat "$dir/gh.log")"
echo "ok $name"

name=publish_check_refuses_moved_tag
for ref in "tag $MOVED" "commit $OBJ"; do
    dir=$(fake "$name")
    printf '{"object":{"type":"%s","sha":"%s"}}\n' "${ref% *}" "${ref#* }" > "$dir/ref.json"
    run_gh "$dir" publish-release.sh --check v0.1.0 "$OBJ"
    expect_refusal "$name ($ref)" "publish-release: v0.1.0 on GitHub points at $ref, not the built tag object $OBJ"
    rm -rf "$dir"
done
echo "ok $name"

# The check reads gh as publishing does: a failed read is never a pass.
name=publish_check_stops_on_gh_error
dir=$(fake "$name")
echo "HTTP 403: API rate limit exceeded" > "$dir/view.fail"
run_gh "$dir" publish-release.sh --check v0.1.0 "$OBJ"
expect_refusal "$name (view)" "publish-release: could not read the release for v0.1.0: HTTP 403: API rate limit exceeded"
dir=$(fake "$name-ref")
echo "HTTP 404: Not Found" > "$dir/ref.fail"
run_gh "$dir" publish-release.sh --check v0.1.0 "$OBJ"
expect_refusal "$name (ref)" "publish-release: could not read tag v0.1.0 from bawdo/gosbee: HTTP 404: Not Found"
echo "ok $name"

name=publish_rejects_bad_tag_object
# shellcheck disable=SC2016 # the $(id) is a literal hostile value, never expanded
for bad in '' abc123 "$(tr a-f A-F <<<"$OBJ")" "$OBJ;id" '$(id)' "${OBJ}0"; do
    dir=$(fake "$name")
    run_gh "$dir" publish-release.sh --check v0.1.0 "$bad"
    expect_refusal "$name (check $bad)" "$PUBLISH_USAGE"
    run_gh "$dir" publish-release.sh v0.1.0 "$bad" "$dir/notes.md" "$dir/bundle.json"
    expect_refusal "$name ($bad)" "$PUBLISH_USAGE"
    [ ! -s "$dir/gh.log" ] || fail "$name ($bad)" "called gh: $(cat "$dir/gh.log")"
    rm -rf "$dir"
done
dir=$(fake "$name-args")
# shellcheck disable=SC2016 # the $(id) is a literal hostile name, never expanded
for argv in "v0.1.0;id|$OBJ" 'v1.0.0$(id)'"|$OBJ" "main|$OBJ" "v0.1.0" "v0.1.0|$OBJ|extra"; do
    IFS='|' read -r -a args <<<"$argv"
    run_gh "$dir" publish-release.sh --check "${args[@]}"
    expect_refusal "$name (check $argv)" "$PUBLISH_USAGE"
done
[ ! -s "$dir/gh.log" ] || fail "$name" "called gh: $(cat "$dir/gh.log")"
echo "ok $name"

# Across every publish case: assets are only ever attached by the create of a
# draft, never uploaded to or replaced on an existing release, and every edit
# is the one that publishes the draft.
name=publish_never_mutates_assets
for log in "$ROOT"/publish_*/gh.log; do
    ! grep -E 'release upload|--clobber' "$log" || fail "$name" "$log uploads assets"
    ! grep '^release edit' "$log" | grep -v -- '--draft=false' || fail "$name" "$log edits without publishing"
done
echo "ok $name"

# The fake curl and sleep for check-module-proxy.sh, in their own directory
# so no other case sees them. FAKE_CURL_DIR holds:
#   proxy.body    what a proxy.golang.org URL answers
#   sumdb.body    what a sum.golang.org URL answers
#   proxy.fail    when present, the number of proxy calls that fail (exit 22)
#                 before one answers; "always" fails every call
#   sumdb.fail    the same for sum.golang.org
#   curl.log      one line per curl call, the arguments joined by spaces
#   sleep.log     one line per sleep call, its arguments
mkdir -p "$ROOT/net"
cat > "$ROOT/net/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "$*" >> "$FAKE_CURL_DIR/curl.log"
url="${*: -1}"
case "$url" in
    https://proxy.golang.org/*) kind=proxy ;;
    https://sum.golang.org/*) kind=sumdb ;;
    *)
        echo "fake curl: unexpected URL: $url" >&2
        exit 2
        ;;
esac
calls=$(grep -c "$kind" "$FAKE_CURL_DIR/curl.log" || true)
if [ -f "$FAKE_CURL_DIR/$kind.fail" ]; then
    fails=$(cat "$FAKE_CURL_DIR/$kind.fail")
    if [ "$fails" = always ] || [ "$calls" -le "$fails" ]; then
        echo "curl: (22) The requested URL returned error: 404" >&2
        exit 22
    fi
fi
cat "$FAKE_CURL_DIR/$kind.body"
EOF
cat > "$ROOT/net/sleep" <<'EOF'
#!/usr/bin/env bash
echo "$*" >> "$FAKE_CURL_DIR/sleep.log"
EOF
chmod +x "$ROOT/net/curl" "$ROOT/net/sleep"

MODULE=github.com/bawdo/gosbee
C=c0ffee0123456789abcdef0123456789abcdef01
D=d00d000123456789abcdef0123456789abcdef02
PROXY_URL="https://proxy.golang.org/$MODULE/@v/v0.3.0.info"
SUMDB_URL="https://sum.golang.org/lookup/$MODULE@v0.3.0"

# info TAG HASH: a proxy .info body shaped like the live one.
info() {
    printf '{"Version":"%s","Time":"2026-11-01T00:00:00Z","Origin":{"VCS":"git","URL":"https://github.com/bawdo/gosbee","Hash":"%s","Ref":"refs/tags/%s"}}\n' \
        "$1" "$2" "$1"
}

# sumdb TAG: a sum.golang.org lookup body recording TAG.
sumdb() {
    printf '%s\n' 4242 \
        "$MODULE $1 h1:Mh2nYLfrlAZbXt7OfUmmanBF91Mv0Aqoqf8MtRVfXA0=" \
        "$MODULE $1/go.mod h1:eKg3XoAuUt0Y8jT9sVGK3wHBDmdPrTmuRhCnjS1Bxek=" \
        "" "go.sum database tree" 4243 "PlusDxqVTUm8qLSgZEvp2ozG6v+0yhj3k1GfMBPCPEk=" "" \
        "— sum.golang.org Az3grlGJGVZtkJXGvMe9VTRtmXsdNk3dwnRtd2Nf7ZHTHvjQ="
}

# proxy_case NAME [TAG] [HASH]: a directory for one case, whose proxy serves
# TAG (default v0.3.0) at HASH (default C) and whose sumdb records TAG.
proxy_case() {
    local dir="$ROOT/$1" tag="${2:-v0.3.0}"
    mkdir -p "$dir"
    : > "$dir/curl.log"
    : > "$dir/sleep.log"
    info "$tag" "${3:-$C}" > "$dir/proxy.body"
    sumdb "$tag" > "$dir/sumdb.body"
    echo "$dir"
}

# run_proxy DIR ARGS...: runs check-module-proxy.sh with the fake curl and
# sleep first on PATH, capturing OUT, ERR and CODE.
run_proxy() {
    local dir="$1"
    shift
    CODE=0
    OUT=$(PATH="$ROOT/net:$PATH" FAKE_CURL_DIR="$dir" bash "$SCRIPTS/check-module-proxy.sh" "$@" \
        2>"$dir/.stderr") || CODE=$?
    ERR=$(cat "$dir/.stderr")
}

# calls DIR HOST: how many curl calls went to HOST.
calls() {
    grep -c "https://$2/" "$1/curl.log" || true
}

name=proxy_accepts_matching_commit
dir=$(proxy_case "$name")
run_proxy "$dir" "$MODULE" v0.3.0 "$C"
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
[ "$OUT" = "check-module-proxy: proxy.golang.org has $MODULE@v0.3.0 at $C
check-module-proxy: sum.golang.org records $MODULE@v0.3.0" ] || fail "$name" "wrong output"
[ "$(cat "$dir/curl.log")" = "-fsS --max-time 5 $PROXY_URL
-fsS --max-time 5 $SUMDB_URL" ] || fail "$name" "wrong curl calls: $(cat "$dir/curl.log")"
[ ! -s "$dir/sleep.log" ] || fail "$name" "slept"
echo "ok $name"

name=proxy_retries_then_succeeds
dir=$(proxy_case "$name")
echo 2 > "$dir/proxy.fail"
run_proxy "$dir" "$MODULE" v0.3.0 "$C"
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
[ "$(calls "$dir" proxy.golang.org)" = 3 ] || fail "$name" "expected 3 proxy calls: $(cat "$dir/curl.log")"
[ "$(cat "$dir/sleep.log")" = "5
5" ] || fail "$name" "expected two 5 second sleeps: $(cat "$dir/sleep.log")"
echo "ok $name"

name=proxy_gives_up_after_five
dir=$(proxy_case "$name")
echo always > "$dir/proxy.fail"
run_proxy "$dir" "$MODULE" v0.3.0 "$C"
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
[ "$OUT" = "::error::proxy.golang.org has no $MODULE@v0.3.0 after 5 attempts" ] || fail "$name" "wrong output"
[ "$(calls "$dir" proxy.golang.org)" = 5 ] || fail "$name" "expected 5 proxy calls: $(cat "$dir/curl.log")"
[ "$(calls "$dir" sum.golang.org)" = 0 ] || fail "$name" "called sumdb"
echo "ok $name"

# The proxy keeps the first commit it saw for a version, so a mismatch never
# goes away and is not retried.
name=proxy_rejects_moved_tag
dir=$(proxy_case "$name" v0.3.0 "$D")
run_proxy "$dir" "$MODULE" v0.3.0 "$C"
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
[ "$OUT" = "::error::proxy.golang.org has $MODULE@v0.3.0 at $D, the tag points at $C" ] || fail "$name" "wrong output"
[ "$(calls "$dir" proxy.golang.org)" = 1 ] || fail "$name" "retried a mismatch: $(cat "$dir/curl.log")"
[ "$(calls "$dir" sum.golang.org)" = 0 ] || fail "$name" "called sumdb"
[ ! -s "$dir/sleep.log" ] || fail "$name" "slept"
echo "ok $name"

name=proxy_rejects_missing_origin
dir=$(proxy_case "$name")
echo '{"Version":"v0.3.0","Time":"2026-11-01T00:00:00Z"}' > "$dir/proxy.body"
run_proxy "$dir" "$MODULE" v0.3.0 "$C"
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
[ "$OUT" = "::error::proxy.golang.org has $MODULE@v0.3.0 at unknown, the tag points at $C" ] || fail "$name" "wrong output"
echo "ok $name"

# The hash reaches the job log, where a line starting "::" is a workflow
# command, so only a commit hash is ever printed.
name=proxy_rejects_hostile_hash
dir=$(proxy_case "$name")
jq -n --arg h "$D"$'\n::warning::injected' '{Version: "v0.3.0", Origin: {VCS: "git", Hash: $h}}' > "$dir/proxy.body"
run_proxy "$dir" "$MODULE" v0.3.0 "$C"
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
[ "$OUT" = "::error::proxy.golang.org has $MODULE@v0.3.0 at unknown, the tag points at $C" ] || fail "$name" "wrong output"
echo "ok $name"

name=proxy_rejects_bad_json
dir=$(proxy_case "$name")
echo 'not json' > "$dir/proxy.body"
run_proxy "$dir" "$MODULE" v0.3.0 "$C"
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
[ "$OUT" = "::error::proxy.golang.org returned unreadable JSON for $MODULE@v0.3.0" ] || fail "$name" "wrong output"
[ "$(calls "$dir" sum.golang.org)" = 0 ] || fail "$name" "called sumdb"
echo "ok $name"

name=sumdb_miss_fails
dir=$(proxy_case "$name")
printf '%s\n' 4242 "$MODULE v0.3.0/go.mod h1:eKg3XoAuUt0Y8jT9sVGK3wHBDmdPrTmuRhCnjS1Bxek=" > "$dir/sumdb.body"
run_proxy "$dir" "$MODULE" v0.3.0 "$C"
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
[ "$OUT" = "check-module-proxy: proxy.golang.org has $MODULE@v0.3.0 at $C
::error::sum.golang.org has no record of $MODULE@v0.3.0 after 2 attempts" ] || fail "$name" "wrong output"
[ "$(calls "$dir" sum.golang.org)" = 2 ] || fail "$name" "expected 2 sumdb calls: $(cat "$dir/curl.log")"
echo "ok $name"

# A record of another version, or of another module, that merely contains
# the tag or the module never satisfies the check.
name=sumdb_rejects_other_version
for body in "$(sumdb v0.3.0-rc.1)" "$(sumdb v0.3.0 | sed "s|^$MODULE |$MODULE/sub |")" \
    "$(sumdb v0.3.0 | sed 's| h1:| x1:|')" "$(sumdb v0.3.0 | sed "s|^$MODULE v0.3.0 |$MODULE v0.3.0x |")"; do
    dir=$(proxy_case "$name")
    printf '%s\n' "$body" > "$dir/sumdb.body"
    run_proxy "$dir" "$MODULE" v0.3.0 "$C"
    [ "$CODE" -eq 1 ] || fail "$name" "expected exit 1 for: $body"
    grep -qxF "::error::sum.golang.org has no record of $MODULE@v0.3.0 after 2 attempts" <<<"$OUT" \
        || fail "$name" "wrong output for: $body"
    rm -rf "$dir"
done
echo "ok $name"

name=proxy_accepts_prerelease
dir=$(proxy_case "$name" v0.3.0-rc.1)
run_proxy "$dir" "$MODULE" v0.3.0-rc.1 "$C"
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
[ "$(cat "$dir/curl.log")" = "-fsS --max-time 5 https://proxy.golang.org/$MODULE/@v/v0.3.0-rc.1.info
-fsS --max-time 5 https://sum.golang.org/lookup/$MODULE@v0.3.0-rc.1" ] || fail "$name" "wrong curl calls: $(cat "$dir/curl.log")"
echo "ok $name"

name=proxy_rejects_bad_args
usage="check-module-proxy: usage: check-module-proxy.sh <module> vX.Y.Z[-suffix] <40-hex commit>"
# shellcheck disable=SC2016 # the $(id) is a literal hostile name, never expanded
for args in "$MODULE|v0.3.0;id|$C" "$MODULE|"'v0.3.0$(id)'"|$C" "$MODULE|main|$C" "github.com/Bawdo/gosbee|v0.3.0|$C" \
    "$MODULE|v0.3.0|abc123" "$MODULE|v0.3.0|$(tr a-f A-F <<<"$C")" "gosbee|v0.3.0|$C" "$MODULE|v0.3.0" "$MODULE|v0.3.0|$C|extra" \
    "$MODULE|v0.3.0|$C"$'\n'"x" "../x/y|v0.3.0|$C" "github.com/bawdo/../x|v0.3.0|$C" "github.com/bawdo/gosbee.|v0.3.0|$C"; do
    IFS='|' read -r -d '' -a argv < <(printf '%s' "$args") || true
    dir=$(proxy_case "$name")
    run_proxy "$dir" "${argv[@]}"
    expect_refusal "$name ($args)" "$usage"
    [ ! -s "$dir/curl.log" ] || fail "$name ($args)" "called curl: $(cat "$dir/curl.log")"
    rm -rf "$dir"
done
echo "ok $name"

# The worst case is bounded: 4 waits for the proxy and 1 for sumdb, and 7
# calls of at most 5 seconds each, so about 60 seconds in all.
name=proxy_time_is_bounded
dir=$(proxy_case "$name")
echo 4 > "$dir/proxy.fail"
echo always > "$dir/sumdb.fail"
run_proxy "$dir" "$MODULE" v0.3.0 "$C"
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
[ "$(awk '{ s += $1 } END { print s }' "$dir/sleep.log")" = 25 ] || fail "$name" "slept: $(cat "$dir/sleep.log")"
[ "$(wc -l < "$dir/curl.log" | tr -d ' ')" = 7 ] || fail "$name" "expected 7 calls: $(cat "$dir/curl.log")"
! grep -v -- '^-fsS --max-time 5 ' "$dir/curl.log" || fail "$name" "a call lacks -fsS --max-time 5"
echo "ok $name"

# release.yml's jobs, bounded as a top-level job key to the next one.
RELEASE_YML="$REPO/.github/workflows/release.yml"
release_job() {
    awk -v key="  $1:" '$0 == key { f = 1; print; next } f && /^  [^ #]/ { exit } f' "$RELEASE_YML"
}

# The tag job checks the tag name before ci runs any of the tag's code, so a
# by-hand dispatch naming a tag outside v* (which no ruleset protects) stops
# there. It holds no token scope, checks nothing out and runs no action.
name=release_yml_tag_job_shape
CODE=0
job=$(release_job tag)
[ -n "$job" ] || fail "$name" "no tag job in release.yml"
grep -qxF '    permissions: {}' <<<"$job" || fail "$name" "tag job does not drop every permission"
# shellcheck disable=SC2016 # the ${{ }} is GitHub expression text, matched literally
grep -qxF '      TAG: ${{ inputs.tag || github.ref_name }}' <<<"$job" || fail "$name" "TAG not passed through env"
! grep -E '^ +(- )?uses:' <<<"$job" || fail "$name" "tag job runs an action"
! grep -qF 'needs:' <<<"$job" || fail "$name" "tag job waits on another job"
echo "ok $name"

# ci needs the tag job and is called with the bare tag, never a ref.
name=release_yml_ci_needs_tag
CODE=0
job=$(release_job ci)
[ -n "$job" ] || fail "$name" "no ci job in release.yml"
grep -qxF '    needs: tag' <<<"$job" || fail "$name" "ci does not need the tag job"
# shellcheck disable=SC2016 # the ${{ }} is GitHub expression text, matched literally
grep -qxF '      tag: ${{ inputs.tag || github.ref_name }}' <<<"$job" || fail "$name" "ci is not called with the tag"
! grep -qE '^ +ref:' <<<"$job" || fail "$name" "ci is still called with a ref"
echo "ok $name"

# The tag job's check, run as the runner would run it, on names it must pass
# and names it must stop.
TAG_CHECK="$ROOT/tag-check.sh"
release_job tag | awk '/^        run: \|$/ { f = 1; next } f && !/^          / && !/^$/ { exit } f { sub(/^          /, ""); print }' > "$TAG_CHECK"
[ -s "$TAG_CHECK" ] || fail release_yml_tag_check "no run block in the tag job"
for good in v0.1.0 v1.22.333 v0.1.0-rc.1 v2.0.0-beta.2-x; do
    name="release_yml_tag_check_passes ($good)"
    CODE=0
    OUT=$(TAG="$good" bash -e "$TAG_CHECK" 2>&1) || CODE=$?
    [ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
    echo "ok $name"
done
# shellcheck disable=SC2016 # $(id) is a literal tag name that must not expand
for bad in main v1 v1.2 x-v1.2.3 v1.2.3- 'v1.2.3 ' 'v1.2.3/x' 'v1.2.3-$(id)' ''; do
    name="release_yml_tag_check_stops ($bad)"
    CODE=0
    OUT=$(TAG="$bad" bash -e "$TAG_CHECK" 2>&1) || CODE=$?
    [ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
    [ "$OUT" = "::error::the tag to release is not of the form vX.Y.Z or vX.Y.Z-suffix" ] || fail "$name" "wrong message"
    echo "ok $name"
done

# The build runs in a copy of the committed tree, so the repository's own
# dist/ and worktree list are never touched. The working tree's
# build-release.sh is the one run, so an uncommitted change to it is what is
# tested.
#
# The copy is its own git repository with an annotated v0.0.0-test tag on its
# only commit, as the release checkout of a tag is, because cyclonedx-gomod
# reads the module SBOM's version from the tag at HEAD.
BUILD="$ROOT/build"
mkdir -p "$BUILD"
git -C "$REPO" archive HEAD | tar -x -C "$BUILD"
# The tools module is what runs cyclonedx-gomod, so its working tree copy is
# used too, as the script's is.
cp "$REPO/tools/go.mod" "$REPO/tools/go.sum" "$BUILD/tools/"
git init -q -b main "$BUILD"
"${G[@]}" -C "$BUILD" add -A
"${G[@]}" -C "$BUILD" commit -q -m "Release copy"
"${G[@]}" -C "$BUILD" tag -a v0.0.0-test -m v0.0.0-test

# run_build ARGS...: runs build-release.sh from the build copy, capturing OUT,
# ERR and CODE.
run_build() {
    CODE=0
    OUT=$(cd "$BUILD" && GIT_CEILING_DIRECTORIES="$ROOT" bash "$SCRIPTS/build-release.sh" "$@" \
        2>"$ROOT/.build-stderr") || CODE=$?
    ERR=$(cat "$ROOT/.build-stderr")
}

name=build_rejects_bad_tag
# shellcheck disable=SC2016 # the $(id) is a literal hostile name, never expanded
for bad in 'v1.0.0;id' 'v1.0.0$(id)' main v1.0 '' '../v1.0.0'; do
    run_build "$bad"
    expect_refusal "$name ($bad)" "build-release: usage: $SCRIPTS/build-release.sh vX.Y.Z[-suffix]"
    [ ! -e "$BUILD/dist" ] || fail "$name ($bad)" "made dist/"
done
echo "ok $name"

# A leftover dist/ could carry stale binaries into SHA256SUMS, so it is
# refused rather than built over.
name=build_refuses_existing_dist
mkdir "$BUILD/dist"
touch "$BUILD/dist/gosbee-repl_v0.0.0-old_linux_amd64"
run_build v0.0.0-test
expect_refusal "$name" "build-release: dist/ already exists; remove it first"
[ "$(ls "$BUILD/dist")" = "gosbee-repl_v0.0.0-old_linux_amd64" ] || fail "$name" "dist/ changed"
rm -rf "$BUILD/dist"
echo "ok $name"

name=build_writes_binaries_sboms_and_sums
run_build v0.0.0-test
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
assets="gosbee-repl_v0.0.0-test_darwin_amd64
gosbee-repl_v0.0.0-test_darwin_amd64.cdx.json
gosbee-repl_v0.0.0-test_darwin_arm64
gosbee-repl_v0.0.0-test_darwin_arm64.cdx.json
gosbee-repl_v0.0.0-test_linux_amd64
gosbee-repl_v0.0.0-test_linux_amd64.cdx.json
gosbee-repl_v0.0.0-test_linux_arm64
gosbee-repl_v0.0.0-test_linux_arm64.cdx.json
gosbee-repl_v0.0.0-test_module.cdx.json"
expected="SHA256SUMS
$assets"
actual=$(cd "$BUILD/dist" && find . -mindepth 1 | sed 's|^\./||' | LC_ALL=C sort)
[ "$actual" = "$expected" ] || fail "$name" "dist/ holds: $(tr '\n' ' ' <<<"$actual")"
[ "$(wc -l < "$BUILD/dist/SHA256SUMS" | tr -d ' ')" = 9 ] || fail "$name" "expected nine checksum lines"
[ "$(sed 's/^[0-9a-f]\{64\}  //' "$BUILD/dist/SHA256SUMS")" = "$assets" ] \
    || fail "$name" "SHA256SUMS does not list each asset once, by name: $(cat "$BUILD/dist/SHA256SUMS")"
if command -v sha256sum >/dev/null 2>&1; then
    (cd "$BUILD/dist" && sha256sum -c --quiet SHA256SUMS) || fail "$name" "checksums do not match"
else
    (cd "$BUILD/dist" && shasum -a 256 -c --quiet SHA256SUMS) || fail "$name" "checksums do not match"
fi
for target in darwin_amd64 darwin_arm64 linux_amd64 linux_arm64; do
    binary="$BUILD/dist/gosbee-repl_v0.0.0-test_$target"
    grep -qE "^[0-9a-f]{64}  gosbee-repl_v0.0.0-test_$target\$" "$BUILD/dist/SHA256SUMS" \
        || fail "$name" "SHA256SUMS does not list $target"
    info=$(go version -m "$binary") || fail "$name" "go version -m failed on $target"
    grep -qF "$(printf 'path\t%s' "github.com/bawdo/gosbee/cmd/repl")" <<<"$info" || fail "$name" "$target is not cmd/repl"
    for setting in CGO_ENABLED=0 -trimpath=true "GOOS=${target%_*}" "GOARCH=${target#*_}"; do
        grep -qF "$(printf 'build\t%s' "$setting")" <<<"$info" || fail "$name" "$target lacks $setting"
    done
    # -trimpath keeps -ldflags out of the build info, so -s is checked on the
    # binary: it defines no symbols (a darwin one still lists its imports, U).
    # nm fails on an ELF binary with no symbol table, and only that failure,
    # word for word, counts as stripped; any other nm error fails the case.
    if nm=$(go tool nm "$binary" 2>&1); then
        defined=$(grep -v ' U ' <<<"$nm" || true)
        [ -z "$defined" ] || fail "$name" "$target is not stripped"
    else
        [ "$nm" = "reading $binary: no symbol section
reading $binary: no symbols" ] || fail "$name" "go tool nm failed on $target: $nm"
    fi
    # Each binary's SBOM is of that binary: its target is in every purl.
    jq -e --arg q "goarch=${target#*_}&goos=${target%_*}" \
        '[.components[].purl | select(contains($q) | not)] == []' "$binary.cdx.json" >/dev/null \
        || fail "$name" "the SBOM for $target describes another target"
done
grep -qF "smoke tested gosbee-repl_v0.0.0-test_$(go env GOHOSTOS)_$(go env GOHOSTARCH)" <<<"$OUT" \
    || fail "$name" "no smoke test reported"
echo "ok $name"

# The case above built a pre-release tag end to end.
name=build_accepts_prerelease_tag
[ -f "$BUILD/dist/gosbee-repl_v0.0.0-test_linux_amd64" ] || fail "$name" "no binary carries v0.0.0-test"
echo "ok $name"

# module_version PATH: the version go list reports for a dependency of the
# main module.
module_version() {
    (cd "$REPO" && go list -m -f '{{.Version}}' "$1")
}

# has_purl NAME FILE PURL_PREFIX: asserts some component's purl starts with
# PURL_PREFIX followed by "?" (the qualifiers) or the end.
has_purl() {
    jq -e --arg p "$3" 'any(.components[].purl; . == $p or startswith($p + "?"))' "$2" >/dev/null \
        || fail "$1" "$2 has no component $3"
}

name=build_module_sbom_describes_module
sbom="$BUILD/dist/gosbee-repl_v0.0.0-test_module.cdx.json"
jq -e '.bomFormat == "CycloneDX" and .specVersion == "1.6"' "$sbom" >/dev/null || fail "$name" "not CycloneDX 1.6"
jq -e '.metadata.component.type == "library"' "$sbom" >/dev/null || fail "$name" "main component is not a library"
jq -e '.metadata.component.purl | startswith("pkg:golang/github.com/bawdo/gosbee@v0.0.0-test?")' "$sbom" >/dev/null \
    || fail "$name" "main component is not github.com/bawdo/gosbee@v0.0.0-test: $(jq -r .metadata.component.purl "$sbom")"
has_purl "$name" "$sbom" "pkg:golang/github.com/jackc/pgx/v5@$(module_version github.com/jackc/pgx/v5)"
has_purl "$name" "$sbom" "pkg:golang/modernc.org/sqlite@$(module_version modernc.org/sqlite)"
# Detected licences are evidence, not assertions (cyclonedx-gomod's default).
jq -e 'any(.components[]; (.evidence.licenses // []) != [])' "$sbom" >/dev/null || fail "$name" "no component has a licence"
echo "ok $name"

name=build_binary_sbom_describes_binary
sbom="$BUILD/dist/gosbee-repl_v0.0.0-test_linux_amd64.cdx.json"
jq -e '.bomFormat == "CycloneDX" and .specVersion == "1.6"' "$sbom" >/dev/null || fail "$name" "not CycloneDX 1.6"
jq -e '.metadata.component.version == "v0.0.0-test"' "$sbom" >/dev/null || fail "$name" "main component is not v0.0.0-test"
has_purl "$name" "$sbom" "pkg:golang/github.com/ergochat/readline@$(module_version github.com/ergochat/readline)"
jq -e 'any(.components[]; .name == "std" and (.purl | startswith("pkg:golang/std@go")))' "$sbom" >/dev/null \
    || fail "$name" "no standard library component"
echo "ok $name"

# A go that fails every cyclonedx-gomod run and is the real go otherwise.
REAL_GO=$(command -v go)
mkdir -p "$ROOT/sbom-fails"
cat > "$ROOT/sbom-fails/go" <<EOF
#!/usr/bin/env bash
case " \$* " in *cyclonedx-gomod*) exit 3 ;; esac
exec "$REAL_GO" "\$@"
EOF
chmod +x "$ROOT/sbom-fails/go"

name=build_fails_when_sbom_fails
rm -rf "$BUILD/dist"
PATH="$ROOT/sbom-fails:$PATH" run_build v0.0.0-test
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
grep -qxF "build-release: SBOM generation failed for gosbee-repl_v0.0.0-test_module.cdx.json" <<<"$ERR" \
    || fail "$name" "no SBOM failure reported"
[ ! -e "$BUILD/dist/SHA256SUMS" ] || fail "$name" "wrote SHA256SUMS"
echo "ok $name"

# A go that resolves cyclonedx-gomod to FAKE_SBOM_TOOL and is the real go
# otherwise, so the build runs a stand-in tool.
mkdir -p "$ROOT/sbom-tool"
cat > "$ROOT/sbom-tool/go" <<EOF
#!/usr/bin/env bash
[ "\$*" = "-C tools tool -n cyclonedx-gomod" ] && { echo "\$FAKE_SBOM_TOOL"; exit 0; }
exec "$REAL_GO" "\$@"
EOF
# A tool that resolves but whose run fails.
printf '#!/usr/bin/env bash\nexit 3\n' > "$ROOT/sbom-tool/fails"
# The real tool, after appending to a binary that was already checksummed.
REAL_TOOL=$(cd "$BUILD" && go -C tools tool -n cyclonedx-gomod)
cat > "$ROOT/sbom-tool/tampers" <<EOF
#!/usr/bin/env bash
echo tampered >> "$BUILD/dist/gosbee-repl_v0.0.0-test_linux_arm64"
exec "$REAL_TOOL" "\$@"
EOF
chmod +x "$ROOT/sbom-tool/go" "$ROOT/sbom-tool/fails" "$ROOT/sbom-tool/tampers"

# The tool resolves, so only its run's exit status can stop the build.
name=build_fails_when_sbom_tool_run_fails
rm -rf "$BUILD/dist"
PATH="$ROOT/sbom-tool:$PATH" FAKE_SBOM_TOOL="$ROOT/sbom-tool/fails" run_build v0.0.0-test
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
grep -qxF "build-release: SBOM generation failed for gosbee-repl_v0.0.0-test_module.cdx.json" <<<"$ERR" \
    || fail "$name" "no SBOM failure reported"
[ ! -e "$BUILD/dist/SHA256SUMS" ] || fail "$name" "wrote SHA256SUMS"
echo "ok $name"

# The SBOM tool is third-party code run after the binaries exist, so they are
# checksummed before it runs and must be unchanged after.
name=build_fails_when_sbom_alters_a_binary
rm -rf "$BUILD/dist"
PATH="$ROOT/sbom-tool:$PATH" FAKE_SBOM_TOOL="$ROOT/sbom-tool/tampers" run_build v0.0.0-test
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
grep -qxF "build-release: gosbee-repl_v0.0.0-test_linux_arm64 changed while the SBOMs were written" <<<"$ERR" \
    || fail "$name" "no changed binary reported"
[ ! -e "$BUILD/dist/SHA256SUMS" ] || fail "$name" "wrote SHA256SUMS"
echo "ok $name"

# cyclonedx-gomod names the module SBOM's version after a tag at HEAD, and a
# commit can carry more than one (a pre-release and its final release). An
# SBOM naming any version but the tag being built is refused.
name=build_refuses_module_sbom_of_other_version
rm -rf "$BUILD/dist"
"${G[@]}" -C "$BUILD" tag -d v0.0.0-test >/dev/null
"${G[@]}" -C "$BUILD" tag -a v0.0.0-other -m v0.0.0-other
run_build v0.0.0-test
[ "$CODE" -eq 1 ] || fail "$name" "expected exit 1"
grep -qxF "build-release: gosbee-repl_v0.0.0-test_module.cdx.json names version v0.0.0-other, not v0.0.0-test" <<<"$ERR" \
    || fail "$name" "no version mismatch reported"
[ ! -e "$BUILD/dist/SHA256SUMS" ] || fail "$name" "wrote SHA256SUMS"
echo "ok $name"

echo "all release script tests passed"
