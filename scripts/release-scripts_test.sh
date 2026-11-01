#!/usr/bin/env bash
#
# Tests for the scripts release.yml runs: verify-tag.sh, publish-release.sh
# and build-release.sh. Every gh call goes to a fake gh put first on PATH,
# which logs its arguments and answers from canned files, so nothing here
# touches the network. Prints "ok <name>" per case and exits non-zero on the
# first failure, with a line starting "FAIL". The build cases cross-compile
# four binaries, so they run last and take a while.
#
# Usage: scripts/release-scripts_test.sh

set -euo pipefail

SCRIPTS="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
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
#   tag.json      body of GET repos/OWNER/REPO/git/tags/SHA
#   is_draft      what "release view --json isDraft" prints; when absent the
#                 release does not exist and the view exits 1. Create, edit
#                 and delete keep it in step, as GitHub would.
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
    "api "*/git/ref/tags/*) answer "$FAKE_GH_DIR/ref.json" ;;
    "api "*/git/tags/*) answer "$FAKE_GH_DIR/tag.json" ;;
    "release view")
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

# fake NAME [TAG]: makes a fresh directory for one case's canned answers, with
# a dist/ holding a stand-in binary for TAG (default v0.1.0), a SHA256SUMS
# listing it, a notes.md and a bundle.json (a two-line pretty JSON object),
# and prints its path.
fake() {
    local dir="$ROOT/$1" tag="${2:-v0.1.0}"
    mkdir -p "$dir/dist"
    : > "$dir/gh.log"
    echo "binary" > "$dir/dist/gosbee-repl_${tag}_linux_amd64"
    printf '%s  %s\n' 0000000000000000000000000000000000000000000000000000000000000000 \
        "gosbee-repl_${tag}_linux_amd64" > "$dir/dist/SHA256SUMS"
    printf '{\n  "mediaType": "bundle"\n}\n' > "$dir/bundle.json"
    echo "notes" > "$dir/notes.md"
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

name=publish_stops_on_published_release
dir=$(fake "$name")
echo false > "$dir/is_draft"
run_gh "$dir" publish-release.sh v0.1.0 "$dir/notes.md" "$dir/bundle.json"
expect_refusal "$name" "publish-release: v0.1.0 is already published"
[ "$(cat "$dir/gh.log")" = "release view v0.1.0 --json isDraft --jq .isDraft" ] \
    || fail "$name" "made a call after finding the release published: $(cat "$dir/gh.log")"
[ "$(cat "$dir/is_draft")" = false ] || fail "$name" "the release changed"
echo "ok $name"

name=publish_replaces_draft
dir=$(fake "$name")
echo true > "$dir/is_draft"
run_gh "$dir" publish-release.sh v0.1.0 "$dir/notes.md" "$dir/bundle.json"
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
log_order "$name" "$dir" "release delete v0.1.0 --yes" "release create v0.1.0 --draft --verify-tag" \
    "release edit v0.1.0 --draft=false --latest"
[ "$OUT" = "https://github.com/bawdo/gosbee/releases/tag/v0.1.0" ] || fail "$name" "did not print the release URL"
echo "ok $name"

name=publish_creates_when_missing
dir=$(fake "$name")
run_gh "$dir" publish-release.sh v0.1.0 "$dir/notes.md" "$dir/bundle.json"
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
! grep -q "release delete" "$dir/gh.log" || fail "$name" "deleted a release"
log_order "$name" "$dir" "release view v0.1.0 --json isDraft" "release create v0.1.0" \
    "release edit v0.1.0 --draft=false --latest" "release view v0.1.0 --json url"
[ "$(create_line "$dir")" = "release create v0.1.0 --draft --verify-tag --title v0.1.0 --notes-file $dir/notes.md dist/gosbee-repl_v0.1.0_linux_amd64 dist/SHA256SUMS dist/gosbee-repl_v0.1.0_provenance.intoto.jsonl" ] \
    || fail "$name" "wrong create line: $(create_line "$dir")"
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
run_gh "$dir" publish-release.sh v0.1.0 "$dir/notes.md" "$dir/bundle.json"
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
run_gh "$dir" publish-release.sh v0.1.0 "$dir/notes.md" "$dir/bundle.json"
expect_refusal "$name" "publish-release: BUNDLE_FILE is not a single JSON document"
[ ! -s "$dir/gh.log" ] || fail "$name" "called gh: $(cat "$dir/gh.log")"
[ "$(cat "$dir/is_draft")" = true ] || fail "$name" "the draft is gone"
[ ! -e "$dir/dist/gosbee-repl_v0.1.0_provenance.intoto.jsonl" ] || fail "$name" "left a bundle in dist/"
echo "ok $name"

name=publish_needs_a_bundle
dir=$(fake "$name")
run_gh "$dir" publish-release.sh v0.1.0 "$dir/notes.md"
expect_refusal "$name" "publish-release: usage: $SCRIPTS/publish-release.sh vX.Y.Z[-suffix] NOTES_FILE BUNDLE_FILE"
[ ! -s "$dir/gh.log" ] || fail "$name" "called gh"
echo "ok $name"

name=publish_needs_the_notes_file
dir=$(fake "$name")
run_gh "$dir" publish-release.sh v0.1.0 "$dir/missing.md" "$dir/bundle.json"
expect_refusal "$name" "publish-release: NOTES_FILE $dir/missing.md is not a file"
[ ! -s "$dir/gh.log" ] || fail "$name" "called gh"
echo "ok $name"

name=publish_rejects_bad_tag
# shellcheck disable=SC2016 # the $(id) is a literal hostile name, never expanded
for bad in 'v1.0.0;id' 'v1.0.0$(id)' main v1.0; do
    dir=$(fake "$name")
    run_gh "$dir" publish-release.sh "$bad" "$dir/notes.md" "$dir/bundle.json"
    expect_refusal "$name ($bad)" "publish-release: usage: $SCRIPTS/publish-release.sh vX.Y.Z[-suffix] NOTES_FILE BUNDLE_FILE"
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
    run_gh "$dir" publish-release.sh v0.1.0 "$dir/notes.md" "$dir/bundle.json"
    expect_refusal "$name ($entry)" "publish-release: SHA256SUMS lists $entry, which is not a v0.1.0 asset in dist/"
    [ ! -s "$dir/gh.log" ] || fail "$name ($entry)" "called gh"
    rm -rf "$dir"
done
dir=$(fake "$name-empty")
: > "$dir/dist/SHA256SUMS"
run_gh "$dir" publish-release.sh v0.1.0 "$dir/notes.md" "$dir/bundle.json"
expect_refusal "$name (empty)" "publish-release: SHA256SUMS lists no assets"
[ ! -s "$dir/gh.log" ] || fail "$name (empty)" "called gh"
dir=$(fake "$name-duplicate")
cat "$dir/dist/SHA256SUMS" "$dir/dist/SHA256SUMS" > "$dir/sums" && mv "$dir/sums" "$dir/dist/SHA256SUMS"
run_gh "$dir" publish-release.sh v0.1.0 "$dir/notes.md" "$dir/bundle.json"
expect_refusal "$name (duplicate)" "publish-release: SHA256SUMS lists gosbee-repl_v0.1.0_linux_amd64 twice"
[ ! -s "$dir/gh.log" ] || fail "$name (duplicate)" "called gh"
dir=$(fake "$name-malformed")
echo "not-a-checksum  gosbee-repl_v0.1.0_linux_amd64" > "$dir/dist/SHA256SUMS"
run_gh "$dir" publish-release.sh v0.1.0 "$dir/notes.md" "$dir/bundle.json"
expect_refusal "$name (malformed)" "publish-release: SHA256SUMS line 1 is not <sha256>  <name>"
[ ! -s "$dir/gh.log" ] || fail "$name (malformed)" "called gh"
echo "ok $name"

name=publish_marks_prerelease
dir=$(fake "$name" v0.1.0-rc.1)
run_gh "$dir" publish-release.sh v0.1.0-rc.1 "$dir/notes.md" "$dir/bundle.json"
[ "$CODE" -eq 0 ] || fail "$name" "expected exit 0"
[ "$(create_line "$dir")" = "release create v0.1.0-rc.1 --draft --verify-tag --title v0.1.0-rc.1 --notes-file $dir/notes.md --prerelease dist/gosbee-repl_v0.1.0-rc.1_linux_amd64 dist/SHA256SUMS dist/gosbee-repl_v0.1.0-rc.1_provenance.intoto.jsonl" ] \
    || fail "$name" "wrong create line: $(create_line "$dir")"
grep -qx "release edit v0.1.0-rc.1 --draft=false" "$dir/gh.log" || fail "$name" "not published: $(cat "$dir/gh.log")"
! grep -q -- "--latest" "$dir/gh.log" || fail "$name" "a pre-release was marked latest"
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

echo "all release script tests passed"
