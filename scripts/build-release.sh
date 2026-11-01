#!/usr/bin/env bash
#
# Builds the REPL release binaries for a tag into dist/, smoke tests the one
# for this host and writes dist/SHA256SUMS.
#
#   scripts/build-release.sh vX.Y.Z[-suffix]
#
# Run from the repository root, with dist/ absent. Builds
# dist/gosbee-repl_<tag>_<os>_<arch> for linux and darwin on amd64 and arm64
# with CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' ./cmd/repl (-s -w
# strips the symbol table and DWARF data to shrink the binaries). The host's
# binary must start, report the sqlite engine from GOSBEE_ENGINE and exit 0
# on "exit", run with an empty HOME. CycloneDX JSON 1.6 SBOMs are then
# written by cyclonedx-gomod from tools/go.mod: dist/gosbee-repl_<tag>_module
# .cdx.json for the module (its version read from the tag at HEAD, which must
# be <tag>) and dist/<binary>.cdx.json for each binary. SHA256SUMS lists every
# binary and SBOM; it uses sha256sum, or shasum -a 256 where that is absent
# (macOS). A malformed tag is refused before anything is built, and a failed
# SBOM stops the build before SHA256SUMS is written. Needs jq.

set -euo pipefail

die() {
    echo "build-release: $*" >&2
    exit 1
}

ROOT="$(pwd -P)"

# sbom FILE GOOS GOARCH KIND ARGS...: runs cyclonedx-gomod KIND with ARGS
# (flags, then the module or binary path), writing dist/FILE, and dies if it
# fails. The pinned tool from tools/go.mod is built by go -C tools tool -n and
# run directly, from tools/ as go tool would, so that GOOS and GOARCH, which
# it copies from go env into every purl, can be a binary's target without go
# building the tool for that target. Paths are absolute for the same reason.
sbom() {
    local file="$1" goos="$2" goarch="$3" kind="$4" tool
    shift 4
    if ! tool=$(go -C tools tool -n cyclonedx-gomod) \
        || ! (cd tools && GOOS="$goos" GOARCH="$goarch" \
            "$tool" "$kind" -json -output-version 1.6 -output "$ROOT/dist/$file" "$@"); then
        die "SBOM generation failed for $file"
    fi
    echo "wrote dist/$file"
}

# write_sboms TAG: the module SBOM, which must name TAG as its version (the
# tool reads it from a tag at HEAD), then one SBOM per binary.
write_sboms() {
    local tag="$1" module="gosbee-repl_${1}_module.cdx.json" version target os arch bin
    sbom "$module" "$(go env GOHOSTOS)" "$(go env GOHOSTARCH)" mod -licenses -type library "$ROOT"
    version=$(jq -r '.metadata.component.version // "none"' "dist/$module") \
        || die "$module is not readable JSON"
    [ "$version" = "$tag" ] || die "$module names version $version, not $tag"
    for target in "${targets[@]}"; do
        os="${target%/*}"
        arch="${target#*/}"
        bin="gosbee-repl_${tag}_${os}_${arch}"
        sbom "$bin.cdx.json" "$os" "$arch" bin -std -version "$tag" "$ROOT/dist/$bin"
    done
}

tag="${1:-}"
[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || die "usage: $0 vX.Y.Z[-suffix]"
[ ! -e dist ] || die "dist/ already exists; remove it first"

targets=(linux/amd64 linux/arm64 darwin/amd64 darwin/arm64)
host="$(go env GOHOSTOS)/$(go env GOHOSTARCH)"

mkdir dist
for target in "${targets[@]}"; do
    os="${target%/*}"
    arch="${target#*/}"
    out="dist/gosbee-repl_${tag}_${os}_${arch}"
    CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath -ldflags='-s -w' -o "$out" ./cmd/repl
    echo "built $out"
done

smoke="gosbee-repl_${tag}_${host%/*}_${host#*/}"
[ -f "dist/$smoke" ] || die "no binary for this host ($host) to smoke test"
home="$(mktemp -d "${TMPDIR:-/tmp}/build-release.XXXXXX")"
code=0
output=$(printf 'exit\n' | HOME="$home" GOSBEE_ENGINE=sqlite "dist/$smoke" 2>&1) || code=$?
rm -rf "$home"
if [ "$code" -ne 0 ] || ! grep -qF 'Engine: sqlite (from GOSBEE_ENGINE)' <<<"$output"; then
    printf '%s\n' "$output" >&2
    die "smoke test failed for $smoke"
fi
echo "smoke tested $smoke"

write_sboms "$tag"

if command -v sha256sum > /dev/null 2>&1; then
    (cd dist && sha256sum gosbee-repl_* > SHA256SUMS)
else
    (cd dist && shasum -a 256 gosbee-repl_* > SHA256SUMS)
fi
echo "wrote dist/SHA256SUMS"
