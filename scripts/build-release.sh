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
# on "exit", run with an empty HOME. SHA256SUMS uses sha256sum, or
# shasum -a 256 where that is absent (macOS). A malformed tag is refused
# before anything is built.

set -euo pipefail

die() {
    echo "build-release: $*" >&2
    exit 1
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

if command -v sha256sum > /dev/null 2>&1; then
    (cd dist && sha256sum gosbee-repl_* > SHA256SUMS)
else
    (cd dist && shasum -a 256 gosbee-repl_* > SHA256SUMS)
fi
echo "wrote dist/SHA256SUMS"
