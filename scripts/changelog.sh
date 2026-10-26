#!/usr/bin/env bash
#
# Reads CHANGELOG.md so releases are built from it rather than written by hand.
#
#   scripts/changelog.sh versions      list released versions, newest first
#   scripts/changelog.sh notes <tag> [owner/repo]
#                                      print the section for <tag> (release notes);
#                                      with a repo, relative links point at the tag
#   scripts/changelog.sh check         validate the file and that every v* tag has a section
#
# Set CHANGELOG to read a different file.

set -euo pipefail

CHANGELOG="${CHANGELOG:-CHANGELOG.md}"
SEMVER='v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?'

die() {
	echo "changelog: $*" >&2
	exit 1
}

[ -f "$CHANGELOG" ] || die "$CHANGELOG not found"

versions() {
	sed -nE "s/^## \[($SEMVER)\].*/\1/p" "$CHANGELOG"
}

notes() {
	local tag="$1"
	# Print the body between "## [tag]" and the next "## " heading, dropping
	# link reference lines and leading/trailing blank lines.
	awk -v tag="$tag" '
		/^## / {
			if (found) exit
			if (index($0, "## [" tag "]") == 1) { found = 1; next }
		}
		found && !/^\[[^]]+\]: / { lines[++n] = $0 }
		END {
			first = 1; while (first <= n && lines[first] ~ /^[[:space:]]*$/) first++
			last = n;  while (last >= first && lines[last] ~ /^[[:space:]]*$/) last--
			for (i = first; i <= last; i++) print lines[i]
			exit (found ? 0 : 2)
		}
	' "$CHANGELOG" || die "no section for $tag in $CHANGELOG"
}

# Release pages do not resolve repository-relative links, so point them at
# the file as it was at the tag.
absolute_links() {
	REPO="$1" REF="$2" perl -pe \
		's{\]\((?!https?:|mailto:|#)([^)\s]+)\)}{](https://github.com/$ENV{REPO}/blob/$ENV{REF}/$1)}g'
}

check() {
	local errors=0 v body

	while IFS= read -r line; do
		if ! printf '%s\n' "$line" | grep -qE "^## \[(Unreleased|$SEMVER)\]( - [0-9]{4}-[0-9]{2}-[0-9]{2})?$"; then
			echo "bad heading: $line" >&2
			errors=$((errors + 1))
		elif printf '%s\n' "$line" | grep -qE "^## \[$SEMVER\]$"; then
			echo "missing release date: $line" >&2
			errors=$((errors + 1))
		fi
	done < <(grep -E '^## ' "$CHANGELOG")

	dupes=$(versions | sort | uniq -d)
	if [ -n "$dupes" ]; then
		echo "duplicate versions: $dupes" >&2
		errors=$((errors + 1))
	fi

	for v in $(versions); do
		if ! grep -qE "^\[$v\]: https://" "$CHANGELOG"; then
			echo "missing link reference for [$v]" >&2
			errors=$((errors + 1))
		fi
		body=$(notes "$v")
		if [ -z "$body" ]; then
			echo "empty section for $v" >&2
			errors=$((errors + 1))
		fi
	done

	# Every release tag must be described. A version with no tag yet is fine:
	# that is the state between merging the changelog entry and tagging.
	for v in $(git tag -l 'v*'); do
		if ! versions | grep -qxF "$v"; then
			echo "tag $v has no section in $CHANGELOG" >&2
			errors=$((errors + 1))
		fi
	done

	[ "$errors" -eq 0 ] || die "$errors problem(s) found"
	echo "$CHANGELOG is consistent ($(versions | wc -l | tr -d ' ') versions)"
}

case "${1:-}" in
versions) versions ;;
notes)
	[ -n "${2:-}" ] || die "usage: $0 notes <tag> [owner/repo]"
	if [ -n "${3:-}" ]; then
		notes "$2" | absolute_links "$3" "$2"
	else
		notes "$2"
	fi
	;;
check) check ;;
*) die "usage: $0 {versions|notes <tag> [owner/repo]|check}" ;;
esac
