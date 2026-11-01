# Security Policy

## Supported Versions

gosbee is pre-1.0. Security fixes land on `main` and ship in the next release
of the latest minor version. Older minor versions are not patched, so upgrade
to the latest release to pick up fixes.

| Version         | Supported |
|-----------------|-----------|
| Latest `v0.x`   | Yes       |
| Older `v0.x`    | No        |

## Reporting a Vulnerability

Please do not open a public issue, pull request or discussion for a security
problem.

Report it privately through GitHub instead:
[Report a vulnerability](https://github.com/bawdo/gosbee/security/advisories/new)
(or the **Security** tab, then **Report a vulnerability**).

Useful things to include:

- the gosbee version or commit
- the dialect (PostgreSQL, MySQL, SQLite) and any plugins involved
- a minimal program or REPL session that reproduces it
- what you expected, and what actually happened (for example, the SQL that was
  generated)

You should get an acknowledgement within a week. Once the problem is
confirmed, a fix and a GitHub security advisory are prepared together, and you
are credited in the advisory unless you would rather not be.

## Verifying a release

Each release ships REPL binaries for Linux and macOS on amd64 and arm64
(`gosbee-repl_<tag>_<os>_<arch>`), a CycloneDX SBOM for the module and one for
each binary, and a `SHA256SUMS` listing every binary and SBOM. The release
workflow attests build provenance for everything `SHA256SUMS` lists, and
attaches that attestation's Sigstore bundle as
`gosbee-repl_<tag>_provenance.intoto.jsonl`. Releases up to and including
`v0.2.1` predate this pipeline and carry none of it, so the checks below
apply to later releases.

To confirm a download came from this repository's CI and has not been
altered, first check that the tag is signed and sits on `main`, then run the
attestation and checksum checks against the files you downloaded.

### The tag

In a clone of this repository:

```bash
curl -fsSL https://github.com/bawdo.gpg | gpg --import
git fetch origin main tag v<version>
git verify-tag v<version>
git merge-base --is-ancestor v<version> origin/main
git rev-parse 'v<version>^{commit}'
```

The first command imports the maintainer's public keys, which
`git verify-tag` needs. That URL serves more than one key. Releases are signed
by Keith Bawden's key with primary fingerprint
`661B 030E 1C0F F788 151B  9525 E8C1 84EB 4B8F 82D1`, through the signing
subkey `9954 8FCC B94B E845 41E5  73AA 4823 4C2C 292E EE78`. A good result
from `git verify-tag` is a good signature naming that subkey. Treat a
signature from any other key as a failure. On a keyring that has not
certified the key, gpg adds "WARNING: This key is not certified with a trusted
signature!". That is expected: the fingerprint check here is the trust
decision, not gpg's web of trust. A new release key will be named
here, in this file on `main`, before any release is signed with it. The key
and this fingerprint both come from GitHub, so for more assurance compare the
fingerprint with one you got another way, such as a keyserver or an earlier
clone of this file.

The fetch updates `origin/main` as well as the tag, so the next check reads
`main` as it is now. `git merge-base --is-ancestor` prints nothing and exits 0
when the tag is on `main`, and exits non-zero when it is not. `git rev-parse` prints the commit
the tag points at: use it as `<commit sha>` below.

### A binary

```bash
gh attestation verify gosbee-repl_v<version>_<os>_<arch> --repo bawdo/gosbee \
  --signer-workflow bawdo/gosbee/.github/workflows/release.yml \
  --source-ref refs/tags/v<version> \
  --source-digest <commit sha> \
  --deny-self-hosted-runners
```

`gh attestation verify` prints its summary only to a terminal. In a script or
a pipe it prints nothing when the check passes, so read its exit status: 0 is
a pass, and anything else is a failure, with the reason on stderr.

The same command checks an SBOM: name the `.cdx.json` file instead of the
binary. To verify without querying GitHub's attestation API, download the
bundle next to the binary and add `--bundle`. A bundle cannot tell whether
`bawdo/gosbee` has since been renamed or its name taken by another owner, so
prefer the command above when you can reach GitHub:

```bash
gh attestation verify gosbee-repl_v<version>_<os>_<arch> --repo bawdo/gosbee \
  --signer-workflow bawdo/gosbee/.github/workflows/release.yml \
  --source-ref refs/tags/v<version> \
  --source-digest <commit sha> \
  --deny-self-hosted-runners \
  --bundle gosbee-repl_v<version>_provenance.intoto.jsonl
```

`SHA256SUMS` is not signed and not itself attested: anyone able to replace a
binary on the release could replace it too. The attestation above is the
authenticity check. The checksums only confirm a download arrived intact, and
are useful for checking several files at once after the attestation has
passed. From the directory holding the downloads, on Linux:

```bash
sha256sum -c SHA256SUMS --ignore-missing
```

On macOS, use `shasum`, not the `sha256sum` macOS also ships, which prints
nothing and exits 0 when none of your files is listed (a renamed download, for
example):

```bash
shasum -a 256 -c SHA256SUMS --ignore-missing
```

Either way, check that each file you downloaded is reported `OK` by name.

### The release itself

Immutable releases are on for this repository (switched on by
`scripts/repo-settings.sh harden` on 2026-11-01), so a published release
cannot have its tag moved or its assets added, replaced or deleted, and GitHub
attests the release when it is published. Releases published before that
(v0.2.1 and earlier) carry no release attestation, so this check does not
apply to them. For a later release:

```bash
gh release verify v<version> --repo bawdo/gosbee
gh release verify-asset v<version> gosbee-repl_v<version>_<os>_<arch> --repo bawdo/gosbee
```

The first checks GitHub's attestation for the release and lists the asset
digests it records; the second checks a file you downloaded is one of them.
This is GitHub's record of what was published, not of how it was built, so
it complements the build provenance check above rather than replacing it.

### What an attestation proves

An attestation is provenance, not a security guarantee. It links a file to
the source and build instructions that produced it, and says nothing about
whether that code is free of bugs or vulnerabilities. With the flags above it
proves the file was built by `release.yml` as it stands at the tag's commit,
on a GitHub-hosted runner, from that tag and commit. It does not prove the
commit was reviewed: a `v*` tag pushed at an unmerged commit would run that
commit's own `release.yml` and pass the same check. The three git commands
above close that gap, because they show the tag is signed by the maintainer
and its commit is on `main`. The repository's rulesets narrow it too: only
an admin can create a `v*` tag, and no push can move or delete one.

A release run started by hand from the Actions tab records the branch it was
dispatched from, and that branch's commit, as its source, not the tag. Both
`--source-ref` and `--source-digest` fail for its assets. `--signer-workflow`
still passes, but it matches `release.yml` as it stands on any branch, so on
its own it does not bind the source at all. A failure there is a reason to ask
in an issue, not to drop the flags. The publish job's `release` environment
deploys only from a `v*` tag or from `main` (`scripts/repo-settings.sh
harden`), so `release.yml` as `main` has it cannot publish from a run
dispatched from another branch. A branch that edits the workflow can drop the
environment, so this narrows mistakes rather than binding someone with write
access; the tag check above is what binds.

### The SBOMs

`gosbee-repl_v<version>_module.cdx.json` lists the module's dependencies and
their licences, as `go.mod` resolves them. Each
`gosbee-repl_v<version>_<os>_<arch>.cdx.json` lists the modules compiled into
that binary, standard library included. They are written during the build by
`cyclonedx-gomod`, pinned in `tools/go.mod`, and are covered by the
attestation through `SHA256SUMS`, so you can check an SBOM came from the
release build. They record what went in, as the build saw it. They do not
show that any component is free of vulnerabilities, and they are not an
independent audit of the binary: feed them to a scanner if you want that
question answered.

### The Go module

Go programs that depend on gosbee do not use the release binaries. The Go
command verifies the module itself against the checksum database,
sum.golang.org, on download:

```bash
go mod download -json github.com/bawdo/gosbee@v<version>
```

`go` refuses a module whose hash does not match the checksum database (or
your `go.sum`), unless `GOSUMDB`, `GONOSUMDB` or `GOPRIVATE` has changed that
for it. Check with `go env GOSUMDB GONOSUMDB GOPRIVATE`, which also shows
values set with `go env -w`: `GOSUMDB` should be `sum.golang.org`, and neither
of the others should cover `github.com/bawdo/gosbee`. The
JSON's `Origin.Hash`, where the proxy supplies it, is the commit the proxy
fetched: it should equal `<commit sha>` from `git rev-parse` above. The
release workflow checks the same thing after publishing, and a mismatch means
the tag moved after the proxy first saw it. The version is then retracted in
`go.mod` and a new tag released.

## Scope

Of most interest are problems in the SQL gosbee generates: injection through
any builder input, and ways around the soft delete or OPA plugins that let a
query read rows or columns it should not. Bugs in the REPL that need a
malicious local user to trigger are lower priority but still welcome.
