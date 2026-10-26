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

## Scope

Of most interest are problems in the SQL gosbee generates: injection through
any builder input, and ways around the soft delete or OPA plugins that let a
query read rows or columns it should not. Bugs in the REPL that need a
malicious local user to trigger are lower priority but still welcome.
