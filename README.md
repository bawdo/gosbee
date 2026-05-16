# jellyfish

A macOS-only Go CLI for the Iru (formerly Kandji) Endpoint Management API.

## Features

- `jellyfish vulns list` - list vulnerability detections across the fleet (one row per device-CVE intersection), filter by device ID or serial number.
- `jellyfish vulns summary` - per-CVE rollup view with status, severity, CVSS, KEV (CISA Known Exploited Vulnerabilities) score, affected software, and device count.
- `jellyfish user show <id-or-email>` - resolve a user, their devices, and the active detections per device.
- `jellyfish configure` - interactively store the tenant subdomain, region and API token (token in the macOS Keychain).

## Install

Requires Go 1.21+ on macOS. The module path `flipgroup.com.au/jellyfish` is
private (no `go-import` meta tag is served), so install from a local checkout
rather than the network.

```bash
git clone <repo-url> jellyfish && cd jellyfish
make install
```

`make install` runs `go install .` with version metadata baked in via
`-ldflags`. It places `jellyfish` in `$GOBIN` if set, otherwise `$GOPATH/bin`
(default `~/go/bin`). Add that directory to your `PATH` once:

```bash
echo 'export PATH="$HOME/go/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

For an in-tree build (no install) use:

```bash
make build       # produces ./bin/jellyfish
./bin/jellyfish version
```

After install, confirm the binary is wired up:

```bash
jellyfish version    # prints e.g. "jellyfish 7f579d3" (the current git ref)
```

### Shell completion

`jellyfish` ships Cobra's auto-generated completion script. To enable Zsh
completion (the macOS default shell), drop the script somewhere Zsh's `fpath`
already covers:

```bash
mkdir -p "$(brew --prefix)/share/zsh/site-functions"
jellyfish completion zsh > "$(brew --prefix)/share/zsh/site-functions/_jellyfish"
exec zsh    # reload
```

For other shells: `jellyfish completion {bash,fish,powershell} --help`.

## Configure

```bash
jellyfish configure
```

Interactive prompts:

1. Tenant subdomain (the bit before `.api.kandji.io`).
2. Region: `us` or `eu`.
3. API token (input is masked).

Subdomain and region are written to `~/.config/jellyfish/config.yml` with mode
`0600`. The token is stored in the macOS Keychain under the service
`au.com.flipgroup.jellyfish` and account `default`. You can inspect or remove
it via Keychain Access or:

```bash
security find-generic-password -s au.com.flipgroup.jellyfish -a default
security delete-generic-password -s au.com.flipgroup.jellyfish -a default
```

### Configure email defaults

```bash
jellyfish configure email
```

Prompts (in order): `From`, default `To`, path to a Gmail service-account
JSON file, header background colour, path to a logo PNG. The first two
values and `header_bg` are written to the `email:` block of
`~/.config/jellyfish/config.yml`. When a Gmail JSON path is provided, the
file is read, validated, then stored in the macOS Keychain (the source
path is not persisted). When a logo path is provided, the PNG is validated
(decodable PNG, no larger than 512 KB) and copied into
`~/.config/jellyfish/logos/<basename>`; the resulting managed path is
written to `email.logo_path`. The source file is left alone.

For each prompt: Enter keeps the current value; type a literal `-` to
clear a field. Clearing the logo also deletes the copy under `logos/`
(but never any file outside that directory).

## Usage

### Vulnerability detections

```bash
jellyfish vulns list                              # everything
jellyfish vulns list --device-id d-123            # one device by ID
jellyfish vulns list --serial C02XL0RKDV4         # one device by serial
jellyfish vulns list --limit 50                   # single page of 50
jellyfish vulns list -o json                      # JSON for jq
jellyfish vulns list -o csv > vulns.csv           # CSV export
jellyfish vulns list --no-cache                   # skip cache, always fetch fresh
```

### Detection cache

Both `vulns list` and `user show` walk Iru's `/vulnerability-management/detections`
endpoint client-side. Iru exposes no per-device filter on that endpoint
(probed against a live tenant — `device_id=`, `device_serial_number=`, and
others are all silently ignored), so the only way to deliver a per-device
view is to fetch every detection and filter in memory.

That walk is the bulk of the wall time. On a tenant with thousands of CVE
detections, expect roughly **30-90 seconds** for the first call (a progress
indicator on stderr shows pages as they come in). To avoid repeating that
walk on every command, the result is cached for **15 minutes** at the OS
cache location:

- macOS: `~/Library/Caches/jellyfish/detections.json` (detections), `~/Library/Caches/jellyfish/vulnerabilities.json` (vulnerability rollups)
- Linux: `$XDG_CACHE_HOME/jellyfish/detections.json` or `~/.cache/jellyfish/detections.json` (detections), same directory for `vulnerabilities.json`

Subsequent commands within the TTL skip the walk and return in under a
second. Pass `--no-cache` (available on both `vulns list` and `user show`)
to force a fresh fetch. Delete the file by hand to invalidate manually.

### Iru detection semantics

Iru's `/detections` endpoint returns one record per (device, CVE, package
version) intersection. There is **no status field on detection records** —
a detection exists exactly while the underlying CVE is present on the
device's installed package. When the package gets patched, Iru re-scans
and the detection disappears.

The corollary: every detection `jellyfish vulns list` or `jellyfish user
show` returns is by definition active / non-remediated. There is no need
to (and no way to) filter to "active only".

For a per-CVE rollup with remediation status, use `jellyfish vulns summary`
(backed by Iru's `/vulnerability-management/vulnerabilities` endpoint).

### Vulnerability summary

Per-CVE rollup across the fleet, backed by Iru's
`/vulnerability-management/vulnerabilities` endpoint. Unlike `vulns list`
(per device, per CVE), this is one row per CVE with status, severity,
CVSS, KEV score, affected software, and how many devices are exposed.

```bash
jellyfish vulns summary                            # all CVEs, severity-sorted
jellyfish vulns summary --status active            # only currently-affecting CVEs
jellyfish vulns summary --severity critical        # critical only
jellyfish vulns summary --sort devices --limit 20  # top 20 by device exposure
jellyfish vulns summary --sort kev                 # sort by KEV (known-exploited)
jellyfish vulns summary -o json                    # JSON for jq
jellyfish vulns summary --no-cache                 # bypass the 15-minute cache
```

Sort keys: `severity` (default - Critical first, then CVSS desc within tier),
`cvss`, `kev`, `devices`, `cve`. Iru ignores status/severity query params on
this endpoint, so filtering is client-side after a full walk (~3000 records
on a typical tenant; a few seconds with the progress indicator). Results
are cached separately from detections at
`~/Library/Caches/jellyfish/vulnerabilities.json`.

**About KEV.** `kev_score` reflects whether the CVE appears in CISA's
[Known Exploited Vulnerabilities catalog](https://www.cisa.gov/known-exploited-vulnerabilities-catalog) -
a list of vulnerabilities observed being actively exploited in the wild, not
just theoretically dangerous. For triage, `--sort kev` is usually a stronger
patch-priority signal than CVSS alone: a Medium-CVSS bug that attackers are
actively using can be more urgent than a Critical-CVSS bug that nobody has
weaponised yet. Iru does not document the exact `kev_score` semantics; on
this tenant the field is numeric (0 for non-KEV entries). Inspect the
distribution with:

```bash
jellyfish vulns summary -o json | jq '[.[].kev_score] | unique'
```

### Per-user view

```bash
jellyfish user show keith@flipgroup.com.au        # by email
jellyfish user show 1f5b...e4                     # by user ID
jellyfish user show keith@flipgroup.com.au -o json
jellyfish user show keith@flipgroup.com.au --no-cache
```

Email lookup is a single direct `?email=` request against Iru. The
per-user-device detection bucketing is what triggers the detection walk
(see Detection cache, above).

### Output formats

`-o` accepts `table` (default), `json`, `yaml`, `csv`.

For `user show -o csv`, the output is flattened: one row per detection, with
user and device columns repeated. Column order:

```
user_id, user_email, user_name, device_id, device_name, serial_number,
cve_id, package_name, package_version, severity, cvss_score,
detection_datetime
```

### Email output

`-o email` writes an RFC 5322 multipart/alternative message (.eml) to stdout.
It carries a styled HTML body (executive summary + per-CVE table with
clickable NVD/MITRE CVE links) and a plain-text alternative. Open the .eml
file in Mail, pipe it to your mail tooling, or pass --send-email to send it via the Gmail API (see below).

```bash
jellyfish vulns summary --severity critical -o email > critical.eml
jellyfish vulns summary -o email | open -f -a Mail            # macOS
jellyfish user show keith@flipgroup.com.au -o email \
    --email-to keith@flipgroup.com.au > exposure.eml
```

On a real tenant `vulns summary` is ~3000 rows. Gmail will clip the message
if you send the unfiltered output - filter with `--severity`, `--status`, or
`--limit` first.

Recipient, sender, and subject default from the `email:` block in
`config.yml`; flags override:

| Flag | Config key | Default |
|---|---|---|
| `--email-to`         | `email.default_to`       | empty (header renders as `<unspecified>`) |
| `--email-from`       | `email.from`             | `git config user.email` |
| `--email-subject`    | `email.subject_template` | per-command default |
| `--email-header-bg`  | `email.header_bg`        | `#7566FF` (Flip brand purple) |
| `--email-logo`       | `email.logo_path`        | empty (no logo) |

The default `#7566FF` is the Flip brand purple. A logo whose recognisable
element is the same purple (the Flip brand mark, for example) will blend into
that background - pair the default with a logo whose distinguishing element
is white or dark, or pick a contrasting header colour such as `#C6B8FE`
(light lavender) or `#6846D8` (deep purple).

`email.subject_template` is a Go template; available variables: `{{.Date}}`
(YYYY-MM-DD) and `{{.Time}}` (HH:MM).

CVE link targets are also config-overridable; defaults are NVD and MITRE:

```yaml
email:
  from: alice@example.com
  default_to: secops@example.com
  subject_template: "Weekly brief - {{.Date}}"
  cve_link_primary: "https://nvd.nist.gov/vuln/detail/{cve}"
  cve_link_secondary: "https://www.cve.org/CVERecord?id={cve}"
```

The `{cve}` token in link templates is a literal substring replacement.

#### Sending via Gmail (`--send-email`)

`--send-email` on `vulns summary` or `user show` renders the .eml internally
and sends it via the Gmail API instead of writing it to stdout. Combine with
any of the existing email/filter flags:

```bash
jellyfish vulns summary --severity critical --send-email --email-to secops@flipgroup.com.au
jellyfish user show keith@flipgroup.com.au --send-email
```

Recipient resolution for `user show --send-email`:

1. `--email-to <addr>` if provided
2. `email.default_to` from config if non-empty
3. The resolved user's own email address

For `vulns summary --send-email` the user fallback does not apply — pass
`--email-to` or set `email.default_to`.

On success, stdout stays empty and stderr gets one line:
`sent: to=<addr> from=<addr> gmail-id=<id>`.

Combining `--send-email` with an explicit `-o` other than `email` errors out
(exit 1). Use one or the other.

The Gmail send path uses a Workspace service account with domain-wide
delegation. The service-account JSON is stored in the macOS Keychain under
service `au.com.flipgroup.jellyfish`, account `gmail_default` — install it
via `jellyfish configure email` (third prompt). Inspect or remove it via:

```bash
security find-generic-password -s au.com.flipgroup.jellyfish -a gmail_default
security delete-generic-password -s au.com.flipgroup.jellyfish -a gmail_default
```

Gmail-side failures surface via exit codes 2 (auth/permissions: bad JWT,
DWD scope not granted, mailbox forbidden) and 4 (rate-limited or 5xx
upstream).

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | User error (bad flags, missing config) |
| 2 | Authentication or permissions failure |
| 3 | User, device or detection not found |
| 4 | Upstream error (5xx, network) |

## Development

```bash
make test
make lint
```

Real-Keychain integration tests:

```bash
JELLYFISH_KEYCHAIN_TESTS=1 go test ./internal/keychain/... -count=1
```

The first run will pop a macOS dialog asking to allow the test binary to read
your Keychain; approve it and re-run.

## Known follow-ups

These items were noted during development but are not blocking. Captured
here so they do not get lost.

### Iru response shape probing (resolved)

`internal/iru/types.go` was originally authored without a live tenant. A
series of v1.1 commits realigned the code against the real Iru API after
live probing:

- `/users` paginates with `?cursor=<value>` and returns `{next, previous,
  results}` where `next` is a full URL.
- `/detections` paginates with `?after=<value>` and returns the same
  wrapper but with `next` as a raw cursor string. `nextCursor()` handles
  both shapes.
- `/devices` returns a top-level JSON array; no wrapper.
- Iru ignores per-device filters on `/detections` entirely. Detection
  filtering happens client-side after a full walk; results are cached
  for 15 minutes.
- User lookup by email uses `/users?email=<address>` directly (single
  request, no walk).
- `Detection` records have no `status` field. `User` and `Device` structs
  expanded with the rich fields Iru returns.

### Retry transport drops the upstream error message body

When `internal/iru/retry.go` exhausts all three attempts on a 429 or 5xx, it
drains and closes each response body in the retry loop, then returns the last
response. By the time `client.do` calls `decodeAPIError(resp)`, the body is
already closed and reads as empty - so `APIError.Message` ends up blank.

Status codes are preserved correctly (so `classifyError` still maps 429 to
exit 4 and 5xx to exit 4), and exit-code behaviour is unaffected. The only
loss is the human-readable error text from Iru.

Fix path: in the retry loop, read the body into a `[]byte` before closing,
then restore it via `io.NopCloser(bytes.NewReader(buf))` on the response
returned to the caller. Small change; needs a test that asserts the message
survives after retries.

### `WithHTTPClient` plus `WithTimeout` option ordering is fragile

If a caller passes both `iru.WithTimeout(d)` and `iru.WithHTTPClient(custom)`
in that order, `WithHTTPClient` replaces the whole `*http.Client` and the
timeout from the previous option is lost. This is not triggered anywhere in
v1 because `WithHTTPClient` is never called by production code (only the
constructor's default `*http.Client` is used). Worth fixing before exposing
the option more broadly. Either make `WithHTTPClient` preserve any timeout
already set, or document the ordering contract on each option.

### CSV column order for `user show` is fixed without a test

The flattened CSV columns for `jellyfish user show -o csv` are listed in the
README above and pinned in `cmd/user.go`'s `renderUserBundleCSV`. There is
currently no test asserting the order. If the order ever needs to change,
that change should land alongside a golden-file test that locks it down.

### Linux / Windows support

The CLI is macOS-only because the credential store is the macOS Keychain.
Porting to Linux would mean swapping in libsecret (or similar); Windows would
need Credential Manager. Non-darwin builds currently fail at compile time on
the `keychain` package, which is the intended signal.

### Other future work

- Multi-profile support: the `--profile` flag is already declared but only
  `default` is honoured. The config file format already keys by profile name,
  so extending this is mainly a `--profile` plumbing change.
- Env-var fallback for the token (`JELLYFISH_API_TOKEN`) for CI environments
  with no Keychain.
- A configurable `--cache-ttl` flag (currently the 15-minute TTL is hardcoded).
- Write operations (acknowledge, suppress, kick off remediation).
- A `-vv` extra-verbose mode that logs response bodies with token + PII
  redaction.
- Promote `internal/iru` to a public package once a second consumer needs the
  client.
