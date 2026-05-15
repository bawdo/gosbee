# jellyfish

A macOS-only Go CLI for the Iru (formerly Kandji) Endpoint Management API.

## Features

- `jellyfish vulns list` - list vulnerability detections across the fleet, filter by device ID or serial number.
- `jellyfish user show <id-or-email>` - resolve a user, their devices, and the active detections per device.
- `jellyfish configure` - interactively store the tenant subdomain, region and API token (token in the macOS Keychain).

## Install

Requires Go 1.21+ on macOS.

```bash
go install flipgroup.com.au/jellyfish@latest
```

This places `jellyfish` in `$GOBIN` if set, otherwise `$GOPATH/bin`
(default `~/go/bin`). Add it to your `PATH` once:

```bash
echo 'export PATH="$HOME/go/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

To build locally instead:

```bash
make build       # ./bin/jellyfish
make install     # installs via `go install`
```

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

## Usage

### Vulnerability detections

```bash
jellyfish vulns list                              # everything
jellyfish vulns list --device-id d-123            # one device by ID
jellyfish vulns list --serial C02XL0RKDV4         # one device by serial
jellyfish vulns list --limit 50                   # single page of 50
jellyfish vulns list -o json                      # JSON for jq
jellyfish vulns list -o csv > vulns.csv           # CSV export
```

### Per-user view

```bash
jellyfish user show keith@flipgroup.com.au        # by email
jellyfish user show 1f5b...e4                     # by user ID
jellyfish user show keith@flipgroup.com.au -o json
```

### Output formats

`-o` accepts `table` (default), `json`, `yaml`, `csv`.

For `user show -o csv`, the output is flattened: one row per detection, with
user and device columns repeated. Column order:

```
user_id, user_email, user_name, device_id, device_name, serial_number,
cve_id, package_name, package_version, severity, cvss_score,
detection_datetime
```

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

These items are deferred from v1. None are blocking; capture them here so they
do not get lost.

### Iru response field names are speculative

**Resolved in v1.1.** `internal/iru/types.go` was authored without a live
tenant. v1.1 (commits `128434b`, `bbde2f4`, plus this cleanup) realigned the
types against real Iru responses: `/users` and `/detections` use cursor
pagination with a `{next, previous, results}` wrapper, the `Detection` struct
was rebuilt against the actual field names (no `detection_id`, no `status`,
`cve` is `cve_id`, etc), and the `User`/`Device` structs were expanded with
the additional fields Iru returns. `/devices` confirmed to still use the
top-level array shape.

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
- Write operations (acknowledge, suppress, kick off remediation).
- A `-vv` extra-verbose mode that logs response bodies with token + PII
  redaction.
- Promote `internal/iru` to a public package once a second consumer needs the
  client.
