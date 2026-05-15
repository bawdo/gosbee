# Jellyfish - design

**Date:** 2026-05-15
**Status:** Approved
**Author:** Keith Bawden

## 1. Overview

Jellyfish is a macOS command-line tool, written in Go, that talks to the Iru
(formerly Kandji) Endpoint Management API. It exists to give an operator quick,
scriptable access to vulnerability and device data without having to log into
the Iru console.

The v1 surface is intentionally narrow. Three read-only workflows plus a setup
flow:

1. List vulnerability detections across the entire device fleet.
2. List vulnerability detections scoped to a single device, by `device_id` or
   `serial_number`.
3. Given a user identifier (UUID or email), return the user's details, the
   devices they own, and the active vulnerability detections for each device.

A `jellyfish configure` command captures the API token (stored in the macOS
Keychain) and the tenant base URL (stored in a plain config file).

## 2. Non-goals (v1)

These were considered and explicitly deferred to keep v1 focused.

- Multiple profiles or tenants. The config format leaves room for it; the
  flag is reserved; v1 honours only `default`.
- Write operations (acknowledging, suppressing, or remediating detections).
- Cross-platform support. macOS only, because the credential store is the
  macOS Keychain.
- Token retrieval fallbacks (env vars, `.netrc`, 1Password CLI, etc).
- A reusable public SDK. `internal/iru` stays internal; we can promote it
  later if a second consumer appears.
- Caching of API responses.

## 3. CLI surface

Binary name: `jellyfish`.

```
jellyfish configure
jellyfish vulns list   [--device-id=<id> | --serial=<sn>]
                       [--status=<status>] [--limit=<n>] [--page=<n>]
                       [-o table|json|yaml|csv]
jellyfish user  show   <id-or-email>
                       [-o table|json|yaml|csv]
jellyfish version
```

### 3.1 Persistent (root) flags

Available on every subcommand.

| Flag | Default | Purpose |
|---|---|---|
| `-o, --output` | `table` | Output format: `table`, `json`, `yaml`, `csv`. |
| `-v, --verbose` | off | Log request URL and response status to stderr. Never logs the token. Never logs response bodies in v1. |
| `--config` | `~/.config/jellyfish/config.yml` | Override config file path. |
| `--profile` | `default` | Reserved. v1 only honours `default`. Flag exists so future multi-profile support does not break callers. |

### 3.2 `configure`

Interactive prompt. No flags in v1.

Prompts, in order:

1. Tenant subdomain. Validated against `^[a-z0-9-]+$`.
2. Region. Single choice: `US` or `EU`.
3. API token. Input masked (no terminal echo).

After capture:

- Compute `base_url` from subdomain + region.
- Write the YAML config file (mode `0600`, parent dir created as needed).
- Write or replace the Keychain item.
- Verify: `GET /api/v1/devices?limit=1` with the new token. On 200 print a
  success tick. On 401 or 403 print a warning but leave the saved config in
  place so the user can re-run after rotating the token.

### 3.3 `vulns list`

Lists vulnerability detections from `/api/v1/vulnerability-management/detections`.

Behaviour:

- No filters: walks every page until exhausted (auto-pagination).
- `--device-id=X`: server-side filter via `?device_id=X`.
- `--serial=Y`: first resolves to a `device_id` via
  `/api/v1/devices?serial_number=Y`, then filters as above. Exits with code 3
  if no device matches the serial.
- `--device-id` and `--serial` are mutually exclusive.
- `--status=<status>`: passes through to the API verbatim. Default returns
  all statuses. Jellyfish does not maintain its own allow-list of status
  values; Iru's response (typically a 400) is surfaced if the value is
  invalid.
- `--limit=N` and `--page=N`: when either is supplied, makes a single bounded
  request instead of walking all pages. The Iru API caps `limit` at 300; values
  above that are clamped with a stderr warning.

### 3.4 `user show <id-or-email>`

Single positional argument. Detection rule: presence of `@` means email,
otherwise treated as a user_id.

Composite flow (see Section 5 for the full data flow):

1. Resolve user.
2. List devices owned by that user.
3. Concurrently fetch active detections for each device (bounded to 5
   in-flight requests).
4. Assemble a `UserBundle{User, []DeviceWithDetections}` and hand to the
   renderer.

Output rendering for the composite:

- `table`: three stacked tables with section headers (User, Devices,
  Detections per device).
- `json`, `yaml`: emit the structured nested object verbatim.
- `csv`: flatten to one row per detection, repeating user and device columns.

### 3.5 Exit codes

| Code | Meaning |
|---|---|
| 0 | Success. |
| 1 | User error - bad flags, missing config, mutually-exclusive flags both supplied. |
| 2 | Auth or permissions error - 401 or 403 from Iru. |
| 3 | Not found - no such user, device, or detection. |
| 4 | Upstream error - 5xx, network failure, timeout. |

## 4. Configuration and secret storage

Two stores with distinct responsibilities. The token, the only secret, lives
in the macOS Keychain. Everything else lives in a YAML file.

### 4.1 Config file

Path: `~/.config/jellyfish/config.yml` (override with `--config`).
Mode: `0600`. Parent directory created with `0700` if missing.

```yaml
default:
  subdomain: acme
  region: us              # us | eu
  base_url: https://acme.api.kandji.io/api/v1
```

- `base_url` is derived from subdomain and region at `configure` time and
  written back so verbose logging and debugging do not need to recompute it.
- The `default:` top-level key reserves space for future profiles
  (`prod:`, `dev:`, etc) without a migration.

### 4.2 Keychain item

| Attribute | Value |
|---|---|
| Service | `au.com.flipgroup.jellyfish` |
| Account | `default` (matches the profile key in the config file) |
| Data | Raw bearer token, no prefix |

Library: `github.com/keybase/go-keychain`. It produces standard
generic-password items, so the user can inspect or remove them via Keychain
Access or `security delete-generic-password`.

### 4.3 Run-time token retrieval

Every subcommand except `configure` calls `config.Load()` then
`keychain.Get(service, account)`. If either step fails the CLI prints a
friendly message pointing at `jellyfish configure` and exits with code 1.

## 5. Iru API client

Lives in `internal/iru/`. Built around a `Client` struct constructed via
`iru.NewClient(baseURL, token string, opts ...Option) *Client`.

### 5.1 Package layout

```
internal/iru/
  client.go            # Client struct, NewClient, functional options
  transport.go         # http.RoundTripper: auth header, User-Agent, retry
  paginator.go         # Walk[T] limit/offset helper
  errors.go            # APIError, typed sentinels
  devices.go           # ListDevices, GetDevice
  users.go             # ListUsers, GetUser, FindUserByEmail
  vulnerabilities.go   # ListDetections, GetDetection
  types.go             # request/response structs
```

### 5.2 Transport

- Adds `Authorization: Bearer <token>` and `User-Agent: jellyfish/<version>`
  to every request.
- Default 30s `http.Client` timeout. Overridable via `WithHTTPClient` or
  `WithTimeout`.
- Retries idempotent requests on 429 and 5xx with exponential backoff,
  3 attempts max, honouring `Retry-After` if present.
- 4xx other than 429 fails immediately with a typed `APIError`.

### 5.3 Errors

Typed sentinels exposed at package level:

- `ErrUnauthorized` - 401
- `ErrForbidden` - 403
- `ErrNotFound` - 404
- `ErrRateLimited` - 429 after retries exhausted

Everything else is wrapped in `APIError{Status int, Code string, Message string}`.
Callers use `errors.Is` and `errors.As` to classify.

### 5.4 Pagination

`paginator.Walk[T](ctx, fetch func(limit, offset int) ([]T, error), cb func([]T) error) error`
drives `limit`/`offset` until a response is shorter than `limit`. List methods
expose two variants:

- `ListDetections(ctx, filters)` - auto-paginates, returns full slice.
- `ListDetectionsPage(ctx, filters, limit, offset)` - single page.

The CLI calls the auto variant by default and the page variant when
`--limit`/`--page` is supplied.

### 5.5 Data flow per command

**`vulns list` (no filter)**

```
Walk[Detection]("/vulnerability-management/detections", filters) -> render
```

**`vulns list --device-id=X`**

Same, with `?device_id=X` in the query.

**`vulns list --serial=Y`**

```
GET /devices?serial_number=Y   -> resolve device_id (404 -> exit 3)
Walk[Detection](..., device_id=...) -> render
```

**`user show <id-or-email>`**

```
1. Resolve user
   - if input contains '@': Walk[User]("/users"), filter by email,
     short-circuit on match
   - else: GetUser(id)
2. ListDevices(user_id=...)
3. errgroup over devices, max 5 in-flight:
     for each device: ListDetections(device_id=..., status=active)
4. Assemble UserBundle{User, []DeviceWithDetections}
5. Render
```

The email resolution is client-side rather than server-side because Iru's
documented user filtering does not guarantee exact email match across all
tenants. Worst case is one extra page of users fetched. If a reliable
server-side filter is confirmed later, swapping in is a small change behind
the same `FindUserByEmail` method signature.

### 5.6 Cancellation

Every public method takes a `context.Context`. The root Cobra command wires
up a signal-cancelled context so SIGINT cleanly aborts in-flight requests.

## 6. Output rendering

`internal/output/` exposes a single interface:

```go
type Renderer interface {
    Render(w io.Writer, v any) error
}
```

Four implementations: `tableRenderer`, `jsonRenderer`, `yamlRenderer`,
`csvRenderer`. Selection happens in `cmd/root.go` based on the `-o` flag.

| Format | Library | Notes |
|---|---|---|
| JSON | `encoding/json` | Indented by default. Struct tags carry field names. |
| YAML | `gopkg.in/yaml.v3` | Mirrors JSON shape. |
| Table | `github.com/jedib0t/go-pretty/v6/table` | Pure Go. Each command supplies its own `[]TableColumn{Header, Extractor}` projection so the output package stays decoupled from Iru types. |
| CSV | `encoding/csv` | Same column projection as tables. |

Composite output (`user show`):

- Table renders three stacked tables with section headers.
- JSON and YAML emit the nested object as-is.
- CSV flattens to one row per detection, repeating user and device columns
  on every row. This denormalisation is the only place the composite shape
  is destructured. The exact column order will be pinned down during
  implementation and documented in the README.

## 7. Error handling

- All command functions return `error`. The root `cmd.Execute()` wrapper
  inspects the error and maps to the exit codes in Section 3.5.
- Errors classified via `errors.Is` / `errors.As` against the sentinels in
  Section 5.3.
- Error messages always go to stderr. stdout is reserved for rendered output
  so callers can pipe safely.
- Verbose mode (`-v`) adds request URL and response status to stderr. It
  never logs the token. It does not log response bodies in v1.
- A missing config file or missing Keychain entry prints
  `No credentials found. Run "jellyfish configure" to set up.` and exits 1.

## 8. Testing strategy

| Package | Approach |
|---|---|
| `internal/iru` | Table-driven tests against `httptest.NewServer`. Covers auth header, pagination walk, 401/403/404/429/5xx mapping, retry-on-429 with `Retry-After`, context cancellation. No real network. |
| `internal/output` | Golden-file tests per renderer. Small input, expected output bytes. Regenerate when format changes intentionally. |
| `internal/config` | Round-trip tests using `t.TempDir()`: write, read, equal. |
| `internal/keychain` | `//go:build darwin` only. Gated behind `JELLYFISH_KEYCHAIN_TESTS=1` env var because it touches the real Keychain. CI macOS runners can opt in. |
| `cmd/` | Integration tests with a fake `iru.Client` injected via an interface. Assert exit codes and stdout for happy paths and each error class. |

No mocking framework. Handwritten fakes behind interfaces. `testify` is used
test-only for `require` and `assert` ergonomics.

## 9. Project layout

```
jellyfish/
  main.go                          # cmd.Execute()
  go.mod                           # module flipgroup.com.au/jellyfish
  go.sum
  Makefile                         # build, install, test, lint
  README.md                        # install, configure, usage
  .golangci.yml
  cmd/
    root.go                        # cobra root, persistent flags, signal context
    configure.go
    vulns.go
    user.go
    version.go
  internal/
    iru/                           # API client (Section 5)
    config/                        # YAML config file (Section 4)
    keychain/                      # macOS Keychain wrapper, darwin-only
    output/                        # renderers (Section 6)
    version/                       # ldflags-injected version string
  docs/
    superpowers/specs/             # design docs (this file)
```

Module path: `flipgroup.com.au/jellyfish`. `package main` lives at the repo
root so `go install .` produces a `jellyfish` binary at the install location.

## 10. Dependencies

Runtime:

| Purpose | Library |
|---|---|
| CLI framework | `github.com/spf13/cobra` |
| YAML config | `gopkg.in/yaml.v3` |
| Keychain | `github.com/keybase/go-keychain` |
| Table render | `github.com/jedib0t/go-pretty/v6` |
| Masked prompt | `golang.org/x/term` |

Test-only:

| Purpose | Library |
|---|---|
| Assertions | `github.com/stretchr/testify` |

JSON and CSV use the standard library.

## 11. Build and install

`go install` honours `$GOBIN` if set, otherwise `$GOPATH/bin` (which defaults
to `~/go/bin`). That is stock Go toolchain behaviour; no extra config needed.
The Makefile makes the common operations explicit.

```makefile
VERSION := $(shell git describe --tags --always --dirty)
LDFLAGS := -X flipgroup.com.au/jellyfish/internal/version.Version=$(VERSION)

build:
	go build -ldflags "$(LDFLAGS)" -o bin/jellyfish .

install:
	go install -ldflags "$(LDFLAGS)" .

test:
	go test ./...

lint:
	golangci-lint run

.PHONY: build install test lint
```

After `make install` (or `go install .`) the `jellyfish` binary is on `$PATH`
provided `$GOBIN` or `$GOPATH/bin` is on the user's `PATH`. The README will
note this and show how to add `~/go/bin` to `PATH` for anyone who has not.

`internal/version.Version` is overridden at build time via `-ldflags -X` and
surfaced by `jellyfish version`. The fallback is `"dev"` when unset.

## 12. Platform support

macOS only for v1. The Keychain package is `//go:build darwin`. Building on
Linux or Windows will fail at compile time, which is the intended signal.
Cross-compilation is out of scope.

Minimum Go toolchain: **Go 1.21**. The paginator uses generics (1.18+) and
the per-device fetch in `user show` uses `errgroup` plus modern context
helpers; 1.21 is the oldest currently-supported stable line, so it is the
sensible floor.

## 13. Lint and format

`gofmt` and `goimports` for formatting. `golangci-lint` with a conservative
ruleset: `errcheck`, `govet`, `staticcheck`, `ineffassign`, `gosec`. Wired
into the Makefile (`make lint`).

## 14. Open questions and future work

Captured here so they do not get lost when the v1 ships.

- Promote `internal/iru` to a public package once a second consumer needs it.
- Env-var fallback for the token, useful for CI environments that have no
  Keychain.
- Multi-profile support: extend `--profile` to honour values other than
  `default`. Config format already supports it.
- Write operations: acknowledge or suppress detections, kick off remediation.
- Linux and Windows support: would require a different credential backend
  (libsecret, Windows Credential Manager).
- A `--vv` (extra-verbose) mode that logs response bodies, with token and
  PII redaction.
