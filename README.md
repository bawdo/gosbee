# jellyfish

A macOS-only Go CLI for the Iru (formerly Kandji) Endpoint Management API.

## Features

- `jellyfish vulns list` - list vulnerability detections across the fleet, filter by device ID, serial number or status.
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
jellyfish vulns list --status active              # active only
jellyfish vulns list --device-id d-123            # one device by ID
jellyfish vulns list --serial C02XL0RKDV4         # one device by serial
jellyfish vulns list --limit 50 --page 1          # single page of 50
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
detection_id, cve, severity, status
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
