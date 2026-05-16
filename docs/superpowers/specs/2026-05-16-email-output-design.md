# Email output for jellyfish

## Goal

Add `-o email` to `jellyfish vulns summary` and `jellyfish user show`. The command writes a complete RFC 5322 multipart/alternative message (`.eml`) to stdout. The message is drop-in for manual review (save and open in Mail.app, pipe to `sendmail -t`), for the Gmail API (base64url-encode the bytes, POST to `users.messages.send`), and for a future `--send-email` flag that reuses the same byte output.

## Non-goals

- No SMTP, no Gmail API calls, no credential storage in this slice. `--send-email` is a separate, follow-up piece of work that consumes the bytes this slice produces.
- No new output rendering for `vulns list`. The per-device-per-CVE shape is too noisy for email; that command stays as it is.
- No hosted "view full report" link, no truncation banner. Email body honours `--limit / --severity / --status`; if the user asks for 3000 rows they get 3000 rows (and a clipped Gmail). Recipe in the README guides them to filter first.

## CLI surface

`-o email` is wired into the existing `--output / -o` flag the same way `json`, `yaml`, and `csv` are. Three additional flags carry email-specific context:

| Flag | Meaning | Resolution order |
|---|---|---|
| `--email-to <addr>` | `To:` header | flag > config (`email.default_to`) > empty |
| `--email-from <addr>` | `From:` header | flag > config (`email.from`) > git `user.email` > hard error |
| `--email-subject <s>` | `Subject:` header | flag > config (`email.subject_template`, rendered) > per-renderer default |

If `email-from` is empty after all three sources, the command errors with exit code 1 ("no email from address - set --email-from, email.from in config, or configure git user.email"). `email-to` may legitimately be empty (header is rendered as `<unspecified>` so the user can fill it in before sending).

New config keys in `~/.config/jellyfish/config.yml` (all optional):

```yaml
email:
  from: keith@flipgroup.com.au
  default_to: security@flipgroup.com.au
  subject_template: "Jellyfish vulnerability summary - {{.Date}}"
  cve_link_primary: "https://nvd.nist.gov/vuln/detail/{cve}"
  cve_link_secondary: "https://www.cve.org/CVERecord?id={cve}"
```

Built-in defaults if the keys are absent:

- `cve_link_primary`: `https://nvd.nist.gov/vuln/detail/{cve}` (NVD)
- `cve_link_secondary`: `https://www.cve.org/CVERecord?id={cve}` (MITRE)
- `subject_template` defaults differ per command (see below).

The `{cve}` token in link templates is a literal substring replacement, not a Go template - so existing URL escaping rules are predictable. The renderer validates at construction time that each template contains `{cve}`.

## Behaviour on each command

**`jellyfish vulns summary -o email`**

- Walks vulnerabilities (cache-respecting, same as today).
- Applies the existing `--status`, `--severity`, `--sort`, `--limit` filters.
- Renders the Operations Dashboard email (see "Email design", below) with KPI tiles reflecting the **filtered** counts (so what the recipient reads matches what the user asked for).
- Default subject: `Jellyfish vulnerability summary - <YYYY-MM-DD>`.

**`jellyfish user show <id-or-email> -o email`**

- Same per-user bundle as today.
- Same Operations Dashboard frame, with the masthead title set to `Vulnerability exposure - <user name or email>`.
- KPI tiles count across the user's devices.
- Body section: one block per device, each containing that device's mini-table of CVEs sorted by severity then CVSS. Devices with no detections render a tile but no table.
- Default subject: `Vulnerability exposure - <user.name or email> - <YYYY-MM-DD>`.

`vulns list` is unchanged. Asking for `vulns list -o email` errors with the same "unsupported output format" path the package already has.

## Message shape

```
From: Jellyfish <keith@flipgroup.com.au>
To: security@flipgroup.com.au
Subject: Jellyfish vulnerability summary - 2026-05-16
Date: Sat, 16 May 2026 10:42:00 +1000
Message-ID: <1747391020000000000.6f3a@flipgroup.com.au>
MIME-Version: 1.0
Content-Type: multipart/alternative; boundary="=_jf_<random>"

--=_jf_<random>
Content-Type: text/plain; charset=UTF-8
Content-Transfer-Encoding: quoted-printable

12 Critical, 28 High, 7 KEV-listed, 23 devices.

[the same plain-text table 'vulns summary' produces today]

--=_jf_<random>
Content-Type: text/html; charset=UTF-8
Content-Transfer-Encoding: quoted-printable

<html>...Operations Dashboard HTML...</html>

--=_jf_<random>--
```

Details:

- `Date:` is RFC 5322-formatted local time. `time.Now()` is injected via `Options.GeneratedAt` so golden-file tests are deterministic.
- `Message-ID:` is `<unix-nanos.<6 random hex chars>@<from-domain>>`. Mail clients use this for dedup; saving the same `.eml` twice deduplicates.
- The boundary is `=_jf_<16 random hex chars>`, also injectable for tests.
- Encoding is `quoted-printable` end-to-end. Implemented via stdlib `mime/quotedprintable`; no new dependency.
- The trailing boundary marker (`--<boundary>--`) and CRLF line endings throughout are mandatory per RFC.

## Email design ("Operations Dashboard", email-safe)

The browser mockup picked in brainstorming uses CSS grid, modern selectors and `box-shadow`. Gmail strips all of those. The real email is `<table>`-based with inline styles. Visual fidelity to the mockup is the goal; pixel-perfect parity is not.

Width: 600px max, centred. System font stack only (`-apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif`) and `'SF Mono', 'Menlo', 'Consolas', monospace` for CVE codes and numeric cells. No web fonts.

Frame elements:

- **Masthead**: a `<table>` with `bgcolor="#0f172a"`, containing the brand tag (small caps, monospace), the title (e.g. "Fleet vulnerability summary"), and a sub line ("16 May 2026 - 10:42 AEST - 90 CVEs across 23 devices").
- **KPI tiles**: a 4-column `<table>` with `border-collapse: separate; border-spacing: 1px; bgcolor="#e2e8f0"` to fake the divider lines from the mockup. Each cell has the numeric stat (`font-weight: 800`, `font-size: 26px`, accent colour per severity) above the label.
- **Section heading bar**: an all-caps small label above the data table.
- **Data table**:
  - First `<td>` of each row is 3px wide with a `bgcolor` matching the row's severity (`#dc2626` critical, `#ea580c` high, `#ca8a04` medium). This is the "severity bar" from the mockup.
  - **CVE column**: `<a href="<NVD URL>">CVE-2024-3094</a> <a href="<MITRE URL>" style="font-size:10px;color:#94a3b8;text-decoration:none">(M)</a>`. Primary action on the CVE code, secondary `(M)` glyph for MITRE.
  - **Severity** cell: a nested 1-cell `<table>` rendered as a pill (`bgcolor` + inline padding + inline radius; Outlook ignores radius and degrades to a square rectangle, which is acceptable).
  - **KEV** cell: pill with `bgcolor="#ede9fe"`, text `#6d28d9`. Empty cell renders as a dash.
  - **CVSS, Devices**: monospaced, right-aligned.
- **Footer**: small muted text. `Generated by jellyfish vulns summary` + KEV definition.

Dark mode: a `<style>` block in the `<head>` with `@media (prefers-color-scheme: dark)` overrides for body and table backgrounds. Gmail desktop ignores it; Gmail iOS honours it; either way the body styles remain legible.

Edge cases:

- **Empty filtered result**: KPI tiles still render (all zeros). The data section becomes a single block: "No matching vulnerabilities."
- **Filters applied**: if any of `--limit / --severity / --status` were non-default, the footer adds "Showing N of M after filters." `M` is the unfiltered total from the walk; `N` is the rendered count.
- **`vulns summary` row truncation**: none beyond what the user asked for via `--limit`. The README documents that piping unfiltered output into email will hit Gmail's clipping; the recipe is to filter first.

## Plain-text alternative

Same content, no markup. For `vulns summary`:

```
12 Critical, 28 High, 7 KEV-listed, 23 devices.

CVE              SEVERITY  CVSS  KEV  DEVICES  STATUS    SOFTWARE
CVE-2024-3094    Critical  10.0  KEV  4        active    xz-utils
CVE-2024-6387    Critical   8.1       11       active    openssh-server
...

Generated by jellyfish vulns summary
KEV = CISA Known Exploited Vulnerabilities catalogue
NVD links: https://nvd.nist.gov/vuln/detail/CVE-2024-3094
```

Reuse the existing table renderer's column logic to produce the body. Append CVE link list at the end for plain-text recipients (one URL per CVE, NVD only).

For `user show`: same shape, grouped per device with a heading line per device.

## Architecture

```
internal/email/
  email.go              # Renderer constructors, Options, assembleMessage()
  vulns_summary.go      # Vuln-summary payload + render
  user_show.go          # User-bundle payload + render
  templates/
    base.html.tmpl
    vulns_summary.html.tmpl
    user_show.html.tmpl
    base.txt.tmpl
    vulns_summary.txt.tmpl
    user_show.txt.tmpl
  testdata/
    vulns_summary.golden.eml
    vulns_summary_empty.golden.eml
    user_show.golden.eml
    user_show_no_detections.golden.eml
  email_test.go
  vulns_summary_test.go
  user_show_test.go
```

The `Options` type:

```go
type Options struct {
    To               string
    From             string    // empty triggers caller-side fallback (cmd layer)
    Subject          string
    CVELinkPrimary   string
    CVELinkSecondary string
    GeneratedAt      time.Time // injected for determinism
    Tenant           string    // shown in masthead - sourced from the active profile's config.Subdomain
    // Internal: boundary + message-ID randomness sources, exposed for tests only.
}
```

Public constructors:

```go
func NewVulnSummaryRenderer(opts Options) output.Renderer
func NewUserShowRenderer(opts Options) output.Renderer
```

Each returns a `Renderer` whose `Render(w io.Writer, v any) error` validates `v`'s type (`[]iru.Vulnerability` or `UserBundle` respectively), builds the presentation struct (severity tallies, KEV count, device count, grouped rows for user show), executes the HTML and text templates, calls a private `assembleMessage(headers, htmlBody, textBody)` helper, and writes the bytes to `w`.

`assembleMessage` does the RFC 5322 wrapping: writes headers, generates the multipart boundary (from `Options.boundary` if set, otherwise `crypto/rand`), quoted-printable-encodes both parts, and emits the final byte slice.

HTML escaping is automatic via `html/template`. The `(M)` secondary link is rendered with `template.URL` for the `href` and the CVE id is escaped normally; the substring replacement of `{cve}` happens in Go before the URL hits the template, so the template sees a plain pre-built URL.

## CLI integration

`cmd/vulns.go` and `cmd/user.go` each grow a small `emailOpts(cmd)` helper that:

1. Reads `--email-to / --email-from / --email-subject`.
2. Falls back to `email.*` config keys via the existing config loader.
3. Final `From:` fallback shells out to `git config user.email` via `os/exec` (the codebase has no existing git usage; this slice adds the helper in `cmd/`, isolated and trivially testable with a `PATH`-stub).
4. Pulls `Tenant` from the active profile's `config.Subdomain` for the masthead.
5. Returns `email.Options` ready to pass to `NewVulnSummaryRenderer` / `NewUserShowRenderer`.

Dispatch:

```go
// in renderVulns:
case "email":
    r := email.NewVulnSummaryRenderer(emailOpts(cmd))
    return r.Render(w, vs)
```

Same shape in `renderUserBundle`. The new flags are registered on both subcommands.

`internal/config/config.go` grows an optional `Email` sub-struct (`From`, `DefaultTo`, `SubjectTemplate`, `CVELinkPrimary`, `CVELinkSecondary`). All fields optional; missing block = empty struct.

## Error handling

| Condition | Behaviour | Exit code |
|---|---|---|
| `--email-from` empty after all fallbacks | Error: "no email from address - set --email-from, email.from in config, or configure git user.email" | 1 (user error) |
| `cve_link_primary` or `cve_link_secondary` missing `{cve}` token | Error at renderer construction, before any walk | 1 |
| `Renderer.Render(w, v)` called with wrong type | Returns `fmt.Errorf("email renderer expected []iru.Vulnerability, got %T", v)` | propagates - this is a defensive guard, never hit in production |
| Detection or vulnerability walk fails | Same as today; renderer never runs | unchanged |
| Template execution error | Propagated as-is | propagates |

The progress indicator on stderr is unchanged - only stdout content changes. Users can still pipe stderr away while saving stdout as a `.eml`.

## Dependencies

None added. Standard library only: `html/template`, `text/template`, `mime/quotedprintable`, `crypto/rand`, `time`, `embed`, `net/mail` (used in tests for round-trip parsing).

## Testing

- **Golden-file tests** (`internal/email/*_test.go`): pin `Options.GeneratedAt`, boundary, and message-ID randomness sources. Assert byte-for-byte against `testdata/*.golden.eml`. Cases:
  - vulns summary: full result (10 CVEs across severity tiers, some KEV), empty filtered result, all-KEV result, single-CVE result.
  - user show: typical user (3 devices, mixed exposure), user with no detections, user with one device.
- **Round-trip parse**: feed each golden output back through `net/mail.ReadMessage` + `mime/multipart.NewReader`, assert that both parts exist, content-types are set correctly, headers parse, and the HTML part contains expected substrings (e.g. CVE code, NVD URL, KEV pill text).
- **`{cve}` substitution test**: explicit cases for templates with and without the token, escaped CVE codes, weird CVE shapes.
- **CLI integration** (`cmd/vulns_test.go`, `cmd/user_test.go`): use the existing temp-config-dir harness. Cases: flag-only, config-only, flag-overrides-config, no-from-anywhere (asserts exit code 1 with the documented message), `subject_template` rendering.
- **No live SMTP / Gmail API tests.** The renderer's contract ends at "produce a valid `.eml` on stdout." `--send-email` will own its own test surface when it lands.

## README changes

A new "Email output" subsection under "Output formats":

````
### Email output

`-o email` writes an RFC 5322 multipart/alternative message (.eml) to stdout.
It carries a styled HTML body (executive summary + per-CVE table with
clickable NVD/MITRE links) and a plain-text alternative. Open it manually,
pipe it to your mail tooling, or feed it to a future `--send-email` flag.

```bash
jellyfish vulns summary --severity critical -o email > critical.eml
jellyfish vulns summary -o email | open -a Mail            # macOS
jellyfish user show keith@flipgroup.com.au -o email \
    --email-to keith@flipgroup.com.au > exposure.eml
```

On a real tenant `vulns summary` is ~3000 rows - filter with `--severity`,
`--status`, or `--limit` before emailing or Gmail will clip the message.

Recipient, sender and subject default from `email:` in `config.yml`; flags
override:

| Flag | Config key | Default |
|---|---|---|
| `--email-to`      | `email.default_to`       | empty (`<unspecified>`) |
| `--email-from`    | `email.from`             | `git config user.email` |
| `--email-subject` | `email.subject_template` | per-command default |

CVE link targets are also config-overridable (`email.cve_link_primary`,
`email.cve_link_secondary`); defaults are NVD and MITRE.
````

## Out of scope (for the follow-up plan)

- `--send-email` flag: send the assembled bytes via the Gmail API. Wants its own OAuth flow, scoped token storage (likely macOS Keychain alongside the existing Iru token), and a separate test surface. The output of this slice is exactly the input that path will consume.
- Per-user "your exposure" email distribution: looping over users and producing N emails. Easily layered on top of `user show -o email` later.
- HTML email rendering for `vulns list`. Out of scope by design.
