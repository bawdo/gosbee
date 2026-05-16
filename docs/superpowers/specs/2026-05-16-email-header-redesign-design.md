# Email header redesign — coloured header + optional logo

## Goal

Replace the hard-coded dark-blue header (`#0f172a`) in the HTML email templates with a configurable background colour, and add an optional PNG logo that renders inside the header. Apply to both renderers (`vulns summary -o email`, `user show -o email`, and their `--send-email` counterparts).

Default header colour is `#7566FF` (Flip brand purple). No logo by default.

## Non-goals

- Logo formats other than PNG. JPG would work but PNG covers all current test assets and is the safest format for email; SVG is widely blocked by mail clients.
- Per-template colour overrides. The two templates share one header colour to keep configuration coherent.
- Animated or responsive logos. A single static raster at one max-height covers the use cases.
- Changing the body / table / footer styling. This change is scoped to the header band only.
- Reworking the `prefers-color-scheme: dark` story per template. The existing dark-mode media query is removed (see "Dark mode" below), but no new dark-mode behaviour is added.
- Multi-profile support. The new keys live under the existing `default` profile, consistent with every other email setting.

## Config surface

Two new keys on the existing `email:` block:

```yaml
email:
  header_bg: "#7566FF"                 # default applied if absent or empty
  logo_path: "/Users/keith/.config/jellyfish/logos/flip-logo.png"
```

Two new CLI flags, following the `--email-to / --email-from / --email-subject` pattern (flag overrides config, config overrides built-in default):

| Flag | Config key | Default |
|---|---|---|
| `--email-header-bg` | `email.header_bg` | `#7566FF` |
| `--email-logo`      | `email.logo_path` | empty (no logo) |

Both flags are registered on `vulns summary` and `user show`.

### Validation

- `header_bg` must match `^#[0-9A-Fa-f]{6}$`. Bad values surface at config-load and at flag-parse with a clear error: `email.header_bg: invalid hex colour "purple"` / `--email-header-bg: invalid hex colour "purple"`. Validation is done once, in one helper, used by both call sites.
- `logo_path`, when non-empty, is resolved at render time (not config-load time). See "Logo error handling".

## `jellyfish configure email` additions

Two new prompts appended after the existing Gmail JSON prompt:

```
Header background colour [#7566FF]: 
Logo PNG path [/Users/keith/.config/jellyfish/logos/flip-logo.png]: 
```

Per-prompt behaviour matches the existing wizard:

| Input | Meaning |
|---|---|
| Enter (empty line) | Keep the current value |
| `-` (literal single dash) | Clear the field |
| Anything else | Use the typed value (trimmed) |

Header colour prompt: validates the hex format inline; on bad input, prints the error and re-prompts (up to `configureEmailMaxAttempts`, matching the existing pattern). On Enter with no current value, the built-in default `#7566FF` is written.

Logo prompt: when a non-empty, non-`-` path is supplied, the wizard:

1. Validates: file exists, is readable, decodes as PNG (`image.Decode` with the PNG decoder registered), and is no larger than 512 KB. Validation failure prints the error and re-prompts.
2. Resolves destination: `<configDir>/logos/<basename(src)>`.
3. `os.MkdirAll(<configDir>/logos, 0o700)`.
4. Copies bytes to the destination with mode `0o600`. If the destination already exists, it is overwritten.
5. Persists `email.logo_path` = destination path in `config.yml`.

The source path on disk is untouched — the operator can move or delete it after.

When the user types `-`, the wizard:

1. Clears `email.logo_path` in the config.
2. If the previous value was a path inside `<configDir>/logos/`, deletes the file. Paths outside `<configDir>/logos/` are left alone (defensive: never delete arbitrary user paths).
3. Prints one stderr line: `removed: <path>` (or nothing if the previous value was outside the managed dir).

The CLI flag `--email-logo` is **not** managed — it accepts any path, is read directly at send time, and is never copied. It's a one-off override.

## Rendering

### Header layout

Logo (when present) sits to the left of the existing text stack (badge → title → subtitle), vertically centred. Implemented as a two-cell row inside the header table. When no logo, the logo cell is omitted entirely; the no-logo header is visually identical to today's layout, just recoloured.

Max logo height: **56px**. Width scales by the logo's natural aspect ratio (`width:auto;height:56px;max-height:56px`). This caps height for tall logos and width for wide ones. A 100×100 square renders 56×56; a 200×100 rectangle renders 112×56.

### Text colour rule

Computed at render time from the WCAG relative luminance of `header_bg`:

```go
type headerStyle struct {
    BG, TextFG, BadgeBG, BadgeFG string
}

func computeHeaderStyle(bgHex string) headerStyle {
    r, g, b := hexToRGB(bgHex)
    L := relativeLuminance(r, g, b)   // WCAG 2.1 linearised, weighted
    if L > 0.5 {
        return headerStyle{
            BG: bgHex, TextFG: "#0f172a",
            BadgeBG: "rgba(15,23,42,0.10)", BadgeFG: "#0f172a",
        }
    }
    return headerStyle{
        BG: bgHex, TextFG: "#f8fafc",
        BadgeBG: "rgba(255,255,255,0.18)", BadgeFG: "#f8fafc",
    }
}
```

`relativeLuminance` uses the standard sRGB linearisation:
`Lc = ((c/255 + 0.055)/1.055)^2.4` for `c/255 > 0.03928`, else `(c/255) / 12.92`, then
`L = 0.2126*R + 0.7152*G + 0.0722*B`.

Mapping for the brand palette:

| `header_bg` | L | Text | Badge bg | Badge fg |
|---|---|---|---|---|
| `#7566FF` | ~0.22 | `#f8fafc` | `rgba(255,255,255,0.18)` | `#f8fafc` |
| `#C6B8FE` | ~0.55 | `#0f172a` | `rgba(15,23,42,0.10)` | `#0f172a` |
| `#6846D8` | ~0.13 | `#f8fafc` | `rgba(255,255,255,0.18)` | `#f8fafc` |

### Dark mode

The current `prefers-color-scheme: dark` `<style>` block (which inverts `<body>`/`<table>` to `#0f172a` in dark-mode clients) is **removed** from both templates. Rationale:

1. With a coloured header that the user picked, an inverted dark body fights the header rather than matching it.
2. Mail clients are inconsistent about applying media queries inside `<style>`. Removing the rule yields predictable rendering across Gmail web, Apple Mail, and Outlook.
3. No new dark-mode handling is added — the white card body on a neutral page background is acceptable in both light and dark client modes.

## MIME structure

`assembleMessage` gains an optional logo parameter:

```go
type logoPart struct {
    Bytes []byte
    Name  string   // for Content-Disposition filename
    CID   string   // "jf-logo"
}
```

When `logoPart == nil`, the existing `multipart/alternative` envelope is emitted unchanged (no MIME-shape regression for the no-logo path).

When `logoPart != nil`:

```
Content-Type: multipart/related; type="multipart/alternative"; boundary="=_jfr_<hex>"

--=_jfr_<hex>
Content-Type: multipart/alternative; boundary="=_jf_<hex>"

  --=_jf_<hex>
  Content-Type: text/plain; charset=UTF-8
  Content-Transfer-Encoding: quoted-printable
  ...

  --=_jf_<hex>
  Content-Type: text/html; charset=UTF-8
  Content-Transfer-Encoding: quoted-printable
  ...
  --=_jf_<hex>--

--=_jfr_<hex>
Content-Type: image/png
Content-Transfer-Encoding: base64
Content-ID: <jf-logo>
Content-Disposition: inline; filename="<basename>"

<base64 wrapped at 76 columns>
--=_jfr_<hex>--
```

The HTML template references the logo as `<img src="cid:jf-logo">`.

Outer boundary (`=_jfr_<hex>`) is independent of the inner boundary (`=_jf_<hex>`); both are generated by `randomBoundary()` with different prefixes. Boundary injection for test determinism:

- The existing `Options.BoundaryOverride` continues to control the inner `multipart/alternative` boundary (no behaviour change for existing tests).
- A new `Options.RelatedBoundaryOverride` controls the outer `multipart/related` boundary. Unused when no logo is present.

## Logo error handling

A configured logo is loaded lazily at render time. On any of:

- file not found
- file not readable (permission denied)
- file fails PNG decode
- file larger than 512 KB

…the renderer:

1. Skips the logo (HTML renders without the `<img>`, MIME stays as `multipart/alternative`).
2. Emits one stderr warning line: `warn: email logo not loaded (<path>: <reason>); rendering without logo`.
3. Does not affect the exit code. The render proceeds, and `-o email` / `--send-email` exit with whatever they would have exited with had no logo been configured.

This applies equally to `-o email` and `--send-email`, and equally to a path supplied via `--email-logo` flag or via `email.logo_path` config.

## File / package layout

```
cmd/
  vulns.go               # add --email-header-bg, --email-logo flags
  user.go                # add --email-header-bg, --email-logo flags
  configure.go           # add header-bg + logo prompts to runConfigureEmail

internal/config/
  config.go              # add HeaderBG, LogoPath fields on Email struct

internal/email/
  email.go               # Options gains HeaderBG, LogoPath; assembleMessage
                         # gains logoPart param
  header.go              # NEW: hexToRGB, relativeLuminance,
                         # computeHeaderStyle, hex validation helpers
  render.go              # (or wherever templates parse) — load _header partial,
                         # wire Header struct into both renderers
  user_show.go           # populate Header on the data struct passed to template
  vulns_summary.go       # populate Header on the data struct passed to template
  templates/
    _header.html.tmpl    # NEW: shared header partial, defines "header"
    vulns_summary.html.tmpl  # use {{template "header" .}}, drop dark-mode <style>
    user_show.html.tmpl      # use {{template "header" .}}, drop dark-mode <style>
```

`internal/email/header.go` is a small, focused unit: hex parsing, luminance, style computation. Pure functions, no I/O, trivial to test.

## Data passed to the header partial

Each renderer's top-level data struct gains a `Header` field:

```go
type Header struct {
    BG       string
    TextFG   string
    BadgeBG  string
    BadgeFG  string
    Badge    string   // e.g. "JELLYFISH / USER"
    Title    string   // e.g. "Vulnerability exposure - Keith Bawden"
    Subtitle string   // e.g. "keith@flipgroup.com.au - 2 devices"
    HasLogo  bool     // true when a logo successfully loaded
}
```

The renderer populates `Badge`, `Title`, `Subtitle` per command (existing strings, just moved out of the template literals). `BG`, `TextFG`, `BadgeBG`, `BadgeFG` come from `computeHeaderStyle`. `HasLogo` is set after the logo load attempt resolves.

## Tests

### Unit

- `internal/email/header_test.go`
  - `hexToRGB`: valid `#RRGGBB`, lowercase + uppercase, error on bad input (no hash, wrong length, non-hex chars).
  - `relativeLuminance`: known values — `#FFFFFF` → 1.0, `#000000` → 0.0, `#7F7F7F` ≈ 0.21.
  - `computeHeaderStyle`: each of the three brand colours produces the expected text + badge pair; `#FFFFFF` produces dark text; `#000000` produces light text.

### Template / golden

- `internal/email/vulns_summary_test.go` + `user_show_test.go` get new golden files for:
  - default colour, no logo
  - default colour, with logo (PNG fixture in `testdata/`)
  - `#C6B8FE`, with logo (verifies dark-text branch)
  - `#6846D8`, no logo (verifies light-text branch on dark bg)

Existing body-content golden assertions stay valid (only the header band changes).

### MIME

- `internal/email/email_test.go`
  - No-logo path: assembled message is `multipart/alternative` with text + html parts (existing assertion).
  - With-logo path: outer `multipart/related`, inner `multipart/alternative`, image part has `Content-ID: <jf-logo>`, `Content-Disposition: inline`, base64-encoded body, base64 wraps at 76 cols.
  - Boundary nesting: outer boundary differs from inner; both injectable via Options.

### Logo loading

- `internal/email/logo_test.go` (new) — loadLogo helper covering: file ok, file missing, file unreadable (chmod 000), bad PNG bytes, file > 512 KB. Returns `(*logoPart, error)`; renderers translate the error to the stderr warn-and-skip behaviour.

### `configure email`

- `cmd/configure_test.go`
  - Header colour prompt: Enter keeps current; `-` clears; valid hex writes; invalid hex re-prompts and eventually errors after `configureEmailMaxAttempts`.
  - Logo prompt: valid PNG copies to `<configDir>/logos/<basename>` with mode 0o600; overwrites existing logo at same path; `-` clears config + deletes managed file (but not files outside the managed dir); invalid PNG re-prompts.
  - Test uses a `t.TempDir()` for the config dir; PNG fixture from `testdata/`.

### End-to-end

- `cmd/send_email_test.go`: when `email.logo_path` is configured, the Gmail send path receives a `multipart/related` message. Use the existing in-process Gmail fake.

## Documentation

README updates:

- "Configure email defaults" section: document the two new prompts and the `<configDir>/logos/` managed location.
- "Email output" section table: add `--email-header-bg` / `email.header_bg` row and `--email-logo` / `email.logo_path` row.
- New short note under "Email output": the default `#7566FF` is the brand purple; logos that rely on the purple geometric mark will lose visibility against it. Recommend either a contrasting colour (`#C6B8FE`) or a logo whose recognisable element is non-purple.

## Migration / backwards compatibility

- Existing config files without `header_bg` / `logo_path` continue to work — both keys default at load time.
- Existing rendered emails (the current dark-blue header) are gone; this is a visual change with no opt-out. The user owns the project, no external surface contract to preserve.
- The `prefers-color-scheme: dark` removal is the only behaviour change for users who never touch the new keys. Acceptable given the new branded header is the intended look.

## Out-of-scope follow-ups (note, do not implement)

- Per-template overrides (different colour for vulns vs user). Add later if there's a real need.
- Logo for the plain-text email body. Text emails don't carry images; the plain-text part stays unchanged.
- JPG/SVG support. Add when a concrete asset needs it.
- A `--no-email-logo` flag to suppress a configured logo for one run. Easy to add later; YAGNI for now.
