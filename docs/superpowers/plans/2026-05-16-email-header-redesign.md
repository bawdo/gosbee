# Email Header Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hard-coded dark-blue header on both HTML email templates with a configurable background colour (default `#7566FF`) and an optional PNG logo, with the logo asset copied into `<configDir>/logos/` when set via `jellyfish configure email`.

**Architecture:** A new pure-function helper in `internal/email/header.go` validates hex colours and derives readable text/badge styles from background luminance. A new `logo.go` helper reads + validates PNGs and returns an inline-attachment record. `assembleMessage` gains an optional `*logoPart` argument; when present it wraps the existing `multipart/alternative` envelope in `multipart/related` and emits the PNG as an inline part referenced via `<img src="cid:jf-logo">`. Both renderers share a `_header.html.tmpl` partial. Flags `--email-header-bg` and `--email-logo` on `vulns summary` and `user show` override the corresponding config keys; `configure email` adds matching prompts (logos are copied into `<configDir>/logos/` and the managed path is persisted).

**Tech Stack:** Go 1.21+, `html/template`, `text/template`, `image/png` (stdlib), `gopkg.in/yaml.v3`, Cobra, existing in-tree `internal/email`, `internal/config`, `internal/keychain`, `internal/gmail`.

**Spec:** [docs/superpowers/specs/2026-05-16-email-header-redesign-design.md](../specs/2026-05-16-email-header-redesign-design.md).

---

## File map

**Create:**
- `internal/email/header.go` — hex parsing, WCAG luminance, `computeHeaderStyle`, `ValidateHexColour`.
- `internal/email/header_test.go`
- `internal/email/logo.go` — `loadLogo` reads + validates a PNG, returns `*logoPart`.
- `internal/email/logo_test.go`
- `internal/email/templates/_header.html.tmpl` — shared header partial defining the `"header"` template.
- `internal/email/testdata/logo_small.png` — 200x100 fixture (copy of `flip-logo-reversed-200x100.png`).
- `internal/email/testdata/logo_too_big.png` — synthetic > 512 KB PNG.

**Modify:**
- `internal/email/email.go` — `Options{HeaderBG, LogoPath, RelatedBoundaryOverride}`; `assembleMessage` gains `logoPart` param.
- `internal/email/vulns_summary.go` — build `Header` struct, call `loadLogo`, pass through to `assembleMessage`. Parse `_header.html.tmpl` alongside `vulns_summary.html.tmpl`.
- `internal/email/user_show.go` — same as `vulns_summary.go`.
- `internal/email/templates/vulns_summary.html.tmpl` — replace inline header markup with `{{template "header" .}}`; remove `<style>` block.
- `internal/email/templates/user_show.html.tmpl` — same as above.
- `internal/email/email_test.go` — extend `assembleMessage` tests for `multipart/related`.
- `internal/email/vulns_summary_test.go` — golden header assertions for three colours × with/without logo.
- `internal/email/user_show_test.go` — same.
- `internal/config/config.go` — add `HeaderBG`, `LogoPath` fields on `EmailConfig`.
- `internal/config/config_test.go` — load/save round-trip for the two new keys.
- `cmd/email.go` — `emailFlagValues{HeaderBG, LogoPath}`; `readEmailFlags` reads them; `resolveEmailOptions` writes them through with config fallback.
- `cmd/vulns.go` — register `--email-header-bg` and `--email-logo` on `vulns summary`.
- `cmd/user.go` — register `--email-header-bg` and `--email-logo` on `user show`.
- `cmd/vulns_test.go` — flag-to-Options assertions.
- `cmd/user_test.go` — flag-to-Options assertions.
- `cmd/configure.go` — append `promptHeaderBG` and `promptLogo` to `runConfigureEmail`; new `configureEmailOpts.LogosDir` for test injection.
- `cmd/configure_test.go` — cases for the new prompts.
- `cmd/send_email_test.go` — assert `multipart/related` end-to-end when logo configured.
- `README.md` — flag table, config keys, `configure email` flow, default-colour caveat.

---

## Task 1: Header style helper (pure functions)

**Files:**
- Create: `internal/email/header.go`
- Create: `internal/email/header_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/email/header_test.go`:

```go
package email

import (
	"math"
	"testing"
)

func TestValidateHexColourAccepts(t *testing.T) {
	cases := []string{"#7566FF", "#c6b8fe", "#000000", "#FFFFFF"}
	for _, c := range cases {
		if err := ValidateHexColour(c); err != nil {
			t.Errorf("ValidateHexColour(%q) unexpected error: %v", c, err)
		}
	}
}

func TestValidateHexColourRejects(t *testing.T) {
	cases := []string{"", "7566FF", "#7566F", "#7566FFA", "#ZZZZZZ", "purple"}
	for _, c := range cases {
		if err := ValidateHexColour(c); err == nil {
			t.Errorf("ValidateHexColour(%q): expected error, got nil", c)
		}
	}
}

func TestHexToRGB(t *testing.T) {
	r, g, b, err := hexToRGB("#7566FF")
	if err != nil {
		t.Fatalf("hexToRGB: %v", err)
	}
	if r != 0x75 || g != 0x66 || b != 0xFF {
		t.Errorf("got (%d,%d,%d) want (117,102,255)", r, g, b)
	}
}

func TestRelativeLuminanceBlackWhite(t *testing.T) {
	if got := relativeLuminance(0, 0, 0); math.Abs(got) > 1e-9 {
		t.Errorf("black: got %v want 0", got)
	}
	if got := relativeLuminance(255, 255, 255); math.Abs(got-1.0) > 1e-9 {
		t.Errorf("white: got %v want 1", got)
	}
}

func TestComputeHeaderStylePalette(t *testing.T) {
	cases := []struct {
		bg     string
		light  bool // true => expect dark text branch
	}{
		{"#7566FF", false},
		{"#C6B8FE", true},
		{"#6846D8", false},
		{"#FFFFFF", true},
		{"#000000", false},
	}
	for _, c := range cases {
		got := computeHeaderStyle(c.bg)
		if got.BG != c.bg {
			t.Errorf("%s: BG echoed %q", c.bg, got.BG)
		}
		darkText := got.TextFG == "#0f172a"
		if c.light && !darkText {
			t.Errorf("%s: expected dark text (light bg), got %+v", c.bg, got)
		}
		if !c.light && darkText {
			t.Errorf("%s: expected light text (dark bg), got %+v", c.bg, got)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/email/ -run 'TestValidateHexColour|TestHexToRGB|TestRelativeLuminance|TestComputeHeaderStyle' -count=1`
Expected: build failure with `undefined: ValidateHexColour, hexToRGB, relativeLuminance, computeHeaderStyle`.

- [ ] **Step 3: Implement `internal/email/header.go`**

```go
package email

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
)

// headerStyle is the resolved set of colours the header partial renders with.
type headerStyle struct {
	BG      string
	TextFG  string
	BadgeBG string
	BadgeFG string
}

var hexColourRe = regexp.MustCompile(`^#[0-9A-Fa-f]{6}$`)

// ValidateHexColour returns an error iff value is not a #RRGGBB hex string.
// Called from config-load and flag-parse so bad input fails early.
func ValidateHexColour(value string) error {
	if !hexColourRe.MatchString(value) {
		return fmt.Errorf("invalid hex colour %q (want #RRGGBB)", value)
	}
	return nil
}

// hexToRGB parses #RRGGBB into 0..255 components.
func hexToRGB(value string) (uint8, uint8, uint8, error) {
	if err := ValidateHexColour(value); err != nil {
		return 0, 0, 0, err
	}
	r, _ := strconv.ParseUint(value[1:3], 16, 8)
	g, _ := strconv.ParseUint(value[3:5], 16, 8)
	b, _ := strconv.ParseUint(value[5:7], 16, 8)
	return uint8(r), uint8(g), uint8(b), nil
}

// relativeLuminance applies the WCAG 2.1 sRGB linearisation and weighted sum.
func relativeLuminance(r, g, b uint8) float64 {
	return 0.2126*linearise(r) + 0.7152*linearise(g) + 0.0722*linearise(b)
}

func linearise(c uint8) float64 {
	f := float64(c) / 255.0
	if f <= 0.03928 {
		return f / 12.92
	}
	return math.Pow((f+0.055)/1.055, 2.4)
}

// computeHeaderStyle picks dark or light text based on background luminance.
// Bad input (rejected by ValidateHexColour upstream) falls back to dark text.
func computeHeaderStyle(bg string) headerStyle {
	r, g, b, err := hexToRGB(bg)
	if err != nil {
		return headerStyle{BG: bg, TextFG: "#0f172a", BadgeBG: "rgba(15,23,42,0.10)", BadgeFG: "#0f172a"}
	}
	if relativeLuminance(r, g, b) > 0.5 {
		return headerStyle{BG: bg, TextFG: "#0f172a", BadgeBG: "rgba(15,23,42,0.10)", BadgeFG: "#0f172a"}
	}
	return headerStyle{BG: bg, TextFG: "#f8fafc", BadgeBG: "rgba(255,255,255,0.18)", BadgeFG: "#f8fafc"}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/email/ -run 'TestValidateHexColour|TestHexToRGB|TestRelativeLuminance|TestComputeHeaderStyle' -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/email/header.go internal/email/header_test.go
git commit -m "feat(email): added header colour validation and luminance helpers"
```

---

## Task 2: Config struct fields

**Files:**
- Modify: `internal/config/config.go:16-23`
- Modify: `internal/config/config_test.go` (add new round-trip cases)

- [ ] **Step 1: Locate and read the existing test file**

Run: `ls internal/config/config_test.go`
Then read it: `cat internal/config/config_test.go | head -80`
Expected: a small file exercising `Load`, `Save`, `BuildBaseURL`. New cases will be appended to it.

- [ ] **Step 2: Write the failing test (append to `internal/config/config_test.go`)**

```go
func TestEmailConfigRoundTripIncludesHeaderBGAndLogoPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	in := config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{
			From:      "alice@example.com",
			HeaderBG:  "#C6B8FE",
			LogoPath:  "/Users/keith/.config/jellyfish/logos/flip-logo.png",
		},
	}}
	if err := config.Save(path, in); err != nil {
		t.Fatalf("Save: %v", err)
	}
	out, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := out["default"].Email
	if got.HeaderBG != "#C6B8FE" {
		t.Errorf("HeaderBG: got %q want #C6B8FE", got.HeaderBG)
	}
	if got.LogoPath != "/Users/keith/.config/jellyfish/logos/flip-logo.png" {
		t.Errorf("LogoPath: got %q", got.LogoPath)
	}
}
```

If `path/filepath` is not already imported in the file, add it.

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestEmailConfigRoundTripIncludesHeaderBGAndLogoPath -count=1`
Expected: build failure with `unknown field HeaderBG / LogoPath in struct literal of type config.EmailConfig`.

- [ ] **Step 4: Add the two fields to `EmailConfig` in `internal/config/config.go`**

Replace lines 13-23 with:

```go
// EmailConfig holds optional defaults for the -o email output. Every field
// is optional. Flags override these values; if both are empty the renderer
// falls back to built-in defaults (or, for "from", git user.email).
type EmailConfig struct {
	From             string `yaml:"from,omitempty"`
	DefaultTo        string `yaml:"default_to,omitempty"`
	SubjectTemplate  string `yaml:"subject_template,omitempty"`
	CVELinkPrimary   string `yaml:"cve_link_primary,omitempty"`
	CVELinkSecondary string `yaml:"cve_link_secondary,omitempty"`
	HeaderBG         string `yaml:"header_bg,omitempty"`
	LogoPath         string `yaml:"logo_path,omitempty"`
	GmailConfigured  bool   `yaml:"gmail_configured,omitempty"`
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/config/ -run TestEmailConfigRoundTripIncludesHeaderBGAndLogoPath -count=1`
Expected: PASS.

- [ ] **Step 6: Run the whole config package**

Run: `go test ./internal/config/ -count=1`
Expected: PASS (no regressions in other tests).

- [ ] **Step 7: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go
git commit -m "feat(config): added email header_bg and logo_path keys"
```

---

## Task 3: Logo loader

**Files:**
- Create: `internal/email/logo.go`
- Create: `internal/email/logo_test.go`
- Create: `internal/email/testdata/logo_small.png` (copy of scratch fixture)
- Create: `internal/email/testdata/logo_too_big.png` (synthetic > 512 KB)

- [ ] **Step 1: Copy a small valid PNG into testdata**

```bash
mkdir -p internal/email/testdata
cp /Users/bawdo/working/scratch/flip-logo-reversed-200x100.png internal/email/testdata/logo_small.png
ls -la internal/email/testdata/
```

Expected: `logo_small.png` exists, around 2.4 KB.

- [ ] **Step 2: Generate a > 512 KB PNG fixture**

Write a tiny throwaway helper to produce a PNG larger than 512 KB. Inline `go run` works:

```bash
go run - <<'EOF'
package main

import (
	"image"
	"image/color"
	"image/png"
	"os"
)

func main() {
	// 2000x2000 image with random-ish colour pattern -> ~ several MB encoded.
	img := image.NewRGBA(image.Rect(0, 0, 2000, 2000))
	for y := 0; y < 2000; y++ {
		for x := 0; x < 2000; x++ {
			img.Set(x, y, color.RGBA{uint8(x), uint8(y), uint8(x ^ y), 255})
		}
	}
	f, err := os.Create("internal/email/testdata/logo_too_big.png")
	if err != nil { panic(err) }
	defer f.Close()
	if err := png.Encode(f, img); err != nil { panic(err) }
}
EOF
ls -l internal/email/testdata/logo_too_big.png
```

Expected: file size > 524288 bytes (the 512 KB limit).

- [ ] **Step 3: Write the failing test**

Create `internal/email/logo_test.go`:

```go
package email

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadLogoSuccess(t *testing.T) {
	p, err := loadLogo("testdata/logo_small.png")
	if err != nil {
		t.Fatalf("loadLogo: %v", err)
	}
	if p == nil {
		t.Fatal("loadLogo: nil part")
	}
	if p.CID != "jf-logo" {
		t.Errorf("CID: got %q want jf-logo", p.CID)
	}
	if p.Name != "logo_small.png" {
		t.Errorf("Name: got %q want logo_small.png", p.Name)
	}
	if len(p.Bytes) < 100 {
		t.Errorf("Bytes: got %d bytes, suspiciously small", len(p.Bytes))
	}
}

func TestLoadLogoMissing(t *testing.T) {
	_, err := loadLogo(filepath.Join(t.TempDir(), "nope.png"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadLogoNotPNG(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "not_png.png")
	if err := os.WriteFile(path, []byte("this is not a png"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	_, err := loadLogo(path)
	if err == nil {
		t.Fatal("expected error for non-PNG bytes")
	}
}

func TestLoadLogoTooBig(t *testing.T) {
	_, err := loadLogo("testdata/logo_too_big.png")
	if err == nil {
		t.Fatal("expected error for oversize PNG")
	}
}

func TestLoadLogoEmptyPathReturnsNilNil(t *testing.T) {
	p, err := loadLogo("")
	if err != nil {
		t.Fatalf("loadLogo(\"\"): unexpected error %v", err)
	}
	if p != nil {
		t.Fatalf("loadLogo(\"\"): expected nil, got %+v", p)
	}
}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `go test ./internal/email/ -run 'TestLoadLogo' -count=1`
Expected: build failure (`undefined: loadLogo, logoPart`).

- [ ] **Step 5: Implement `internal/email/logo.go`**

```go
package email

import (
	"bytes"
	"errors"
	"fmt"
	"image/png"
	"os"
	"path/filepath"
)

// logoPart is the inline image attachment included in the multipart/related
// envelope when a logo is configured.
type logoPart struct {
	Bytes []byte
	Name  string
	CID   string
}

// MaxLogoBytes caps the on-disk PNG size accepted by loadLogo. 512 KiB is
// generous for a header logo and well under any mail-client size limit.
const MaxLogoBytes = 512 * 1024

// loadLogo reads path, validates it as a PNG no larger than MaxLogoBytes,
// and returns a *logoPart. An empty path returns (nil, nil): callers treat
// that as "no logo configured", not an error.
func loadLogo(path string) (*logoPart, error) {
	if path == "" {
		return nil, nil
	}
	// #nosec G304 - path is the operator's own config or flag input
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	if info.Size() > MaxLogoBytes {
		return nil, fmt.Errorf("logo %s is %d bytes (max %d)", path, info.Size(), MaxLogoBytes)
	}
	// #nosec G304 - see above
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if _, err := png.Decode(bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("decode PNG %s: %w", path, err)
	}
	return &logoPart{
		Bytes: data,
		Name:  filepath.Base(path),
		CID:   "jf-logo",
	}, nil
}

// errLogoNotConfigured is returned only when callers want to distinguish
// "no logo configured" from a hard load error. loadLogo itself returns
// (nil, nil) in that case; this sentinel is reserved for future use.
var errLogoNotConfigured = errors.New("no logo configured")
```

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./internal/email/ -run 'TestLoadLogo' -count=1`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/email/logo.go internal/email/logo_test.go internal/email/testdata/logo_small.png internal/email/testdata/logo_too_big.png
git commit -m "feat(email): added PNG logo loader with size and format validation"
```

---

## Task 4: MIME wrapper — multipart/related when logo present

**Files:**
- Modify: `internal/email/email.go` (Options struct, assembleMessage signature + body)
- Modify: `internal/email/email_test.go` (extend with logo cases)

- [ ] **Step 1: Write the failing test (append to `internal/email/email_test.go`)**

```go
func TestAssembleMessageWithLogoEmitsMultipartRelated(t *testing.T) {
	hdr := messageHeaders{
		From: "alice@example.com", To: "bob@example.com",
		Subject: "x", Date: time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC),
	}
	logo := &logoPart{
		Bytes: []byte("\x89PNG\r\n\x1a\n" + strings.Repeat("a", 100)), // not a real PNG, but assembleMessage doesn't decode
		Name:  "logo.png",
		CID:   "jf-logo",
	}
	out, err := assembleMessage(hdr,
		"<html>hi</html>", "hi plain",
		"=_jf_INNER", "<id@example.com>",
		"=_jfr_OUTER", logo,
	)
	if err != nil {
		t.Fatalf("assembleMessage: %v", err)
	}
	msg, err := mail.ReadMessage(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("parse: %v\nraw:\n%s", err, out)
	}
	mediaType, params, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("Content-Type parse: %v", err)
	}
	if mediaType != "multipart/related" {
		t.Fatalf("outer media type: got %q want multipart/related", mediaType)
	}
	if params["boundary"] != "=_jfr_OUTER" {
		t.Errorf("outer boundary: got %q", params["boundary"])
	}
	if params["type"] != "multipart/alternative" {
		t.Errorf("outer type param: got %q", params["type"])
	}

	mr := multipart.NewReader(msg.Body, params["boundary"])
	first, err := mr.NextPart()
	if err != nil {
		t.Fatalf("first part: %v", err)
	}
	if ct := first.Header.Get("Content-Type"); !strings.HasPrefix(ct, "multipart/alternative") {
		t.Errorf("first part type: got %q", ct)
	}
	second, err := mr.NextPart()
	if err != nil {
		t.Fatalf("second part: %v", err)
	}
	if ct := second.Header.Get("Content-Type"); ct != "image/png" {
		t.Errorf("second part type: got %q want image/png", ct)
	}
	if cid := second.Header.Get("Content-ID"); cid != "<jf-logo>" {
		t.Errorf("Content-ID: got %q want <jf-logo>", cid)
	}
	if cd := second.Header.Get("Content-Disposition"); !strings.Contains(cd, "inline") || !strings.Contains(cd, "logo.png") {
		t.Errorf("Content-Disposition: got %q", cd)
	}
	if cte := second.Header.Get("Content-Transfer-Encoding"); cte != "base64" {
		t.Errorf("Content-Transfer-Encoding: got %q want base64", cte)
	}
}

func TestAssembleMessageNoLogoEmitsMultipartAlternative(t *testing.T) {
	hdr := messageHeaders{
		From: "a@example.com", To: "b@example.com",
		Subject: "x", Date: time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
	}
	out, err := assembleMessage(hdr, "<html>x</html>", "x",
		"=_jf_FIXED", "<id@example.com>",
		"", nil,
	)
	if err != nil {
		t.Fatalf("assembleMessage: %v", err)
	}
	msg, err := mail.ReadMessage(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("parse: %v\nraw:\n%s", err, out)
	}
	mediaType, _, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("Content-Type parse: %v", err)
	}
	if mediaType != "multipart/alternative" {
		t.Errorf("no-logo path: got %q want multipart/alternative", mediaType)
	}
}
```

Update the existing `TestAssembleMessageHeadersAndStructure` call site to pass the two new args (`"", nil`):

```go
out, err := assembleMessage(hdr, "<html><body>hi</body></html>", "hello plain text\n",
	"=_jf_FIXEDBOUNDARY", "<fixed-id@example.com>",
	"", nil,
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/email/ -run 'TestAssembleMessage' -count=1`
Expected: build failure — `assembleMessage` takes too few args.

- [ ] **Step 3: Extend `Options` in `internal/email/email.go`**

Edit lines 23-38 (`type Options struct`). Add three fields just before `BoundaryOverride`:

```go
	GeneratedAt time.Time // pinned by tests; cmd layer passes time.Now()
	Tenant      string    // shown in masthead, sourced from config.Profile.Subdomain

	HeaderBG string // hex #RRGGBB; renderer applies DefaultHeaderBG if empty
	LogoPath string // optional path to a PNG; empty disables the logo

	// Injected for tests; production leaves these zero so assembleMessage
	// pulls from crypto/rand.
	BoundaryOverride        string
	RelatedBoundaryOverride string
	MessageIDOverride       string
```

Just above the const block (around line 14-18), add:

```go
const (
	DefaultCVELinkPrimary   = "https://nvd.nist.gov/vuln/detail/{cve}"
	DefaultCVELinkSecondary = "https://www.cve.org/CVERecord?id={cve}"
	DefaultHeaderBG         = "#7566FF"
)
```

Extend `withDefaults`:

```go
func (o Options) withDefaults() Options {
	if o.CVELinkPrimary == "" {
		o.CVELinkPrimary = DefaultCVELinkPrimary
	}
	if o.CVELinkSecondary == "" {
		o.CVELinkSecondary = DefaultCVELinkSecondary
	}
	if o.HeaderBG == "" {
		o.HeaderBG = DefaultHeaderBG
	}
	if o.GeneratedAt.IsZero() {
		o.GeneratedAt = time.Now()
	}
	return o
}
```

- [ ] **Step 4: Update `assembleMessage` signature and body**

Replace the entire `assembleMessage` function (lines 77-136) with:

```go
// assembleMessage produces a full RFC 5322 message. When logo is nil, the
// result is multipart/alternative carrying the text and HTML bodies. When
// logo is non-nil, that multipart/alternative is wrapped in a
// multipart/related envelope and the logo bytes are emitted as an inline
// image part referenced by Content-ID.
//
// innerBoundary and outerBoundary are caller-supplied for test determinism;
// outerBoundary is ignored when logo is nil.
func assembleMessage(
	h messageHeaders,
	htmlBody, textBody string,
	innerBoundary, messageID string,
	outerBoundary string,
	logo *logoPart,
) ([]byte, error) {
	var sb strings.Builder
	writeHeader := func(name, value string) {
		sb.WriteString(name)
		sb.WriteString(": ")
		sb.WriteString(sanitiseHeaderValue(value))
		sb.WriteString("\r\n")
	}

	to := h.To
	if to == "" {
		to = "<unspecified>"
	}

	writeHeader("From", h.From)
	writeHeader("To", to)
	writeHeader("Subject", h.Subject)
	writeHeader("Date", h.Date.Format(time.RFC1123Z))
	writeHeader("Message-ID", messageID)
	writeHeader("MIME-Version", "1.0")
	if logo == nil {
		writeHeader("Content-Type", fmt.Sprintf("multipart/alternative; boundary=%q", sanitiseHeaderValue(innerBoundary)))
	} else {
		writeHeader("Content-Type", fmt.Sprintf("multipart/related; type=%q; boundary=%q",
			"multipart/alternative", sanitiseHeaderValue(outerBoundary)))
	}
	sb.WriteString("\r\n")

	if logo != nil {
		sb.WriteString("--")
		sb.WriteString(outerBoundary)
		sb.WriteString("\r\n")
		sb.WriteString("Content-Type: ")
		sb.WriteString(fmt.Sprintf("multipart/alternative; boundary=%q", sanitiseHeaderValue(innerBoundary)))
		sb.WriteString("\r\n\r\n")
	}

	writePart := func(contentType, body string) error {
		sb.WriteString("--")
		sb.WriteString(innerBoundary)
		sb.WriteString("\r\n")
		sb.WriteString("Content-Type: ")
		sb.WriteString(contentType)
		sb.WriteString("\r\n")
		sb.WriteString("Content-Transfer-Encoding: quoted-printable\r\n\r\n")

		encoded, err := quotedPrintableEncode(body)
		if err != nil {
			return err
		}
		sb.WriteString(encoded)
		if !strings.HasSuffix(encoded, "\r\n") {
			sb.WriteString("\r\n")
		}
		return nil
	}

	if err := writePart("text/plain; charset=UTF-8", textBody); err != nil {
		return nil, err
	}
	if err := writePart("text/html; charset=UTF-8", htmlBody); err != nil {
		return nil, err
	}
	sb.WriteString("--")
	sb.WriteString(innerBoundary)
	sb.WriteString("--\r\n")

	if logo != nil {
		sb.WriteString("--")
		sb.WriteString(outerBoundary)
		sb.WriteString("\r\n")
		sb.WriteString("Content-Type: image/png\r\n")
		sb.WriteString("Content-Transfer-Encoding: base64\r\n")
		sb.WriteString(fmt.Sprintf("Content-ID: <%s>\r\n", logo.CID))
		sb.WriteString(fmt.Sprintf("Content-Disposition: inline; filename=%q\r\n\r\n", logo.Name))
		sb.WriteString(base64Wrap(logo.Bytes, 76))
		sb.WriteString("\r\n--")
		sb.WriteString(outerBoundary)
		sb.WriteString("--\r\n")
	}

	return []byte(sb.String()), nil
}

// base64Wrap returns the base64 encoding of data with CRLF inserted every
// lineWidth output chars (RFC 2045 line-length).
func base64Wrap(data []byte, lineWidth int) string {
	enc := base64.StdEncoding.EncodeToString(data)
	var sb strings.Builder
	for i := 0; i < len(enc); i += lineWidth {
		end := i + lineWidth
		if end > len(enc) {
			end = len(enc)
		}
		sb.WriteString(enc[i:end])
		sb.WriteString("\r\n")
	}
	return strings.TrimSuffix(sb.String(), "\r\n")
}
```

Add `"encoding/base64"` to the imports.

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/email/ -run 'TestAssembleMessage' -count=1`
Expected: build still fails — renderers in `vulns_summary.go` and `user_show.go` call `assembleMessage` with the old signature.

- [ ] **Step 6: Update both renderer call sites to the new signature**

In `internal/email/vulns_summary.go`, find the `assembleMessage` call (around line 206) and update:

```go
bytesOut, err := assembleMessage(messageHeaders{
	From:    r.opts.From,
	To:      r.opts.To,
	Subject: subject,
	Date:    r.opts.GeneratedAt,
}, htmlBody, textBody, boundary, messageID, "", nil)
```

Same edit in `internal/email/user_show.go` (around line 206).

- [ ] **Step 7: Run test again**

Run: `go test ./internal/email/ -count=1`
Expected: PASS (all email tests, including new and old).

- [ ] **Step 8: Commit**

```bash
git add internal/email/email.go internal/email/email_test.go internal/email/vulns_summary.go internal/email/user_show.go
git commit -m "feat(email): wrapped message in multipart/related when logo present"
```

---

## Task 5: Shared header partial and template rewires

**Files:**
- Create: `internal/email/templates/_header.html.tmpl`
- Modify: `internal/email/templates/vulns_summary.html.tmpl` (replace header block, drop dark-mode `<style>`)
- Modify: `internal/email/templates/user_show.html.tmpl` (same)
- Modify: `internal/email/vulns_summary.go` (embed + parse the partial; supply `Header` field)
- Modify: `internal/email/user_show.go` (same)

- [ ] **Step 1: Write the failing test (append to `internal/email/vulns_summary_test.go`)**

```go
func TestVulnSummaryHTMLHeaderColoursAndLogo(t *testing.T) {
	cases := []struct {
		name      string
		bg        string
		logoPath  string
		wantText  string // a substring that proves the right text-colour branch
		wantLogo  bool
	}{
		{"default no-logo", "", "", "color:#f8fafc", false},
		{"lavender no-logo", "#C6B8FE", "", "color:#0f172a", false},
		{"deep with logo", "#6846D8", "testdata/logo_small.png", "color:#f8fafc", true},
		{"lavender with logo", "#C6B8FE", "testdata/logo_small.png", "color:#0f172a", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{
				From:        "alice@example.com",
				HeaderBG:    tc.bg,
				LogoPath:    tc.logoPath,
				GeneratedAt: time.Date(2026, 5, 16, 18, 42, 0, 0, time.UTC),
			}.withDefaults()
			view := buildVulnSummaryView(nil, opts)
			view.Header = buildHeader("JELLYFISH / VULNS", "Fleet vulnerability summary",
				"2026-05-16 18:42 - 0 CVEs", opts.HeaderBG, opts.LogoPath != "")
			html, err := renderVulnSummaryHTML(view)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			if !strings.Contains(html, tc.wantText) {
				t.Errorf("expected text-colour substring %q in html", tc.wantText)
			}
			hasCID := strings.Contains(html, `src="cid:jf-logo"`)
			if hasCID != tc.wantLogo {
				t.Errorf("logo presence: got %v want %v", hasCID, tc.wantLogo)
			}
			if strings.Contains(html, "prefers-color-scheme") {
				t.Errorf("dark-mode media query should be removed")
			}
		})
	}
}
```

Add `"strings"` and `"time"` to imports if not already present.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/email/ -run TestVulnSummaryHTMLHeaderColoursAndLogo -count=1`
Expected: build failure — `view.Header undefined`, `buildHeader undefined`.

- [ ] **Step 3: Create the partial `internal/email/templates/_header.html.tmpl`**

```html
{{define "header"}}
<tr><td bgcolor="{{.Header.BG}}" style="background:{{.Header.BG}};color:{{.Header.TextFG}};padding:18px 24px;">
  <table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;"><tr>
    {{- if .Header.HasLogo}}
    <td valign="middle" style="padding-right:16px;">
      <img src="cid:jf-logo" alt="" height="56" style="display:block;max-height:56px;height:56px;width:auto;border:0;outline:none;text-decoration:none;">
    </td>
    {{- end}}
    <td valign="middle">
      <div style="font:700 10px/1 'SF Mono','Menlo','Consolas',monospace;letter-spacing:.14em;padding:4px 7px;background:{{.Header.BadgeBG}};color:{{.Header.BadgeFG}};border-radius:3px;display:inline-block;">{{.Header.Badge}}</div>
      <div style="font:700 20px/1.2 -apple-system,system-ui,sans-serif;margin:10px 0 4px;color:{{.Header.TextFG}};">{{.Header.Title}}</div>
      <div style="font:12px/1.4 -apple-system,system-ui,sans-serif;opacity:.7;color:{{.Header.TextFG}};">{{.Header.Subtitle}}</div>
    </td>
  </tr></table>
</td></tr>
{{end}}
```

- [ ] **Step 4: Rewire `internal/email/templates/vulns_summary.html.tmpl`**

Replace the file's first `<head>...<style>...</style></head>` block AND the existing dark-blue `<tr><td bgcolor="#0f172a">...</td></tr>` header row with a call to the partial. The final file should look like (just the parts that change):

```html
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vulnerability summary</title>
</head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#0f172a;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:20px 0;">
<tr><td align="center">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;width:600px;max-width:600px;">

{{template "header" .}}

<tr><td style="padding:0;">
...
```

Everything from the stats-strip row downward (currently lines ~24-110) stays as-is. Note the removal of the `@media (prefers-color-scheme: dark)` rule.

- [ ] **Step 5: Rewire `internal/email/templates/user_show.html.tmpl`**

Same change: drop the `<style>` block, replace the inline header row with `{{template "header" .}}`. Everything below the header (stats strip, per-device tables, footer) stays as-is.

- [ ] **Step 6: Add `Header` type and `buildHeader` helper in `internal/email/header.go`**

Append to `internal/email/header.go`:

```go
// Header is the data the shared header partial renders against. The
// per-renderer view struct must expose this on its top-level `Header` field.
type Header struct {
	BG       string
	TextFG   string
	BadgeBG  string
	BadgeFG  string
	Badge    string
	Title    string
	Subtitle string
	HasLogo  bool
}

// buildHeader composes a Header from per-command strings plus the chosen
// background colour and a hasLogo flag.
func buildHeader(badge, title, subtitle, bg string, hasLogo bool) Header {
	s := computeHeaderStyle(bg)
	return Header{
		BG: s.BG, TextFG: s.TextFG, BadgeBG: s.BadgeBG, BadgeFG: s.BadgeFG,
		Badge: badge, Title: title, Subtitle: subtitle, HasLogo: hasLogo,
	}
}
```

- [ ] **Step 7: Wire `Header` into `vulnSummaryView` and parse the partial**

In `internal/email/vulns_summary.go`:

Add `Header` field to the struct (around line 22):

```go
type vulnSummaryView struct {
	Header         Header
	Tenant         string
	GeneratedAtStr string
	...
}
```

Extend the embed directive at the top of the file to include the partial:

```go
//go:embed templates/vulns_summary.txt.tmpl templates/vulns_summary.html.tmpl templates/_header.html.tmpl
var vulnSummaryFS embed.FS
```

Update `renderVulnSummaryHTML` to parse both files:

```go
func renderVulnSummaryHTML(v vulnSummaryView) (string, error) {
	tmpl, err := htmltmpl.New("vulns_summary.html.tmpl").Funcs(htmltmpl.FuncMap{
		"sevRowBG":  sevRowBG,
		"sevPillBG": sevPillBG,
		"sevPillFG": sevPillFG,
	}).ParseFS(vulnSummaryFS,
		"templates/_header.html.tmpl",
		"templates/vulns_summary.html.tmpl",
	)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	if err := tmpl.Execute(&sb, v); err != nil {
		return "", err
	}
	return sb.String(), nil
}
```

In the `Render` method, populate `view.Header` before rendering. Add this block right after `view := buildVulnSummaryView(vs, r.opts)`:

```go
subtitle := view.GeneratedAtStr
if view.Tenant != "" {
	subtitle += " - " + view.Tenant
}
subtitle += fmt.Sprintf(" - %d CVEs across %d devices (max per CVE)", view.TotalCVEs, view.DeviceCount)
view.Header = buildHeader(
	"JELLYFISH / VULNS",
	"Fleet vulnerability summary",
	subtitle,
	r.opts.HeaderBG,
	r.opts.LogoPath != "",
)
```

The existing `assembleMessage` call stays as Task 4 left it (`outerBoundary=""`, `logo=nil`). Real logo loading and MIME wrapping are added in Task 10; for now the Header's `HasLogo` is derived purely from whether a path is configured, which is enough to make the template emit the `<img src="cid:jf-logo">` tag for the Task 5 tests. The MIME output stays as `multipart/alternative` regardless — that's an intermediate state that Task 10 corrects before any user-facing claim of completeness.

- [ ] **Step 8: Mirror Step 7 in `internal/email/user_show.go`**

Add `Header` field to `userShowView`, extend `//go:embed`, extend `renderUserShowHTML.ParseFS`, populate `view.Header` before rendering. Subtitle text:

```go
subtitle := r.opts.GeneratedAt.Format("2 Jan 2006 - 15:04 MST")
if bundle.User.Email != "" {
	subtitle = bundle.User.Email + " - " + subtitle
}
if view.Tenant != "" {
	subtitle += " - " + view.Tenant
}
subtitle += fmt.Sprintf(" - %d CVEs across %d device(s)", view.TotalCVEs, view.DeviceCount)
view.Header = buildHeader("JELLYFISH / USER",
	"Vulnerability exposure - "+bundle.User.Name,
	subtitle, r.opts.HeaderBG, r.opts.LogoPath != "",
)
```

As in Step 7, the `assembleMessage` call still passes `""` and `nil` for the new args — Task 10 introduces the real load + multipart/related wiring.

- [ ] **Step 9: Run all email tests**

Run: `go test ./internal/email/ -count=1`
Expected: PASS (existing tests still green, the new header colours test passes, the new MIME test passes).

- [ ] **Step 10: Run the equivalent user_show test**

Repeat Step 1 in `internal/email/user_show_test.go` with the appropriate fixture (`UserBundleInput{}`) and a call to `buildUserShowView`. Verify it passes.

Test body for `internal/email/user_show_test.go`:

```go
func TestUserShowHTMLHeaderColoursAndLogo(t *testing.T) {
	bundle := UserBundleInput{User: iru.User{Name: "Keith Bawden", Email: "k@example.com"}}
	cases := []struct {
		name      string
		bg        string
		logoPath  string
		wantText  string
		wantLogo  bool
	}{
		{"default no-logo", "", "", "color:#f8fafc", false},
		{"lavender no-logo", "#C6B8FE", "", "color:#0f172a", false},
		{"lavender with logo", "#C6B8FE", "testdata/logo_small.png", "color:#0f172a", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{
				From:        "alice@example.com",
				HeaderBG:    tc.bg,
				LogoPath:    tc.logoPath,
				GeneratedAt: time.Date(2026, 5, 16, 18, 42, 0, 0, time.UTC),
			}.withDefaults()
			view := buildUserShowView(bundle, opts)
			view.Header = buildHeader("JELLYFISH / USER",
				"Vulnerability exposure - "+bundle.User.Name,
				"k@example.com - 0 devices", opts.HeaderBG, opts.LogoPath != "")
			html, err := renderUserShowHTML(view)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			if !strings.Contains(html, tc.wantText) {
				t.Errorf("expected text-colour substring %q in html", tc.wantText)
			}
			hasCID := strings.Contains(html, `src="cid:jf-logo"`)
			if hasCID != tc.wantLogo {
				t.Errorf("logo presence: got %v want %v", hasCID, tc.wantLogo)
			}
		})
	}
}
```

Add the `iru`, `strings`, and `time` imports as needed.

- [ ] **Step 11: Commit**

```bash
git add internal/email/templates/_header.html.tmpl internal/email/templates/vulns_summary.html.tmpl internal/email/templates/user_show.html.tmpl internal/email/header.go internal/email/vulns_summary.go internal/email/user_show.go internal/email/email.go internal/email/vulns_summary_test.go internal/email/user_show_test.go
git commit -m "feat(email): shared header partial with luminance-derived text"
```

---

## Task 6: CLI flags on `vulns summary`

**Files:**
- Modify: `cmd/email.go` (extend `emailFlagValues`, `readEmailFlags`, `resolveEmailOptions`)
- Modify: `cmd/vulns.go` (register `--email-header-bg` and `--email-logo`)
- Modify: `cmd/vulns_test.go` (assert flag-to-options plumbing)

- [ ] **Step 1: Write the failing test (append to `cmd/vulns_test.go`)**

```go
func TestResolveEmailOptionsHeaderBGAndLogoFromFlag(t *testing.T) {
	flags := emailFlagValues{
		From:     "alice@example.com",
		HeaderBG: "#6846D8",
		LogoPath: "/abs/path/logo.png",
	}
	prof := config.Profile{Subdomain: "acme", Email: config.EmailConfig{
		From:     "config-from@example.com",
		HeaderBG: "#C6B8FE",
		LogoPath: "/cfg/path/other.png",
	}}
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	got, err := resolveEmailOptions(flags, prof, func() (string, error) { return "", nil }, now)
	if err != nil {
		t.Fatalf("resolveEmailOptions: %v", err)
	}
	if got.HeaderBG != "#6846D8" {
		t.Errorf("HeaderBG: flag should win, got %q", got.HeaderBG)
	}
	if got.LogoPath != "/abs/path/logo.png" {
		t.Errorf("LogoPath: flag should win, got %q", got.LogoPath)
	}
}

func TestResolveEmailOptionsHeaderBGAndLogoFromConfigWhenFlagsEmpty(t *testing.T) {
	flags := emailFlagValues{From: "alice@example.com"}
	prof := config.Profile{Subdomain: "acme", Email: config.EmailConfig{
		HeaderBG: "#C6B8FE",
		LogoPath: "/cfg/path/other.png",
	}}
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	got, err := resolveEmailOptions(flags, prof, func() (string, error) { return "", nil }, now)
	if err != nil {
		t.Fatalf("resolveEmailOptions: %v", err)
	}
	if got.HeaderBG != "#C6B8FE" {
		t.Errorf("HeaderBG: got %q want #C6B8FE", got.HeaderBG)
	}
	if got.LogoPath != "/cfg/path/other.png" {
		t.Errorf("LogoPath: got %q", got.LogoPath)
	}
}

func TestResolveEmailOptionsRejectsInvalidHeaderBG(t *testing.T) {
	flags := emailFlagValues{From: "alice@example.com", HeaderBG: "purple"}
	prof := config.Profile{Subdomain: "acme"}
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	_, err := resolveEmailOptions(flags, prof, func() (string, error) { return "", nil }, now)
	if err == nil {
		t.Fatal("expected error for invalid hex colour")
	}
}
```

Add the necessary imports (`time`, `config`, and the package's own `email` import already exists).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/ -run 'TestResolveEmailOptionsHeaderBGAndLogo|TestResolveEmailOptionsRejectsInvalidHeaderBG' -count=1`
Expected: build failure — `unknown field HeaderBG / LogoPath` on `emailFlagValues`.

- [ ] **Step 3: Update `emailFlagValues` and `readEmailFlags` in `cmd/email.go`**

```go
type emailFlagValues struct {
	To       string
	From     string
	Subject  string
	HeaderBG string
	LogoPath string
	Send     bool
}

func readEmailFlags(cmd *cobra.Command) emailFlagValues {
	to, _ := cmd.Flags().GetString("email-to")
	from, _ := cmd.Flags().GetString("email-from")
	subject, _ := cmd.Flags().GetString("email-subject")
	headerBG, _ := cmd.Flags().GetString("email-header-bg")
	logoPath, _ := cmd.Flags().GetString("email-logo")
	send, _ := cmd.Flags().GetBool("send-email")
	return emailFlagValues{To: to, From: from, Subject: subject, HeaderBG: headerBG, LogoPath: logoPath, Send: send}
}
```

- [ ] **Step 4: Update `resolveEmailOptions` in `cmd/email.go`**

Inside the function, after the existing `opts := email.Options{...}` block:

```go
opts.HeaderBG = firstNonEmpty(flags.HeaderBG, prof.Email.HeaderBG)
if opts.HeaderBG != "" {
	if err := email.ValidateHexColour(opts.HeaderBG); err != nil {
		return email.Options{}, fmt.Errorf("email header bg: %w", err)
	}
}
opts.LogoPath = firstNonEmpty(flags.LogoPath, prof.Email.LogoPath)
```

- [ ] **Step 5: Register the flags on `vulns summary` in `cmd/vulns.go`**

Insert after the existing `c.Flags().String("email-subject", ...)` line:

```go
c.Flags().String("email-header-bg", "", "Email header background colour as #RRGGBB (default: email.header_bg or #7566FF)")
c.Flags().String("email-logo", "", "Path to a PNG to show in the email header (default: email.logo_path)")
```

- [ ] **Step 6: Run the tests**

Run: `go test ./cmd/ -count=1`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add cmd/email.go cmd/vulns.go cmd/vulns_test.go
git commit -m "feat(vulns): added --email-header-bg and --email-logo flags"
```

---

## Task 7: CLI flags on `user show`

**Files:**
- Modify: `cmd/user.go`
- Modify: `cmd/user_test.go`

- [ ] **Step 1: Write the failing test (append to `cmd/user_test.go`)**

```go
func TestUserShowFlagsIncludeHeaderBGAndLogo(t *testing.T) {
	c := newUserCmd()
	show := findSubcommand(t, c, "show")
	if f := show.Flags().Lookup("email-header-bg"); f == nil {
		t.Fatal("--email-header-bg flag is missing")
	}
	if f := show.Flags().Lookup("email-logo"); f == nil {
		t.Fatal("--email-logo flag is missing")
	}
}

func findSubcommand(t *testing.T, parent *cobra.Command, name string) *cobra.Command {
	t.Helper()
	for _, sub := range parent.Commands() {
		if sub.Name() == name {
			return sub
		}
	}
	t.Fatalf("subcommand %q not found under %s", name, parent.Name())
	return nil
}
```

(If `findSubcommand` already exists in another `cmd/*_test.go`, omit the helper and just use the existing one.)

Add `"github.com/spf13/cobra"` to imports as needed.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/ -run TestUserShowFlagsIncludeHeaderBGAndLogo -count=1`
Expected: FAIL with `--email-header-bg flag is missing`.

- [ ] **Step 3: Register the flags in `cmd/user.go`**

Insert after `c.Flags().String("email-subject", ...)` (line ~94):

```go
c.Flags().String("email-header-bg", "", "Email header background colour as #RRGGBB (default: email.header_bg or #7566FF)")
c.Flags().String("email-logo", "", "Path to a PNG to show in the email header (default: email.logo_path)")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/ -run TestUserShowFlagsIncludeHeaderBGAndLogo -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/user.go cmd/user_test.go
git commit -m "feat(user): added --email-header-bg and --email-logo flags"
```

---

## Task 8: `configure email` — header colour prompt

**Files:**
- Modify: `cmd/configure.go` (extend `runConfigureEmail` with new prompt; add `promptHeaderBG` helper)
- Modify: `cmd/configure_test.go`

- [ ] **Step 1: Write the failing test (append to `cmd/configure_test.go`)**

```go
func TestConfigureEmailPromptHeaderBGValidAndClear(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	// Seed: requires existing default profile (configure email refuses otherwise).
	if err := config.Save(path, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{From: "a@example.com"},
	}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Inputs (one per prompt): from kept, defaultTo kept, gmail kept (none),
	// header_bg = #C6B8FE, logo = "" (Enter to skip — separate task).
	in := strings.NewReader("\n\n\n#C6B8FE\n\n")
	var out, errBuf bytes.Buffer
	if err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: path, Stdin: in, Stdout: &out, Stderr: &errBuf,
	}); err != nil {
		t.Fatalf("runConfigureEmail: %v\nstderr:\n%s", err, errBuf.String())
	}
	file, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if file["default"].Email.HeaderBG != "#C6B8FE" {
		t.Errorf("HeaderBG: got %q want #C6B8FE", file["default"].Email.HeaderBG)
	}
}

func TestConfigureEmailPromptHeaderBGRejectsInvalidThenAccepts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	_ = config.Save(path, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{From: "a@example.com"},
	}})
	// Inputs: from kept, defaultTo kept, gmail kept, bad colour twice, then valid, logo blank.
	in := strings.NewReader("\n\n\npurple\nnotahex\n#7566FF\n\n")
	var out, errBuf bytes.Buffer
	if err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: path, Stdin: in, Stdout: &out, Stderr: &errBuf,
	}); err != nil {
		t.Fatalf("runConfigureEmail: %v", err)
	}
	if !strings.Contains(errBuf.String(), "invalid hex colour") {
		t.Errorf("expected stderr to mention invalid hex; got:\n%s", errBuf.String())
	}
	file, _ := config.Load(path)
	if file["default"].Email.HeaderBG != "#7566FF" {
		t.Errorf("HeaderBG: got %q", file["default"].Email.HeaderBG)
	}
}
```

If imports for `bytes`, `context`, `strings`, `path/filepath`, `config` are missing, add them.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/ -run TestConfigureEmailPromptHeaderBG -count=1`
Expected: FAIL — neither test sees the new prompt, the header_bg is never written.

- [ ] **Step 3: Add `promptHeaderBG` to `cmd/configure.go`**

Append to `cmd/configure.go`:

```go
func promptHeaderBG(stdout, stderr io.Writer, r *bufio.Reader, current string) (string, error) {
	for attempt := 1; attempt <= configureEmailMaxAttempts; attempt++ {
		value, err := promptWithDefault(stdout, r, "Header background colour", current)
		if err != nil {
			return "", err
		}
		if value == "" {
			return "", nil // user cleared
		}
		if vErr := email.ValidateHexColour(value); vErr != nil {
			_, _ = fmt.Fprintln(stderr, vErr)
			continue
		}
		return value, nil
	}
	return "", fmt.Errorf("invalid header background colour after %d attempts", configureEmailMaxAttempts)
}
```

Add the `"flipgroup.com.au/jellyfish/internal/email"` import to the file.

- [ ] **Step 4: Call `promptHeaderBG` from `runConfigureEmail` after `promptGmailJSON`**

Just before the `file["default"] = prof` line (around line 266):

```go
headerBG, err := promptHeaderBG(o.Stdout, o.Stderr, r, prof.Email.HeaderBG)
if err != nil {
	return err
}
prof.Email.HeaderBG = headerBG
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./cmd/ -run TestConfigureEmailPromptHeaderBG -count=1`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add cmd/configure.go cmd/configure_test.go
git commit -m "feat(configure): added header background colour prompt"
```

---

## Task 9: `configure email` — logo prompt + managed-dir copy

**Files:**
- Modify: `cmd/configure.go` (add `promptLogo`, `LogosDir` opts field)
- Modify: `cmd/configure_test.go`

- [ ] **Step 1: Write the failing test (append to `cmd/configure_test.go`)**

```go
func TestConfigureEmailLogoCopiesIntoLogosDir(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yml")
	logosDir := filepath.Join(dir, "logos")
	srcLogo := filepath.Join(dir, "src", "flip.png")
	if err := os.MkdirAll(filepath.Dir(srcLogo), 0o700); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	// Reuse the fixture from internal/email/testdata as a real PNG.
	srcBytes, err := os.ReadFile("../internal/email/testdata/logo_small.png")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if err := os.WriteFile(srcLogo, srcBytes, 0o600); err != nil {
		t.Fatalf("write src: %v", err)
	}
	_ = config.Save(cfgPath, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{From: "a@example.com"},
	}})
	// Inputs: from kept, defaultTo kept, gmail kept, header_bg blank kept, logo path supplied.
	in := strings.NewReader("\n\n\n\n" + srcLogo + "\n")
	var out, errBuf bytes.Buffer
	if err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, LogosDir: logosDir,
		Stdin: in, Stdout: &out, Stderr: &errBuf,
	}); err != nil {
		t.Fatalf("runConfigureEmail: %v\nstderr:\n%s", err, errBuf.String())
	}
	dst := filepath.Join(logosDir, "flip.png")
	gotBytes, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("expected logo at %s: %v", dst, err)
	}
	if !bytes.Equal(gotBytes, srcBytes) {
		t.Errorf("copied bytes differ from src")
	}
	file, _ := config.Load(cfgPath)
	if file["default"].Email.LogoPath != dst {
		t.Errorf("LogoPath: got %q want %q", file["default"].Email.LogoPath, dst)
	}
}

func TestConfigureEmailLogoClearDeletesManagedFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yml")
	logosDir := filepath.Join(dir, "logos")
	if err := os.MkdirAll(logosDir, 0o700); err != nil {
		t.Fatalf("mkdir logos: %v", err)
	}
	managed := filepath.Join(logosDir, "old.png")
	if err := os.WriteFile(managed, []byte("png-bytes"), 0o600); err != nil {
		t.Fatalf("seed managed file: %v", err)
	}
	_ = config.Save(cfgPath, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{From: "a@example.com", LogoPath: managed},
	}})
	in := strings.NewReader("\n\n\n\n-\n")
	var out, errBuf bytes.Buffer
	if err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, LogosDir: logosDir,
		Stdin: in, Stdout: &out, Stderr: &errBuf,
	}); err != nil {
		t.Fatalf("runConfigureEmail: %v\nstderr:\n%s", err, errBuf.String())
	}
	if _, err := os.Stat(managed); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected managed file deleted, stat err: %v", err)
	}
	file, _ := config.Load(cfgPath)
	if file["default"].Email.LogoPath != "" {
		t.Errorf("LogoPath: got %q want empty", file["default"].Email.LogoPath)
	}
}

func TestConfigureEmailLogoClearLeavesUnmanagedFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yml")
	logosDir := filepath.Join(dir, "logos")
	unmanaged := filepath.Join(dir, "elsewhere.png")
	if err := os.WriteFile(unmanaged, []byte("png"), 0o600); err != nil {
		t.Fatalf("seed unmanaged: %v", err)
	}
	_ = config.Save(cfgPath, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{From: "a@example.com", LogoPath: unmanaged},
	}})
	in := strings.NewReader("\n\n\n\n-\n")
	var out, errBuf bytes.Buffer
	if err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, LogosDir: logosDir,
		Stdin: in, Stdout: &out, Stderr: &errBuf,
	}); err != nil {
		t.Fatalf("runConfigureEmail: %v", err)
	}
	if _, err := os.Stat(unmanaged); err != nil {
		t.Errorf("unmanaged file should still exist: %v", err)
	}
}
```

Add `"errors"` to imports if missing.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/ -run TestConfigureEmailLogo -count=1`
Expected: build failure — `unknown field LogosDir in struct literal of type configureEmailOpts`.

- [ ] **Step 3: Extend `configureEmailOpts` and propagate `LogosDir`**

In `cmd/configure.go`:

```go
type configureEmailOpts struct {
	ConfigPath      string
	LogosDir        string // managed location for copied logos; defaults to <dirname(ConfigPath)>/logos
	Stdin           io.Reader
	Stdout          io.Writer
	Stderr          io.Writer
	StoreGmailJSON  func(jsonBytes []byte) error
	DeleteGmailJSON func() error
}
```

Also update `newConfigureEmailCmd` to leave `LogosDir` empty (so production defaults apply) — no code change needed there, but verify the call already constructs the opts struct without LogosDir.

- [ ] **Step 4: Add `promptLogo` to `cmd/configure.go`**

```go
func promptLogo(stdout, stderr io.Writer, r *bufio.Reader, current, logosDir string) (string, error) {
	for attempt := 1; attempt <= configureEmailMaxAttempts; attempt++ {
		value, err := promptWithDefault(stdout, r, "Logo PNG path", current)
		if err != nil {
			return "", err
		}
		switch value {
		case current:
			// Enter on existing value -> keep
			return current, nil
		case "":
			// dash collapsed -> clear; caller handles unlinking
			return "", nil
		default:
			dst, copyErr := copyLogoToManagedDir(value, logosDir)
			if copyErr != nil {
				_, _ = fmt.Fprintln(stderr, copyErr)
				continue
			}
			return dst, nil
		}
	}
	return "", fmt.Errorf("invalid logo after %d attempts", configureEmailMaxAttempts)
}

// copyLogoToManagedDir validates src as a PNG <= MaxLogoBytes and copies it
// to <logosDir>/<basename(src)> with mode 0o600. Returns the destination path.
func copyLogoToManagedDir(src, logosDir string) (string, error) {
	if _, err := email.ValidateLogoFile(src); err != nil {
		return "", err
	}
	if err := os.MkdirAll(logosDir, 0o700); err != nil {
		return "", fmt.Errorf("create logos dir %s: %w", logosDir, err)
	}
	// #nosec G304 - src is the operator's own input
	data, err := os.ReadFile(src)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", src, err)
	}
	dst := filepath.Join(logosDir, filepath.Base(src))
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		return "", fmt.Errorf("write %s: %w", dst, err)
	}
	return dst, nil
}

// removeManagedLogo deletes the file at path iff it sits inside logosDir.
// Anything outside that directory is left alone.
func removeManagedLogo(path, logosDir string) error {
	if path == "" || logosDir == "" {
		return nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	absDir, err := filepath.Abs(logosDir)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(absDir, abs)
	if err != nil || strings.HasPrefix(rel, "..") || rel == "." {
		return nil // outside the managed dir
	}
	if err := os.Remove(abs); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
```

Add `"path/filepath"` to the imports if it isn't present.

- [ ] **Step 5: Add `ValidateLogoFile` to `internal/email/logo.go`**

Append to `internal/email/logo.go`:

```go
// ValidateLogoFile checks that path points at a readable PNG no larger than
// MaxLogoBytes. Returns the byte length on success.
func ValidateLogoFile(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat %s: %w", path, err)
	}
	if info.Size() > MaxLogoBytes {
		return 0, fmt.Errorf("logo %s is %d bytes (max %d)", path, info.Size(), MaxLogoBytes)
	}
	// #nosec G304 - path validated by caller as operator input
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", path, err)
	}
	if _, err := png.Decode(bytes.NewReader(data)); err != nil {
		return 0, fmt.Errorf("decode PNG %s: %w", path, err)
	}
	return info.Size(), nil
}
```

- [ ] **Step 6: Wire `promptLogo` into `runConfigureEmail`**

After the `prof.Email.HeaderBG = headerBG` line from Task 8, add:

```go
logosDir := o.LogosDir
if logosDir == "" {
	logosDir = filepath.Join(filepath.Dir(o.ConfigPath), "logos")
}
newLogo, err := promptLogo(o.Stdout, o.Stderr, r, prof.Email.LogoPath, logosDir)
if err != nil {
	return err
}
if newLogo != prof.Email.LogoPath {
	if prof.Email.LogoPath != "" {
		if rmErr := removeManagedLogo(prof.Email.LogoPath, logosDir); rmErr != nil {
			_, _ = fmt.Fprintf(o.Stderr, "warn: failed to remove previous logo %s: %v\n", prof.Email.LogoPath, rmErr)
		}
	}
}
prof.Email.LogoPath = newLogo
```

- [ ] **Step 7: Run all configure tests**

Run: `go test ./cmd/ -run TestConfigureEmail -count=1`
Expected: PASS for all four new tests plus existing ones.

- [ ] **Step 8: Commit**

```bash
git add cmd/configure.go cmd/configure_test.go internal/email/logo.go
git commit -m "feat(configure): added logo prompt copying into managed logos dir"
```

---

## Task 10: Logo loading + multipart/related wiring at the renderer

**Files:**
- Modify: `internal/email/email.go` (add `randomRelatedBoundary`)
- Modify: `internal/email/vulns_summary.go` (load logo, warn on failure, downgrade HasLogo, pass to assembleMessage)
- Modify: `internal/email/user_show.go` (same)
- Modify: `internal/email/vulns_summary_test.go` (assert warn on missing logo, assert multipart/related with valid logo)
- Modify: `cmd/vulns.go`, `cmd/user.go` (route command stderr into the renderer)

- [ ] **Step 1: Write the failing tests (append to `internal/email/vulns_summary_test.go`)**

```go
func TestVulnSummaryRendererWarnsOnLogoLoadFailure(t *testing.T) {
	opts := Options{
		From:        "alice@example.com",
		LogoPath:    "/no/such/path/logo.png",
		GeneratedAt: time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
	}
	var warnBuf bytes.Buffer
	r := NewVulnSummaryRendererWithStderr(opts, &warnBuf).(*vulnSummaryRenderer)
	var out bytes.Buffer
	if err := r.Render(&out, []iru.Vulnerability(nil)); err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !strings.Contains(warnBuf.String(), "warn: email logo not loaded") {
		t.Errorf("expected warn on stderr, got:\n%s", warnBuf.String())
	}
	if strings.Contains(out.String(), "cid:jf-logo") {
		t.Errorf("expected no logo img in HTML when load fails")
	}
	// And the MIME envelope must stay multipart/alternative (not /related).
	if !strings.Contains(out.String(), "multipart/alternative") {
		t.Errorf("expected multipart/alternative on failed-logo path")
	}
	if strings.Contains(out.String(), "multipart/related") {
		t.Errorf("expected NO multipart/related on failed-logo path")
	}
}

func TestVulnSummaryRendererWithValidLogoEmitsMultipartRelated(t *testing.T) {
	opts := Options{
		From:                    "alice@example.com",
		LogoPath:                "testdata/logo_small.png",
		HeaderBG:                "#C6B8FE",
		GeneratedAt:             time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		BoundaryOverride:        "=_jf_TEST",
		RelatedBoundaryOverride: "=_jfr_TEST",
		MessageIDOverride:       "<m@example.com>",
	}
	var warnBuf, out bytes.Buffer
	r := NewVulnSummaryRendererWithStderr(opts, &warnBuf).(*vulnSummaryRenderer)
	if err := r.Render(&out, []iru.Vulnerability(nil)); err != nil {
		t.Fatalf("Render: %v", err)
	}
	if warnBuf.Len() != 0 {
		t.Errorf("expected no warnings, got: %s", warnBuf.String())
	}
	if !strings.Contains(out.String(), "multipart/related") {
		t.Errorf("expected multipart/related, raw:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "Content-ID: <jf-logo>") {
		t.Errorf("expected Content-ID: <jf-logo>")
	}
	if !strings.Contains(out.String(), `src="cid:jf-logo"`) {
		t.Errorf("expected HTML to reference cid:jf-logo")
	}
}
```

Add `"bytes"` import if missing.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/email/ -run 'TestVulnSummaryRendererWarnsOnLogoLoadFailure|TestVulnSummaryRendererWithValidLogoEmitsMultipartRelated' -count=1`
Expected: build failure — `NewVulnSummaryRendererWithStderr undefined`.

- [ ] **Step 3: Add `randomRelatedBoundary` to `internal/email/email.go`**

Append near `randomBoundary`:

```go
func randomRelatedBoundary() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "=_jfr_" + hex.EncodeToString(b[:]), nil
}
```

- [ ] **Step 4: Add `warn io.Writer` field and constructor to both renderers**

In `internal/email/vulns_summary.go`:

```go
type vulnSummaryRenderer struct {
	opts Options
	warn io.Writer
}

// NewVulnSummaryRendererWithStderr is like NewVulnSummaryRenderer but routes
// renderer-level warnings (e.g. logo load failures) to the supplied writer
// instead of os.Stderr.
func NewVulnSummaryRendererWithStderr(opts Options, stderr io.Writer) output.Renderer {
	return &vulnSummaryRenderer{opts: opts.withDefaults(), warn: stderr}
}
```

In `Render`, replace the existing `view.Header = buildHeader(...)` block (added in Task 5) plus the existing `assembleMessage` call with:

```go
warn := r.warn
if warn == nil {
	warn = os.Stderr
}
logo, logoErr := loadLogo(r.opts.LogoPath)
if logoErr != nil {
	fmt.Fprintf(warn, "warn: email logo not loaded (%v); rendering without logo\n", logoErr)
}

subtitle := view.GeneratedAtStr
if view.Tenant != "" {
	subtitle += " - " + view.Tenant
}
subtitle += fmt.Sprintf(" - %d CVEs across %d devices (max per CVE)", view.TotalCVEs, view.DeviceCount)
view.Header = buildHeader(
	"JELLYFISH / VULNS",
	"Fleet vulnerability summary",
	subtitle,
	r.opts.HeaderBG,
	logo != nil,
)

// (existing htmlBody/textBody rendering stays here — unchanged.)

outerBoundary := r.opts.RelatedBoundaryOverride
if outerBoundary == "" && logo != nil {
	outerBoundary, err = randomRelatedBoundary()
	if err != nil {
		return err
	}
}
bytesOut, err := assembleMessage(messageHeaders{
	From:    r.opts.From,
	To:      r.opts.To,
	Subject: subject,
	Date:    r.opts.GeneratedAt,
}, htmlBody, textBody, boundary, messageID, outerBoundary, logo)
```

Add `"os"` import.

Mirror all of these changes in `internal/email/user_show.go` (same `warn` field, same `NewUserShowRendererWithStderr` constructor, same logo loading + warn + outerBoundary block).

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/email/ -run 'TestVulnSummaryRendererWarnsOnLogoLoadFailure|TestVulnSummaryRendererWithValidLogoEmitsMultipartRelated' -count=1`
Expected: PASS.

- [ ] **Step 6: Wire stderr through from the cmd layer**

In `cmd/vulns.go`, `renderVulns` already has access to `stderr` only when `runVulnsSummary` passes it. Today `renderVulns` does not take stderr — extend its signature.

Change `renderVulns(w io.Writer, opts vulnsSummaryOpts, vs []iru.Vulnerability) error` to:

```go
func renderVulns(w io.Writer, stderr io.Writer, opts vulnsSummaryOpts, vs []iru.Vulnerability) error {
```

Update the single call site in `runVulnsSummary` (around line 261) from:

```go
return renderVulns(w, opts, filtered)
```

to:

```go
return renderVulns(w, stderr, opts, filtered)
```

Inside `renderVulns`, change the email branch:

```go
return email.NewVulnSummaryRendererWithStderr(emailOpts, stderr).Render(w, vs)
```

In `runSendVulnsSummary` (around line 344), change:

```go
if err := email.NewVulnSummaryRenderer(emailOpts).Render(&buf, vs); err != nil {
```

to:

```go
if err := email.NewVulnSummaryRendererWithStderr(emailOpts, stderr).Render(&buf, vs); err != nil {
```

Mirror the same three changes in `cmd/user.go`'s equivalent paths (`renderUser` / `runSendUserShow`-style function — adapt to the actual function names).

- [ ] **Step 7: Run the whole repo**

Run: `go test ./... -count=1`
Expected: all packages PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/email/email.go internal/email/vulns_summary.go internal/email/user_show.go internal/email/vulns_summary_test.go cmd/vulns.go cmd/user.go
git commit -m "feat(email): loaded and embedded logo PNG via multipart/related"
```

---

## Task 11: End-to-end `--send-email` MIME assertion

**Files:**
- Modify: `cmd/send_email_test.go`

The existing scaffolding (`fakeGmailSender` at `cmd/send_email_test.go:17`, `newFakeSenderFactory` at line 34, `stubKeychain` at line 40) captures the .eml bytes the sender receives via `sender.sent`. Drive `runSendVulnsSummary` directly with a `vulnsSummaryOpts` constructed in-line; that's the simplest path to a captured payload.

- [ ] **Step 1: Write the failing test (append to `cmd/send_email_test.go`)**

```go
func TestRunSendVulnsSummaryWithLogoEmitsMultipartRelated(t *testing.T) {
	sender := &fakeGmailSender{}
	opts := vulnsSummaryOpts{
		ExplicitOutput: "",
		Profile: config.Profile{
			Subdomain: "acme",
			Email: config.EmailConfig{
				From:            "alice@example.com",
				DefaultTo:       "ops@example.com",
				GmailConfigured: true,
			},
		},
		EmailFlags: emailFlagValues{
			Send:     true,
			HeaderBG: "#C6B8FE",
			LogoPath: "../internal/email/testdata/logo_small.png",
		},
		EmailNow:    time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		KeychainGet: stubKeychain(`{}`),
		NewSender:   newFakeSenderFactory(sender),
		gitEmail:    func() (string, error) { return "git@example.com", nil },
	}
	var stderr bytes.Buffer
	if err := runSendVulnsSummary(context.Background(), &stderr, opts, nil); err != nil {
		t.Fatalf("runSendVulnsSummary: %v\nstderr:\n%s", err, stderr.String())
	}
	if len(sender.sent) == 0 {
		t.Fatal("fake sender captured no bytes")
	}
	msg, err := mail.ReadMessage(bytes.NewReader(sender.sent))
	if err != nil {
		t.Fatalf("parse captured: %v\nraw:\n%s", err, sender.sent)
	}
	mt, _, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("Content-Type parse: %v", err)
	}
	if mt != "multipart/related" {
		t.Errorf("Content-Type: got %q want multipart/related", mt)
	}
	if !bytes.Contains(sender.sent, []byte("Content-ID: <jf-logo>")) {
		t.Errorf("expected Content-ID: <jf-logo> in captured bytes")
	}
}

func TestRunSendVulnsSummaryWithoutLogoEmitsMultipartAlternative(t *testing.T) {
	sender := &fakeGmailSender{}
	opts := vulnsSummaryOpts{
		Profile: config.Profile{
			Subdomain: "acme",
			Email: config.EmailConfig{
				From:            "alice@example.com",
				DefaultTo:       "ops@example.com",
				GmailConfigured: true,
			},
		},
		EmailFlags:  emailFlagValues{Send: true},
		EmailNow:    time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		KeychainGet: stubKeychain(`{}`),
		NewSender:   newFakeSenderFactory(sender),
		gitEmail:    func() (string, error) { return "git@example.com", nil },
	}
	var stderr bytes.Buffer
	if err := runSendVulnsSummary(context.Background(), &stderr, opts, nil); err != nil {
		t.Fatalf("runSendVulnsSummary: %v", err)
	}
	msg, _ := mail.ReadMessage(bytes.NewReader(sender.sent))
	mt, _, _ := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	if mt != "multipart/alternative" {
		t.Errorf("no-logo path: Content-Type got %q want multipart/alternative", mt)
	}
}
```

Add imports: `"mime"`, `"net/mail"`, `"time"`. `"bytes"` and `"context"` are already there.

- [ ] **Step 2: Run the test**

Run: `go test ./cmd/ -run 'TestRunSendVulnsSummary(With|Without)' -count=1`
Expected: PASS provided Tasks 1-10 are complete. If it fails with `unknown field gitEmail` or similar, check that `vulnsSummaryOpts` has the unexported `gitEmail` field — if not, the existing options struct uses a different lookup mechanism; thread the git lookup the same way the production code does (read `cmd/vulns.go` line 320-323 for the pattern).

- [ ] **Step 3: Commit**

```bash
git add cmd/send_email_test.go
git commit -m "test(send-email): asserted multipart/related end-to-end with logo"
```

---

## Task 12: Real-world smoke test (manual)

**Files:** None modified — this task is a manual verification.

- [ ] **Step 1: Run configure email with a logo**

```bash
go run . configure email
# When prompted for "Logo PNG path", supply:
# /Users/bawdo/working/scratch/flip-logo-reversed-200x100.png
# When prompted for "Header background colour", supply:
# #C6B8FE
```

Expected: command exits 0; check `~/.config/jellyfish/config.yml` shows `header_bg: "#C6B8FE"` and `logo_path: <something inside ~/.config/jellyfish/logos/>`; check the file at that path exists.

- [ ] **Step 2: Render a sample email to stdout**

```bash
go run . vulns summary --severity critical -o email > /tmp/critical.eml
open -f -a Mail /tmp/critical.eml
```

Expected: Mail renders the .eml with a lavender header containing the FLIP logo on the left. No dark-blue band.

- [ ] **Step 3: Render with the default colour (no override)**

```bash
go run . configure email
# When prompted for "Header background colour", clear with: -
# When prompted for "Logo PNG path", clear with: -
go run . vulns summary --severity critical -o email > /tmp/default.eml
open -f -a Mail /tmp/default.eml
```

Expected: header renders in `#7566FF`. No logo (cleared).

- [ ] **Step 4: Run lint + the whole test suite one more time**

```bash
make lint
make test
```

Expected: both clean.

- [ ] **Step 5: Commit (no-op if no changes; this is just the gate)**

If the manual run surfaced any tweaks (e.g. typography that needs more padding on the lavender bg), apply them here as a focused commit.

---

## Task 13: README updates

**Files:**
- Modify: `README.md` (Email output section)

- [ ] **Step 1: Locate the existing "Email output" section**

Run: `grep -n "### Email output" README.md`
Expected: one match at the top of the email block.

- [ ] **Step 2: Update the flag/config table**

Add two new rows to the existing flag/config table (in `README.md`, in the "Email output" section, around line 234-241):

```markdown
| Flag | Config key | Default |
|---|---|---|
| `--email-to`         | `email.default_to`       | empty (header renders as `<unspecified>`) |
| `--email-from`       | `email.from`             | `git config user.email` |
| `--email-subject`    | `email.subject_template` | per-command default |
| `--email-header-bg`  | `email.header_bg`        | `#7566FF` (Flip brand purple) |
| `--email-logo`       | `email.logo_path`        | empty (no logo) |
```

- [ ] **Step 3: Add a short paragraph below the table about the default colour caveat**

```markdown
The default `#7566FF` is the Flip brand purple. A logo whose recognisable
element is the same purple (the Flip brand mark, for example) will blend into
that background — pair the default with a logo whose distinguishing element
is white or dark, or pick a contrasting header colour such as `#C6B8FE`
(light lavender) or `#6846D8` (deep purple).
```

- [ ] **Step 4: Update the "Configure email defaults" section to mention the new prompts**

Find the existing block (around line 82-97). Replace the description's tail with:

```markdown
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
```

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: documented configurable email header colour and logo"
```

---

## Self-Review

Re-read the spec and check each requirement against the task list:

- **`#7566FF` as default header colour** → Task 4 (`DefaultHeaderBG`), Task 6/7 (flag fallthrough), Task 8 (config prompt default), README in Task 13.
- **`#RRGGBB` validation at config and flag layer** → Task 1 (`ValidateHexColour`), Task 6 (`resolveEmailOptions`), Task 8 (`promptHeaderBG`).
- **PNG-only, ≤ 512 KB** → Task 3 (`loadLogo`), Task 9 (`ValidateLogoFile`).
- **CID inline attachment via `multipart/related`** → Task 4 (`assembleMessage` rewrite), Task 5 (template `cid:jf-logo` reference).
- **No-logo path stays `multipart/alternative`** → Task 4 (explicit assertion `TestAssembleMessageNoLogoEmitsMultipartAlternative`).
- **Auto-derived text/badge colour from luminance** → Task 1 (`computeHeaderStyle`), Task 5 (header partial).
- **`prefers-color-scheme: dark` removed** → Task 5 (template rewires drop the `<style>` block; test asserts the substring is gone).
- **Both templates updated** → Task 5 (vulns + user templates share the partial; Task 5 Step 10 tests user_show).
- **Both CLIs gain flags** → Task 6 (vulns), Task 7 (user).
- **`configure email` prompts for both** → Task 8 (colour), Task 9 (logo).
- **Logo copied to `<configDir>/logos/<basename>`** → Task 9 (`copyLogoToManagedDir`).
- **`-` clears + deletes managed file only** → Task 9 (`removeManagedLogo`).
- **Stderr warn on logo load failure, exit code unchanged** → Task 10 (renderer `warn` writer, warn line, no error returned).
- **End-to-end `multipart/related` on send** → Task 11.
- **Manual smoke** → Task 12.
- **Docs** → Task 13.

No placeholders, no undefined types. `Header`, `headerStyle`, `logoPart`, `MaxLogoBytes`, `DefaultHeaderBG`, `ValidateHexColour`, `ValidateLogoFile`, `loadLogo`, `buildHeader`, `computeHeaderStyle`, `randomRelatedBoundary` are all defined where they are first referenced.

Type-name consistency: `Header` (the data passed to the partial), `headerStyle` (the four-colour helper return value), `logoPart` (the MIME attachment record) — used identically across tasks. Flag names `--email-header-bg` and `--email-logo` match the config keys `email.header_bg` and `email.logo_path` in case and intent.
