# Email Output Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `-o email` to `jellyfish vulns summary` and `jellyfish user show` that writes a complete RFC 5322 multipart/alternative `.eml` to stdout (Operations Dashboard HTML body + plain-text alternative, clickable NVD/MITRE CVE links, system-font-only, Gmail-safe).

**Architecture:** A new `internal/email` package holds typed renderers that satisfy the existing `output.Renderer` interface. Each renderer owns its presentation struct, its `html/template` and `text/template`, and produces a final byte slice through a shared `assembleMessage` helper that does the multipart wrap, header writing, and quoted-printable encoding (stdlib only). The cmd layer reads `--email-*` flags, falls back to a new `email:` block in `config.yml`, then to `git config user.email`, and constructs the renderer.

**Tech Stack:** Go 1.25, stdlib only (`html/template`, `text/template`, `mime/quotedprintable`, `net/mail`, `crypto/rand`, `os/exec`, `embed`), existing `internal/output` and `internal/config` packages, existing test harness in `cmd/main_test.go`.

**Spec:** `docs/superpowers/specs/2026-05-16-email-output-design.md`

---

## Task 1: Add `Email` config block to `Profile`

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/config/config_test.go`:

```go
func TestLoadParsesEmailBlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	content := []byte(`default:
  subdomain: acme
  region: us
  email:
    from: alice@example.com
    default_to: secops@example.com
    subject_template: "Weekly brief - {{.Date}}"
    cve_link_primary: "https://example.test/{cve}"
    cve_link_secondary: "https://mirror.test/{cve}"
`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	out, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := out["default"].Email
	want := EmailConfig{
		From:             "alice@example.com",
		DefaultTo:        "secops@example.com",
		SubjectTemplate:  "Weekly brief - {{.Date}}",
		CVELinkPrimary:   "https://example.test/{cve}",
		CVELinkSecondary: "https://mirror.test/{cve}",
	}
	if got != want {
		t.Fatalf("Email mismatch.\n got: %#v\nwant: %#v", got, want)
	}
}

func TestLoadEmailBlockOptional(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	if err := os.WriteFile(path, []byte("default:\n  subdomain: acme\n  region: us\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	out, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if (out["default"].Email != EmailConfig{}) {
		t.Fatalf("expected zero EmailConfig, got %#v", out["default"].Email)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/... -run TestLoadParses -count=1`
Expected: compile error — `EmailConfig` not defined, `Profile.Email` not defined.

- [ ] **Step 3: Add the `EmailConfig` type and `Email` field**

Edit `internal/config/config.go`. Add right above the `Profile` struct:

```go
// EmailConfig holds optional defaults for the -o email output. Every field
// is optional. Flags override these values; if both are empty the renderer
// falls back to built-in defaults (or, for "from", git user.email).
type EmailConfig struct {
	From             string `yaml:"from"`
	DefaultTo        string `yaml:"default_to"`
	SubjectTemplate  string `yaml:"subject_template"`
	CVELinkPrimary   string `yaml:"cve_link_primary"`
	CVELinkSecondary string `yaml:"cve_link_secondary"`
}
```

Modify `Profile` to add the new field:

```go
type Profile struct {
	Subdomain string      `yaml:"subdomain"`
	Region    string      `yaml:"region"`
	BaseURL   string      `yaml:"base_url"`
	Email     EmailConfig `yaml:"email,omitempty"`
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/config/... -count=1`
Expected: PASS for all tests including the two new ones.

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go
git commit -m "feat: added optional email config block to Profile"
```

---

## Task 2: Scaffold `internal/email` package — `Options` + `buildCVELink`

**Files:**
- Create: `internal/email/email.go`
- Create: `internal/email/email_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/email/email_test.go`:

```go
package email

import "testing"

func TestBuildCVELinkSubstitutes(t *testing.T) {
	got := buildCVELink("https://nvd.nist.gov/vuln/detail/{cve}", "CVE-2024-3094")
	want := "https://nvd.nist.gov/vuln/detail/CVE-2024-3094"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestBuildCVELinkMultipleTokens(t *testing.T) {
	got := buildCVELink("https://x.test/{cve}/info?id={cve}", "CVE-1")
	want := "https://x.test/CVE-1/info?id=CVE-1"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestValidateLinkTemplateAcceptsToken(t *testing.T) {
	if err := validateLinkTemplate("primary", "https://x.test/{cve}"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateLinkTemplateRejectsMissingToken(t *testing.T) {
	err := validateLinkTemplate("primary", "https://x.test/foo")
	if err == nil {
		t.Fatal("expected error for template without {cve}")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/email/... -count=1`
Expected: build error — package does not exist.

- [ ] **Step 3: Create the package with `Options`, `buildCVELink`, and `validateLinkTemplate`**

Create `internal/email/email.go`:

```go
package email

import (
	"fmt"
	"strings"
	"time"
)

// Built-in defaults applied when an Options field is empty after the cmd
// layer's flag + config resolution.
const (
	DefaultCVELinkPrimary   = "https://nvd.nist.gov/vuln/detail/{cve}"
	DefaultCVELinkSecondary = "https://www.cve.org/CVERecord?id={cve}"
)

// Options carries everything an email renderer needs that isn't part of the
// rendered domain data. All fields are optional except as noted; missing
// optional fields fall back to built-in defaults at renderer construction.
type Options struct {
	To      string // header value; empty renders as "<unspecified>"
	From    string // required at the cmd layer; renderer errors if empty
	Subject string // empty triggers per-renderer default

	CVELinkPrimary   string // empty defaults to DefaultCVELinkPrimary
	CVELinkSecondary string // empty defaults to DefaultCVELinkSecondary

	GeneratedAt time.Time // pinned by tests; cmd layer passes time.Now()
	Tenant      string    // shown in masthead, sourced from config.Profile.Subdomain

	// Injected for tests; production leaves these zero so assembleMessage
	// pulls from crypto/rand.
	BoundaryOverride  string
	MessageIDOverride string
}

// withDefaults returns a copy of opts with empty optional fields filled in.
func (o Options) withDefaults() Options {
	if o.CVELinkPrimary == "" {
		o.CVELinkPrimary = DefaultCVELinkPrimary
	}
	if o.CVELinkSecondary == "" {
		o.CVELinkSecondary = DefaultCVELinkSecondary
	}
	if o.GeneratedAt.IsZero() {
		o.GeneratedAt = time.Now()
	}
	return o
}

// buildCVELink substitutes the literal {cve} token in template with cve.
// All occurrences are replaced.
func buildCVELink(template, cve string) string {
	return strings.ReplaceAll(template, "{cve}", cve)
}

// validateLinkTemplate ensures a CVE link template contains the {cve} token.
// label appears in the error to disambiguate primary vs secondary.
func validateLinkTemplate(label, template string) error {
	if !strings.Contains(template, "{cve}") {
		return fmt.Errorf("email %s CVE link template must contain {cve}: got %q", label, template)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/email/... -count=1`
Expected: PASS for all four tests.

- [ ] **Step 5: Commit**

```bash
git add internal/email/email.go internal/email/email_test.go
git commit -m "feat: scaffolded email package with Options and CVE link helpers"
```

---

## Task 3: `assembleMessage` — RFC 5322 multipart/alternative writer

**Files:**
- Modify: `internal/email/email.go`
- Modify: `internal/email/email_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/email/email_test.go`:

```go
import (
	"bytes"
	"mime"
	"mime/multipart"
	"net/mail"
	"strings"
	"testing"
	"time"
)

func TestAssembleMessageHeadersAndStructure(t *testing.T) {
	hdr := messageHeaders{
		From:    "Jellyfish <alice@example.com>",
		To:      "secops@example.com",
		Subject: "Test subject",
		Date:    time.Date(2026, 5, 16, 10, 42, 0, 0, time.FixedZone("AEST", 10*3600)),
	}
	out, err := assembleMessage(hdr, "<html><body>hi</body></html>", "hello plain text\n",
		"=_jf_FIXEDBOUNDARY", "<fixed-id@example.com>")
	if err != nil {
		t.Fatalf("assembleMessage: %v", err)
	}

	msg, err := mail.ReadMessage(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("parse: %v\nraw:\n%s", err, out)
	}

	if got := msg.Header.Get("From"); got != hdr.From {
		t.Errorf("From: got %q want %q", got, hdr.From)
	}
	if got := msg.Header.Get("To"); got != hdr.To {
		t.Errorf("To: got %q want %q", got, hdr.To)
	}
	if got := msg.Header.Get("Subject"); got != hdr.Subject {
		t.Errorf("Subject: got %q want %q", got, hdr.Subject)
	}
	if got := msg.Header.Get("Message-ID"); got != "<fixed-id@example.com>" {
		t.Errorf("Message-ID: got %q", got)
	}
	if got := msg.Header.Get("MIME-Version"); got != "1.0" {
		t.Errorf("MIME-Version: got %q", got)
	}

	mediaType, params, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("Content-Type parse: %v", err)
	}
	if mediaType != "multipart/alternative" {
		t.Errorf("media type: got %q want multipart/alternative", mediaType)
	}
	if params["boundary"] != "=_jf_FIXEDBOUNDARY" {
		t.Errorf("boundary: got %q", params["boundary"])
	}

	mr := multipart.NewReader(msg.Body, params["boundary"])
	parts := map[string]string{}
	for {
		p, err := mr.NextPart()
		if err != nil {
			break
		}
		buf := &bytes.Buffer{}
		if _, copyErr := buf.ReadFrom(p); copyErr != nil {
			t.Fatalf("read part: %v", copyErr)
		}
		ct := p.Header.Get("Content-Type")
		parts[strings.Split(ct, ";")[0]] = buf.String()
	}
	if _, ok := parts["text/plain"]; !ok {
		t.Errorf("missing text/plain part; got %v", parts)
	}
	if _, ok := parts["text/html"]; !ok {
		t.Errorf("missing text/html part; got %v", parts)
	}
}

func TestAssembleMessageUsesCRLF(t *testing.T) {
	hdr := messageHeaders{
		From: "a@b.c", To: "d@e.f", Subject: "s",
		Date: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	out, err := assembleMessage(hdr, "<p>h</p>", "h\n", "=_jf_X", "<i@b.c>")
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if !bytes.Contains(out, []byte("\r\n")) {
		t.Fatalf("expected CRLF line endings in output")
	}
	// LF-only lines would indicate a bug; allow CRLF only.
	for _, line := range bytes.Split(out, []byte("\r\n")) {
		if bytes.Contains(line, []byte("\n")) {
			t.Fatalf("found bare LF inside line: %q", line)
		}
	}
}

func TestRandomBoundaryShape(t *testing.T) {
	b, err := randomBoundary()
	if err != nil {
		t.Fatalf("randomBoundary: %v", err)
	}
	if !strings.HasPrefix(b, "=_jf_") {
		t.Errorf("boundary missing prefix: %q", b)
	}
	if len(b) != len("=_jf_")+16 {
		t.Errorf("boundary length: got %d want %d (%q)", len(b), len("=_jf_")+16, b)
	}
}

func TestRandomMessageIDShape(t *testing.T) {
	id, err := randomMessageID("example.com")
	if err != nil {
		t.Fatalf("randomMessageID: %v", err)
	}
	if !strings.HasPrefix(id, "<") || !strings.HasSuffix(id, "@example.com>") {
		t.Errorf("message-id shape unexpected: %q", id)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/email/... -run TestAssemble -count=1`
Expected: compile error — `assembleMessage`, `messageHeaders`, `randomBoundary`, `randomMessageID` not defined.

- [ ] **Step 3: Implement message assembly**

Append to `internal/email/email.go`:

```go
import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"mime/quotedprintable"
	"strings"
	"time"
)

// messageHeaders is the minimum set of headers assembleMessage writes.
type messageHeaders struct {
	From    string
	To      string
	Subject string
	Date    time.Time
}

// assembleMessage produces a full RFC 5322 multipart/alternative message
// from a plain-text body and an HTML body. boundary and messageID are
// caller-supplied for test determinism; production callers pass values
// from randomBoundary() and randomMessageID().
func assembleMessage(h messageHeaders, htmlBody, textBody, boundary, messageID string) ([]byte, error) {
	var sb strings.Builder
	writeHeader := func(name, value string) {
		sb.WriteString(name)
		sb.WriteString(": ")
		sb.WriteString(value)
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
	writeHeader("Content-Type", fmt.Sprintf("multipart/alternative; boundary=%q", boundary))
	sb.WriteString("\r\n")

	writePart := func(contentType, body string) error {
		sb.WriteString("--")
		sb.WriteString(boundary)
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
	sb.WriteString(boundary)
	sb.WriteString("--\r\n")

	return []byte(sb.String()), nil
}

// quotedPrintableEncode runs body through stdlib quoted-printable, then
// normalises line endings to CRLF (stdlib emits LF after soft-break inserts
// on some Go versions; RFC 5322 requires CRLF throughout the message).
func quotedPrintableEncode(body string) (string, error) {
	buf := &strings.Builder{}
	w := quotedprintable.NewWriter(buf)
	if _, err := io.WriteString(w, body); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}
	return strings.ReplaceAll(strings.ReplaceAll(buf.String(), "\r\n", "\n"), "\n", "\r\n"), nil
}

// randomBoundary returns "=_jf_" + 16 lowercase hex chars (8 random bytes).
func randomBoundary() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "=_jf_" + hex.EncodeToString(b[:]), nil
}

// randomMessageID returns "<nanos.<6 hex chars>@<domain>>".
func randomMessageID(domain string) (string, error) {
	var b [3]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("<%d.%s@%s>", time.Now().UnixNano(), hex.EncodeToString(b[:]), domain), nil
}
```

Adjust the existing `import` block at the top of `email.go` so it merges the new imports — final import block at the top of the file should read:

```go
import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"mime/quotedprintable"
	"strings"
	"time"
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/email/... -count=1`
Expected: PASS for all tests (Task 2's four plus Task 3's four).

- [ ] **Step 5: Commit**

```bash
git add internal/email/email.go internal/email/email_test.go
git commit -m "feat: implemented multipart RFC 5322 message assembly"
```

---

## Task 4: Vulns summary — presentation struct and plain-text body

**Files:**
- Create: `internal/email/vulns_summary.go`
- Create: `internal/email/vulns_summary_test.go`
- Create: `internal/email/templates/vulns_summary.txt.tmpl`

- [ ] **Step 1: Write the failing test**

Create `internal/email/vulns_summary_test.go`:

```go
package email

import (
	"strings"
	"testing"
	"time"

	"flipgroup.com.au/jellyfish/internal/iru"
)

func sampleVulns() []iru.Vulnerability {
	return []iru.Vulnerability{
		{CVEID: "CVE-2024-3094", Severity: "Critical", CVSSScore: 10.0, KEVScore: 1.0, DeviceCount: 4, Status: "Active", Software: []string{"xz-utils"}},
		{CVEID: "CVE-2024-6387", Severity: "Critical", CVSSScore: 8.1, KEVScore: 0, DeviceCount: 11, Status: "Active", Software: []string{"openssh-server"}},
		{CVEID: "CVE-2024-1086", Severity: "High", CVSSScore: 7.8, KEVScore: 1.0, DeviceCount: 17, Status: "Active", Software: []string{"linux-image"}},
		{CVEID: "CVE-2024-21626", Severity: "High", CVSSScore: 7.2, KEVScore: 0, DeviceCount: 3, Status: "Active", Software: []string{"runc"}},
		{CVEID: "CVE-2023-50164", Severity: "Critical", CVSSScore: 9.8, KEVScore: 1.0, DeviceCount: 2, Status: "Active", Software: []string{"struts2-core"}},
	}
}

func TestBuildVulnSummaryView(t *testing.T) {
	view := buildVulnSummaryView(sampleVulns(), Options{
		Tenant:      "flipgroup",
		GeneratedAt: time.Date(2026, 5, 16, 10, 42, 0, 0, time.UTC),
	}.withDefaults())

	if view.TotalCVEs != 5 {
		t.Errorf("TotalCVEs: got %d want 5", view.TotalCVEs)
	}
	if view.CriticalCount != 3 {
		t.Errorf("CriticalCount: got %d want 3", view.CriticalCount)
	}
	if view.HighCount != 2 {
		t.Errorf("HighCount: got %d want 2", view.HighCount)
	}
	if view.KEVCount != 3 {
		t.Errorf("KEVCount: got %d want 3", view.KEVCount)
	}
	if view.DeviceCount != 17 {
		t.Errorf("DeviceCount (max across rows): got %d want 17", view.DeviceCount)
	}
	if len(view.Rows) != 5 {
		t.Fatalf("Rows: got %d want 5", len(view.Rows))
	}
	if view.Rows[0].NVDLink != "https://nvd.nist.gov/vuln/detail/CVE-2024-3094" {
		t.Errorf("NVDLink: got %q", view.Rows[0].NVDLink)
	}
	if view.Rows[0].MITRELink != "https://www.cve.org/CVERecord?id=CVE-2024-3094" {
		t.Errorf("MITRELink: got %q", view.Rows[0].MITRELink)
	}
}

func TestRenderVulnSummaryText(t *testing.T) {
	view := buildVulnSummaryView(sampleVulns(), Options{Tenant: "flipgroup"}.withDefaults())
	got, err := renderVulnSummaryText(view)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		"3 Critical",
		"2 High",
		"3 KEV-listed",
		"CVE-2024-3094",
		"openssh-server",
		"https://nvd.nist.gov/vuln/detail/CVE-2024-3094",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("plain text missing %q\nfull output:\n%s", want, got)
		}
	}
}

func TestRenderVulnSummaryTextEmpty(t *testing.T) {
	view := buildVulnSummaryView(nil, Options{Tenant: "flipgroup"}.withDefaults())
	got, err := renderVulnSummaryText(view)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(got, "No matching vulnerabilities") {
		t.Errorf("expected empty marker, got:\n%s", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/email/... -run TestVuln -count=1`
Expected: compile error — `buildVulnSummaryView`, `renderVulnSummaryText`, and the view types do not exist.

- [ ] **Step 3: Build the presentation struct and the text renderer**

Create `internal/email/vulns_summary.go`:

```go
package email

import (
	"embed"
	"fmt"
	"strings"
	"text/template"

	"flipgroup.com.au/jellyfish/internal/iru"
)

//go:embed templates/vulns_summary.txt.tmpl templates/vulns_summary.html.tmpl
var vulnSummaryFS embed.FS

// vulnSummaryView is the data shape vulns_summary templates render against.
// Every field is pre-formatted so templates contain no Go logic.
type vulnSummaryView struct {
	Tenant         string
	GeneratedAtStr string // e.g. "16 May 2026 - 10:42 UTC"
	GeneratedDate  string // e.g. "2026-05-16"
	TotalCVEs      int
	CriticalCount  int
	HighCount      int
	MediumCount    int
	LowCount       int
	KEVCount       int
	DeviceCount    int // max DeviceCount across rendered rows (fleet exposure proxy)
	Rows           []vulnSummaryRow
}

type vulnSummaryRow struct {
	CVEID     string
	Severity  string  // "Critical" | "High" | "Medium" | "Low" | "Undefined"
	SeverityClass string // "crit" | "high" | "med" | "low" | "und" (CSS class hook)
	CVSS      float64
	CVSSStr   string  // pre-formatted to 1dp
	IsKEV     bool
	Devices   int
	Software  string  // comma-joined
	Status    string
	NVDLink   string
	MITRELink string
}

func severityClass(sev string) string {
	switch strings.ToLower(sev) {
	case "critical":
		return "crit"
	case "high":
		return "high"
	case "medium":
		return "med"
	case "low":
		return "low"
	default:
		return "und"
	}
}

func buildVulnSummaryView(vs []iru.Vulnerability, opts Options) vulnSummaryView {
	view := vulnSummaryView{
		Tenant:         opts.Tenant,
		GeneratedAtStr: opts.GeneratedAt.Format("2 Jan 2006 - 15:04 MST"),
		GeneratedDate:  opts.GeneratedAt.Format("2006-01-02"),
		TotalCVEs:      len(vs),
		Rows:           make([]vulnSummaryRow, 0, len(vs)),
	}
	maxDevices := 0
	for _, v := range vs {
		isKEV := v.KEVScore > 0
		if isKEV {
			view.KEVCount++
		}
		switch strings.ToLower(v.Severity) {
		case "critical":
			view.CriticalCount++
		case "high":
			view.HighCount++
		case "medium":
			view.MediumCount++
		case "low":
			view.LowCount++
		}
		if v.DeviceCount > maxDevices {
			maxDevices = v.DeviceCount
		}
		view.Rows = append(view.Rows, vulnSummaryRow{
			CVEID:         v.CVEID,
			Severity:      v.Severity,
			SeverityClass: severityClass(v.Severity),
			CVSS:          v.CVSSScore,
			CVSSStr:       fmt.Sprintf("%.1f", v.CVSSScore),
			IsKEV:         isKEV,
			Devices:       v.DeviceCount,
			Software:      strings.Join(v.Software, ", "),
			Status:        v.Status,
			NVDLink:       buildCVELink(opts.CVELinkPrimary, v.CVEID),
			MITRELink:     buildCVELink(opts.CVELinkSecondary, v.CVEID),
		})
	}
	view.DeviceCount = maxDevices
	return view
}

func renderVulnSummaryText(v vulnSummaryView) (string, error) {
	tmpl, err := template.ParseFS(vulnSummaryFS, "templates/vulns_summary.txt.tmpl")
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

Create `internal/email/templates/vulns_summary.txt.tmpl`:

```
{{.CriticalCount}} Critical, {{.HighCount}} High, {{.MediumCount}} Medium, {{.LowCount}} Low. {{.KEVCount}} KEV-listed across {{.DeviceCount}} devices (max per CVE).

Generated {{.GeneratedAtStr}}{{if .Tenant}} for tenant {{.Tenant}}{{end}}.

{{if eq (len .Rows) 0 -}}
No matching vulnerabilities.
{{- else -}}
CVE              SEVERITY  CVSS  KEV  DEVICES  STATUS      SOFTWARE
{{range .Rows -}}
{{printf "%-16s %-9s %-5s %-4s %-8d %-11s %s" .CVEID .Severity .CVSSStr (printf "%s" (cond .IsKEV "KEV" "-")) .Devices .Status .Software}}
{{end -}}

NVD links:
{{range .Rows}}{{.CVEID}}: {{.NVDLink}}
{{end -}}
{{- end -}}

KEV = CISA Known Exploited Vulnerabilities catalogue.
```

The template uses a small custom `cond` function (Go's text/template has no ternary). Register it in `renderVulnSummaryText` by changing the parse call:

Replace this block in `renderVulnSummaryText`:

```go
	tmpl, err := template.ParseFS(vulnSummaryFS, "templates/vulns_summary.txt.tmpl")
```

with:

```go
	tmpl, err := template.New("vulns_summary.txt.tmpl").Funcs(template.FuncMap{
		"cond": func(b bool, t, f string) string {
			if b {
				return t
			}
			return f
		},
	}).ParseFS(vulnSummaryFS, "templates/vulns_summary.txt.tmpl")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/email/... -run TestVuln -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/email/vulns_summary.go internal/email/vulns_summary_test.go internal/email/templates/vulns_summary.txt.tmpl
git commit -m "feat: built vulns summary email view and plain-text body"
```

---

## Task 5: Vulns summary — HTML body (Operations Dashboard)

**Files:**
- Modify: `internal/email/vulns_summary.go`
- Modify: `internal/email/vulns_summary_test.go`
- Create: `internal/email/templates/vulns_summary.html.tmpl`

- [ ] **Step 1: Write the failing test**

Append to `internal/email/vulns_summary_test.go`:

```go
func TestRenderVulnSummaryHTML(t *testing.T) {
	view := buildVulnSummaryView(sampleVulns(), Options{Tenant: "flipgroup"}.withDefaults())
	got, err := renderVulnSummaryHTML(view)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		`bgcolor="#0f172a"`,                                // masthead colour
		`>CVE-2024-3094<`,                                  // CVE text in body
		`href="https://nvd.nist.gov/vuln/detail/CVE-2024-3094"`,
		`href="https://www.cve.org/CVERecord?id=CVE-2024-3094"`,
		`>KEV<`,                                            // KEV pill text
		`Critical`,                                         // severity pill text
		`openssh-server`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("HTML missing %q", want)
		}
	}
}

func TestRenderVulnSummaryHTMLEmpty(t *testing.T) {
	view := buildVulnSummaryView(nil, Options{Tenant: "flipgroup"}.withDefaults())
	got, err := renderVulnSummaryHTML(view)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(got, "No matching vulnerabilities") {
		t.Errorf("expected empty marker, got:\n%s", got)
	}
}

func TestRenderVulnSummaryHTMLEscapesUnsafeInput(t *testing.T) {
	view := buildVulnSummaryView([]iru.Vulnerability{{
		CVEID:    "CVE-XSS-1",
		Severity: "Critical",
		Software: []string{`<script>alert(1)</script>`},
	}}, Options{}.withDefaults())
	got, err := renderVulnSummaryHTML(view)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if strings.Contains(got, "<script>alert(1)</script>") {
		t.Errorf("unescaped <script> tag leaked into HTML output")
	}
	if !strings.Contains(got, "&lt;script&gt;") {
		t.Errorf("expected escaped script tag in output")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/email/... -run TestRenderVulnSummaryHTML -count=1`
Expected: compile error — `renderVulnSummaryHTML` not defined.

- [ ] **Step 3: Implement HTML renderer and template**

Append to `internal/email/vulns_summary.go`:

```go
import "html/template"

func renderVulnSummaryHTML(v vulnSummaryView) (string, error) {
	tmpl, err := template.New("vulns_summary.html.tmpl").Funcs(template.FuncMap{
		"sevRowBG": func(class string) string {
			switch class {
			case "crit":
				return "#dc2626"
			case "high":
				return "#ea580c"
			case "med":
				return "#ca8a04"
			default:
				return "#64748b"
			}
		},
		"sevPillBG": func(class string) string {
			switch class {
			case "crit":
				return "#fee2e2"
			case "high":
				return "#ffedd5"
			case "med":
				return "#fef3c7"
			default:
				return "#f1f5f9"
			}
		},
		"sevPillFG": func(class string) string {
			switch class {
			case "crit":
				return "#991b1b"
			case "high":
				return "#9a3412"
			case "med":
				return "#854d0e"
			default:
				return "#334155"
			}
		},
	}).ParseFS(vulnSummaryFS, "templates/vulns_summary.html.tmpl")
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

Note: this file now imports both `text/template` (top of file from Task 4) and `html/template`. Disambiguate by aliasing in the import block at the top:

Replace the current import block at the top of `vulns_summary.go` with:

```go
import (
	"embed"
	"fmt"
	htmltmpl "html/template"
	"strings"
	texttmpl "text/template"

	"flipgroup.com.au/jellyfish/internal/iru"
)
```

Update `renderVulnSummaryText` to use `texttmpl.New(...)` instead of `template.New(...)`, and `renderVulnSummaryHTML` to use `htmltmpl.New(...)`. Delete the duplicate `import "html/template"` line that was appended above; the aliased imports cover both.

Create `internal/email/templates/vulns_summary.html.tmpl`:

```html
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vulnerability summary</title>
<style>
@media (prefers-color-scheme: dark) {
  body, table { background:#0f172a !important; color:#e2e8f0 !important; }
}
</style>
</head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#0f172a;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:20px 0;">
<tr><td align="center">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;width:600px;max-width:600px;">

<!-- Masthead -->
<tr><td bgcolor="#0f172a" style="background:#0f172a;color:#f8fafc;padding:18px 24px;">
  <div style="font:700 10px/1 'SF Mono','Menlo','Consolas',monospace;letter-spacing:.14em;padding:4px 7px;background:rgba(255,255,255,0.12);border-radius:3px;display:inline-block;">JELLYFISH / VULNS</div>
  <div style="font:700 20px/1.2 -apple-system,system-ui,sans-serif;margin:10px 0 4px;">Fleet vulnerability summary</div>
  <div style="font:12px/1.4 -apple-system,system-ui,sans-serif;opacity:.7;">{{.GeneratedAtStr}}{{if .Tenant}} - {{.Tenant}}{{end}} - {{.TotalCVEs}} CVEs across {{.DeviceCount}} devices (max per CVE)</div>
</td></tr>

<!-- KPI tiles -->
<tr><td style="padding:0;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="1" bgcolor="#e2e8f0" style="border-collapse:separate;border-spacing:1px;background:#e2e8f0;width:100%;">
<tr>
  <td bgcolor="#ffffff" align="left" style="background:#ffffff;padding:14px 12px;width:25%;">
    <div style="font:800 26px/1 -apple-system,system-ui,sans-serif;letter-spacing:-0.02em;color:#dc2626;">{{.CriticalCount}}</div>
    <div style="font:600 10px/1.2 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;margin-top:6px;">Critical</div>
  </td>
  <td bgcolor="#ffffff" align="left" style="background:#ffffff;padding:14px 12px;width:25%;">
    <div style="font:800 26px/1 -apple-system,system-ui,sans-serif;letter-spacing:-0.02em;color:#ea580c;">{{.HighCount}}</div>
    <div style="font:600 10px/1.2 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;margin-top:6px;">High</div>
  </td>
  <td bgcolor="#ffffff" align="left" style="background:#ffffff;padding:14px 12px;width:25%;">
    <div style="font:800 26px/1 -apple-system,system-ui,sans-serif;letter-spacing:-0.02em;color:#7c3aed;">{{.KEVCount}}</div>
    <div style="font:600 10px/1.2 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;margin-top:6px;">KEV listed</div>
  </td>
  <td bgcolor="#ffffff" align="left" style="background:#ffffff;padding:14px 12px;width:25%;">
    <div style="font:800 26px/1 -apple-system,system-ui,sans-serif;letter-spacing:-0.02em;color:#0f172a;">{{.DeviceCount}}</div>
    <div style="font:600 10px/1.2 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;margin-top:6px;">Devices</div>
  </td>
</tr>
</table>
</td></tr>

<!-- Section heading -->
<tr><td style="padding:16px 24px 10px;border-bottom:1px solid #e2e8f0;font:700 12px/1 -apple-system,system-ui,sans-serif;color:#0f172a;letter-spacing:.1em;text-transform:uppercase;">
  Top by priority
</td></tr>

{{if eq (len .Rows) 0}}
<tr><td style="padding:24px;font:14px/1.5 -apple-system,system-ui,sans-serif;color:#64748b;">No matching vulnerabilities.</td></tr>
{{else}}
<!-- Data table -->
<tr><td style="padding:0;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;">
<thead>
<tr>
  <th align="left" style="font:700 10px/1 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;padding:12px 8px 8px 24px;border-bottom:1px solid #e2e8f0;">CVE</th>
  <th align="left" style="font:700 10px/1 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;padding:12px 8px 8px;border-bottom:1px solid #e2e8f0;">Sev</th>
  <th align="right" style="font:700 10px/1 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;padding:12px 8px 8px;border-bottom:1px solid #e2e8f0;">CVSS</th>
  <th align="left" style="font:700 10px/1 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;padding:12px 8px 8px;border-bottom:1px solid #e2e8f0;">KEV</th>
  <th align="right" style="font:700 10px/1 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;padding:12px 8px 8px;border-bottom:1px solid #e2e8f0;">Devices</th>
  <th align="left" style="font:700 10px/1 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;padding:12px 8px 8px 8px;border-bottom:1px solid #e2e8f0;padding-right:24px;">Software</th>
</tr>
</thead>
<tbody>
{{range .Rows}}
<tr>
  <td valign="middle" style="padding:0;border-bottom:1px solid #f1f5f9;">
    <table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
      <tr>
        <td bgcolor="{{sevRowBG .SeverityClass}}" width="3" style="width:3px;background:{{sevRowBG .SeverityClass}};">&nbsp;</td>
        <td style="padding:12px 8px 12px 21px;font:600 12px/1 'SF Mono','Menlo','Consolas',monospace;">
          <a href="{{.NVDLink}}" style="color:#0f172a;text-decoration:none;border-bottom:1px dotted #94a3b8;padding-bottom:1px;">{{.CVEID}}</a>
          <a href="{{.MITRELink}}" style="font-size:10px;color:#94a3b8;text-decoration:none;margin-left:4px;">(M)</a>
        </td>
      </tr>
    </table>
  </td>
  <td valign="middle" style="padding:12px 8px;border-bottom:1px solid #f1f5f9;">
    <table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
      <tr><td bgcolor="{{sevPillBG .SeverityClass}}" style="background:{{sevPillBG .SeverityClass}};color:{{sevPillFG .SeverityClass}};font:700 9px/1 -apple-system,system-ui,sans-serif;letter-spacing:.08em;text-transform:uppercase;padding:4px 6px;border-radius:3px;">{{.Severity}}</td></tr>
    </table>
  </td>
  <td valign="middle" align="right" style="padding:12px 8px;border-bottom:1px solid #f1f5f9;font:600 13px/1 'SF Mono','Menlo','Consolas',monospace;color:#0f172a;">{{.CVSSStr}}</td>
  <td valign="middle" style="padding:12px 8px;border-bottom:1px solid #f1f5f9;">
    {{if .IsKEV}}
    <table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
      <tr><td bgcolor="#ede9fe" style="background:#ede9fe;color:#6d28d9;font:700 9px/1 -apple-system,system-ui,sans-serif;letter-spacing:.08em;text-transform:uppercase;padding:4px 6px;border-radius:3px;">KEV</td></tr>
    </table>
    {{else}}<span style="color:#94a3b8;">-</span>{{end}}
  </td>
  <td valign="middle" align="right" style="padding:12px 8px;border-bottom:1px solid #f1f5f9;font:600 13px/1 'SF Mono','Menlo','Consolas',monospace;color:#0f172a;">{{.Devices}}</td>
  <td valign="middle" style="padding:12px 8px 12px 8px;padding-right:24px;border-bottom:1px solid #f1f5f9;font:12px/1.4 -apple-system,system-ui,sans-serif;color:#334155;">{{.Software}}</td>
</tr>
{{end}}
</tbody>
</table>
</td></tr>
{{end}}

<!-- Footer -->
<tr><td bgcolor="#f8fafc" style="background:#f8fafc;padding:14px 24px 18px;font:11px/1.5 -apple-system,system-ui,sans-serif;color:#64748b;border-top:1px solid #e2e8f0;">
Generated by jellyfish vulns summary. KEV = CISA Known Exploited Vulnerabilities catalogue.
</td></tr>

</table>
</td></tr>
</table>
</body>
</html>
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/email/... -count=1`
Expected: PASS for every test in Tasks 2-5.

- [ ] **Step 5: Commit**

```bash
git add internal/email/vulns_summary.go internal/email/vulns_summary_test.go internal/email/templates/vulns_summary.html.tmpl
git commit -m "feat: added Operations Dashboard HTML body for vulns summary"
```

---

## Task 6: `NewVulnSummaryRenderer` + golden-file test

**Files:**
- Modify: `internal/email/vulns_summary.go`
- Modify: `internal/email/vulns_summary_test.go`
- Create: `internal/email/testdata/vulns_summary.golden.eml`
- Create: `internal/email/testdata/vulns_summary_empty.golden.eml`

- [ ] **Step 1: Write the failing test**

Append to `internal/email/vulns_summary_test.go`:

```go
import (
	"bytes"
	"flag"
	"os"
	"path/filepath"

	"flipgroup.com.au/jellyfish/internal/output"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite golden testdata files instead of asserting against them")

func goldenAssert(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run with -update-golden to regenerate)", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("golden mismatch for %s\n--- got ---\n%s\n--- want ---\n%s", path, got, want)
	}
}

func newPinnedOpts() Options {
	return Options{
		From:              "Jellyfish <alice@example.com>",
		To:                "secops@example.com",
		Subject:           "Jellyfish vulnerability summary - 2026-05-16",
		Tenant:            "flipgroup",
		GeneratedAt:       time.Date(2026, 5, 16, 10, 42, 0, 0, time.FixedZone("AEST", 10*3600)),
		BoundaryOverride:  "=_jf_FIXEDBOUNDARY00",
		MessageIDOverride: "<fixed-id@example.com>",
	}
}

func TestNewVulnSummaryRendererGolden(t *testing.T) {
	var buf bytes.Buffer
	r := NewVulnSummaryRenderer(newPinnedOpts())
	if err := r.Render(&buf, sampleVulns()); err != nil {
		t.Fatalf("Render: %v", err)
	}
	goldenAssert(t, "vulns_summary.golden.eml", buf.Bytes())
}

func TestNewVulnSummaryRendererGoldenEmpty(t *testing.T) {
	var buf bytes.Buffer
	r := NewVulnSummaryRenderer(newPinnedOpts())
	if err := r.Render(&buf, []iru.Vulnerability{}); err != nil {
		t.Fatalf("Render: %v", err)
	}
	goldenAssert(t, "vulns_summary_empty.golden.eml", buf.Bytes())
}

func TestNewVulnSummaryRendererRejectsWrongType(t *testing.T) {
	r := NewVulnSummaryRenderer(newPinnedOpts())
	err := r.Render(&bytes.Buffer{}, "not a slice of vulnerabilities")
	if err == nil {
		t.Fatal("expected type error")
	}
}

func TestNewVulnSummaryRendererRejectsBadLinkTemplate(t *testing.T) {
	opts := newPinnedOpts()
	opts.CVELinkPrimary = "https://no-token.example/"
	r := NewVulnSummaryRenderer(opts)
	err := r.Render(&bytes.Buffer{}, sampleVulns())
	if err == nil {
		t.Fatal("expected validation error for missing {cve} token")
	}
}

func TestNewVulnSummaryRendererSatisfiesOutputRenderer(t *testing.T) {
	var _ output.Renderer = NewVulnSummaryRenderer(newPinnedOpts())
}

func TestVulnSummaryRoundTripParses(t *testing.T) {
	var buf bytes.Buffer
	if err := NewVulnSummaryRenderer(newPinnedOpts()).Render(&buf, sampleVulns()); err != nil {
		t.Fatalf("Render: %v", err)
	}
	msg, err := mail.ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if msg.Header.Get("Subject") == "" {
		t.Fatal("missing Subject")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/email/... -run TestNewVulnSummary -count=1`
Expected: compile error — `NewVulnSummaryRenderer` not defined.

- [ ] **Step 3: Implement the renderer**

Append to `internal/email/vulns_summary.go`:

```go
import (
	"fmt"
	"io"

	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/output"
)

type vulnSummaryRenderer struct {
	opts Options
}

// NewVulnSummaryRenderer returns an output.Renderer whose Render(w, v) expects
// v to be []iru.Vulnerability and writes a complete .eml message to w.
func NewVulnSummaryRenderer(opts Options) output.Renderer {
	return &vulnSummaryRenderer{opts: opts.withDefaults()}
}

func (r *vulnSummaryRenderer) Render(w io.Writer, v any) error {
	vs, ok := v.([]iru.Vulnerability)
	if !ok {
		return fmt.Errorf("email vulns summary renderer expected []iru.Vulnerability, got %T", v)
	}
	if err := validateLinkTemplate("primary", r.opts.CVELinkPrimary); err != nil {
		return err
	}
	if err := validateLinkTemplate("secondary", r.opts.CVELinkSecondary); err != nil {
		return err
	}
	if r.opts.From == "" {
		return fmt.Errorf("email renderer requires a non-empty From address")
	}

	view := buildVulnSummaryView(vs, r.opts)

	subject := r.opts.Subject
	if subject == "" {
		subject = "Jellyfish vulnerability summary - " + view.GeneratedDate
	}

	htmlBody, err := renderVulnSummaryHTML(view)
	if err != nil {
		return err
	}
	textBody, err := renderVulnSummaryText(view)
	if err != nil {
		return err
	}

	boundary := r.opts.BoundaryOverride
	if boundary == "" {
		boundary, err = randomBoundary()
		if err != nil {
			return err
		}
	}
	messageID := r.opts.MessageIDOverride
	if messageID == "" {
		domain := domainFromAddress(r.opts.From)
		messageID, err = randomMessageID(domain)
		if err != nil {
			return err
		}
	}

	bytesOut, err := assembleMessage(messageHeaders{
		From:    r.opts.From,
		To:      r.opts.To,
		Subject: subject,
		Date:    r.opts.GeneratedAt,
	}, htmlBody, textBody, boundary, messageID)
	if err != nil {
		return err
	}
	_, err = w.Write(bytesOut)
	return err
}

// domainFromAddress extracts the right-hand side of an email address used in
// a From header. Returns "localhost" if no '@' is present (defensive only).
func domainFromAddress(addr string) string {
	at := strings.LastIndex(addr, "@")
	if at < 0 {
		return "localhost"
	}
	rest := addr[at+1:]
	if end := strings.IndexAny(rest, "> "); end >= 0 {
		rest = rest[:end]
	}
	return rest
}
```

Merge the new imports — final top-of-file import block for `internal/email/vulns_summary.go`:

```go
import (
	"embed"
	"fmt"
	htmltmpl "html/template"
	"io"
	"strings"
	texttmpl "text/template"

	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/output"
)
```

- [ ] **Step 4: Generate the golden files**

Run: `go test ./internal/email/... -run TestNewVulnSummary -update-golden -count=1`
Expected: PASS, with `internal/email/testdata/vulns_summary.golden.eml` and `internal/email/testdata/vulns_summary_empty.golden.eml` now present.

Run again without the flag to confirm: `go test ./internal/email/... -run TestNewVulnSummary -count=1`
Expected: PASS.

- [ ] **Step 5: Spot-check the golden by opening it**

Run: `open internal/email/testdata/vulns_summary.golden.eml`
Expected: macOS Mail.app opens the message. Confirm visually that the masthead, KPI tiles, and CVE table render. (This is a manual sanity check — the test asserts byte equality regardless.)

- [ ] **Step 6: Run the full test suite**

Run: `go test ./... -count=1`
Expected: PASS across the whole module.

- [ ] **Step 7: Commit**

```bash
git add internal/email/vulns_summary.go internal/email/vulns_summary_test.go internal/email/testdata/
git commit -m "feat: added NewVulnSummaryRenderer with golden-file tests"
```

---

## Task 7: `cmd/email.go` — `emailOpts` helper with git fallback

**Files:**
- Create: `cmd/email.go`
- Create: `cmd/email_test.go`

- [ ] **Step 1: Write the failing test**

Create `cmd/email_test.go`:

```go
package cmd

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/email"
)

func TestResolveEmailOptionsFlagsBeatConfig(t *testing.T) {
	got, err := resolveEmailOptions(emailFlagValues{
		To: "flag-to@example.com", From: "flag-from@example.com", Subject: "flag-subject",
	}, config.Profile{
		Subdomain: "acme",
		Email: config.EmailConfig{
			From: "config-from@example.com", DefaultTo: "config-to@example.com",
			SubjectTemplate: "ignored",
		},
	}, fixedGitEmail("git@example.com"), time.Date(2026, 5, 16, 10, 42, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.From != "flag-from@example.com" {
		t.Errorf("From: got %q", got.From)
	}
	if got.To != "flag-to@example.com" {
		t.Errorf("To: got %q", got.To)
	}
	if got.Subject != "flag-subject" {
		t.Errorf("Subject: got %q", got.Subject)
	}
	if got.Tenant != "acme" {
		t.Errorf("Tenant: got %q", got.Tenant)
	}
}

func TestResolveEmailOptionsFallsBackToConfigThenGit(t *testing.T) {
	got, err := resolveEmailOptions(emailFlagValues{}, config.Profile{
		Subdomain: "acme",
		Email: config.EmailConfig{
			DefaultTo: "secops@example.com",
		},
	}, fixedGitEmail("git@example.com"), time.Date(2026, 5, 16, 10, 42, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.From != "git@example.com" {
		t.Errorf("From should fall back to git, got %q", got.From)
	}
	if got.To != "secops@example.com" {
		t.Errorf("To should fall back to config default_to, got %q", got.To)
	}
}

func TestResolveEmailOptionsErrorsWhenNoFromAnywhere(t *testing.T) {
	_, err := resolveEmailOptions(emailFlagValues{}, config.Profile{},
		fixedGitEmail(""), time.Date(2026, 5, 16, 10, 42, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected error when From is empty everywhere")
	}
}

func TestResolveEmailOptionsRendersSubjectTemplate(t *testing.T) {
	got, err := resolveEmailOptions(emailFlagValues{}, config.Profile{
		Email: config.EmailConfig{
			From:            "alice@example.com",
			SubjectTemplate: "Weekly brief - {{.Date}}",
		},
	}, fixedGitEmail(""), time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	want := "Weekly brief - 2026-05-16"
	if got.Subject != want {
		t.Errorf("Subject: got %q want %q", got.Subject, want)
	}
}

func TestResolveEmailOptionsAppliesLinkTemplates(t *testing.T) {
	got, err := resolveEmailOptions(emailFlagValues{}, config.Profile{
		Email: config.EmailConfig{
			From:             "alice@example.com",
			CVELinkPrimary:   "https://x.test/{cve}",
			CVELinkSecondary: "https://y.test/{cve}",
		},
	}, fixedGitEmail(""), time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.CVELinkPrimary != "https://x.test/{cve}" {
		t.Errorf("primary: %q", got.CVELinkPrimary)
	}
	if got.CVELinkSecondary != "https://y.test/{cve}" {
		t.Errorf("secondary: %q", got.CVELinkSecondary)
	}
}

func TestGitUserEmailUsesPATHStub(t *testing.T) {
	dir := t.TempDir()
	stub := filepath.Join(dir, "git")
	script := "#!/bin/sh\nif [ \"$1\" = \"config\" ] && [ \"$2\" = \"user.email\" ]; then echo stubbed@example.com; exit 0; fi\nexit 1\n"
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	t.Setenv("PATH", dir)
	got, err := gitUserEmail()
	if err != nil {
		t.Fatalf("gitUserEmail: %v", err)
	}
	if got != "stubbed@example.com" {
		t.Errorf("got %q", got)
	}
}

// fixedGitEmail returns a gitEmailLookup that always returns the given value
// (empty string indicates "no git email found"; nil err in both cases).
func fixedGitEmail(value string) gitEmailLookup {
	return func() (string, error) { return value, nil }
}

// silence unused import
var _ = email.Options{}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/... -run TestResolveEmailOptions -count=1`
Expected: compile error — `resolveEmailOptions`, `emailFlagValues`, `gitEmailLookup`, `gitUserEmail` not defined.

- [ ] **Step 3: Implement the helper**

Create `cmd/email.go`:

```go
package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"text/template"
	"time"

	"github.com/spf13/cobra"

	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/email"
)

// emailFlagValues holds the literal flag inputs from cobra; empty string means
// the flag was not set.
type emailFlagValues struct {
	To      string
	From    string
	Subject string
}

// gitEmailLookup is the function signature for "find a from address by asking
// git". Production passes gitUserEmail; tests inject a fixture.
type gitEmailLookup func() (string, error)

// readEmailFlags pulls --email-to / --email-from / --email-subject off a cobra
// command. Missing flags return empty strings (no error).
func readEmailFlags(cmd *cobra.Command) emailFlagValues {
	to, _ := cmd.Flags().GetString("email-to")
	from, _ := cmd.Flags().GetString("email-from")
	subject, _ := cmd.Flags().GetString("email-subject")
	return emailFlagValues{To: to, From: from, Subject: subject}
}

// resolveEmailOptions applies the precedence:
//
//	From    : flag > config.Email.From > git user.email > error
//	To      : flag > config.Email.DefaultTo > "" (renderer prints <unspecified>)
//	Subject : flag > rendered config.Email.SubjectTemplate > "" (renderer's default)
//
// CVE link templates and tenant are pulled from config; renderer fills in
// built-in defaults for unset link templates.
func resolveEmailOptions(flags emailFlagValues, prof config.Profile, lookupGit gitEmailLookup, now time.Time) (email.Options, error) {
	opts := email.Options{
		Tenant:           prof.Subdomain,
		GeneratedAt:      now,
		To:               firstNonEmpty(flags.To, prof.Email.DefaultTo),
		CVELinkPrimary:   prof.Email.CVELinkPrimary,
		CVELinkSecondary: prof.Email.CVELinkSecondary,
	}

	opts.From = firstNonEmpty(flags.From, prof.Email.From)
	if opts.From == "" && lookupGit != nil {
		gitVal, err := lookupGit()
		if err == nil {
			opts.From = strings.TrimSpace(gitVal)
		}
	}
	if opts.From == "" {
		return email.Options{}, errors.New(`no email from address - set --email-from, email.from in config, or configure git user.email`)
	}

	switch {
	case flags.Subject != "":
		opts.Subject = flags.Subject
	case prof.Email.SubjectTemplate != "":
		rendered, err := renderSubjectTemplate(prof.Email.SubjectTemplate, now)
		if err != nil {
			return email.Options{}, fmt.Errorf("render email subject_template: %w", err)
		}
		opts.Subject = rendered
	}
	return opts, nil
}

func renderSubjectTemplate(tmplStr string, now time.Time) (string, error) {
	tmpl, err := template.New("subject").Parse(tmplStr)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	data := struct {
		Date string
		Time string
	}{
		Date: now.Format("2006-01-02"),
		Time: now.Format("15:04"),
	}
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// gitUserEmail shells out to `git config user.email`. Returns "" with nil
// error when the command runs but produces no value; returns an error only
// when git itself cannot be invoked (PATH miss, etc.).
func gitUserEmail() (string, error) {
	cmd := exec.Command("git", "config", "user.email")
	out, err := cmd.Output()
	if err != nil {
		// Distinguish "git not on PATH" (real error) from "no value set"
		// (silent empty). exec.LookPath gives the cleanest signal.
		if _, lookErr := exec.LookPath("git"); lookErr != nil {
			return "", fmt.Errorf("git not found on PATH: %w", lookErr)
		}
		// git ran, returned non-zero - treat as "no value set".
		return "", nil
	}
	return strings.TrimSpace(string(out)), nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/... -run "TestResolveEmail|TestGitUserEmail" -count=1`
Expected: PASS for all six tests.

- [ ] **Step 5: Commit**

```bash
git add cmd/email.go cmd/email_test.go
git commit -m "feat: added email option resolver with git fallback"
```

---

## Task 8: Wire `-o email` into `vulns summary` + register flags

**Files:**
- Modify: `cmd/vulns.go`
- Modify: `cmd/vulns_test.go`

- [ ] **Step 1: Write the failing test**

Append to `cmd/vulns_test.go`:

```go
import (
	"net/mail"
	"time"
)

func TestVulnsSummaryEmailWritesEml(t *testing.T) {
	client := &fakeClient{vulnerabilities: []iru.Vulnerability{
		{CVEID: "CVE-A", Severity: "Critical", CVSSScore: 9.5, KEVScore: 1, DeviceCount: 2, Status: "Active", Software: []string{"foo"}},
	}}
	buf := &bytes.Buffer{}
	opts := vulnsSummaryOpts{Output: "email", NoCache: true, EmailFlags: emailFlagValues{
		From:    "alice@example.com",
		To:      "secops@example.com",
		Subject: "test subject",
	}, EmailNow: time.Date(2026, 5, 16, 10, 42, 0, 0, time.UTC)}
	if err := runVulnsSummary(context.Background(), client, buf, io.Discard, opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	msg, err := mail.ReadMessage(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("parse: %v\nraw:\n%s", err, buf.String())
	}
	if got := msg.Header.Get("Subject"); got != "test subject" {
		t.Errorf("Subject: got %q", got)
	}
	if got := msg.Header.Get("From"); got != "alice@example.com" {
		t.Errorf("From: got %q", got)
	}
}

func TestVulnsSummaryEmailErrorsWithoutFrom(t *testing.T) {
	client := &fakeClient{vulnerabilities: []iru.Vulnerability{{CVEID: "CVE-A", Severity: "Critical"}}}
	err := runVulnsSummary(context.Background(), client, &bytes.Buffer{}, io.Discard, vulnsSummaryOpts{
		Output: "email", NoCache: true,
	})
	if err == nil {
		t.Fatal("expected error when no From address available")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/... -run TestVulnsSummaryEmail -count=1`
Expected: compile error — `vulnsSummaryOpts` has no `EmailFlags` / `EmailNow` field; "email" format not handled in `renderVulns`.

- [ ] **Step 3: Wire the dispatch**

Edit `cmd/vulns.go`:

Extend `vulnsSummaryOpts`:

```go
type vulnsSummaryOpts struct {
	Status     string
	Severity   string
	Sort       string
	Limit      int
	Output     string
	NoCache    bool
	EmailFlags emailFlagValues // populated in RunE when Output == "email"
	EmailNow   time.Time       // pinned in tests; cmd layer sets time.Now()
	Profile    config.Profile  // populated in RunE
}
```

Adjust the imports of `cmd/vulns.go` to include `time` and `flipgroup.com.au/jellyfish/internal/config`.

Inside `newVulnsSummaryCmd`, after the existing `c.Flags()` calls, register the email flags:

```go
	c.Flags().String("email-to", "", "Email To: header (default: email.default_to from config)")
	c.Flags().String("email-from", "", "Email From: header (default: email.from from config, then git user.email)")
	c.Flags().String("email-subject", "", "Email Subject: header (default: rendered email.subject_template or a per-command default)")
```

Inside the `RunE` closure, pull the email values before calling `runVulnsSummary`:

```go
		RunE: func(cmd *cobra.Command, _ []string) error {
			outFmt, _ := cmd.Flags().GetString("output")
			opts.Output = outFmt
			opts.EmailFlags = readEmailFlags(cmd)
			opts.EmailNow = time.Now()
			if outFmt == "email" {
				prof, err := activeProfile(cmd)
				if err != nil {
					return err
				}
				opts.Profile = prof
			}
			client, err := buildClient(cmd)
			if err != nil {
				return err
			}
			return runVulnsSummary(cmd.Context(), client, cmd.OutOrStdout(), cmd.ErrOrStderr(), opts)
		},
```

Add `activeProfile` to `cmd/client.go` (next to `buildClient`):

```go
// activeProfile returns the named profile from config (only "default" honoured
// today). Shares its config-load semantics with buildClient.
func activeProfile(cmd *cobra.Command) (config.Profile, error) {
	cfgPath, _ := cmd.Flags().GetString("config")
	if cfgPath == "" {
		p, err := config.DefaultPath()
		if err != nil {
			return config.Profile{}, err
		}
		cfgPath = p
	}
	f, err := config.Load(cfgPath)
	if err != nil {
		return config.Profile{}, nil // config missing is fine - flags/git can still supply email From
	}
	if prof, ok := f["default"]; ok {
		return prof, nil
	}
	return config.Profile{}, nil
}
```

Update `renderVulns` in `cmd/vulns.go` to handle the email case by signature change (it now needs the email options). Replace the existing `renderVulns(w io.Writer, format string, vs []iru.Vulnerability) error` and its caller with:

```go
func renderVulns(w io.Writer, opts vulnsSummaryOpts, vs []iru.Vulnerability) error {
	switch opts.Output {
	case "table", "":
		t := output.Table().WithColumns(vulnColumns())
		return t.Render(w, vs)
	case "csv":
		c := output.CSV().WithColumns(vulnColumns())
		return c.Render(w, vs)
	case "email":
		now := opts.EmailNow
		if now.IsZero() {
			now = time.Now()
		}
		emailOpts, err := resolveEmailOptions(opts.EmailFlags, opts.Profile, gitUserEmail, now)
		if err != nil {
			return err
		}
		return email.NewVulnSummaryRenderer(emailOpts).Render(w, vs)
	}
	r, err := output.For(opts.Output)
	if err != nil {
		return err
	}
	return r.Render(w, vs)
}
```

Update `runVulnsSummary` to call the new signature:

```go
	return renderVulns(w, opts, filtered)
```

Adjust the import block at the top of `cmd/vulns.go`:

```go
import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/email"
	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/output"
)
```

The existing `renderDetections` function and the `vulns list` command are unchanged.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/... -count=1`
Expected: PASS for all existing tests plus the two new email tests. If existing tests like `TestVulnsSummaryFiltersByStatus` fail because `renderVulns` no longer takes `format string`, recheck the signature change applied above.

- [ ] **Step 5: Commit**

```bash
git add cmd/vulns.go cmd/vulns_test.go cmd/client.go
git commit -m "feat: wired -o email into vulns summary"
```

---

## Task 9: User show — presentation struct + plain-text body

**Files:**
- Create: `internal/email/user_show.go`
- Create: `internal/email/user_show_test.go`
- Create: `internal/email/templates/user_show.txt.tmpl`

- [ ] **Step 1: Write the failing test**

Create `internal/email/user_show_test.go`:

```go
package email

import (
	"strings"
	"testing"
	"time"

	"flipgroup.com.au/jellyfish/internal/iru"
)

// UserBundleInput matches the cmd-layer UserBundle shape. The email package
// is decoupled from cmd: it accepts this typed alias so cmd can pass its own
// UserBundle directly.
type UserBundleInput struct {
	User    iru.User
	Devices []UserBundleDevice
}

type UserBundleDevice struct {
	Device     iru.Device
	Detections []iru.Detection
}

func sampleUserBundle() UserBundleInput {
	return UserBundleInput{
		User: iru.User{ID: "u-1", Name: "Alice Example", Email: "alice@example.com"},
		Devices: []UserBundleDevice{
			{
				Device: iru.Device{DeviceID: "d-1", DeviceName: "Alice MBP", SerialNumber: "SN1", OSVersion: "14.4"},
				Detections: []iru.Detection{
					{CVEID: "CVE-2024-3094", Severity: "Critical", CVSSScore: 10.0, Name: "xz-utils", Version: "5.6.1"},
					{CVEID: "CVE-2024-6387", Severity: "Critical", CVSSScore: 8.1, Name: "openssh-server", Version: "9.6"},
				},
			},
			{
				Device:     iru.Device{DeviceID: "d-2", DeviceName: "Alice iPad", SerialNumber: "SN2"},
				Detections: nil,
			},
		},
	}
}

func TestBuildUserShowView(t *testing.T) {
	view := buildUserShowView(sampleUserBundle(), Options{
		GeneratedAt: time.Date(2026, 5, 16, 10, 42, 0, 0, time.UTC),
	}.withDefaults())
	if view.User.Name != "Alice Example" {
		t.Errorf("User.Name: got %q", view.User.Name)
	}
	if view.TotalCVEs != 2 {
		t.Errorf("TotalCVEs: got %d want 2", view.TotalCVEs)
	}
	if view.CriticalCount != 2 {
		t.Errorf("CriticalCount: got %d want 2", view.CriticalCount)
	}
	if len(view.Devices) != 2 {
		t.Fatalf("Devices: got %d", len(view.Devices))
	}
	if len(view.Devices[0].Rows) != 2 {
		t.Errorf("device 0 rows: got %d", len(view.Devices[0].Rows))
	}
	if len(view.Devices[1].Rows) != 0 {
		t.Errorf("device 1 rows: got %d want 0", len(view.Devices[1].Rows))
	}
}

func TestRenderUserShowText(t *testing.T) {
	view := buildUserShowView(sampleUserBundle(), Options{}.withDefaults())
	got, err := renderUserShowText(view)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		"Alice Example",
		"2 Critical",
		"Alice MBP",
		"CVE-2024-3094",
		"Alice iPad",
		"(no detections)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("plain text missing %q\noutput:\n%s", want, got)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/email/... -run "TestBuildUserShow|TestRenderUserShowText" -count=1`
Expected: compile error — types and functions not defined.

- [ ] **Step 3: Implement the user show view and text renderer**

Create `internal/email/user_show.go`:

```go
package email

import (
	"embed"
	"fmt"
	htmltmpl "html/template"
	"strings"
	texttmpl "text/template"

	"flipgroup.com.au/jellyfish/internal/iru"
)

//go:embed templates/user_show.txt.tmpl templates/user_show.html.tmpl
var userShowFS embed.FS

// userShowView is the data shape user_show templates render against.
type userShowView struct {
	User           iru.User
	Tenant         string
	GeneratedAtStr string
	GeneratedDate  string
	TotalCVEs      int
	CriticalCount  int
	HighCount      int
	MediumCount    int
	LowCount       int
	DeviceCount    int
	Devices        []userShowDeviceView
}

type userShowDeviceView struct {
	Device iru.Device
	Rows   []userShowRow
}

type userShowRow struct {
	CVEID         string
	Severity      string
	SeverityClass string
	CVSS          float64
	CVSSStr       string
	Package       string // "name@version"
	NVDLink       string
	MITRELink     string
}

func buildUserShowView(b UserBundleInput, opts Options) userShowView {
	view := userShowView{
		User:           b.User,
		Tenant:         opts.Tenant,
		GeneratedAtStr: opts.GeneratedAt.Format("2 Jan 2006 - 15:04 MST"),
		GeneratedDate:  opts.GeneratedAt.Format("2006-01-02"),
		DeviceCount:    len(b.Devices),
		Devices:        make([]userShowDeviceView, len(b.Devices)),
	}
	for i, dev := range b.Devices {
		rows := make([]userShowRow, 0, len(dev.Detections))
		for _, det := range dev.Detections {
			switch strings.ToLower(det.Severity) {
			case "critical":
				view.CriticalCount++
			case "high":
				view.HighCount++
			case "medium":
				view.MediumCount++
			case "low":
				view.LowCount++
			}
			view.TotalCVEs++
			pkg := det.Name
			if det.Version != "" {
				pkg = det.Name + "@" + det.Version
			}
			rows = append(rows, userShowRow{
				CVEID:         det.CVEID,
				Severity:      det.Severity,
				SeverityClass: severityClass(det.Severity),
				CVSS:          det.CVSSScore,
				CVSSStr:       fmt.Sprintf("%.1f", det.CVSSScore),
				Package:       pkg,
				NVDLink:       buildCVELink(opts.CVELinkPrimary, det.CVEID),
				MITRELink:     buildCVELink(opts.CVELinkSecondary, det.CVEID),
			})
		}
		view.Devices[i] = userShowDeviceView{Device: dev.Device, Rows: rows}
	}
	return view
}

func renderUserShowText(v userShowView) (string, error) {
	tmpl, err := texttmpl.New("user_show.txt.tmpl").Funcs(texttmpl.FuncMap{
		"cond": func(b bool, t, f string) string {
			if b {
				return t
			}
			return f
		},
	}).ParseFS(userShowFS, "templates/user_show.txt.tmpl")
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	if err := tmpl.Execute(&sb, v); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// Reserved for Task 10.
var _ = htmltmpl.New
```

Create `internal/email/templates/user_show.txt.tmpl`:

```
Vulnerability exposure for {{.User.Name}}{{if .User.Email}} ({{.User.Email}}){{end}}.

{{.CriticalCount}} Critical, {{.HighCount}} High, {{.MediumCount}} Medium, {{.LowCount}} Low across {{.DeviceCount}} device(s).
Generated {{.GeneratedAtStr}}{{if .Tenant}} for tenant {{.Tenant}}{{end}}.

{{range .Devices}}
{{.Device.DeviceName}} ({{.Device.SerialNumber}}){{if .Device.OSVersion}} - macOS {{.Device.OSVersion}}{{end}}
{{if eq (len .Rows) 0 -}}
  (no detections)
{{else -}}
  CVE              SEVERITY  CVSS  PACKAGE
{{range .Rows -}}
  {{printf "%-16s %-9s %-5s %s" .CVEID .Severity .CVSSStr .Package}}
{{end -}}
{{- end -}}
{{end}}
KEV = CISA Known Exploited Vulnerabilities catalogue.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/email/... -run "TestBuildUserShow|TestRenderUserShowText" -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/email/user_show.go internal/email/user_show_test.go internal/email/templates/user_show.txt.tmpl
git commit -m "feat: built user show email view and plain-text body"
```

---

## Task 10: User show — HTML body (per-device sections)

**Files:**
- Modify: `internal/email/user_show.go`
- Modify: `internal/email/user_show_test.go`
- Create: `internal/email/templates/user_show.html.tmpl`

- [ ] **Step 1: Write the failing test**

Append to `internal/email/user_show_test.go`:

```go
func TestRenderUserShowHTML(t *testing.T) {
	view := buildUserShowView(sampleUserBundle(), Options{}.withDefaults())
	got, err := renderUserShowHTML(view)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		`bgcolor="#0f172a"`,
		"Alice Example",
		"Alice MBP",
		"Alice iPad",
		`>CVE-2024-3094<`,
		"(no detections)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("HTML missing %q", want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/email/... -run TestRenderUserShowHTML -count=1`
Expected: compile error — `renderUserShowHTML` not defined.

- [ ] **Step 3: Implement HTML renderer + template**

Append to `internal/email/user_show.go`:

```go
func renderUserShowHTML(v userShowView) (string, error) {
	tmpl, err := htmltmpl.New("user_show.html.tmpl").Funcs(htmltmpl.FuncMap{
		"sevRowBG":  sevRowBG,
		"sevPillBG": sevPillBG,
		"sevPillFG": sevPillFG,
	}).ParseFS(userShowFS, "templates/user_show.html.tmpl")
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

The `sevRowBG`, `sevPillBG`, and `sevPillFG` helpers are duplicated from `vulns_summary.go`'s inline closures. Promote them into shared package-level functions. In `vulns_summary.go`, delete the three closures inside `renderVulnSummaryHTML`'s `Funcs(...)` map and replace the call with:

```go
	tmpl, err := htmltmpl.New("vulns_summary.html.tmpl").Funcs(htmltmpl.FuncMap{
		"sevRowBG":  sevRowBG,
		"sevPillBG": sevPillBG,
		"sevPillFG": sevPillFG,
	}).ParseFS(vulnSummaryFS, "templates/vulns_summary.html.tmpl")
```

Add the shared helpers at the bottom of `email.go`:

```go
func sevRowBG(class string) string {
	switch class {
	case "crit":
		return "#dc2626"
	case "high":
		return "#ea580c"
	case "med":
		return "#ca8a04"
	default:
		return "#64748b"
	}
}

func sevPillBG(class string) string {
	switch class {
	case "crit":
		return "#fee2e2"
	case "high":
		return "#ffedd5"
	case "med":
		return "#fef3c7"
	default:
		return "#f1f5f9"
	}
}

func sevPillFG(class string) string {
	switch class {
	case "crit":
		return "#991b1b"
	case "high":
		return "#9a3412"
	case "med":
		return "#854d0e"
	default:
		return "#334155"
	}
}
```

Remove the placeholder `var _ = htmltmpl.New` line from `user_show.go` (the renderer function now uses it).

Create `internal/email/templates/user_show.html.tmpl`:

```html
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vulnerability exposure - {{.User.Name}}</title>
<style>
@media (prefers-color-scheme: dark) {
  body, table { background:#0f172a !important; color:#e2e8f0 !important; }
}
</style>
</head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#0f172a;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:20px 0;">
<tr><td align="center">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;width:600px;max-width:600px;">

<tr><td bgcolor="#0f172a" style="background:#0f172a;color:#f8fafc;padding:18px 24px;">
  <div style="font:700 10px/1 'SF Mono','Menlo','Consolas',monospace;letter-spacing:.14em;padding:4px 7px;background:rgba(255,255,255,0.12);border-radius:3px;display:inline-block;">JELLYFISH / USER</div>
  <div style="font:700 20px/1.2 -apple-system,system-ui,sans-serif;margin:10px 0 4px;">Vulnerability exposure - {{.User.Name}}</div>
  <div style="font:12px/1.4 -apple-system,system-ui,sans-serif;opacity:.7;">{{if .User.Email}}{{.User.Email}} - {{end}}{{.GeneratedAtStr}}{{if .Tenant}} - {{.Tenant}}{{end}} - {{.TotalCVEs}} CVEs across {{.DeviceCount}} device(s)</div>
</td></tr>

<tr><td style="padding:0;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="1" bgcolor="#e2e8f0" style="border-collapse:separate;border-spacing:1px;background:#e2e8f0;width:100%;">
<tr>
  <td bgcolor="#ffffff" align="left" style="background:#ffffff;padding:14px 12px;width:25%;">
    <div style="font:800 26px/1 -apple-system,system-ui,sans-serif;color:#dc2626;">{{.CriticalCount}}</div>
    <div style="font:600 10px/1.2 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;margin-top:6px;">Critical</div>
  </td>
  <td bgcolor="#ffffff" align="left" style="background:#ffffff;padding:14px 12px;width:25%;">
    <div style="font:800 26px/1 -apple-system,system-ui,sans-serif;color:#ea580c;">{{.HighCount}}</div>
    <div style="font:600 10px/1.2 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;margin-top:6px;">High</div>
  </td>
  <td bgcolor="#ffffff" align="left" style="background:#ffffff;padding:14px 12px;width:25%;">
    <div style="font:800 26px/1 -apple-system,system-ui,sans-serif;color:#ca8a04;">{{.MediumCount}}</div>
    <div style="font:600 10px/1.2 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;margin-top:6px;">Medium</div>
  </td>
  <td bgcolor="#ffffff" align="left" style="background:#ffffff;padding:14px 12px;width:25%;">
    <div style="font:800 26px/1 -apple-system,system-ui,sans-serif;color:#0f172a;">{{.DeviceCount}}</div>
    <div style="font:600 10px/1.2 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;margin-top:6px;">Devices</div>
  </td>
</tr>
</table>
</td></tr>

{{range .Devices}}
<tr><td style="padding:16px 24px 10px;border-bottom:1px solid #e2e8f0;font:700 12px/1 -apple-system,system-ui,sans-serif;color:#0f172a;letter-spacing:.1em;text-transform:uppercase;">
  {{.Device.DeviceName}} <span style="color:#64748b;letter-spacing:0;text-transform:none;font-weight:400;">- {{.Device.SerialNumber}}{{if .Device.OSVersion}} - macOS {{.Device.OSVersion}}{{end}}</span>
</td></tr>
{{if eq (len .Rows) 0}}
<tr><td style="padding:14px 24px;font:13px/1.5 -apple-system,system-ui,sans-serif;color:#64748b;">(no detections)</td></tr>
{{else}}
<tr><td style="padding:0;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;">
<thead>
<tr>
  <th align="left" style="font:700 10px/1 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;padding:12px 8px 8px 24px;border-bottom:1px solid #e2e8f0;">CVE</th>
  <th align="left" style="font:700 10px/1 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;padding:12px 8px 8px;border-bottom:1px solid #e2e8f0;">Sev</th>
  <th align="right" style="font:700 10px/1 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;padding:12px 8px 8px;border-bottom:1px solid #e2e8f0;">CVSS</th>
  <th align="left" style="font:700 10px/1 -apple-system,system-ui,sans-serif;color:#64748b;letter-spacing:.08em;text-transform:uppercase;padding:12px 8px 8px 8px;padding-right:24px;border-bottom:1px solid #e2e8f0;">Package</th>
</tr>
</thead>
<tbody>
{{range .Rows}}
<tr>
  <td valign="middle" style="padding:0;border-bottom:1px solid #f1f5f9;">
    <table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
      <tr>
        <td bgcolor="{{sevRowBG .SeverityClass}}" width="3" style="width:3px;background:{{sevRowBG .SeverityClass}};">&nbsp;</td>
        <td style="padding:12px 8px 12px 21px;font:600 12px/1 'SF Mono','Menlo','Consolas',monospace;">
          <a href="{{.NVDLink}}" style="color:#0f172a;text-decoration:none;border-bottom:1px dotted #94a3b8;padding-bottom:1px;">{{.CVEID}}</a>
          <a href="{{.MITRELink}}" style="font-size:10px;color:#94a3b8;text-decoration:none;margin-left:4px;">(M)</a>
        </td>
      </tr>
    </table>
  </td>
  <td valign="middle" style="padding:12px 8px;border-bottom:1px solid #f1f5f9;">
    <table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
      <tr><td bgcolor="{{sevPillBG .SeverityClass}}" style="background:{{sevPillBG .SeverityClass}};color:{{sevPillFG .SeverityClass}};font:700 9px/1 -apple-system,system-ui,sans-serif;letter-spacing:.08em;text-transform:uppercase;padding:4px 6px;border-radius:3px;">{{.Severity}}</td></tr>
    </table>
  </td>
  <td valign="middle" align="right" style="padding:12px 8px;border-bottom:1px solid #f1f5f9;font:600 13px/1 'SF Mono','Menlo','Consolas',monospace;color:#0f172a;">{{.CVSSStr}}</td>
  <td valign="middle" style="padding:12px 8px 12px 8px;padding-right:24px;border-bottom:1px solid #f1f5f9;font:12px/1.4 -apple-system,system-ui,sans-serif;color:#334155;">{{.Package}}</td>
</tr>
{{end}}
</tbody>
</table>
</td></tr>
{{end}}
{{end}}

<tr><td bgcolor="#f8fafc" style="background:#f8fafc;padding:14px 24px 18px;font:11px/1.5 -apple-system,system-ui,sans-serif;color:#64748b;border-top:1px solid #e2e8f0;">
Generated by jellyfish user show. KEV = CISA Known Exploited Vulnerabilities catalogue.
</td></tr>

</table>
</td></tr>
</table>
</body>
</html>
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/email/... -count=1`
Expected: PASS for every test in the email package.

- [ ] **Step 5: Commit**

```bash
git add internal/email/user_show.go internal/email/user_show_test.go internal/email/templates/user_show.html.tmpl internal/email/vulns_summary.go internal/email/email.go
git commit -m "feat: added user show HTML body and promoted severity helpers"
```

---

## Task 11: `NewUserShowRenderer` + golden test

**Files:**
- Modify: `internal/email/user_show.go`
- Modify: `internal/email/user_show_test.go`
- Create: `internal/email/testdata/user_show.golden.eml`
- Create: `internal/email/testdata/user_show_no_detections.golden.eml`

- [ ] **Step 1: Write the failing test**

Append to `internal/email/user_show_test.go`:

```go
import (
	"bytes"
	"net/mail"

	"flipgroup.com.au/jellyfish/internal/output"
)

func newPinnedUserShowOpts() Options {
	return Options{
		From:              "Jellyfish <alice@example.com>",
		To:                "alice@example.com",
		Subject:           "Vulnerability exposure - Alice Example - 2026-05-16",
		Tenant:            "flipgroup",
		GeneratedAt:       time.Date(2026, 5, 16, 10, 42, 0, 0, time.FixedZone("AEST", 10*3600)),
		BoundaryOverride:  "=_jf_FIXEDBOUNDARY01",
		MessageIDOverride: "<fixed-user-id@example.com>",
	}
}

func TestNewUserShowRendererGolden(t *testing.T) {
	var buf bytes.Buffer
	if err := NewUserShowRenderer(newPinnedUserShowOpts()).Render(&buf, sampleUserBundle()); err != nil {
		t.Fatalf("Render: %v", err)
	}
	goldenAssert(t, "user_show.golden.eml", buf.Bytes())
}

func TestNewUserShowRendererGoldenNoDetections(t *testing.T) {
	bundle := UserBundleInput{
		User: iru.User{ID: "u-9", Name: "Bob Empty", Email: "bob@example.com"},
		Devices: []UserBundleDevice{
			{Device: iru.Device{DeviceID: "d-9", DeviceName: "Bob MBA", SerialNumber: "SN9"}, Detections: nil},
		},
	}
	var buf bytes.Buffer
	if err := NewUserShowRenderer(newPinnedUserShowOpts()).Render(&buf, bundle); err != nil {
		t.Fatalf("Render: %v", err)
	}
	goldenAssert(t, "user_show_no_detections.golden.eml", buf.Bytes())
}

func TestNewUserShowRendererSatisfiesOutputRenderer(t *testing.T) {
	var _ output.Renderer = NewUserShowRenderer(newPinnedUserShowOpts())
}

func TestNewUserShowRendererRejectsWrongType(t *testing.T) {
	err := NewUserShowRenderer(newPinnedUserShowOpts()).Render(&bytes.Buffer{}, "nope")
	if err == nil {
		t.Fatal("expected type error")
	}
}

func TestUserShowRoundTripParses(t *testing.T) {
	var buf bytes.Buffer
	if err := NewUserShowRenderer(newPinnedUserShowOpts()).Render(&buf, sampleUserBundle()); err != nil {
		t.Fatalf("Render: %v", err)
	}
	if _, err := mail.ReadMessage(&buf); err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/email/... -run TestNewUserShow -count=1`
Expected: compile error — `NewUserShowRenderer` not defined.

- [ ] **Step 3: Implement the renderer**

Append to `internal/email/user_show.go`:

```go
import (
	"io"

	"flipgroup.com.au/jellyfish/internal/output"
)

type userShowRenderer struct {
	opts Options
}

// NewUserShowRenderer returns an output.Renderer whose Render(w, v) expects
// v to be a UserBundleInput.
func NewUserShowRenderer(opts Options) output.Renderer {
	return &userShowRenderer{opts: opts.withDefaults()}
}

func (r *userShowRenderer) Render(w io.Writer, v any) error {
	bundle, ok := v.(UserBundleInput)
	if !ok {
		return fmt.Errorf("email user show renderer expected UserBundleInput, got %T", v)
	}
	if err := validateLinkTemplate("primary", r.opts.CVELinkPrimary); err != nil {
		return err
	}
	if err := validateLinkTemplate("secondary", r.opts.CVELinkSecondary); err != nil {
		return err
	}
	if r.opts.From == "" {
		return fmt.Errorf("email renderer requires a non-empty From address")
	}

	view := buildUserShowView(bundle, r.opts)

	subject := r.opts.Subject
	if subject == "" {
		who := bundle.User.Name
		if who == "" {
			who = bundle.User.Email
		}
		subject = "Vulnerability exposure - " + who + " - " + view.GeneratedDate
	}

	htmlBody, err := renderUserShowHTML(view)
	if err != nil {
		return err
	}
	textBody, err := renderUserShowText(view)
	if err != nil {
		return err
	}

	boundary := r.opts.BoundaryOverride
	if boundary == "" {
		boundary, err = randomBoundary()
		if err != nil {
			return err
		}
	}
	messageID := r.opts.MessageIDOverride
	if messageID == "" {
		messageID, err = randomMessageID(domainFromAddress(r.opts.From))
		if err != nil {
			return err
		}
	}

	bytesOut, err := assembleMessage(messageHeaders{
		From:    r.opts.From,
		To:      r.opts.To,
		Subject: subject,
		Date:    r.opts.GeneratedAt,
	}, htmlBody, textBody, boundary, messageID)
	if err != nil {
		return err
	}
	_, err = w.Write(bytesOut)
	return err
}
```

Merge the new imports — final top-of-file import block for `internal/email/user_show.go`:

```go
import (
	"embed"
	"fmt"
	htmltmpl "html/template"
	"io"
	"strings"
	texttmpl "text/template"

	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/output"
)
```

- [ ] **Step 4: Generate the golden files**

Run: `go test ./internal/email/... -run TestNewUserShow -update-golden -count=1`
Expected: PASS, with `internal/email/testdata/user_show.golden.eml` and `internal/email/testdata/user_show_no_detections.golden.eml` now present.

Run again without the flag to confirm: `go test ./internal/email/... -run TestNewUserShow -count=1`
Expected: PASS.

- [ ] **Step 5: Spot-check the golden**

Run: `open internal/email/testdata/user_show.golden.eml`
Expected: macOS Mail.app shows the per-device sections rendered.

- [ ] **Step 6: Run the full test suite**

Run: `go test ./... -count=1`
Expected: PASS across the module.

- [ ] **Step 7: Commit**

```bash
git add internal/email/user_show.go internal/email/user_show_test.go internal/email/testdata/
git commit -m "feat: added NewUserShowRenderer with golden-file tests"
```

---

## Task 12: Wire `-o email` into `user show` + register flags

**Files:**
- Modify: `cmd/user.go`
- Modify: `cmd/user_test.go`

- [ ] **Step 1: Write the failing test**

Append to `cmd/user_test.go`:

```go
import (
	"bytes"
	"context"
	"io"
	"net/mail"
	"strings"
	"testing"
	"time"

	"flipgroup.com.au/jellyfish/internal/iru"
)

func TestUserShowEmailWritesEml(t *testing.T) {
	client := &fakeClient{
		users:   []iru.User{{ID: "u-1", Name: "Alice", Email: "alice@example.com"}},
		devices: []iru.Device{{DeviceID: "d-1", DeviceName: "Alice MBP", SerialNumber: "SN1"}},
		detections: []iru.Detection{
			{DeviceID: "d-1", CVEID: "CVE-A", Severity: "Critical", CVSSScore: 9.5, Name: "x", Version: "1.0"},
		},
	}
	buf := &bytes.Buffer{}
	opts := userShowOpts{
		Identifier: "u-1",
		Output:     "email",
		NoCache:    true,
		EmailFlags: emailFlagValues{From: "alice@example.com", To: "alice@example.com", Subject: "test"},
		EmailNow:   time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
	}
	if err := runUserShow(context.Background(), client, buf, io.Discard, opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	msg, err := mail.ReadMessage(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("parse: %v\nraw:\n%s", err, buf.String())
	}
	if got := msg.Header.Get("Subject"); got != "test" {
		t.Errorf("Subject: got %q", got)
	}
	if !strings.Contains(buf.String(), "CVE-A") {
		t.Errorf("expected CVE-A in body")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/... -run TestUserShowEmail -count=1`
Expected: compile error — `userShowOpts` has no `EmailFlags` / `EmailNow` field; "email" case not handled in `renderUserBundle`.

- [ ] **Step 3: Wire the dispatch**

Edit `cmd/user.go`:

Extend `userShowOpts`:

```go
type userShowOpts struct {
	Identifier string
	Output     string
	NoCache    bool
	EmailFlags emailFlagValues
	EmailNow   time.Time
	Profile    config.Profile
}
```

Adjust imports of `cmd/user.go` to add `time`, `flipgroup.com.au/jellyfish/internal/config`, and `flipgroup.com.au/jellyfish/internal/email`.

Inside `newUserShowCmd`, after the existing `c.Flags()` calls:

```go
	c.Flags().String("email-to", "", "Email To: header (default: email.default_to from config)")
	c.Flags().String("email-from", "", "Email From: header (default: email.from from config, then git user.email)")
	c.Flags().String("email-subject", "", "Email Subject: header (default: rendered email.subject_template or a per-command default)")
```

Inside the `RunE` closure:

```go
		RunE: func(cmd *cobra.Command, args []string) error {
			outFmt, _ := cmd.Flags().GetString("output")
			client, err := buildClient(cmd)
			if err != nil {
				return err
			}
			opts.Identifier = args[0]
			opts.Output = outFmt
			opts.EmailFlags = readEmailFlags(cmd)
			opts.EmailNow = time.Now()
			if outFmt == "email" {
				prof, err := activeProfile(cmd)
				if err != nil {
					return err
				}
				opts.Profile = prof
			}
			return runUserShow(cmd.Context(), client, cmd.OutOrStdout(), cmd.ErrOrStderr(), opts)
		},
```

Update `renderUserBundle` to take the full opts (so it can reach the email pieces):

```go
func renderUserBundle(w io.Writer, opts userShowOpts, b UserBundle) error {
	switch opts.Output {
	case "json", "yaml":
		r, err := output.For(opts.Output)
		if err != nil {
			return err
		}
		return r.Render(w, b)
	case "csv":
		return renderUserBundleCSV(w, b)
	case "table", "":
		return renderUserBundleTable(w, b)
	case "email":
		now := opts.EmailNow
		if now.IsZero() {
			now = time.Now()
		}
		emailOpts, err := resolveEmailOptions(opts.EmailFlags, opts.Profile, gitUserEmail, now)
		if err != nil {
			return err
		}
		return email.NewUserShowRenderer(emailOpts).Render(w, bundleToEmailInput(b))
	default:
		return fmt.Errorf("unsupported output format %q", opts.Output)
	}
}

// bundleToEmailInput translates the cmd-layer UserBundle into the email
// package's UserBundleInput. This keeps the email package free of any cmd-
// layer dependency while reusing the same in-memory shape.
func bundleToEmailInput(b UserBundle) email.UserBundleInput {
	devs := make([]email.UserBundleDevice, len(b.Devices))
	for i, d := range b.Devices {
		devs[i] = email.UserBundleDevice{Device: d.Device, Detections: d.Detections}
	}
	return email.UserBundleInput{User: b.User, Devices: devs}
}
```

Update the caller in `runUserShow`:

```go
	return renderUserBundle(w, opts, bundle)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/... -count=1`
Expected: PASS for every test, including the new email test.

- [ ] **Step 5: Run the whole module**

Run: `go test ./... -count=1`
Expected: PASS module-wide.

- [ ] **Step 6: Commit**

```bash
git add cmd/user.go cmd/user_test.go
git commit -m "feat: wired -o email into user show"
```

---

## Task 13: README — Email output section

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add the section**

Edit `README.md`. Find the existing "### Output formats" section (around line 184) and append immediately after it (before the "### Exit codes" section):

````markdown
### Email output

`-o email` writes an RFC 5322 multipart/alternative message (.eml) to stdout.
It carries a styled HTML body (executive summary + per-CVE table with
clickable NVD/MITRE CVE links) and a plain-text alternative. Open the .eml
file in Mail, pipe it to your mail tooling, or feed it to a future
`--send-email` flag.

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
| `--email-to`      | `email.default_to`       | empty (header renders as `<unspecified>`) |
| `--email-from`    | `email.from`             | `git config user.email` |
| `--email-subject` | `email.subject_template` | per-command default |

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
````

- [ ] **Step 2: Verify it renders**

Run: `glow README.md 2>/dev/null | head -200 || cat README.md | head -200`
Expected: README renders with the new section between "Output formats" and "Exit codes".

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: documented -o email output"
```

---

## Self-Review

Spec coverage check (against `docs/superpowers/specs/2026-05-16-email-output-design.md`):

| Spec section | Implemented by |
|---|---|
| `-o email` wired into existing `--output / -o` | Task 8 (vulns summary), Task 12 (user show) |
| `--email-to / --email-from / --email-subject` flags | Tasks 8, 12 (registration); Task 7 (resolution) |
| Resolution order: flag > config > git for From | Task 7 (`resolveEmailOptions`, `gitUserEmail`); covered by `TestResolveEmailOptionsFallsBackToConfigThenGit` and `TestResolveEmailOptionsErrorsWhenNoFromAnywhere` |
| `email:` config block | Task 1 (`EmailConfig` type, parsing) |
| `{cve}` substitution + validation | Task 2 (`buildCVELink`, `validateLinkTemplate`); Task 6 (`TestNewVulnSummaryRendererRejectsBadLinkTemplate`) |
| RFC 5322 multipart/alternative wire format | Task 3 (`assembleMessage`); golden tests in Tasks 6 and 11 |
| Operations Dashboard HTML body | Task 5 (vulns summary template), Task 10 (user show template) |
| Plain-text alternative | Task 4 (vulns summary text template), Task 9 (user show text template) |
| Empty-result handling | Task 4 (`TestRenderVulnSummaryTextEmpty`), Task 5 (`TestRenderVulnSummaryHTMLEmpty`), Task 6 (`TestNewVulnSummaryRendererGoldenEmpty`) |
| Subject defaults per command | Task 6 (vulns summary default), Task 11 (user show default) |
| Tenant in masthead from `config.Subdomain` | Task 7 (`resolveEmailOptions` populates `opts.Tenant = prof.Subdomain`) |
| `Message-ID:`, `Date:`, CRLF correctness | Task 3 (`randomMessageID`, `time.RFC1123Z`, `TestAssembleMessageUsesCRLF`) |
| Round-trip parse via `net/mail` | Task 6 (`TestVulnSummaryRoundTripParses`), Task 11 (`TestUserShowRoundTripParses`) |
| Type-mismatch defensive guard | Task 6 (`TestNewVulnSummaryRendererRejectsWrongType`), Task 11 (`TestNewUserShowRendererRejectsWrongType`) |
| No live SMTP / Gmail tests | Confirmed - no test in this plan calls a network |
| README docs | Task 13 |

Placeholder scan: no "TBD" / "TODO" / "fill in" in any task body. Each test has its own code block; each implementation step has its own code block.

Type consistency: `Options`, `vulnSummaryView`, `userShowView`, `messageHeaders`, `emailFlagValues`, `gitEmailLookup`, `UserBundleInput`, `UserBundleDevice`, `resolveEmailOptions`, `renderSubjectTemplate`, `readEmailFlags`, `activeProfile`, `bundleToEmailInput` are all referenced consistently across tasks. `severityClass` / `sevRowBG` / `sevPillBG` / `sevPillFG` are defined once (Task 4 + Task 10's promotion) and reused.

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-16-email-output.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
