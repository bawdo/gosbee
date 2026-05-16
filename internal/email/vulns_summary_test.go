package email

import (
	"bytes"
	"flag"
	"net/mail"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/output"
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

func TestBuildVulnSummaryViewSortsRowsBySeverity(t *testing.T) {
	in := []iru.Vulnerability{
		{CVEID: "CVE-low", Severity: "Low", CVSSScore: 3.0},
		{CVEID: "CVE-crit", Severity: "Critical", CVSSScore: 9.5},
		{CVEID: "CVE-med", Severity: "Medium", CVSSScore: 5.0},
		{CVEID: "CVE-high", Severity: "High", CVSSScore: 8.0},
	}
	view := buildVulnSummaryView(in, Options{}.withDefaults())
	if len(view.Rows) != 4 {
		t.Fatalf("Rows: got %d want 4", len(view.Rows))
	}
	got := []string{view.Rows[0].CVEID, view.Rows[1].CVEID, view.Rows[2].CVEID, view.Rows[3].CVEID}
	want := []string{"CVE-crit", "CVE-high", "CVE-med", "CVE-low"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("severity sort order wrong:\n got:  %v\n want: %v", got, want)
		}
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
	if !strings.Contains(got, "KEV = CISA") {
		t.Errorf("expected KEV footnote, got:\n%s", got)
	}
	if strings.Contains(got, "No matching vulnerabilities.KEV") {
		t.Errorf("empty marker fused with KEV footer (whitespace bug):\n%s", got)
	}
}

func TestRenderVulnSummaryHTML(t *testing.T) {
	opts := Options{Tenant: "flipgroup"}.withDefaults()
	view := buildVulnSummaryView(sampleVulns(), opts)
	view.Header = buildHeader("JELLYFISH / VULNS", "Fleet vulnerability summary", "subtitle", opts.HeaderBG, false)
	got, err := renderVulnSummaryHTML(view)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		`bgcolor="#7566FF"`,
		`>CVE-2024-3094<`,
		`href="https://nvd.nist.gov/vuln/detail/CVE-2024-3094"`,
		`href="https://www.cve.org/CVERecord?id=CVE-2024-3094"`,
		`>KEV<`,
		`Critical`,
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

func TestNewVulnSummaryRendererRejectsEmptyFrom(t *testing.T) {
	opts := newPinnedOpts()
	opts.From = ""
	r := NewVulnSummaryRenderer(opts)
	err := r.Render(&bytes.Buffer{}, sampleVulns())
	if err == nil {
		t.Fatal("expected error for empty From address")
	}
	if !strings.Contains(err.Error(), "From") {
		t.Errorf("error should mention From; got %v", err)
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

func TestVulnSummaryHTMLHeaderColoursAndLogo(t *testing.T) {
	cases := []struct {
		name     string
		bg       string
		logoPath string
		wantText string // a substring that proves the right text-colour branch
		wantLogo bool
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

func TestDomainFromAddress(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"user@domain.com", "domain.com"},
		{"Display Name <alice@example.com>", "example.com"},
		{"alice@example.com>", "example.com"},
		{"not-an-email", "localhost"},
		{"", "localhost"},
	}
	for _, tc := range cases {
		got := domainFromAddress(tc.in)
		if got != tc.want {
			t.Errorf("domainFromAddress(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
