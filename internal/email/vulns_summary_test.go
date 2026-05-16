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
