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
