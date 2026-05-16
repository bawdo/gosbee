package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveAndLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")

	in := File{
		"default": Profile{
			Subdomain: "acme",
			Region:    "us",
			BaseURL:   "https://acme.api.kandji.io/api/v1",
		},
	}

	if err := Save(path, in); err != nil {
		t.Fatalf("save: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected mode 0600, got %o", info.Mode().Perm())
	}

	out, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if out["default"].Subdomain != "acme" {
		t.Fatalf("got %+v", out)
	}
	if out["default"].BaseURL != "https://acme.api.kandji.io/api/v1" {
		t.Fatalf("got %+v", out)
	}
}

func TestSaveCreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "child", "config.yml")

	if err := Save(path, File{"default": Profile{Subdomain: "x", Region: "us"}}); err != nil {
		t.Fatalf("save: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file at %s: %v", path, err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.yml"))
	if !os.IsNotExist(err) {
		t.Fatalf("expected os.IsNotExist, got %v", err)
	}
}

func TestBuildBaseURL(t *testing.T) {
	cases := []struct {
		sub, region, want string
	}{
		{"acme", "us", "https://acme.api.kandji.io/api/v1"},
		{"acme", "eu", "https://acme.api.eu.kandji.io/api/v1"},
	}
	for _, c := range cases {
		got, err := BuildBaseURL(c.sub, c.region)
		if err != nil {
			t.Fatalf("BuildBaseURL(%q,%q): %v", c.sub, c.region, err)
		}
		if got != c.want {
			t.Fatalf("BuildBaseURL(%q,%q)=%q want %q", c.sub, c.region, got, c.want)
		}
	}
}

func TestBuildBaseURLRejectsBadInput(t *testing.T) {
	if _, err := BuildBaseURL("", "us"); err == nil {
		t.Fatal("expected error for empty subdomain")
	}
	if _, err := BuildBaseURL("acme", "apac"); err == nil {
		t.Fatal("expected error for invalid region")
	}
	if _, err := BuildBaseURL("Bad_Sub", "us"); err == nil {
		t.Fatal("expected error for invalid subdomain characters")
	}
}

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
