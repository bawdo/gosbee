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
