package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/mail"
	"strings"
	"testing"
	"time"

	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/gmail"
	"flipgroup.com.au/jellyfish/internal/iru"
)

type fakeClient struct {
	detections      []iru.Detection
	devices         []iru.Device
	users           []iru.User
	bySerial        func(string) (iru.Device, error)
	vulnerabilities []iru.Vulnerability
}

func (f *fakeClient) ListDetections(ctx context.Context, _ iru.DetectionFilters) ([]iru.Detection, error) {
	return f.detections, nil
}
func (f *fakeClient) ListDetectionsPage(_ context.Context, _ iru.DetectionFilters, _ int, _ string) ([]iru.Detection, string, error) {
	return f.detections, "", nil
}
func (f *fakeClient) ListDetectionsStream(_ context.Context, _ iru.DetectionFilters, cb func(page []iru.Detection) error) error {
	if len(f.detections) == 0 {
		return nil
	}
	return cb(f.detections)
}
func (f *fakeClient) ListDevices(_ context.Context, _ iru.DeviceFilters) ([]iru.Device, error) {
	return f.devices, nil
}
func (f *fakeClient) ListDevicesPage(_ context.Context, _ iru.DeviceFilters, _, _ int) ([]iru.Device, error) {
	return f.devices, nil
}
func (f *fakeClient) GetDeviceBySerial(_ context.Context, s string) (iru.Device, error) {
	if f.bySerial != nil {
		return f.bySerial(s)
	}
	return iru.Device{}, iru.ErrNotFound
}
func (f *fakeClient) GetUser(_ context.Context, id string) (iru.User, error) {
	for _, u := range f.users {
		if u.ID == id {
			return u, nil
		}
	}
	return iru.User{}, iru.ErrNotFound
}
func (f *fakeClient) FindUserByEmail(_ context.Context, e string) (iru.User, error) {
	for _, u := range f.users {
		if strings.EqualFold(u.Email, e) {
			return u, nil
		}
	}
	return iru.User{}, iru.ErrNotFound
}
func (f *fakeClient) ListVulnerabilities(_ context.Context, _ iru.VulnerabilityFilters) ([]iru.Vulnerability, error) {
	return f.vulnerabilities, nil
}
func (f *fakeClient) ListVulnerabilitiesStream(_ context.Context, _ iru.VulnerabilityFilters, cb func(page []iru.Vulnerability) error) error {
	if len(f.vulnerabilities) == 0 {
		return nil
	}
	return cb(f.vulnerabilities)
}
func (f *fakeClient) ListVulnerabilitiesPage(_ context.Context, _ iru.VulnerabilityFilters, _, _ int) ([]iru.Vulnerability, int, error) {
	return f.vulnerabilities, len(f.vulnerabilities), nil
}

func TestVulnsListJSON(t *testing.T) {
	client := &fakeClient{detections: []iru.Detection{
		{CVEID: "CVE-2025-0001", Name: "git", Version: "2.37.2", Severity: "High", DeviceID: "d-1"},
	}}
	buf := &bytes.Buffer{}
	err := runVulnsList(context.Background(), client, buf, io.Discard, vulnsListOpts{Output: "json", NoCache: true})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(buf.String(), `"cve_id": "CVE-2025-0001"`) {
		t.Fatalf("output: %q", buf.String())
	}
}

func TestVulnsListSortsBySeverity(t *testing.T) {
	client := &fakeClient{detections: []iru.Detection{
		{CVEID: "CVE-low", Severity: "Low", CVSSScore: 3.0, DeviceID: "d-1"},
		{CVEID: "CVE-crit", Severity: "Critical", CVSSScore: 9.5, DeviceID: "d-1"},
		{CVEID: "CVE-med", Severity: "Medium", CVSSScore: 5.0, DeviceID: "d-1"},
		{CVEID: "CVE-high", Severity: "High", CVSSScore: 8.0, DeviceID: "d-1"},
	}}
	buf := &bytes.Buffer{}
	err := runVulnsList(context.Background(), client, buf, io.Discard, vulnsListOpts{Output: "json", NoCache: true})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	out := buf.String()
	critIdx := strings.Index(out, "CVE-crit")
	highIdx := strings.Index(out, "CVE-high")
	medIdx := strings.Index(out, "CVE-med")
	lowIdx := strings.Index(out, "CVE-low")
	if critIdx >= highIdx || highIdx >= medIdx || medIdx >= lowIdx {
		t.Fatalf("expected severity ordering Critical < High < Medium < Low, got:\n%s", out)
	}
}

func TestVulnsListSerialResolvesDeviceID(t *testing.T) {
	client := &fakeClient{
		bySerial: func(s string) (iru.Device, error) {
			if s != "SN1" {
				return iru.Device{}, iru.ErrNotFound
			}
			return iru.Device{DeviceID: "d-9"}, nil
		},
		detections: []iru.Detection{
			{CVEID: "CVE-x-9", DeviceID: "d-9"},
			{CVEID: "CVE-other", DeviceID: "d-other"},
		},
	}
	buf := &bytes.Buffer{}
	err := runVulnsList(context.Background(), client, buf, io.Discard, vulnsListOpts{
		Output:  "json",
		Serial:  "SN1",
		NoCache: true,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "CVE-x-9") {
		t.Fatalf("expected CVE-x-9 in output: %q", out)
	}
	if strings.Contains(out, "CVE-other") {
		t.Fatalf("CVE-other should have been filtered out: %q", out)
	}
}

func TestVulnsListSerialNotFoundExitsNotFound(t *testing.T) {
	client := &fakeClient{}
	err := runVulnsList(context.Background(), client, &bytes.Buffer{}, io.Discard, vulnsListOpts{
		Output: "json",
		Serial: "SN-missing",
	})
	if !errors.Is(err, iru.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestVulnsListRejectsMutuallyExclusiveFlags(t *testing.T) {
	client := &fakeClient{}
	err := runVulnsList(context.Background(), client, &bytes.Buffer{}, io.Discard, vulnsListOpts{
		Output:   "json",
		DeviceID: "d-1",
		Serial:   "SN1",
	})
	if err == nil {
		t.Fatal("expected error for both flags")
	}
}

func TestVulnsSummaryFiltersByStatus(t *testing.T) {
	client := &fakeClient{vulnerabilities: []iru.Vulnerability{
		{CVEID: "CVE-A", Status: "Active", Severity: "High", CVSSScore: 8.0},
		{CVEID: "CVE-B", Status: "Remediated", Severity: "High", CVSSScore: 6.0},
		{CVEID: "CVE-C", Status: "Active", Severity: "Low", CVSSScore: 3.0},
	}}
	buf := &bytes.Buffer{}
	err := runVulnsSummary(context.Background(), client, buf, io.Discard, vulnsSummaryOpts{
		Output: "json", Status: "active", NoCache: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "CVE-A") || !strings.Contains(out, "CVE-C") {
		t.Fatalf("expected Active records in output: %s", out)
	}
	if strings.Contains(out, "CVE-B") {
		t.Fatalf("Remediated record leaked: %s", out)
	}
}

func TestVulnsSummarySortsBySeverityByDefault(t *testing.T) {
	client := &fakeClient{vulnerabilities: []iru.Vulnerability{
		{CVEID: "CVE-low", Severity: "Low", CVSSScore: 3.0},
		{CVEID: "CVE-crit", Severity: "Critical", CVSSScore: 9.5},
		{CVEID: "CVE-med", Severity: "Medium", CVSSScore: 5.0},
		{CVEID: "CVE-high", Severity: "High", CVSSScore: 8.0},
	}}
	buf := &bytes.Buffer{}
	err := runVulnsSummary(context.Background(), client, buf, io.Discard, vulnsSummaryOpts{
		Output: "json", NoCache: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	critIdx := strings.Index(out, "CVE-crit")
	highIdx := strings.Index(out, "CVE-high")
	medIdx := strings.Index(out, "CVE-med")
	lowIdx := strings.Index(out, "CVE-low")
	if critIdx >= highIdx || highIdx >= medIdx || medIdx >= lowIdx {
		t.Fatalf("expected severity ordering Critical < High < Medium < Low, got order:\n%s", out)
	}
}

func TestVulnsSummaryAppliesLimit(t *testing.T) {
	client := &fakeClient{vulnerabilities: []iru.Vulnerability{
		{CVEID: "CVE-1", Severity: "Critical", CVSSScore: 9.0},
		{CVEID: "CVE-2", Severity: "Critical", CVSSScore: 8.5},
		{CVEID: "CVE-3", Severity: "Critical", CVSSScore: 8.0},
	}}
	buf := &bytes.Buffer{}
	err := runVulnsSummary(context.Background(), client, buf, io.Discard, vulnsSummaryOpts{
		Output: "json", Limit: 2, NoCache: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "CVE-1") || !strings.Contains(out, "CVE-2") {
		t.Fatalf("expected top 2 in output: %s", out)
	}
	if strings.Contains(out, "CVE-3") {
		t.Fatalf("expected CVE-3 to be limited out: %s", out)
	}
}

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
		Output:    "email",
		NoCache:   true,
		gitEmail:  fixedGitEmail(""),
	})
	if err == nil {
		t.Fatal("expected error when no From address available")
	}
}

func TestVulnsSummarySendEmailRequiresRecipient(t *testing.T) {
	client := &fakeClient{vulnerabilities: []iru.Vulnerability{{CVEID: "CVE-A", Severity: "Critical"}}}
	opts := vulnsSummaryOpts{
		EmailFlags: emailFlagValues{Send: true, From: "ops@flipgroup.com.au"},
		EmailNow:   time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		Profile:    config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		NoCache:    true,
		KeychainGet: func() ([]byte, error) { return []byte(`{"type":"service_account"}`), nil },
		NewSender:   func(_ context.Context, _ []byte, _ string) (gmail.Sender, error) { return &fakeGmailSender{}, nil },
	}
	err := runVulnsSummary(context.Background(), client, &bytes.Buffer{}, &bytes.Buffer{}, opts)
	if err == nil {
		t.Fatal("expected error for missing recipient")
	}
	if !strings.Contains(err.Error(), "recipient") {
		t.Errorf("error wording: got %v", err)
	}
}

func TestVulnsSummarySendEmailSends(t *testing.T) {
	client := &fakeClient{vulnerabilities: []iru.Vulnerability{
		{CVEID: "CVE-A", Severity: "Critical", CVSSScore: 9.5, KEVScore: 1, DeviceCount: 2, Status: "Active", Software: []string{"foo"}},
	}}
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	sender := &fakeGmailSender{returnID: "msg-abc"}
	opts := vulnsSummaryOpts{
		EmailFlags: emailFlagValues{Send: true, From: "ops@flipgroup.com.au", To: "secops@example.com"},
		EmailNow:   time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		Profile:    config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		NoCache:    true,
		KeychainGet: func() ([]byte, error) { return []byte(`{"type":"service_account"}`), nil },
		NewSender:   func(_ context.Context, _ []byte, _ string) (gmail.Sender, error) { return sender, nil },
	}
	if err := runVulnsSummary(context.Background(), client, stdout, stderr, opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	if stdout.Len() > 0 {
		t.Errorf("stdout should be empty; got %q", stdout.String())
	}
	want := "sent: to=secops@example.com from=ops@flipgroup.com.au gmail-id=msg-abc"
	if !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr confirmation:\n got %q\nwant %q", stderr.String(), want)
	}
	if sender.sent == nil {
		t.Fatal("sender was not called")
	}
}

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
