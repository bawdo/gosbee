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

func TestUserShowByEmailJSON(t *testing.T) {
	client := &fakeClient{
		users: []iru.User{{ID: "u-1", Name: "Keith", Email: "keith@flipgroup.com.au"}},
		devices: []iru.Device{
			{DeviceID: "d-1", DeviceName: "MBP", SerialNumber: "SN1", User: iru.User{ID: "u-1"}},
		},
		detections: []iru.Detection{
			{CVEID: "CVE-2025-0001", DeviceID: "d-1"},
			{CVEID: "CVE-unrelated", DeviceID: "d-stranger"},
		},
	}
	buf := &bytes.Buffer{}
	err := runUserShow(context.Background(), client, buf, io.Discard, userShowOpts{
		Identifier: "keith@flipgroup.com.au",
		Output:     "json",
		NoCache:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"keith@flipgroup.com.au", "d-1", "CVE-2025-0001"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q in output:\n%s", want, out)
		}
	}
	if strings.Contains(out, "CVE-unrelated") {
		t.Fatalf("CVE-unrelated should have been bucketed out: %s", out)
	}
}

func TestUserShowByIDFallback(t *testing.T) {
	client := &fakeClient{
		users: []iru.User{{ID: "u-9", Name: "Test", Email: "t@x"}},
	}
	buf := &bytes.Buffer{}
	err := runUserShow(context.Background(), client, buf, io.Discard, userShowOpts{Identifier: "u-9", Output: "json", NoCache: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "u-9") {
		t.Fatalf("output %q", buf.String())
	}
}

func TestUserShowUserNotFound(t *testing.T) {
	client := &fakeClient{}
	err := runUserShow(context.Background(), client, &bytes.Buffer{}, io.Discard, userShowOpts{Identifier: "u-x", Output: "json", NoCache: true})
	if !errors.Is(err, iru.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestUserShowCSVSortsBySeverity(t *testing.T) {
	client := &fakeClient{
		users:   []iru.User{{ID: "u-1", Name: "Alice", Email: "alice@example.com"}},
		devices: []iru.Device{{DeviceID: "d-1", DeviceName: "MBP", SerialNumber: "SN1"}},
		detections: []iru.Detection{
			{DeviceID: "d-1", CVEID: "CVE-low", Severity: "Low", CVSSScore: 3.0},
			{DeviceID: "d-1", CVEID: "CVE-crit", Severity: "Critical", CVSSScore: 9.5},
			{DeviceID: "d-1", CVEID: "CVE-med", Severity: "Medium", CVSSScore: 5.0},
			{DeviceID: "d-1", CVEID: "CVE-high", Severity: "High", CVSSScore: 8.0},
		},
	}
	buf := &bytes.Buffer{}
	err := runUserShow(context.Background(), client, buf, io.Discard, userShowOpts{
		Identifier: "u-1", Output: "csv", NoCache: true,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	out := buf.String()
	critIdx := strings.Index(out, "CVE-crit")
	highIdx := strings.Index(out, "CVE-high")
	medIdx := strings.Index(out, "CVE-med")
	lowIdx := strings.Index(out, "CVE-low")
	if critIdx < 0 || highIdx < 0 || medIdx < 0 || lowIdx < 0 {
		t.Fatalf("missing rows in CSV:\n%s", out)
	}
	if critIdx >= highIdx || highIdx >= medIdx || medIdx >= lowIdx {
		t.Fatalf("expected severity ordering Critical < High < Medium < Low in CSV, got:\n%s", out)
	}
}

func TestUserShowTextSortsDetectionsBySeverity(t *testing.T) {
	client := &fakeClient{
		users:   []iru.User{{ID: "u-1", Name: "Alice", Email: "alice@example.com"}},
		devices: []iru.Device{{DeviceID: "d-1", DeviceName: "MBP", SerialNumber: "SN1"}},
		detections: []iru.Detection{
			{DeviceID: "d-1", CVEID: "CVE-low", Severity: "Low", CVSSScore: 3.0},
			{DeviceID: "d-1", CVEID: "CVE-crit", Severity: "Critical", CVSSScore: 9.5},
			{DeviceID: "d-1", CVEID: "CVE-med", Severity: "Medium", CVSSScore: 5.0},
			{DeviceID: "d-1", CVEID: "CVE-high", Severity: "High", CVSSScore: 8.0},
		},
	}
	buf := &bytes.Buffer{}
	err := runUserShow(context.Background(), client, buf, io.Discard, userShowOpts{
		Identifier: "u-1", Output: "table", NoCache: true,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	out := buf.String()
	critIdx := strings.Index(out, "CVE-crit")
	highIdx := strings.Index(out, "CVE-high")
	medIdx := strings.Index(out, "CVE-med")
	lowIdx := strings.Index(out, "CVE-low")
	if critIdx < 0 || highIdx < 0 || medIdx < 0 || lowIdx < 0 {
		t.Fatalf("missing rows in text table:\n%s", out)
	}
	if critIdx >= highIdx || highIdx >= medIdx || medIdx >= lowIdx {
		t.Fatalf("expected severity ordering Critical < High < Medium < Low in detection table, got:\n%s", out)
	}
}

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

func TestUserShowEmailErrorsWithoutFrom(t *testing.T) {
	client := &fakeClient{
		users:   []iru.User{{ID: "u-1", Name: "Alice", Email: "alice@example.com"}},
		devices: []iru.Device{{DeviceID: "d-1", DeviceName: "Alice MBP", SerialNumber: "SN1"}},
	}
	err := runUserShow(context.Background(), client, &bytes.Buffer{}, io.Discard, userShowOpts{
		Identifier: "u-1",
		Output:     "email",
		NoCache:    true,
		gitEmail:   fixedGitEmail(""),
	})
	if err == nil {
		t.Fatal("expected error when no From address available")
	}
}

func TestUserShowSendEmailDefaultsToUserEmail(t *testing.T) {
	client := &fakeClient{
		users:   []iru.User{{ID: "u-1", Name: "Alice", Email: "alice@example.com"}},
		devices: []iru.Device{{DeviceID: "d-1", DeviceName: "MBP", SerialNumber: "SN1"}},
		detections: []iru.Detection{
			{DeviceID: "d-1", CVEID: "CVE-A", Severity: "Critical", CVSSScore: 9.5, Name: "x", Version: "1.0"},
		},
	}
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	sender := &fakeGmailSender{returnID: "msg-xyz"}
	opts := userShowOpts{
		Identifier: "u-1",
		NoCache:    true,
		EmailFlags: emailFlagValues{Send: true, From: "ops@flipgroup.com.au"},
		EmailNow:   time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		Profile:    config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		KeychainGet: func() ([]byte, error) { return []byte(`{"type":"service_account"}`), nil },
		NewSender:   func(_ context.Context, _ []byte, _ string) (gmail.Sender, error) { return sender, nil },
	}
	if err := runUserShow(context.Background(), client, stdout, stderr, opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	if stdout.Len() > 0 {
		t.Errorf("stdout should be empty when --send-email; got %q", stdout.String())
	}
	want := "sent: to=alice@example.com from=ops@flipgroup.com.au gmail-id=msg-xyz"
	if !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr confirmation:\n got %q\nwant substring %q", stderr.String(), want)
	}
	if sender.sent == nil {
		t.Fatal("sender was not called")
	}
	msg, err := mail.ReadMessage(bytes.NewReader(sender.sent))
	if err != nil {
		t.Fatalf("parse sent eml: %v\nraw:\n%s", err, sender.sent)
	}
	if got := msg.Header.Get("To"); got != "alice@example.com" {
		t.Errorf("To: got %q", got)
	}
}

func TestUserShowSendEmailExplicitToOverridesUser(t *testing.T) {
	client := &fakeClient{
		users:   []iru.User{{ID: "u-1", Name: "Alice", Email: "alice@example.com"}},
		devices: []iru.Device{{DeviceID: "d-1", DeviceName: "MBP", SerialNumber: "SN1"}},
		detections: []iru.Detection{
			{DeviceID: "d-1", CVEID: "CVE-A", Severity: "Critical", CVSSScore: 9.5, Name: "x", Version: "1.0"},
		},
	}
	stderr := &bytes.Buffer{}
	sender := &fakeGmailSender{}
	opts := userShowOpts{
		Identifier: "u-1",
		NoCache:    true,
		EmailFlags: emailFlagValues{Send: true, From: "ops@flipgroup.com.au", To: "other@example.com"},
		EmailNow:   time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		Profile:    config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		KeychainGet: func() ([]byte, error) { return []byte(`{"type":"service_account"}`), nil },
		NewSender:   func(_ context.Context, _ []byte, _ string) (gmail.Sender, error) { return sender, nil },
	}
	if err := runUserShow(context.Background(), client, &bytes.Buffer{}, stderr, opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(stderr.String(), "to=other@example.com") {
		t.Errorf("expected explicit To to win; stderr=%q", stderr.String())
	}
}

func TestUserShowSendEmailPropagatesSenderError(t *testing.T) {
	client := &fakeClient{
		users:   []iru.User{{ID: "u-1", Name: "Alice", Email: "alice@example.com"}},
		devices: []iru.Device{{DeviceID: "d-1", DeviceName: "MBP", SerialNumber: "SN1"}},
	}
	sender := &fakeGmailSender{err: gmail.ErrUnauthorized}
	opts := userShowOpts{
		Identifier: "u-1",
		NoCache:    true,
		EmailFlags: emailFlagValues{Send: true, From: "ops@flipgroup.com.au"},
		EmailNow:   time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		Profile:    config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		KeychainGet: func() ([]byte, error) { return []byte(`{"type":"service_account"}`), nil },
		NewSender:   func(_ context.Context, _ []byte, _ string) (gmail.Sender, error) { return sender, nil },
	}
	err := runUserShow(context.Background(), client, &bytes.Buffer{}, &bytes.Buffer{}, opts)
	if !errors.Is(err, gmail.ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized propagated; got %v", err)
	}
}
