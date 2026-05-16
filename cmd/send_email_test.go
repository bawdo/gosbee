package cmd

import (
	"bytes"
	"context"
	"errors"
	"mime"
	"net/mail"
	"strings"
	"testing"
	"time"

	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/email"
	"flipgroup.com.au/jellyfish/internal/gmail"
)

// fakeGmailSender captures the bytes passed to Send so cmd tests can assert
// on the rendered .eml without calling Google.
type fakeGmailSender struct {
	sent     []byte
	err      error
	returnID string
}

func (f *fakeGmailSender) Send(_ context.Context, raw []byte) (string, error) {
	f.sent = append([]byte(nil), raw...)
	if f.err != nil {
		return "", f.err
	}
	if f.returnID == "" {
		return "msg-fake", nil
	}
	return f.returnID, nil
}

func newFakeSenderFactory(s *fakeGmailSender) gmailNewSender {
	return func(_ context.Context, _ []byte, _ string) (gmail.Sender, error) {
		return s, nil
	}
}

func stubKeychain(blob string) func() ([]byte, error) {
	return func() ([]byte, error) { return []byte(blob), nil }
}

func TestResolveSendOptionsErrorsWhenGmailNotConfigured(t *testing.T) {
	_, _, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: "ops@example.com"},
		"",
		config.Profile{Email: config.EmailConfig{GmailConfigured: false}},
		"",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "configure email") {
		t.Errorf("error should mention 'configure email'; got %v", err)
	}
}

func TestResolveSendOptionsErrorsOnExplicitNonEmailOutput(t *testing.T) {
	_, _, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: "ops@example.com"},
		"csv",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		"",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "csv") || !strings.Contains(err.Error(), "--send-email") {
		t.Errorf("error wording: got %v", err)
	}
}

func TestResolveSendOptionsAllowsExplicitEmailOutput(t *testing.T) {
	_, to, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: "ops@example.com"},
		"email",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		"",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if to != "ops@example.com" {
		t.Errorf("to: got %q", to)
	}
}

func TestResolveSendOptionsUsesUserDefaultToWhenEmpty(t *testing.T) {
	_, to, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: ""},
		"",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		"alice@example.com",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if to != "alice@example.com" {
		t.Errorf("expected user fallback; got %q", to)
	}
}

func TestResolveSendOptionsConfigDefaultToBeatsUserDefault(t *testing.T) {
	// email.Options.To is already populated from email.default_to by
	// resolveEmailOptions, so simulate that here: To non-empty + userDefaultTo
	// non-empty -> To wins.
	_, to, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: "secops@example.com"},
		"",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true, DefaultTo: "secops@example.com"}},
		"alice@example.com",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if to != "secops@example.com" {
		t.Errorf("expected default_to to win; got %q", to)
	}
}

func TestResolveSendOptionsErrorsWithNoRecipientAnywhere(t *testing.T) {
	_, _, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: ""},
		"",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		"",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "recipient") {
		t.Errorf("error wording: got %v", err)
	}
}

func TestResolveSendOptionsExplicitToWins(t *testing.T) {
	_, to, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: "flag@example.com"},
		"",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true, DefaultTo: "ignored@example.com"}},
		"alice@example.com",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if to != "flag@example.com" {
		t.Errorf("expected explicit To to win; got %q", to)
	}
}

func TestResolveSendOptionsPropagatesKeychainError(t *testing.T) {
	bogus := errors.New("keychain locked")
	_, _, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: "ops@example.com"},
		"",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		"",
		func() ([]byte, error) { return nil, bogus },
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, bogus) {
		t.Errorf("expected wrapped keychain error; got %v", err)
	}
}

// Sanity that the test helpers compile.
var _ = bytes.Buffer{}

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
