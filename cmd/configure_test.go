package cmd

import (
	"bufio"
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"flipgroup.com.au/jellyfish/internal/config"
)

func TestConfigureWritesConfigAndCallsKeychain(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tkn-123" {
			t.Errorf("auth header %q", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(srv.Close)

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "config.yml")

	var keychainCalls []string
	store := func(_, token string) error {
		keychainCalls = append(keychainCalls, token)
		return nil
	}

	in := strings.NewReader("acme\nus\n")
	out := &bytes.Buffer{}

	err := runConfigure(context.Background(), configureOpts{
		ConfigPath:    cfgPath,
		Stdin:         in,
		Stdout:        out,
		Stderr:        out,
		StoreToken:    store,
		ReadTokenLine: func(_ *bufio.Reader) (string, error) { return "tkn-123", nil },
		VerifyBaseURL: srv.URL,
	})
	if err != nil {
		t.Fatalf("runConfigure: %v", err)
	}

	if len(keychainCalls) != 1 || keychainCalls[0] != "tkn-123" {
		t.Fatalf("keychain calls: %v", keychainCalls)
	}

	loaded, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load saved config: %v", err)
	}
	got := loaded["default"]
	if got.Subdomain != "acme" || got.Region != "us" {
		t.Fatalf("saved profile %+v", got)
	}

	info, err := os.Stat(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode %o", info.Mode().Perm())
	}
}

func TestPromptWithDefaultKeepsCurrentOnEnter(t *testing.T) {
	out := &bytes.Buffer{}
	r := bufio.NewReader(strings.NewReader("\n"))
	got, err := promptWithDefault(out, r, "Email From", "old@x")
	if err != nil {
		t.Fatalf("promptWithDefault: %v", err)
	}
	if got != "old@x" {
		t.Errorf("value: got %q want %q", got, "old@x")
	}
	if !strings.Contains(out.String(), "Email From [old@x]: ") {
		t.Errorf("prompt text: got %q", out.String())
	}
}

func TestPromptWithDefaultReplacesOnTypedValue(t *testing.T) {
	out := &bytes.Buffer{}
	r := bufio.NewReader(strings.NewReader("new@y\n"))
	got, err := promptWithDefault(out, r, "Email From", "old@x")
	if err != nil {
		t.Fatalf("promptWithDefault: %v", err)
	}
	if got != "new@y" {
		t.Errorf("value: got %q want %q", got, "new@y")
	}
}

func TestPromptWithDefaultDashClears(t *testing.T) {
	out := &bytes.Buffer{}
	r := bufio.NewReader(strings.NewReader("-\n"))
	got, err := promptWithDefault(out, r, "Email From", "old@x")
	if err != nil {
		t.Fatalf("promptWithDefault: %v", err)
	}
	if got != "" {
		t.Errorf("value: got %q want empty", got)
	}
}

func TestPromptWithDefaultOmitsBracketsWhenNoCurrent(t *testing.T) {
	out := &bytes.Buffer{}
	r := bufio.NewReader(strings.NewReader("new@y\n"))
	got, err := promptWithDefault(out, r, "Email From", "")
	if err != nil {
		t.Fatalf("promptWithDefault: %v", err)
	}
	if got != "new@y" {
		t.Errorf("value: got %q", got)
	}
	if strings.Contains(out.String(), "[") {
		t.Errorf("prompt should not show brackets when current is empty; got %q", out.String())
	}
	if !strings.Contains(out.String(), "Email From: ") {
		t.Errorf("prompt text: got %q", out.String())
	}
}

func TestPromptWithDefaultTrimsWhitespace(t *testing.T) {
	out := &bytes.Buffer{}
	r := bufio.NewReader(strings.NewReader("  alice@x  \n"))
	got, err := promptWithDefault(out, r, "Email From", "")
	if err != nil {
		t.Fatalf("promptWithDefault: %v", err)
	}
	if got != "alice@x" {
		t.Errorf("value: got %q", got)
	}
}
