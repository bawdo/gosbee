package cmd

import (
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
		ReadTokenLine: func() (string, error) { return "tkn-123", nil },
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
