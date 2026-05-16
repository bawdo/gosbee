//go:build darwin

package keychain

import (
	"errors"
	"os"
	"testing"
	"time"
)

// Real-Keychain tests are gated. CI macOS runners can opt in.
func skipIfNoKeychain(t *testing.T) {
	if os.Getenv("JELLYFISH_KEYCHAIN_TESTS") != "1" {
		t.Skip("set JELLYFISH_KEYCHAIN_TESTS=1 to run macOS Keychain integration tests")
	}
}

func TestRoundTrip(t *testing.T) {
	skipIfNoKeychain(t)
	account := "jellyfish-test-" + time.Now().Format("150405.000000")
	t.Cleanup(func() { _ = Delete(account) })

	if err := Set(account, "secret-1"); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, err := Get(account)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != "secret-1" {
		t.Fatalf("got %q want %q", got, "secret-1")
	}

	if err := Set(account, "secret-2"); err != nil {
		t.Fatalf("replace: %v", err)
	}
	got2, err := Get(account)
	if err != nil {
		t.Fatalf("get after replace: %v", err)
	}
	if got2 != "secret-2" {
		t.Fatalf("got %q want %q", got2, "secret-2")
	}
}

func TestGetMissing(t *testing.T) {
	skipIfNoKeychain(t)
	_, err := Get("definitely-not-set-" + time.Now().Format("150405.000000"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGmailServiceAccountRoundTrip(t *testing.T) {
	skipIfNoKeychain(t)
	t.Cleanup(func() { _ = DeleteGmailServiceAccount() })

	want := []byte(`{"type":"service_account","client_email":"x@y.iam.gserviceaccount.com"}`)
	if err := SetGmailServiceAccount(want); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, err := GetGmailServiceAccount()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("round-trip mismatch:\n got: %s\nwant: %s", got, want)
	}

	if err := DeleteGmailServiceAccount(); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := GetGmailServiceAccount(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete: want ErrNotFound, got %v", err)
	}
}
