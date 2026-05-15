package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMain redirects HOME (and XDG_CACHE_HOME) to a temp directory for the
// duration of test runs. This stops any test that accidentally hits the real
// filesystem — for instance via os.UserCacheDir() inside fetchAllDetections —
// from writing to the actual user's cache or config paths. Belt-and-braces;
// the test fixtures should also use NoCache: true and similar opt-outs, but
// this guards against future test code forgetting them.
func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "jellyfish-test-home-*")
	if err != nil {
		// If we cannot create a tempdir we still want to run the tests,
		// just without the redirection. Worst case: a test bug writes to
		// the real path. The committed fixtures all set NoCache: true so
		// this is unlikely to bite — but flag it on stderr just in case.
		_, _ = os.Stderr.WriteString("TestMain: could not create tempdir, running tests without HOME redirection: " + err.Error() + "\n")
		os.Exit(m.Run())
	}

	origHome := os.Getenv("HOME")
	origXDG := os.Getenv("XDG_CACHE_HOME")
	_ = os.Setenv("HOME", tmp)
	_ = os.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, ".cache"))

	code := m.Run()

	// Restore originals. os.Exit doesn't run defers, so do this inline.
	_ = os.Setenv("HOME", origHome)
	_ = os.Setenv("XDG_CACHE_HOME", origXDG)
	_ = os.RemoveAll(tmp)

	os.Exit(code)
}

func TestUserCacheDirIsRedirected(t *testing.T) {
	dir, err := os.UserCacheDir()
	if err != nil {
		t.Fatalf("UserCacheDir: %v", err)
	}
	home := os.Getenv("HOME")
	if home == "" {
		t.Fatal("HOME unset during test run")
	}
	// On macOS UserCacheDir is $HOME/Library/Caches; on Linux it's $XDG_CACHE_HOME.
	// In both cases, dir should be inside the tempdir we set as HOME.
	if !strings.HasPrefix(dir, home) {
		t.Fatalf("UserCacheDir %q is outside HOME %q — redirection broken", dir, home)
	}
}
