package cmd

import (
	"bytes"
	"strings"
	"testing"

	"flipgroup.com.au/jellyfish/internal/version"
)

func TestVersionCommandPrintsVersion(t *testing.T) {
	version.Version = "test-1.2.3"
	t.Cleanup(func() { version.Version = "dev" })

	buf := &bytes.Buffer{}
	root := newRootCmd()
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"version"})

	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "jellyfish test-1.2.3") {
		t.Fatalf("expected output to contain version, got %q", out)
	}
}
