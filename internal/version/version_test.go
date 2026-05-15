package version

import "testing"

func TestDefault(t *testing.T) {
	if Version == "" {
		t.Fatal("Version should not be empty")
	}
}

func TestDefaultIsDevWhenUnset(t *testing.T) {
	if Version != "dev" {
		t.Fatalf(`expected default Version to be "dev", got %q`, Version)
	}
}
