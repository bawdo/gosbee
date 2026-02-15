package testutil

import (
	"testing"

	"github.com/bawdo/gosbee/nodes"
)

// AssertEqual checks that got == want and reports a descriptive error if not.
func AssertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("expected:\n  %v\ngot:\n  %v", want, got)
	}
}

// AssertSQL accepts a visitor and node, renders the SQL, and compares it with the expected string.
func AssertSQL(t *testing.T, v nodes.Visitor, node nodes.Node, expected string) {
	t.Helper()
	got := node.Accept(v)
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

// AssertNoError fails the test if err is non-nil.
func AssertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// AssertError fails the test if err is nil.
func AssertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error but got nil")
	}
}
