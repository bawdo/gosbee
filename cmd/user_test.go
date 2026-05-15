package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"flipgroup.com.au/jellyfish/internal/iru"
)

func TestUserShowByEmailJSON(t *testing.T) {
	client := &fakeClient{
		users: []iru.User{{ID: "u-1", Name: "Keith", Email: "keith@flipgroup.com.au"}},
		devices: []iru.Device{
			{DeviceID: "d-1", DeviceName: "MBP", SerialNumber: "SN1", User: iru.User{ID: "u-1"}},
		},
		detections: []iru.Detection{
			{DetectionID: "x-1", DeviceID: "d-1", CVE: "CVE-2025-0001", Status: "active"},
		},
	}
	buf := &bytes.Buffer{}
	err := runUserShow(context.Background(), client, buf, userShowOpts{
		Identifier: "keith@flipgroup.com.au",
		Output:     "json",
	})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"keith@flipgroup.com.au", "d-1", "x-1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q in output:\n%s", want, out)
		}
	}
}

func TestUserShowByIDFallback(t *testing.T) {
	client := &fakeClient{
		users: []iru.User{{ID: "u-9", Name: "Test", Email: "t@x"}},
	}
	buf := &bytes.Buffer{}
	err := runUserShow(context.Background(), client, buf, userShowOpts{Identifier: "u-9", Output: "json"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "u-9") {
		t.Fatalf("output %q", buf.String())
	}
}

func TestUserShowUserNotFound(t *testing.T) {
	client := &fakeClient{}
	err := runUserShow(context.Background(), client, &bytes.Buffer{}, userShowOpts{Identifier: "u-x", Output: "json"})
	if !errors.Is(err, iru.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
