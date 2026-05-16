package email

import (
	"strings"
	"testing"
	"time"

	"flipgroup.com.au/jellyfish/internal/iru"
)

func sampleUserBundle() UserBundleInput {
	return UserBundleInput{
		User: iru.User{ID: "u-1", Name: "Alice Example", Email: "alice@example.com"},
		Devices: []UserBundleDevice{
			{
				Device: iru.Device{DeviceID: "d-1", DeviceName: "Alice MBP", SerialNumber: "SN1", OSVersion: "14.4"},
				Detections: []iru.Detection{
					{CVEID: "CVE-2024-3094", Severity: "Critical", CVSSScore: 10.0, Name: "xz-utils", Version: "5.6.1"},
					{CVEID: "CVE-2024-6387", Severity: "Critical", CVSSScore: 8.1, Name: "openssh-server", Version: "9.6"},
				},
			},
			{
				Device:     iru.Device{DeviceID: "d-2", DeviceName: "Alice iPad", SerialNumber: "SN2"},
				Detections: nil,
			},
		},
	}
}

func TestBuildUserShowView(t *testing.T) {
	view := buildUserShowView(sampleUserBundle(), Options{
		GeneratedAt: time.Date(2026, 5, 16, 10, 42, 0, 0, time.UTC),
	}.withDefaults())
	if view.User.Name != "Alice Example" {
		t.Errorf("User.Name: got %q", view.User.Name)
	}
	if view.TotalCVEs != 2 {
		t.Errorf("TotalCVEs: got %d want 2", view.TotalCVEs)
	}
	if view.CriticalCount != 2 {
		t.Errorf("CriticalCount: got %d want 2", view.CriticalCount)
	}
	if len(view.Devices) != 2 {
		t.Fatalf("Devices: got %d", len(view.Devices))
	}
	if len(view.Devices[0].Rows) != 2 {
		t.Errorf("device 0 rows: got %d", len(view.Devices[0].Rows))
	}
	if len(view.Devices[1].Rows) != 0 {
		t.Errorf("device 1 rows: got %d want 0", len(view.Devices[1].Rows))
	}
}

func TestRenderUserShowText(t *testing.T) {
	view := buildUserShowView(sampleUserBundle(), Options{}.withDefaults())
	got, err := renderUserShowText(view)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		"Alice Example",
		"2 Critical",
		"Alice MBP",
		"CVE-2024-3094",
		"Alice iPad",
		"(no detections)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("plain text missing %q\noutput:\n%s", want, got)
		}
	}
}
