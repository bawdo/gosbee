package email

import (
	"bytes"
	"net/mail"
	"strings"
	"testing"
	"time"

	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/output"
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

func TestBuildUserShowViewSortsRowsBySeverity(t *testing.T) {
	bundle := UserBundleInput{
		User: iru.User{ID: "u-1", Name: "Alice", Email: "alice@example.com"},
		Devices: []UserBundleDevice{
			{
				Device: iru.Device{DeviceID: "d-1", DeviceName: "MBP", SerialNumber: "SN1"},
				Detections: []iru.Detection{
					{CVEID: "CVE-low", Severity: "Low", CVSSScore: 3.0},
					{CVEID: "CVE-crit", Severity: "Critical", CVSSScore: 9.5},
					{CVEID: "CVE-med", Severity: "Medium", CVSSScore: 5.0},
					{CVEID: "CVE-high", Severity: "High", CVSSScore: 8.0},
				},
			},
		},
	}
	view := buildUserShowView(bundle, Options{}.withDefaults())
	if len(view.Devices) != 1 || len(view.Devices[0].Rows) != 4 {
		t.Fatalf("unexpected view shape: %+v", view)
	}
	got := make([]string, 0, len(view.Devices[0].Rows))
	for _, r := range view.Devices[0].Rows {
		got = append(got, r.CVEID)
	}
	want := []string{"CVE-crit", "CVE-high", "CVE-med", "CVE-low"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("severity sort order wrong:\n got:  %v\n want: %v", got, want)
		}
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

func TestRenderUserShowHTML(t *testing.T) {
	view := buildUserShowView(sampleUserBundle(), Options{}.withDefaults())
	got, err := renderUserShowHTML(view)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		`bgcolor="#0f172a"`,
		"Alice Example",
		"Alice MBP",
		"Alice iPad",
		`>CVE-2024-3094<`,
		"(no detections)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("HTML missing %q", want)
		}
	}
}

func newPinnedUserShowOpts() Options {
	return Options{
		From:              "Jellyfish <alice@example.com>",
		To:                "alice@example.com",
		Subject:           "Vulnerability exposure - Alice Example - 2026-05-16",
		Tenant:            "flipgroup",
		GeneratedAt:       time.Date(2026, 5, 16, 10, 42, 0, 0, time.FixedZone("AEST", 10*3600)),
		BoundaryOverride:  "=_jf_FIXEDBOUNDARY01",
		MessageIDOverride: "<fixed-user-id@example.com>",
	}
}

func TestNewUserShowRendererGolden(t *testing.T) {
	var buf bytes.Buffer
	if err := NewUserShowRenderer(newPinnedUserShowOpts()).Render(&buf, sampleUserBundle()); err != nil {
		t.Fatalf("Render: %v", err)
	}
	goldenAssert(t, "user_show.golden.eml", buf.Bytes())
}

func TestNewUserShowRendererGoldenNoDetections(t *testing.T) {
	bundle := UserBundleInput{
		User: iru.User{ID: "u-9", Name: "Bob Empty", Email: "bob@example.com"},
		Devices: []UserBundleDevice{
			{Device: iru.Device{DeviceID: "d-9", DeviceName: "Bob MBA", SerialNumber: "SN9"}, Detections: nil},
		},
	}
	var buf bytes.Buffer
	if err := NewUserShowRenderer(newPinnedUserShowOpts()).Render(&buf, bundle); err != nil {
		t.Fatalf("Render: %v", err)
	}
	goldenAssert(t, "user_show_no_detections.golden.eml", buf.Bytes())
}

func TestNewUserShowRendererSatisfiesOutputRenderer(t *testing.T) {
	var _ output.Renderer = NewUserShowRenderer(newPinnedUserShowOpts())
}

func TestNewUserShowRendererRejectsWrongType(t *testing.T) {
	err := NewUserShowRenderer(newPinnedUserShowOpts()).Render(&bytes.Buffer{}, "nope")
	if err == nil {
		t.Fatal("expected type error")
	}
}

func TestNewUserShowRendererRejectsEmptyFrom(t *testing.T) {
	opts := newPinnedUserShowOpts()
	opts.From = ""
	err := NewUserShowRenderer(opts).Render(&bytes.Buffer{}, sampleUserBundle())
	if err == nil {
		t.Fatal("expected error for empty From")
	}
}

func TestNewUserShowRendererRejectsBadLinkTemplate(t *testing.T) {
	opts := newPinnedUserShowOpts()
	opts.CVELinkPrimary = "https://no-token.example/"
	err := NewUserShowRenderer(opts).Render(&bytes.Buffer{}, sampleUserBundle())
	if err == nil {
		t.Fatal("expected validation error for missing {cve} token")
	}
}

func TestUserShowRoundTripParses(t *testing.T) {
	var buf bytes.Buffer
	if err := NewUserShowRenderer(newPinnedUserShowOpts()).Render(&buf, sampleUserBundle()); err != nil {
		t.Fatalf("Render: %v", err)
	}
	if _, err := mail.ReadMessage(&buf); err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
}
