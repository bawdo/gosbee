package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"flipgroup.com.au/jellyfish/internal/iru"
)

type fakeClient struct {
	detections []iru.Detection
	devices    []iru.Device
	users      []iru.User
	bySerial   func(string) (iru.Device, error)
}

func (f *fakeClient) ListDetections(ctx context.Context, _ iru.DetectionFilters) ([]iru.Detection, error) {
	return f.detections, nil
}
func (f *fakeClient) ListDetectionsPage(_ context.Context, _ iru.DetectionFilters, _ int, _ string) ([]iru.Detection, string, error) {
	return f.detections, "", nil
}
func (f *fakeClient) ListDetectionsStream(_ context.Context, _ iru.DetectionFilters, cb func(page []iru.Detection) error) error {
	if len(f.detections) == 0 {
		return nil
	}
	return cb(f.detections)
}
func (f *fakeClient) ListDevices(_ context.Context, _ iru.DeviceFilters) ([]iru.Device, error) {
	return f.devices, nil
}
func (f *fakeClient) ListDevicesPage(_ context.Context, _ iru.DeviceFilters, _, _ int) ([]iru.Device, error) {
	return f.devices, nil
}
func (f *fakeClient) GetDeviceBySerial(_ context.Context, s string) (iru.Device, error) {
	if f.bySerial != nil {
		return f.bySerial(s)
	}
	return iru.Device{}, iru.ErrNotFound
}
func (f *fakeClient) GetUser(_ context.Context, id string) (iru.User, error) {
	for _, u := range f.users {
		if u.ID == id {
			return u, nil
		}
	}
	return iru.User{}, iru.ErrNotFound
}
func (f *fakeClient) FindUserByEmail(_ context.Context, e string) (iru.User, error) {
	for _, u := range f.users {
		if strings.EqualFold(u.Email, e) {
			return u, nil
		}
	}
	return iru.User{}, iru.ErrNotFound
}

func TestVulnsListJSON(t *testing.T) {
	client := &fakeClient{detections: []iru.Detection{
		{CVEID: "CVE-2025-0001", Name: "git", Version: "2.37.2", Severity: "High", DeviceID: "d-1"},
	}}
	buf := &bytes.Buffer{}
	err := runVulnsList(context.Background(), client, buf, io.Discard, vulnsListOpts{Output: "json"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(buf.String(), `"cve_id": "CVE-2025-0001"`) {
		t.Fatalf("output: %q", buf.String())
	}
}

func TestVulnsListSerialResolvesDeviceID(t *testing.T) {
	client := &fakeClient{
		bySerial: func(s string) (iru.Device, error) {
			if s != "SN1" {
				return iru.Device{}, iru.ErrNotFound
			}
			return iru.Device{DeviceID: "d-9"}, nil
		},
		detections: []iru.Detection{
			{CVEID: "CVE-x-9", DeviceID: "d-9"},
			{CVEID: "CVE-other", DeviceID: "d-other"},
		},
	}
	buf := &bytes.Buffer{}
	err := runVulnsList(context.Background(), client, buf, io.Discard, vulnsListOpts{
		Output: "json",
		Serial: "SN1",
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "CVE-x-9") {
		t.Fatalf("expected CVE-x-9 in output: %q", out)
	}
	if strings.Contains(out, "CVE-other") {
		t.Fatalf("CVE-other should have been filtered out: %q", out)
	}
}

func TestVulnsListSerialNotFoundExitsNotFound(t *testing.T) {
	client := &fakeClient{}
	err := runVulnsList(context.Background(), client, &bytes.Buffer{}, io.Discard, vulnsListOpts{
		Output: "json",
		Serial: "SN-missing",
	})
	if !errors.Is(err, iru.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestVulnsListRejectsMutuallyExclusiveFlags(t *testing.T) {
	client := &fakeClient{}
	err := runVulnsList(context.Background(), client, &bytes.Buffer{}, io.Discard, vulnsListOpts{
		Output:   "json",
		DeviceID: "d-1",
		Serial:   "SN1",
	})
	if err == nil {
		t.Fatal("expected error for both flags")
	}
}
