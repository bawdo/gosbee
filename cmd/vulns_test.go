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
func (f *fakeClient) ListDetectionsPage(ctx context.Context, _ iru.DetectionFilters, _, _ int) ([]iru.Detection, error) {
	return f.detections, nil
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
		{DetectionID: "x-1", DeviceID: "d-1", CVE: "CVE-2025-0001", Severity: "high", Status: "active"},
	}}
	buf := &bytes.Buffer{}
	err := runVulnsList(context.Background(), client, buf, io.Discard, vulnsListOpts{Output: "json"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(buf.String(), `"detection_id": "x-1"`) {
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
		detections: []iru.Detection{{DetectionID: "x-9", DeviceID: "d-9"}},
	}
	buf := &bytes.Buffer{}
	err := runVulnsList(context.Background(), client, buf, io.Discard, vulnsListOpts{
		Output: "json",
		Serial: "SN1",
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(buf.String(), "x-9") {
		t.Fatalf("output: %q", buf.String())
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
