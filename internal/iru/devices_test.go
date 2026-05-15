package iru

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListDevicesPagePassesQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("limit") != "50" {
			t.Errorf("limit %q", q.Get("limit"))
		}
		if q.Get("offset") != "100" {
			t.Errorf("offset %q", q.Get("offset"))
		}
		if q.Get("user_id") != "u-1" {
			t.Errorf("user_id %q", q.Get("user_id"))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"device_id":"d-1","device_name":"Keith's Mac","serial_number":"SN1","user":{"id":"u-1","email":"k@x"}}]`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	got, err := c.ListDevicesPage(context.Background(), DeviceFilters{UserID: "u-1"}, 50, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].DeviceID != "d-1" {
		t.Fatalf("got %+v", got)
	}
}

func TestListDevicesAutoPaginates(t *testing.T) {
	var page int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page++
		w.WriteHeader(http.StatusOK)
		// Two full pages of 300 then a short page of 2.
		var n int
		switch page {
		case 1, 2:
			n = 300
		default:
			n = 2
		}
		devices := make([]map[string]string, n)
		for i := range devices {
			devices[i] = map[string]string{"device_id": fmt.Sprintf("d-%d-%d", page, i)}
		}
		_ = json.NewEncoder(w).Encode(devices)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	got, err := c.ListDevices(context.Background(), DeviceFilters{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 602 {
		t.Fatalf("expected 602 devices, got %d", len(got))
	}
}

func TestGetDeviceBySerial(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("serial_number") != "SN9" {
			t.Errorf("serial filter not propagated: %q", r.URL.Query().Get("serial_number"))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"device_id":"d-9","serial_number":"SN9"}]`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	d, err := c.GetDeviceBySerial(context.Background(), "SN9")
	if err != nil {
		t.Fatal(err)
	}
	if d.DeviceID != "d-9" {
		t.Fatalf("got %+v", d)
	}
}

func TestGetDeviceBySerialNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	_, err := c.GetDeviceBySerial(context.Background(), "SN-missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
