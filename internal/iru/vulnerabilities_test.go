package iru

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListDetectionsPagePassesFilters(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("device_id") != "d-1" {
			t.Errorf("device_id %q", q.Get("device_id"))
		}
		if q.Get("status") != "active" {
			t.Errorf("status %q", q.Get("status"))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"detection_id":"x-1","device_id":"d-1","cve":"CVE-2025-0001","status":"active"}]`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	got, err := c.ListDetectionsPage(context.Background(), DetectionFilters{DeviceID: "d-1", Status: "active"}, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].DetectionID != "x-1" {
		t.Fatalf("got %+v", got)
	}
}

func TestListDetectionsAutoPaginates(t *testing.T) {
	var page int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		page++
		switch page {
		case 1:
			body := "["
			for i := 0; i < 300; i++ {
				if i > 0 {
					body += ","
				}
				body += `{"detection_id":"x"}`
			}
			body += "]"
			_, _ = w.Write([]byte(body))
		default:
			_, _ = w.Write([]byte(`[{"detection_id":"y"}]`))
		}
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	got, err := c.ListDetections(context.Background(), DetectionFilters{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 301 {
		t.Fatalf("expected 301 detections, got %d", len(got))
	}
}
