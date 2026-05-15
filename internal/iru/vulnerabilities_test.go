package iru

import (
	"context"
	"fmt"
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
		// cursor should be absent on the first page call
		if q.Get("cursor") != "" {
			t.Errorf("expected no cursor on first page, got %q", q.Get("cursor"))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"next":"https://example.iru?cursor=X","previous":null,"results":[{"detection_id":"x-1","device_id":"d-1","cve":"CVE-2025-0001","status":"active"}]}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	got, _, err := c.ListDetectionsPage(context.Background(), DetectionFilters{DeviceID: "d-1", Status: "active"}, 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].DetectionID != "x-1" {
		t.Fatalf("got %+v", got)
	}
}

func TestListDetectionsAutoPaginates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")
		if cursor == "page2" {
			// Second page: 1 result, no next.
			_, _ = w.Write([]byte(`{"next":null,"previous":null,"results":[{"detection_id":"y"}]}`))
			return
		}
		// First page: 300 results, next cursor points to page2.
		body := `{"next":"https://example.iru?cursor=page2","previous":null,"results":[`
		for i := 0; i < 300; i++ {
			if i > 0 {
				body += ","
			}
			body += fmt.Sprintf(`{"detection_id":"x-%d"}`, i)
		}
		body += `]}`
		_, _ = w.Write([]byte(body))
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
