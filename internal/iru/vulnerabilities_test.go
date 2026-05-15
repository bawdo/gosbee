package iru

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestListDetectionsPagePassesPaginationParams(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("limit") != "50" {
			t.Errorf("limit %q", q.Get("limit"))
		}
		// cursor should be absent on the first page call
		if q.Get("cursor") != "" {
			t.Errorf("expected no cursor on first page, got %q", q.Get("cursor"))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"next":"https://example.iru?cursor=X","previous":null,"results":[{"cve_id":"CVE-2025-0001","name":"git","version":"2.37.2","severity":"High","device_id":"d-1"}]}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	got, _, err := c.ListDetectionsPage(context.Background(), DetectionFilters{}, 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].CVEID != "CVE-2025-0001" {
		t.Fatalf("got %+v", got)
	}
}

func TestListDetectionsAutoPaginates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		after := r.URL.Query().Get("after")
		if after == "page2" {
			// Second page: 1 result, no next.
			_, _ = w.Write([]byte(`{"next":null,"previous":null,"results":[{"cve_id":"CVE-Y"}]}`))
			return
		}
		// First page: 300 results, next cursor points to page2.
		// The /detections endpoint returns raw cursor strings, not full URLs.
		body := `{"next":"page2","previous":null,"results":[`
		for i := 0; i < 300; i++ {
			if i > 0 {
				body += ","
			}
			body += fmt.Sprintf(`{"cve_id":"CVE-X-%d"}`, i)
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

func TestListDetectionsPageSendsAfterCursor(t *testing.T) {
	var gotQueries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQueries = append(gotQueries, r.URL.RawQuery)
		_, _ = w.Write([]byte(`{"next": null, "previous": null, "results": []}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	_, _, err := c.ListDetectionsPage(context.Background(), DetectionFilters{}, 50, "page-2-cursor")
	if err != nil {
		t.Fatal(err)
	}
	if len(gotQueries) != 1 {
		t.Fatalf("expected 1 request, got %d", len(gotQueries))
	}
	if !strings.Contains(gotQueries[0], "after=page-2-cursor") {
		t.Fatalf("expected after=page-2-cursor in query, got %q", gotQueries[0])
	}
	if strings.Contains(gotQueries[0], "cursor=") {
		t.Fatalf("expected NO cursor= in query, got %q", gotQueries[0])
	}
}
