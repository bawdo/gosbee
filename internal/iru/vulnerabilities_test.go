package iru

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestListVulnerabilitiesPageSendsPageAndSize(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("page") != "1" {
			t.Errorf("expected page=1, got %q", q.Get("page"))
		}
		if q.Get("size") != "300" {
			t.Errorf("expected size=300, got %q", q.Get("size"))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"page":1,"size":300,"total":1,"results":[{"cve_id":"CVE-1999-1386","severity":"Medium","cvss_score":5.5,"kev_score":0,"status":"Remediated","software":["perl"],"device_count":0}]}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	got, total, err := c.ListVulnerabilitiesPage(context.Background(), VulnerabilityFilters{}, 1, 300)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("expected total=1, got %d", total)
	}
	if len(got) != 1 || got[0].CVEID != "CVE-1999-1386" {
		t.Fatalf("got %+v", got)
	}
}

func TestListVulnerabilitiesStreamWalksAllPages(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		switch page {
		case "1":
			body := `{"page":1,"size":300,"total":650,"results":[`
			for i := 0; i < 300; i++ {
				if i > 0 {
					body += ","
				}
				body += fmt.Sprintf(`{"cve_id":"CVE-P1-%d"}`, i)
			}
			body += `]}`
			_, _ = w.Write([]byte(body))
		case "2":
			body := `{"page":2,"size":300,"total":650,"results":[`
			for i := 0; i < 300; i++ {
				if i > 0 {
					body += ","
				}
				body += fmt.Sprintf(`{"cve_id":"CVE-P2-%d"}`, i)
			}
			body += `]}`
			_, _ = w.Write([]byte(body))
		case "3":
			// Short page — 50 records — signals end.
			body := `{"page":3,"size":300,"total":650,"results":[`
			for i := 0; i < 50; i++ {
				if i > 0 {
					body += ","
				}
				body += fmt.Sprintf(`{"cve_id":"CVE-P3-%d"}`, i)
			}
			body += `]}`
			_, _ = w.Write([]byte(body))
		default:
			_, _ = w.Write([]byte(`{"page":4,"size":300,"total":650,"results":[]}`))
		}
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	total := 0
	err := c.ListVulnerabilitiesStream(context.Background(), VulnerabilityFilters{}, func(page []Vulnerability) error {
		total += len(page)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if total != 650 {
		t.Fatalf("expected 650 total records, got %d", total)
	}
}

func TestListVulnerabilitiesRespectsTotal(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		// Return 5 records with total=5; walk should stop after one page.
		_, _ = w.Write([]byte(`{"page":1,"size":300,"total":5,"results":[
			{"cve_id":"CVE-A"},{"cve_id":"CVE-B"},{"cve_id":"CVE-C"},{"cve_id":"CVE-D"},{"cve_id":"CVE-E"}
		]}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	got, err := c.ListVulnerabilities(context.Background(), VulnerabilityFilters{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5 records, got %d", len(got))
	}
	if calls != 1 {
		t.Fatalf("expected 1 server call (total reached), got %d", calls)
	}
}

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
