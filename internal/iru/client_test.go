package iru

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientSetsAuthAndUserAgent(t *testing.T) {
	var got *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn-abc", WithUserAgent("jellyfish/test"))
	if err := c.do(context.Background(), http.MethodGet, "/ping", nil, nil); err != nil {
		t.Fatalf("do: %v", err)
	}

	if got == nil {
		t.Fatal("no request captured")
	}
	if h := got.Header.Get("Authorization"); h != "Bearer tkn-abc" {
		t.Fatalf("auth header %q", h)
	}
	if h := got.Header.Get("User-Agent"); !strings.HasPrefix(h, "jellyfish/") {
		t.Fatalf("user-agent header %q", h)
	}
}

func TestClientHonoursContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn", WithTimeout(50*time.Millisecond))
	err := c.do(context.Background(), http.MethodGet, "/slow", nil, nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}
