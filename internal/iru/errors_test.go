package iru

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestErrorMapping(t *testing.T) {
	cases := []struct {
		status int
		want   error
	}{
		{http.StatusUnauthorized, ErrUnauthorized},
		{http.StatusForbidden, ErrForbidden},
		{http.StatusNotFound, ErrNotFound},
		{http.StatusTooManyRequests, ErrRateLimited},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(c.status)
			_, _ = w.Write([]byte(`{"detail":"nope"}`))
		}))
		client := NewClient(srv.URL, "tkn")
		err := client.do(context.Background(), http.MethodGet, "/x", nil, nil)
		srv.Close()
		if !errors.Is(err, c.want) {
			t.Fatalf("status %d: expected %v, got %v", c.status, c.want, err)
		}
	}
}

func TestAPIErrorCarriesStatusAndMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"detail":"bad subdomain"}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	err := c.do(context.Background(), http.MethodGet, "/x", nil, nil)

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %v", err)
	}
	if apiErr.Status != 400 {
		t.Fatalf("status: %d", apiErr.Status)
	}
	if apiErr.Message == "" {
		t.Fatalf("expected non-empty message")
	}
}
