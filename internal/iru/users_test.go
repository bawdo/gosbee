package iru

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetUserByID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/users/u-1") {
			t.Errorf("path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"u-1","name":"Keith","email":"k@x"}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	u, err := c.GetUser(context.Background(), "u-1")
	if err != nil {
		t.Fatal(err)
	}
	if u.ID != "u-1" || u.Email != "k@x" {
		t.Fatalf("got %+v", u)
	}
}

func TestFindUserByEmailUsesServerFilter(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(`{"next": null, "previous": null, "results": [{"id": "u-match", "email": "Keith@FlipGroup.com.au"}]}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	u, err := c.FindUserByEmail(context.Background(), "keith@flipgroup.com.au")
	if err != nil {
		t.Fatal(err)
	}
	if u.ID != "u-match" {
		t.Fatalf("got %+v", u)
	}
	if !strings.Contains(gotQuery, "email=keith%40flipgroup.com.au") {
		t.Fatalf("expected email filter in query, got %q", gotQuery)
	}
}

func TestFindUserByEmailNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"next": null, "previous": null, "results": []}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	_, err := c.FindUserByEmail(context.Background(), "nobody@x")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
