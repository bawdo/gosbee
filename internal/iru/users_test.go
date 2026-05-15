package iru

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestFindUserByEmailScansPages(t *testing.T) {
	var page int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page++
		switch page {
		case 1:
			users := make([]map[string]string, DefaultLimit)
			for i := range users {
				users[i] = map[string]string{"id": fmt.Sprintf("u-%d", i), "email": fmt.Sprintf("a%d@x", i)}
			}
			_ = json.NewEncoder(w).Encode(users)
		case 2:
			_ = json.NewEncoder(w).Encode([]map[string]string{
				{"id": "u-match", "email": "Keith@FlipGroup.com.au"},
			})
		}
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
}

func TestFindUserByEmailNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	_, err := c.FindUserByEmail(context.Background(), "nobody@x")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
