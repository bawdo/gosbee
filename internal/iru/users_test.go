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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")
		if cursor == "page2" {
			// Second page: return the matching user with no next page.
			resp := map[string]any{
				"next":     nil,
				"previous": nil,
				"results": []map[string]string{
					{"id": "u-match", "email": "Keith@FlipGroup.com.au"},
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		// First page: 300 non-matching users with a next cursor.
		users := make([]map[string]string, DefaultLimit)
		for i := range users {
			users[i] = map[string]string{"id": fmt.Sprintf("u-%d", i), "email": fmt.Sprintf("a%d@x", i)}
		}
		nextURL := fmt.Sprintf("%s/api/v1/users?cursor=page2&limit=%d", "https://example.iru", DefaultLimit)
		resp := map[string]any{
			"next":     nextURL,
			"previous": nil,
			"results":  users,
		}
		_ = json.NewEncoder(w).Encode(resp)
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
		_, _ = w.Write([]byte(`{"next":null,"previous":null,"results":[]}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	_, err := c.FindUserByEmail(context.Background(), "nobody@x")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
