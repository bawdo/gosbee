package iru

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// GetUser fetches a single user by ID.
func (c *Client) GetUser(ctx context.Context, id string) (User, error) {
	var u User
	if err := c.do(ctx, http.MethodGet, "/users/"+url.PathEscape(id), nil, &u); err != nil {
		return User{}, err
	}
	return u, nil
}

// ListUsersPage fetches one page of users. cursor is the opaque value taken
// from the previous page's next URL; pass "" for the first page. The returned
// nextCursor is "" when there are no more pages.
func (c *Client) ListUsersPage(ctx context.Context, limit int, cursor string) ([]User, string, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	var p paginated[User]
	if err := c.do(ctx, http.MethodGet, "/users", q, &p); err != nil {
		return nil, "", err
	}
	return p.Results, p.nextCursor(), nil
}

// FindUserByEmail walks the user list and returns the first case-insensitive
// email match. Returns ErrNotFound if no user matches.
func (c *Client) FindUserByEmail(ctx context.Context, email string) (User, error) {
	target := strings.ToLower(email)
	var found User
	stop := errors.New("found")
	err := WalkCursor[User](ctx, DefaultLimit,
		c.ListUsersPage,
		func(page []User) error {
			for _, u := range page {
				if strings.ToLower(u.Email) == target {
					found = u
					return stop
				}
			}
			return nil
		},
	)
	if errors.Is(err, stop) {
		return found, nil
	}
	if err != nil {
		return User{}, err
	}
	return User{}, ErrNotFound
}
