package iru

import (
	"context"
	"net/url"
)

// DefaultLimit is the page size used when callers do not specify one. Iru caps
// limit at 300 server-side; this matches that.
const DefaultLimit = 300

// Walk paginates limit/offset style until the fetch returns a short page.
//
// limit must be > 0. fetch is called once per page. cb is called with each page
// of results; if cb returns an error, the walk stops and that error is returned.
func Walk[T any](
	ctx context.Context,
	limit int,
	fetch func(ctx context.Context, limit, offset int) ([]T, error),
	cb func(page []T) error,
) error {
	if limit <= 0 {
		limit = DefaultLimit
	}
	for offset := 0; ; offset += limit {
		page, err := fetch(ctx, limit, offset)
		if err != nil {
			return err
		}
		if len(page) > 0 {
			if err := cb(page); err != nil {
				return err
			}
		}
		if len(page) < limit {
			return nil
		}
	}
}

// paginated wraps an Iru list response that uses {next, previous, results}
// cursor-based pagination.
type paginated[T any] struct {
	Next     *string `json:"next"`
	Previous *string `json:"previous"`
	Results  []T     `json:"results"`
}

// nextCursor returns the cursor query param from p.Next, or "" if there is no
// next page or the URL cannot be parsed.
//
// Iru returns two different shapes for the `next` field across endpoints:
//   - /users: a fully-qualified URL containing a ?cursor=X query param.
//   - /vulnerability-management/detections: just the raw cursor string.
//
// Distinguish by whether the value parses as an absolute URL (has a scheme).
// If so, extract the `cursor` query param. Otherwise, treat the whole value
// as the cursor.
func (p paginated[T]) nextCursor() string {
	if p.Next == nil || *p.Next == "" {
		return ""
	}
	next := *p.Next
	if u, err := url.Parse(next); err == nil && u.Scheme != "" {
		return u.Query().Get("cursor")
	}
	return next
}

// WalkCursor drives cursor-based pagination. fetch is called once per page
// with the current cursor (empty string for the first page) and must return
// the page's results plus the next cursor (empty when no more pages). cb is
// invoked per non-empty page; an error from cb stops the walk and is returned.
func WalkCursor[T any](
	ctx context.Context,
	limit int,
	fetch func(ctx context.Context, limit int, cursor string) ([]T, string, error),
	cb func(page []T) error,
) error {
	if limit <= 0 {
		limit = DefaultLimit
	}
	cursor := ""
	for {
		page, next, err := fetch(ctx, limit, cursor)
		if err != nil {
			return err
		}
		if len(page) > 0 {
			if err := cb(page); err != nil {
				return err
			}
		}
		if next == "" {
			return nil
		}
		cursor = next
	}
}
