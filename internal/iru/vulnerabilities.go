package iru

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
)

const detectionsPath = "/vulnerability-management/detections"

// ListDetectionsPage fetches one page of detections. cursor is the opaque
// value taken from the previous page's next URL; pass "" for the first page.
// The returned nextCursor is "" when there are no more pages.
func (c *Client) ListDetectionsPage(ctx context.Context, _ DetectionFilters, limit int, cursor string) ([]Detection, string, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	var p paginated[Detection]
	if err := c.do(ctx, http.MethodGet, detectionsPath, q, &p); err != nil {
		return nil, "", err
	}
	return p.Results, p.nextCursor(), nil
}

// ListDetections auto-paginates the detections endpoint.
func (c *Client) ListDetections(ctx context.Context, f DetectionFilters) ([]Detection, error) {
	var all []Detection
	err := WalkCursor[Detection](ctx, DefaultLimit,
		func(ctx context.Context, limit int, cursor string) ([]Detection, string, error) {
			return c.ListDetectionsPage(ctx, f, limit, cursor)
		},
		func(page []Detection) error {
			all = append(all, page...)
			return nil
		},
	)
	return all, err
}
