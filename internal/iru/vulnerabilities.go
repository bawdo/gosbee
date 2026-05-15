package iru

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
)

const detectionsPath = "/vulnerability-management/detections"
const vulnerabilitiesPath = "/vulnerability-management/vulnerabilities"

// ListDetectionsPage fetches one page of detections. cursor is the opaque
// value taken from the previous page's next URL; pass "" for the first page.
// The returned nextCursor is "" when there are no more pages.
func (c *Client) ListDetectionsPage(ctx context.Context, _ DetectionFilters, limit int, cursor string) ([]Detection, string, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	// Iru's /detections endpoint uses `after`, not `cursor`, for forward
	// pagination (confirmed by probing — sending `cursor` is silently ignored
	// and returns the same first page each time, causing an infinite walk).
	if cursor != "" {
		q.Set("after", cursor)
	}
	var p paginated[Detection]
	if err := c.do(ctx, http.MethodGet, detectionsPath, q, &p); err != nil {
		return nil, "", err
	}
	return p.Results, p.nextCursor(), nil
}

// ListDetectionsStream walks the detections endpoint and invokes cb with each
// page. Returns the first error from fetch or cb.
func (c *Client) ListDetectionsStream(ctx context.Context, f DetectionFilters, cb func(page []Detection) error) error {
	return WalkCursor[Detection](ctx, DefaultLimit,
		func(ctx context.Context, limit int, cursor string) ([]Detection, string, error) {
			return c.ListDetectionsPage(ctx, f, limit, cursor)
		},
		cb,
	)
}

// ListDetections accumulates all detections in memory.
func (c *Client) ListDetections(ctx context.Context, f DetectionFilters) ([]Detection, error) {
	var all []Detection
	err := c.ListDetectionsStream(ctx, f, func(page []Detection) error {
		all = append(all, page...)
		return nil
	})
	return all, err
}

// ListVulnerabilitiesPage fetches one page of vulnerabilities. page is
// 1-indexed; size is the page size (Iru honours up to ~300; values above
// 300 may be clamped server-side).
func (c *Client) ListVulnerabilitiesPage(ctx context.Context, _ VulnerabilityFilters, page, size int) ([]Vulnerability, int, error) {
	q := url.Values{}
	q.Set("page", strconv.Itoa(page))
	q.Set("size", strconv.Itoa(size))
	var p pagedList[Vulnerability]
	if err := c.do(ctx, http.MethodGet, vulnerabilitiesPath, q, &p); err != nil {
		return nil, 0, err
	}
	return p.Results, p.Total, nil
}

// ListVulnerabilitiesStream walks the vulnerabilities endpoint and invokes
// cb with each page. Use this when you want progress reporting; the
// non-streaming ListVulnerabilities just accumulates.
func (c *Client) ListVulnerabilitiesStream(ctx context.Context, f VulnerabilityFilters, cb func(page []Vulnerability) error) error {
	return WalkPaged[Vulnerability](ctx, DefaultLimit,
		func(ctx context.Context, page, size int) ([]Vulnerability, int, error) {
			return c.ListVulnerabilitiesPage(ctx, f, page, size)
		},
		cb,
	)
}

// ListVulnerabilities accumulates every vulnerability in the tenant.
func (c *Client) ListVulnerabilities(ctx context.Context, f VulnerabilityFilters) ([]Vulnerability, error) {
	var all []Vulnerability
	err := c.ListVulnerabilitiesStream(ctx, f, func(page []Vulnerability) error {
		all = append(all, page...)
		return nil
	})
	return all, err
}
