package iru

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
)

const detectionsPath = "/vulnerability-management/detections"

// ListDetectionsPage fetches one page of detections.
func (c *Client) ListDetectionsPage(ctx context.Context, f DetectionFilters, limit, offset int) ([]Detection, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	q.Set("offset", strconv.Itoa(offset))
	if f.DeviceID != "" {
		q.Set("device_id", f.DeviceID)
	}
	if f.Status != "" {
		q.Set("status", f.Status)
	}
	var out []Detection
	if err := c.do(ctx, http.MethodGet, detectionsPath, q, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ListDetections auto-paginates /vulnerability-management/detections using DefaultLimit.
func (c *Client) ListDetections(ctx context.Context, f DetectionFilters) ([]Detection, error) {
	var all []Detection
	err := Walk[Detection](ctx, DefaultLimit,
		func(ctx context.Context, limit, offset int) ([]Detection, error) {
			return c.ListDetectionsPage(ctx, f, limit, offset)
		},
		func(page []Detection) error {
			all = append(all, page...)
			return nil
		},
	)
	return all, err
}
