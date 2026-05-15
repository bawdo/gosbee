package iru

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
)

// ListDevicesPage fetches one page of devices.
func (c *Client) ListDevicesPage(ctx context.Context, f DeviceFilters, limit, offset int) ([]Device, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	q.Set("offset", strconv.Itoa(offset))
	if f.UserID != "" {
		q.Set("user_id", f.UserID)
	}
	if f.SerialNumber != "" {
		q.Set("serial_number", f.SerialNumber)
	}
	var out []Device
	if err := c.do(ctx, http.MethodGet, "/devices", q, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ListDevices auto-paginates /devices using DefaultLimit.
func (c *Client) ListDevices(ctx context.Context, f DeviceFilters) ([]Device, error) {
	var all []Device
	err := Walk[Device](ctx, DefaultLimit,
		func(ctx context.Context, limit, offset int) ([]Device, error) {
			return c.ListDevicesPage(ctx, f, limit, offset)
		},
		func(page []Device) error {
			all = append(all, page...)
			return nil
		},
	)
	return all, err
}

// GetDeviceBySerial returns the device with the given serial number, or ErrNotFound.
func (c *Client) GetDeviceBySerial(ctx context.Context, serial string) (Device, error) {
	page, err := c.ListDevicesPage(ctx, DeviceFilters{SerialNumber: serial}, 1, 0)
	if err != nil {
		return Device{}, err
	}
	if len(page) == 0 {
		return Device{}, ErrNotFound
	}
	return page[0], nil
}
