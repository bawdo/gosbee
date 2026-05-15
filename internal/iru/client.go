package iru

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Client talks to the Iru/Kandji API.
type Client struct {
	baseURL    string
	httpClient *http.Client
	userAgent  string
}

// Option configures a Client at construction time.
type Option func(*Client)

func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.httpClient = h } }
func WithUserAgent(ua string) Option       { return func(c *Client) { c.userAgent = ua } }
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if c.httpClient == nil {
			c.httpClient = &http.Client{}
		}
		c.httpClient.Timeout = d
	}
}

// NewClient constructs a Client. baseURL must end with /api/v1 (no trailing slash).
func NewClient(baseURL, token string, opts ...Option) *Client {
	c := &Client{
		baseURL:   baseURL,
		userAgent: "jellyfish/dev",
		httpClient: &http.Client{
			Transport: &authTransport{token: token, base: http.DefaultTransport},
			Timeout:   30 * time.Second,
		},
	}
	for _, o := range opts {
		o(c)
	}
	// Preserve the auth transport when callers swap in their own http.Client.
	if c.httpClient.Transport == nil {
		c.httpClient.Transport = &authTransport{token: token, base: http.DefaultTransport}
	}
	return c
}

// do builds and sends a request. If out is non-nil and the response status is 2xx,
// the body is JSON-decoded into out.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, out any) error {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	var body io.Reader
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return decodeAPIError(resp)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(out); err != nil {
		raw, _ := io.ReadAll(io.MultiReader(bytes.NewReader([]byte{}), resp.Body))
		return fmt.Errorf("decode response: %w (raw=%q)", err, string(raw))
	}
	return nil
}
