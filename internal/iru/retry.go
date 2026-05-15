package iru

import (
	"net/http"
	"strconv"
	"time"
)

const maxAttempts = 3

// retryTransport retries idempotent requests on 429 and 5xx with backoff.
type retryTransport struct {
	base http.RoundTripper
	// sleep allows tests to inject a no-op sleeper.
	sleep func(time.Duration)
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.sleep == nil {
		t.sleep = time.Sleep
	}
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return t.base.RoundTrip(req)
	}

	var lastResp *http.Response
	var lastErr error
	backoff := 200 * time.Millisecond

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp, err := t.base.RoundTrip(req.Clone(req.Context()))
		if err != nil {
			lastErr = err
			t.sleep(backoff)
			backoff *= 2
			continue
		}
		if !shouldRetry(resp.StatusCode) {
			return resp, nil
		}
		// Drain and close so the conn can be reused.
		_ = resp.Body.Close()
		lastResp = resp

		wait := backoff
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil {
				wait = time.Duration(secs) * time.Second
			}
		}
		if attempt < maxAttempts {
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-time.After(wait):
			}
			backoff *= 2
		}
	}

	if lastResp != nil {
		return lastResp, nil
	}
	return nil, lastErr
}

func shouldRetry(status int) bool {
	if status == http.StatusTooManyRequests {
		return true
	}
	return status >= 500 && status <= 599
}
