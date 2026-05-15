package iru

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// Sentinel errors callers compare against with errors.Is.
var (
	ErrUnauthorized = errors.New("iru: unauthorized")
	ErrForbidden    = errors.New("iru: forbidden")
	ErrNotFound     = errors.New("iru: not found")
	ErrRateLimited  = errors.New("iru: rate limited")
)

// APIError is the typed error returned for non-2xx responses that do not map
// to a sentinel.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("iru: api error: status %d", e.Status)
	}
	return fmt.Sprintf("iru: api error: status %d: %s", e.Status, e.Message)
}

// Is lets callers use errors.Is to recover the sentinel for well-known statuses.
func (e *APIError) Is(target error) bool {
	switch e.Status {
	case http.StatusUnauthorized:
		return target == ErrUnauthorized
	case http.StatusForbidden:
		return target == ErrForbidden
	case http.StatusNotFound:
		return target == ErrNotFound
	case http.StatusTooManyRequests:
		return target == ErrRateLimited
	}
	return false
}

// decodeAPIError reads the response body and produces an APIError.
func decodeAPIError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	apiErr := &APIError{Status: resp.StatusCode}

	// Iru typically returns {"detail":"..."} or {"errors":[...]} - try both.
	var payload struct {
		Detail string `json:"detail"`
		Code   string `json:"code"`
	}
	if err := json.Unmarshal(body, &payload); err == nil {
		apiErr.Message = payload.Detail
		apiErr.Code = payload.Code
	}
	if apiErr.Message == "" && len(body) > 0 && len(body) < 512 {
		apiErr.Message = string(body)
	}
	return apiErr
}
