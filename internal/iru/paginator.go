package iru

import "context"

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
