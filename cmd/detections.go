package cmd

import (
	"context"
	"fmt"
	"io"

	"flipgroup.com.au/jellyfish/internal/cache"
	"flipgroup.com.au/jellyfish/internal/iru"
)

// fetchAllDetections returns every detection in the tenant, either from the
// on-disk cache (when fresh and useCache is true) or by walking the API. The
// progress indicator on stderr lets the user see the walk advancing.
func fetchAllDetections(ctx context.Context, client iruClient, stderr io.Writer, useCache bool) ([]iru.Detection, error) {
	cachePath, err := cache.DefaultPath()
	if err != nil {
		// Non-fatal: just proceed without cache.
		cachePath = ""
	}

	if useCache && cachePath != "" {
		if cached, hit, err := cache.Load(cachePath, cache.DefaultTTL); err == nil && hit {
			_, _ = fmt.Fprintf(stderr, "using cached detections (%d records); pass --no-cache for fresh data\n", len(cached))
			return cached, nil
		}
	}

	var all []iru.Detection
	pages := 0
	err = client.ListDetectionsStream(ctx, iru.DetectionFilters{}, func(page []iru.Detection) error {
		all = append(all, page...)
		pages++
		_, _ = fmt.Fprintf(stderr, "\rfetching detections: %d pages, %d records...", pages, len(all))
		return nil
	})
	// Clear the progress line with a newline so subsequent output is on its own line.
	if pages > 0 {
		_, _ = fmt.Fprintln(stderr)
	}
	if err != nil {
		return nil, err
	}

	if useCache && cachePath != "" {
		// Cache save is best-effort; warn but don't fail the command.
		if saveErr := cache.Save(cachePath, all); saveErr != nil {
			_, _ = fmt.Fprintf(stderr, "warning: could not write cache at %s: %v\n", cachePath, saveErr)
		}
	}
	return all, nil
}
