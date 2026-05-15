package iru

import (
	"context"
	"errors"
	"testing"
)

// ---- WalkCursor tests ----

func TestWalkCursorPaginates(t *testing.T) {
	pages := []struct {
		items   []int
		nextRaw string
	}{
		{[]int{1, 2, 3}, "X"},
		{[]int{4, 5, 6}, "Y"},
		{[]int{7, 8}, ""}, // last page
	}
	var seenCursors []string
	idx := 0
	fetch := func(_ context.Context, _ int, cursor string) ([]int, string, error) {
		seenCursors = append(seenCursors, cursor)
		p := pages[idx]
		idx++
		return p.items, p.nextRaw, nil
	}
	var collected []int
	if err := WalkCursor[int](context.Background(), 0, fetch, func(page []int) error {
		collected = append(collected, page...)
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	want := []int{1, 2, 3, 4, 5, 6, 7, 8}
	if len(collected) != len(want) {
		t.Fatalf("got %v want %v", collected, want)
	}
	if seenCursors[0] != "" {
		t.Fatalf("first cursor should be empty, got %q", seenCursors[0])
	}
}

func TestWalkCursorStopsOnCallbackError(t *testing.T) {
	fetch := func(_ context.Context, _ int, _ string) ([]int, string, error) {
		return []int{1, 2, 3}, "next", nil
	}
	var calls int
	err := WalkCursor[int](context.Background(), 5, fetch, func(page []int) error {
		calls++
		return context.Canceled
	})
	if err == nil {
		t.Fatal("expected error from callback to propagate")
	}
	if calls != 1 {
		t.Fatalf("expected 1 callback invocation, got %d", calls)
	}
}

func TestWalkCursorPropagatesFetchError(t *testing.T) {
	customErr := errors.New("fetch failed")
	calls := 0
	fetch := func(_ context.Context, _ int, _ string) ([]int, string, error) {
		calls++
		if calls == 1 {
			return []int{1, 2, 3}, "next", nil
		}
		return nil, "", customErr
	}
	err := WalkCursor[int](context.Background(), 5, fetch, func(page []int) error {
		return nil
	})
	if !errors.Is(err, customErr) {
		t.Fatalf("expected fetch error to propagate, got %v", err)
	}
}

func TestWalkCursorUsesDefaultLimitWhenNonPositive(t *testing.T) {
	var seenLimit int
	fetch := func(_ context.Context, limit int, _ string) ([]int, string, error) {
		if seenLimit == 0 {
			seenLimit = limit
		}
		return nil, "", nil // empty page, no next cursor terminates immediately
	}
	if err := WalkCursor[int](context.Background(), 0, fetch, func(page []int) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if seenLimit != DefaultLimit {
		t.Fatalf("expected DefaultLimit (%d), got %d", DefaultLimit, seenLimit)
	}
}

// ---- Walk tests ----

func TestWalkPaginates(t *testing.T) {
	pages := [][]int{
		{1, 2, 3, 4, 5},
		{6, 7, 8, 9, 10},
		{11, 12}, // short page signals end
	}

	var seenLimits []int
	var seenOffsets []int
	fetch := func(_ context.Context, limit, offset int) ([]int, error) {
		seenLimits = append(seenLimits, limit)
		seenOffsets = append(seenOffsets, offset)
		idx := offset / limit
		if idx >= len(pages) {
			return nil, nil
		}
		return pages[idx], nil
	}

	var collected []int
	err := Walk[int](context.Background(), 5, fetch, func(page []int) error {
		collected = append(collected, page...)
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	want := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	if len(collected) != len(want) {
		t.Fatalf("got %v want %v", collected, want)
	}
	for i, v := range want {
		if collected[i] != v {
			t.Fatalf("idx %d: got %d want %d", i, collected[i], v)
		}
	}
	if len(seenOffsets) != 3 {
		t.Fatalf("expected 3 fetches, got %d (offsets=%v)", len(seenOffsets), seenOffsets)
	}
}

func TestWalkStopsOnCallbackError(t *testing.T) {
	fetch := func(_ context.Context, limit, offset int) ([]int, error) {
		return []int{1, 2, 3, 4, 5}, nil
	}
	var calls int
	err := Walk[int](context.Background(), 5, fetch, func(page []int) error {
		calls++
		return context.Canceled
	})
	if err == nil {
		t.Fatal("expected error from callback to propagate")
	}
	if calls != 1 {
		t.Fatalf("expected 1 callback invocation, got %d", calls)
	}
}

func TestWalkSkipsEmptyPages(t *testing.T) {
	calls := 0
	fetch := func(_ context.Context, limit, offset int) ([]int, error) {
		calls++
		if calls == 1 {
			return nil, nil // empty page terminates without calling callback
		}
		return []int{1, 2}, nil // unreachable; walk stops on empty page
	}
	var cbCalls int
	var collected []int
	err := Walk[int](context.Background(), 5, fetch, func(page []int) error {
		cbCalls++
		collected = append(collected, page...)
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if cbCalls != 0 {
		t.Fatalf("expected callback to not fire for empty page, got %d calls", cbCalls)
	}
	if len(collected) != 0 {
		t.Fatalf("expected 0 items from empty page, got %v", collected)
	}
	if calls != 1 {
		t.Fatalf("expected 1 fetch (stops on empty page), got %d", calls)
	}
}

func TestWalkPropagatesFetchError(t *testing.T) {
	customErr := errors.New("fetch failed")
	calls := 0
	fetch := func(_ context.Context, limit, offset int) ([]int, error) {
		calls++
		if calls == 1 {
			return []int{1, 2, 3, 4, 5}, nil
		}
		return nil, customErr
	}
	err := Walk[int](context.Background(), 5, fetch, func(page []int) error {
		return nil
	})
	if !errors.Is(err, customErr) {
		t.Fatalf("expected fetch error to propagate, got %v", err)
	}
}

func TestWalkUsesDefaultLimitWhenNonPositive(t *testing.T) {
	var seenLimit int
	fetch := func(_ context.Context, limit, offset int) ([]int, error) {
		if seenLimit == 0 {
			seenLimit = limit
		}
		return nil, nil // empty page terminates immediately
	}
	if err := Walk[int](context.Background(), 0, fetch, func(page []int) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if seenLimit != DefaultLimit {
		t.Fatalf("expected DefaultLimit (%d), got %d", DefaultLimit, seenLimit)
	}
}

func TestWalkCursorHandlesBothNextShapes(t *testing.T) {
	cases := []struct {
		name     string
		nextRaws []string // value Iru would put in "next"; "" means last page
		want     []int    // expected accumulated results
	}{
		{
			name:     "raw cursor strings (detections style)",
			nextRaws: []string{"raw-cursor-1", "raw-cursor-2", ""},
			want:     []int{1, 2, 3, 4, 5, 6},
		},
		{
			name: "full URL with cursor query param (users style)",
			nextRaws: []string{
				"https://example.com/api/v1/users?cursor=url-cursor-1&limit=2",
				"https://example.com/api/v1/users?cursor=url-cursor-2&limit=2",
				"",
			},
			want: []int{1, 2, 3, 4, 5, 6},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			pages := []paginated[int]{
				{Next: stringPtr(c.nextRaws[0]), Results: []int{1, 2}},
				{Next: stringPtr(c.nextRaws[1]), Results: []int{3, 4}},
				{Next: stringPtr(c.nextRaws[2]), Results: []int{5, 6}},
			}

			var seenCursors []string
			idx := 0
			fetch := func(_ context.Context, _ int, cursor string) ([]int, string, error) {
				seenCursors = append(seenCursors, cursor)
				p := pages[idx]
				idx++
				return p.Results, p.nextCursor(), nil
			}

			var collected []int
			if err := WalkCursor[int](context.Background(), 2, fetch, func(page []int) error {
				collected = append(collected, page...)
				return nil
			}); err != nil {
				t.Fatalf("walk: %v", err)
			}

			if len(collected) != len(c.want) {
				t.Fatalf("collected %v want %v", collected, c.want)
			}
			for i, v := range c.want {
				if collected[i] != v {
					t.Fatalf("idx %d: got %d want %d", i, collected[i], v)
				}
			}
			if seenCursors[0] != "" {
				t.Fatalf("first cursor should be empty, got %q", seenCursors[0])
			}
			// Second and third pages should have a non-empty cursor that's
			// distinct between iterations.
			if seenCursors[1] == "" || seenCursors[2] == "" {
				t.Fatalf("expected non-empty cursors after first page, got %v", seenCursors)
			}
			if seenCursors[1] == seenCursors[2] {
				t.Fatalf("expected distinct cursors per page, got %v", seenCursors)
			}
		})
	}
}

func stringPtr(s string) *string { return &s }
