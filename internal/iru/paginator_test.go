package iru

import (
	"context"
	"errors"
	"testing"
)

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
