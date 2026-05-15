package cmd

import (
	"errors"
	"testing"

	"flipgroup.com.au/jellyfish/internal/iru"
)

func TestClassifyError(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{nil, 0},
		{errors.New("oops"), 1},
		{iru.ErrUnauthorized, 2},
		{iru.ErrForbidden, 2},
		{iru.ErrNotFound, 3},
		{&iru.APIError{Status: 500}, 4},
		{iru.ErrRateLimited, 4},
		{&iru.APIError{Status: 429}, 4},
	}
	for _, c := range cases {
		got := classifyError(c.err)
		if got != c.want {
			t.Fatalf("classify(%v)=%d want %d", c.err, got, c.want)
		}
	}
}
