package cmd

import (
	"errors"
	"testing"

	"flipgroup.com.au/jellyfish/internal/gmail"
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

func TestClassifyErrorMapsGmailUnauthorizedToExit2(t *testing.T) {
	if got := classifyError(gmail.ErrUnauthorized); got != 2 {
		t.Errorf("got %d, want 2", got)
	}
}

func TestClassifyErrorMapsGmailForbiddenToExit2(t *testing.T) {
	if got := classifyError(gmail.ErrForbidden); got != 2 {
		t.Errorf("got %d, want 2", got)
	}
}

func TestClassifyErrorMapsGmailRateLimitedToExit4(t *testing.T) {
	if got := classifyError(gmail.ErrRateLimited); got != 4 {
		t.Errorf("got %d, want 4", got)
	}
}

func TestClassifyErrorMapsGmailUpstreamToExit4(t *testing.T) {
	if got := classifyError(gmail.ErrUpstream); got != 4 {
		t.Errorf("got %d, want 4", got)
	}
}
