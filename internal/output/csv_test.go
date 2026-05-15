package output

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func TestCSVRendererSliceWithColumns(t *testing.T) {
	rows := []tableItem{
		{Name: "rover", Age: 4},
		{Name: "spot", Age: 7},
	}

	r := CSV().WithColumns([]Column{
		{Header: "name", Extract: func(v any) string { return v.(tableItem).Name }},
		{Header: "age", Extract: func(v any) string { return strconv.Itoa(v.(tableItem).Age) }},
	})

	buf := &bytes.Buffer{}
	if err := r.Render(buf, rows); err != nil {
		t.Fatal(err)
	}

	want := "name,age\nrover,4\nspot,7\n"
	if buf.String() != want {
		t.Fatalf("got %q want %q", buf.String(), want)
	}
}

func TestCSVRendererEscapesCommas(t *testing.T) {
	r := CSV().WithColumns([]Column{
		{Header: "v", Extract: func(v any) string { return v.(string) }},
	})
	buf := &bytes.Buffer{}
	if err := r.Render(buf, []string{"a,b"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"a,b"`) {
		t.Fatalf("expected quoting, got %q", buf.String())
	}
}
