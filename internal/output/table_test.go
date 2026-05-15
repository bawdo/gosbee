package output

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

type tableItem struct {
	Name string
	Age  int
}

func intStr(i int) string { return strconv.Itoa(i) }

func TestTableRendererSliceWithColumns(t *testing.T) {
	rows := []tableItem{
		{Name: "rover", Age: 4},
		{Name: "spot", Age: 7},
	}

	r := Table().WithColumns([]Column{
		{Header: "NAME", Extract: func(v any) string { return v.(tableItem).Name }},
		{Header: "AGE", Extract: func(v any) string { return intStr(v.(tableItem).Age) }},
	})

	buf := &bytes.Buffer{}
	if err := r.Render(buf, rows); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"NAME", "AGE", "rover", "spot", "4", "7"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q in output:\n%s", want, out)
		}
	}
}

func TestTableRendererSingleStruct(t *testing.T) {
	r := Table().WithColumns([]Column{
		{Header: "NAME", Extract: func(v any) string { return v.(tableItem).Name }},
		{Header: "AGE", Extract: func(v any) string { return intStr(v.(tableItem).Age) }},
	})

	buf := &bytes.Buffer{}
	if err := r.Render(buf, tableItem{Name: "rover", Age: 4}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "rover") {
		t.Fatalf("expected name in output, got: %s", buf.String())
	}
}
