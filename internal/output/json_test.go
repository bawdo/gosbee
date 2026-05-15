package output

import (
	"bytes"
	"strings"
	"testing"
)

func TestJSONRendererSimpleStruct(t *testing.T) {
	r := JSON()
	buf := &bytes.Buffer{}
	type item struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	if err := r.Render(buf, item{Name: "rover", Age: 4}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, `"name": "rover"`) {
		t.Fatalf("bad JSON output: %q", out)
	}
	if !strings.HasSuffix(out, "\n") {
		t.Fatalf("expected trailing newline, got %q", out)
	}
}
