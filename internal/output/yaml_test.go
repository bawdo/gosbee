package output

import (
	"bytes"
	"strings"
	"testing"
)

func TestYAMLRendererSimpleStruct(t *testing.T) {
	r := YAML()
	buf := &bytes.Buffer{}
	type item struct {
		Name string `yaml:"name"`
		Age  int    `yaml:"age"`
	}
	if err := r.Render(buf, item{Name: "rover", Age: 4}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "name: rover") {
		t.Fatalf("bad YAML output: %q", out)
	}
	if !strings.Contains(out, "age: 4") {
		t.Fatalf("bad YAML output: %q", out)
	}
}
