package output

import (
	"fmt"
	"io"
)

// Renderer writes a value v to w in a specific format.
type Renderer interface {
	Render(w io.Writer, v any) error
}

// For renders a value v to w using the renderer chosen by format.
// Supported formats: table, json, yaml, csv (later tasks add the others).
func For(format string) (Renderer, error) {
	switch format {
	case "json":
		return JSON(), nil
	case "yaml":
		return YAML(), nil
	case "csv":
		return CSV(), nil
	case "table", "":
		// Table requires columns per-command; callers should construct it directly.
		return Table(), nil
	default:
		return nil, fmt.Errorf("unsupported output format %q", format)
	}
}
