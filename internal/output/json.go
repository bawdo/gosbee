package output

import (
	"encoding/json"
	"io"
)

type jsonRenderer struct{}

func JSON() Renderer { return jsonRenderer{} }

func (jsonRenderer) Render(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
