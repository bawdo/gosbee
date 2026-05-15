package output

import (
	"io"

	"gopkg.in/yaml.v3"
)

type yamlRenderer struct{}

func YAML() Renderer { return yamlRenderer{} }

func (yamlRenderer) Render(w io.Writer, v any) error {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	defer func() { _ = enc.Close() }()
	return enc.Encode(v)
}
