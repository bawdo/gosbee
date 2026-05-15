package output

import (
	"errors"
	"io"
	"reflect"

	"github.com/jedib0t/go-pretty/v6/table"
)

// Column declares one column in a table render. Extract is called per row.
type Column struct {
	Header  string
	Extract func(v any) string
}

type tableRenderer struct {
	columns []Column
}

// Table returns a renderer that needs WithColumns called before Render.
func Table() *tableRenderer { return &tableRenderer{} }

// WithColumns returns a renderer configured for the given columns.
func (r *tableRenderer) WithColumns(cols []Column) *tableRenderer {
	r.columns = cols
	return r
}

// Render writes v as a table. v must be a struct or a slice of structs.
func (r *tableRenderer) Render(w io.Writer, v any) error {
	if len(r.columns) == 0 {
		return errors.New("table renderer requires WithColumns before Render")
	}

	t := table.NewWriter()
	t.SetOutputMirror(w)

	header := make(table.Row, len(r.columns))
	for i, c := range r.columns {
		header[i] = c.Header
	}
	t.AppendHeader(header)

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			t.AppendRow(r.rowFor(rv.Index(i).Interface()))
		}
	default:
		t.AppendRow(r.rowFor(v))
	}

	t.Render()
	return nil
}

func (r *tableRenderer) rowFor(v any) table.Row {
	row := make(table.Row, len(r.columns))
	for i, c := range r.columns {
		row[i] = c.Extract(v)
	}
	return row
}
