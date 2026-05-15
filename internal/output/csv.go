package output

import (
	"encoding/csv"
	"errors"
	"io"
	"reflect"
)

type csvRenderer struct {
	columns []Column
}

func CSV() *csvRenderer { return &csvRenderer{} }

func (r *csvRenderer) WithColumns(cols []Column) *csvRenderer {
	r.columns = cols
	return r
}

func (r *csvRenderer) Render(w io.Writer, v any) error {
	if len(r.columns) == 0 {
		return errors.New("csv renderer requires WithColumns before Render")
	}

	cw := csv.NewWriter(w)
	defer cw.Flush()

	header := make([]string, len(r.columns))
	for i, c := range r.columns {
		header[i] = c.Header
	}
	if err := cw.Write(header); err != nil {
		return err
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			if err := r.writeRow(cw, rv.Index(i).Interface()); err != nil {
				return err
			}
		}
	default:
		if err := r.writeRow(cw, v); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func (r *csvRenderer) writeRow(cw *csv.Writer, v any) error {
	row := make([]string, len(r.columns))
	for i, c := range r.columns {
		row[i] = c.Extract(v)
	}
	return cw.Write(row)
}
