package managers

import (
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// treeManager is the shared base for all manager types. It holds the
// transformer pipeline common to Select, Insert, Update, and Delete managers.
type treeManager struct {
	transformers []plugins.Transformer
}

// addTransformer appends a transformer plugin to the pipeline.
func (tm *treeManager) addTransformer(t plugins.Transformer) {
	tm.transformers = append(tm.transformers, t)
}

// Transformers returns the registered transformer pipeline.
func (tm *treeManager) Transformers() []plugins.Transformer {
	return tm.transformers
}

// toSQLParams is a helper that resets a parameterizer (if present), calls
// the provided generate function, and returns SQL + params.
func toSQLParams(v nodes.Visitor, generate func(nodes.Visitor) (string, error)) (string, []any, error) {
	p, _ := v.(nodes.Parameterizer)
	if p != nil {
		p.Reset()
	}

	sql, err := generate(v)
	if err != nil {
		return "", nil, err
	}

	if p != nil {
		return sql, p.Params(), nil
	}
	return sql, nil, nil
}
