package managers

import (
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// SetOperationManager provides transformer support for set operations
// (UNION, INTERSECT, EXCEPT) so plugins police every branch.
type SetOperationManager struct {
	treeManager
	Node *nodes.SetOperationNode
}

// NewSetOperationManager creates a new SetOperationManager wrapping the
// given set operation.
func NewSetOperationManager(op *nodes.SetOperationNode) *SetOperationManager {
	return &SetOperationManager{Node: op}
}

// Use registers a transformer plugin.
func (m *SetOperationManager) Use(t plugins.Transformer) *SetOperationManager {
	m.addTransformer(t)
	return m
}

// toSQLCore applies transformers to a copy of the set operation tree and
// generates SQL. The wrapped node is never modified.
func (m *SetOperationManager) toSQLCore(v nodes.Visitor) (string, error) {
	var n nodes.Node = m.Node
	for _, t := range m.transformers {
		var err error
		n, err = plugins.ApplyNode(t, n)
		if err != nil {
			return "", err
		}
	}
	return n.Accept(v), nil
}

// ToSQL applies transformers and generates SQL with parameters.
// Returns SQL string, parameter values (if parameterised), and any error.
func (m *SetOperationManager) ToSQL(v nodes.Visitor) (string, []any, error) {
	return toSQLParams(v, m.toSQLCore)
}
