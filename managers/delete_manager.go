package managers

import (
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// DeleteManager provides a fluent API for building DELETE statements.
type DeleteManager struct {
	treeManager
	Statement *nodes.DeleteStatement
}

// NewDeleteManager creates a new DeleteManager targeting the given table.
func NewDeleteManager(from nodes.Node) *DeleteManager {
	return &DeleteManager{
		Statement: &nodes.DeleteStatement{From: from},
	}
}

// Where appends conditions to the WHERE clause.
func (m *DeleteManager) Where(conditions ...nodes.Node) *DeleteManager {
	m.Statement.Wheres = append(m.Statement.Wheres, conditions...)
	return m
}

// Returning sets the RETURNING clause columns.
func (m *DeleteManager) Returning(cols ...nodes.Node) *DeleteManager {
	m.Statement.Returning = cols
	return m
}

// Use registers a transformer plugin.
func (m *DeleteManager) Use(t plugins.Transformer) *DeleteManager {
	m.addTransformer(t)
	return m
}

// toSQLCore applies transformers and generates SQL.
func (m *DeleteManager) toSQLCore(v nodes.Visitor) (string, error) {
	stmt := m.cloneStatement()
	for _, t := range m.transformers {
		var err error
		stmt, err = t.TransformDelete(stmt)
		if err != nil {
			return "", err
		}
	}
	return stmt.Accept(v), nil
}

// ToSQL applies transformers and generates SQL with parameters.
// Returns SQL string, parameter values (if parameterised), and any error.
func (m *DeleteManager) ToSQL(v nodes.Visitor) (string, []any, error) {
	return toSQLParams(v, m.toSQLCore)
}

// ToSQLParams applies transformers and generates parameterized SQL.
//
// Deprecated: Use ToSQL() instead, which now always returns params.
func (m *DeleteManager) ToSQLParams(v nodes.Visitor) (string, []any, error) {
	return m.ToSQL(v)
}

func (m *DeleteManager) cloneStatement() *nodes.DeleteStatement {
	wheres := make([]nodes.Node, len(m.Statement.Wheres))
	copy(wheres, m.Statement.Wheres)

	returning := make([]nodes.Node, len(m.Statement.Returning))
	copy(returning, m.Statement.Returning)

	return &nodes.DeleteStatement{
		From:      m.Statement.From,
		Wheres:    wheres,
		Returning: returning,
	}
}
