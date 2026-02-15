package managers

import (
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// UpdateManager provides a fluent API for building UPDATE statements.
type UpdateManager struct {
	treeManager
	Statement *nodes.UpdateStatement
}

// NewUpdateManager creates a new UpdateManager targeting the given table.
func NewUpdateManager(table nodes.Node) *UpdateManager {
	return &UpdateManager{
		Statement: &nodes.UpdateStatement{Table: table},
	}
}

// Set adds a column assignment to the SET clause.
// val can be a raw Go value or a Node.
func (m *UpdateManager) Set(col nodes.Node, val any) *UpdateManager {
	m.Statement.Assignments = append(m.Statement.Assignments, &nodes.AssignmentNode{
		Left:  col,
		Right: nodes.Literal(val),
	})
	return m
}

// Where appends conditions to the WHERE clause.
func (m *UpdateManager) Where(conditions ...nodes.Node) *UpdateManager {
	m.Statement.Wheres = append(m.Statement.Wheres, conditions...)
	return m
}

// Returning sets the RETURNING clause columns.
func (m *UpdateManager) Returning(cols ...nodes.Node) *UpdateManager {
	m.Statement.Returning = cols
	return m
}

// Use registers a transformer plugin.
func (m *UpdateManager) Use(t plugins.Transformer) *UpdateManager {
	m.addTransformer(t)
	return m
}

// toSQLCore applies transformers and generates SQL.
func (m *UpdateManager) toSQLCore(v nodes.Visitor) (string, error) {
	stmt := m.cloneStatement()
	for _, t := range m.transformers {
		var err error
		stmt, err = t.TransformUpdate(stmt)
		if err != nil {
			return "", err
		}
	}
	return stmt.Accept(v), nil
}

// ToSQL applies transformers and generates SQL with parameters.
// Returns SQL string, parameter values (if parameterised), and any error.
func (m *UpdateManager) ToSQL(v nodes.Visitor) (string, []any, error) {
	return toSQLParams(v, m.toSQLCore)
}

// ToSQLParams applies transformers and generates parameterized SQL.
//
// Deprecated: Use ToSQL() instead, which now always returns params.
func (m *UpdateManager) ToSQLParams(v nodes.Visitor) (string, []any, error) {
	return m.ToSQL(v)
}

func (m *UpdateManager) cloneStatement() *nodes.UpdateStatement {
	assignments := make([]*nodes.AssignmentNode, len(m.Statement.Assignments))
	copy(assignments, m.Statement.Assignments)

	wheres := make([]nodes.Node, len(m.Statement.Wheres))
	copy(wheres, m.Statement.Wheres)

	returning := make([]nodes.Node, len(m.Statement.Returning))
	copy(returning, m.Statement.Returning)

	return &nodes.UpdateStatement{
		Table:       m.Statement.Table,
		Assignments: assignments,
		Wheres:      wheres,
		Returning:   returning,
	}
}
