package managers

import (
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// InsertManager provides a fluent API for building INSERT statements.
type InsertManager struct {
	treeManager
	Statement *nodes.InsertStatement
}

// NewInsertManager creates a new InsertManager targeting the given table.
func NewInsertManager(into nodes.Node) *InsertManager {
	return &InsertManager{
		Statement: &nodes.InsertStatement{Into: into},
	}
}

// Columns sets the column list for the INSERT statement.
func (m *InsertManager) Columns(cols ...nodes.Node) *InsertManager {
	m.Statement.Columns = cols
	return m
}

// Values appends a row of values to the INSERT statement.
// Each call to Values adds one row. Pass raw Go values; they are
// wrapped with nodes.Literal automatically.
func (m *InsertManager) Values(vals ...any) *InsertManager {
	row := make([]nodes.Node, len(vals))
	for i, v := range vals {
		row[i] = nodes.Literal(v)
	}
	m.Statement.Values = append(m.Statement.Values, row)
	return m
}

// FromSelect sets a SELECT subquery as the source of rows.
// Mutually exclusive with Values — if Select is set, Values are ignored
// by the visitor.
func (m *InsertManager) FromSelect(sel *SelectManager) *InsertManager {
	m.Statement.Select = sel
	return m
}

// Returning sets the RETURNING clause columns.
func (m *InsertManager) Returning(cols ...nodes.Node) *InsertManager {
	m.Statement.Returning = cols
	return m
}

// OnConflict begins an ON CONFLICT clause targeting the given columns.
// Returns an OnConflictContext for specifying the action.
func (m *InsertManager) OnConflict(cols ...nodes.Node) *OnConflictContext {
	oc := &nodes.OnConflictNode{Columns: cols}
	m.Statement.OnConflict = oc
	return &OnConflictContext{manager: m, node: oc}
}

// Use registers a transformer plugin.
func (m *InsertManager) Use(t plugins.Transformer) *InsertManager {
	m.addTransformer(t)
	return m
}

// ToSQL applies transformers and generates SQL.
func (m *InsertManager) ToSQL(v nodes.Visitor) (string, error) {
	stmt := m.cloneStatement()
	for _, t := range m.transformers {
		var err error
		stmt, err = t.TransformInsert(stmt)
		if err != nil {
			return "", err
		}
	}
	return stmt.Accept(v), nil
}

// ToSQLParams applies transformers and generates parameterized SQL.
func (m *InsertManager) ToSQLParams(v nodes.Visitor) (string, []any, error) {
	return toSQLParams(v, m.ToSQL)
}

func (m *InsertManager) cloneStatement() *nodes.InsertStatement {
	columns := make([]nodes.Node, len(m.Statement.Columns))
	copy(columns, m.Statement.Columns)

	values := make([][]nodes.Node, len(m.Statement.Values))
	for i, row := range m.Statement.Values {
		r := make([]nodes.Node, len(row))
		copy(r, row)
		values[i] = r
	}

	returning := make([]nodes.Node, len(m.Statement.Returning))
	copy(returning, m.Statement.Returning)

	return &nodes.InsertStatement{
		Into:       m.Statement.Into,
		Columns:    columns,
		Values:     values,
		Select:     m.Statement.Select,
		Returning:  returning,
		OnConflict: m.Statement.OnConflict,
	}
}

// OnConflictContext guides ON CONFLICT clause construction.
type OnConflictContext struct {
	manager *InsertManager
	node    *nodes.OnConflictNode
}

// DoNothing sets the action to DO NOTHING and returns the InsertManager.
func (c *OnConflictContext) DoNothing() *InsertManager {
	c.node.Action = nodes.DoNothing
	return c.manager
}

// DoUpdate sets the action to DO UPDATE with the given assignments.
// Returns an OnConflictUpdateContext for an optional WHERE clause.
func (c *OnConflictContext) DoUpdate(assignments ...*nodes.AssignmentNode) *OnConflictUpdateContext {
	c.node.Action = nodes.DoUpdate
	c.node.Assignments = assignments
	return &OnConflictUpdateContext{manager: c.manager, node: c.node}
}

// OnConflictUpdateContext allows adding a WHERE to DO UPDATE.
type OnConflictUpdateContext struct {
	manager *InsertManager
	node    *nodes.OnConflictNode
}

// Where adds conditions to the ON CONFLICT DO UPDATE clause.
func (c *OnConflictUpdateContext) Where(conditions ...nodes.Node) *InsertManager {
	c.node.Wheres = conditions
	return c.manager
}
