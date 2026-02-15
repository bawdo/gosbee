// Package managers provides high-level fluent APIs for building SQL ASTs.
package managers

import (
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// SelectManager provides a fluent API for building SELECT queries.
// It wraps a SelectCore and applies transformer plugins before SQL generation.
type SelectManager struct {
	treeManager
	Core *nodes.SelectCore
}

// NewSelectManager creates a new SelectManager with the given table as FROM.
// If from is nil, the FROM clause is left unset.
func NewSelectManager(from nodes.Node) *SelectManager {
	return &SelectManager{
		Core: &nodes.SelectCore{From: from},
	}
}

// Select sets the projection list, replacing any existing projections.
// Pass column attributes, stars, literals, or any Node.
func (m *SelectManager) Select(projections ...nodes.Node) *SelectManager {
	m.Core.Projections = projections
	return m
}

// Project is an alias for Select (Ruby Arel uses "project").
func (m *SelectManager) Project(projections ...nodes.Node) *SelectManager {
	return m.Select(projections...)
}

// Distinct enables or disables the DISTINCT modifier on the SELECT clause.
func (m *SelectManager) Distinct(on ...bool) *SelectManager {
	if len(on) == 0 || on[0] {
		m.Core.Distinct = true
	} else {
		m.Core.Distinct = false
	}
	return m
}

// DistinctOn sets the DISTINCT ON columns (PostgreSQL).
func (m *SelectManager) DistinctOn(cols ...nodes.Node) *SelectManager {
	m.Core.DistinctOn = cols
	return m
}

// Where appends one or more conditions to the WHERE clause.
// Multiple calls to Where are combined with AND at the visitor level.
func (m *SelectManager) Where(conditions ...nodes.Node) *SelectManager {
	m.Core.Wheres = append(m.Core.Wheres, conditions...)
	return m
}

// From sets or changes the FROM source.
func (m *SelectManager) From(table nodes.Node) *SelectManager {
	m.Core.From = table
	return m
}

// Join adds a join to the query and returns a JoinContext for specifying
// the ON condition. The default join type is InnerJoin.
func (m *SelectManager) Join(table nodes.Node, joinTypes ...nodes.JoinType) *JoinContext {
	jt := nodes.InnerJoin
	if len(joinTypes) > 0 {
		jt = joinTypes[0]
	}
	join := &nodes.JoinNode{
		Left:  m.Core.From,
		Right: table,
		Type:  jt,
	}
	m.Core.Joins = append(m.Core.Joins, join)
	return &JoinContext{manager: m, join: join}
}

// OuterJoin is a convenience for Join with LeftOuterJoin type.
func (m *SelectManager) OuterJoin(table nodes.Node) *JoinContext {
	return m.Join(table, nodes.LeftOuterJoin)
}

// LateralJoin adds a LATERAL join (PostgreSQL). Default join type is InnerJoin.
func (m *SelectManager) LateralJoin(table nodes.Node, joinTypes ...nodes.JoinType) *JoinContext {
	jt := nodes.InnerJoin
	if len(joinTypes) > 0 {
		jt = joinTypes[0]
	}
	join := &nodes.JoinNode{
		Left:    m.Core.From,
		Right:   table,
		Type:    jt,
		Lateral: true,
	}
	m.Core.Joins = append(m.Core.Joins, join)
	return &JoinContext{manager: m, join: join}
}

// StringJoin adds a raw SQL join fragment.
//
// SECURITY: The raw string is injected verbatim into SQL output.
// Never pass user-controlled input to this method.
func (m *SelectManager) StringJoin(raw string) *SelectManager {
	join := &nodes.JoinNode{
		Left:  m.Core.From,
		Right: &nodes.SqlLiteral{Raw: raw},
		Type:  nodes.StringJoin,
	}
	m.Core.Joins = append(m.Core.Joins, join)
	return m
}

// Group appends one or more expressions to the GROUP BY clause.
func (m *SelectManager) Group(columns ...nodes.Node) *SelectManager {
	m.Core.Groups = append(m.Core.Groups, columns...)
	return m
}

// Having appends one or more conditions to the HAVING clause.
// Multiple calls to Having are combined with AND at the visitor level.
func (m *SelectManager) Having(conditions ...nodes.Node) *SelectManager {
	m.Core.Havings = append(m.Core.Havings, conditions...)
	return m
}

// Window appends one or more named window definitions to the WINDOW clause.
func (m *SelectManager) Window(defs ...*nodes.WindowDefinition) *SelectManager {
	m.Core.Windows = append(m.Core.Windows, defs...)
	return m
}

// Order sets the ORDER BY clause. Pass OrderingNode values
// (e.g., table.Col("name").Asc()).
func (m *SelectManager) Order(orderings ...nodes.Node) *SelectManager {
	m.Core.Orders = append(m.Core.Orders, orderings...)
	return m
}

// Limit sets the LIMIT value.
func (m *SelectManager) Limit(n int) *SelectManager {
	m.Core.Limit = nodes.Literal(n)
	return m
}

// Offset sets the OFFSET value.
func (m *SelectManager) Offset(n int) *SelectManager {
	m.Core.Offset = nodes.Literal(n)
	return m
}

// Take is an alias for Limit (Ruby Arel convention).
func (m *SelectManager) Take(n int) *SelectManager {
	return m.Limit(n)
}

// CrossJoin adds a cross join (no ON clause).
func (m *SelectManager) CrossJoin(table nodes.Node) *SelectManager {
	join := &nodes.JoinNode{
		Left:  m.Core.From,
		Right: table,
		Type:  nodes.CrossJoin,
	}
	m.Core.Joins = append(m.Core.Joins, join)
	return m
}

// ForUpdate sets the FOR UPDATE lock mode.
func (m *SelectManager) ForUpdate() *SelectManager {
	m.Core.Lock = nodes.ForUpdate
	return m
}

// ForShare sets the FOR SHARE lock mode.
func (m *SelectManager) ForShare() *SelectManager {
	m.Core.Lock = nodes.ForShare
	return m
}

// ForNoKeyUpdate sets the FOR NO KEY UPDATE lock mode.
func (m *SelectManager) ForNoKeyUpdate() *SelectManager {
	m.Core.Lock = nodes.ForNoKeyUpdate
	return m
}

// ForKeyShare sets the FOR KEY SHARE lock mode.
func (m *SelectManager) ForKeyShare() *SelectManager {
	m.Core.Lock = nodes.ForKeyShare
	return m
}

// SkipLocked adds SKIP LOCKED to the current lock mode.
func (m *SelectManager) SkipLocked() *SelectManager {
	m.Core.SkipLocked = true
	return m
}

// Comment sets a query comment (rendered as /* ... */).
// Any occurrence of */ in the text is sanitized to prevent comment breakout.
func (m *SelectManager) Comment(text string) *SelectManager {
	m.Core.Comment = text
	return m
}

// Hint adds an optimizer hint (rendered as /*+ ... */ after SELECT).
// Any occurrence of */ in the hint is sanitized to prevent comment breakout.
func (m *SelectManager) Hint(hint string) *SelectManager {
	m.Core.Hints = append(m.Core.Hints, hint)
	return m
}

// With adds a Common Table Expression (WITH clause).
func (m *SelectManager) With(name string, query nodes.Node) *SelectManager {
	m.Core.CTEs = append(m.Core.CTEs, &nodes.CTENode{Name: name, Query: query})
	return m
}

// WithRecursive adds a recursive Common Table Expression (WITH RECURSIVE clause).
func (m *SelectManager) WithRecursive(name string, query nodes.Node) *SelectManager {
	m.Core.CTEs = append(m.Core.CTEs, &nodes.CTENode{Name: name, Query: query, Recursive: true})
	return m
}

// Union creates a UNION set operation between this query and another.
func (m *SelectManager) Union(other *SelectManager) *nodes.SetOperationNode {
	return &nodes.SetOperationNode{Left: m.Core, Right: other.Core, Type: nodes.Union}
}

// UnionAll creates a UNION ALL set operation between this query and another.
func (m *SelectManager) UnionAll(other *SelectManager) *nodes.SetOperationNode {
	return &nodes.SetOperationNode{Left: m.Core, Right: other.Core, Type: nodes.UnionAll}
}

// Intersect creates an INTERSECT set operation between this query and another.
func (m *SelectManager) Intersect(other *SelectManager) *nodes.SetOperationNode {
	return &nodes.SetOperationNode{Left: m.Core, Right: other.Core, Type: nodes.Intersect}
}

// IntersectAll creates an INTERSECT ALL set operation between this query and another.
func (m *SelectManager) IntersectAll(other *SelectManager) *nodes.SetOperationNode {
	return &nodes.SetOperationNode{Left: m.Core, Right: other.Core, Type: nodes.IntersectAll}
}

// Except creates an EXCEPT set operation between this query and another.
func (m *SelectManager) Except(other *SelectManager) *nodes.SetOperationNode {
	return &nodes.SetOperationNode{Left: m.Core, Right: other.Core, Type: nodes.Except}
}

// ExceptAll creates an EXCEPT ALL set operation between this query and another.
func (m *SelectManager) ExceptAll(other *SelectManager) *nodes.SetOperationNode {
	return &nodes.SetOperationNode{Left: m.Core, Right: other.Core, Type: nodes.ExceptAll}
}

// Use registers a transformer plugin to be applied before SQL generation.
func (m *SelectManager) Use(t plugins.Transformer) *SelectManager {
	m.addTransformer(t)
	return m
}

// toSQLCore applies all registered transformers to a copy of the SelectCore,
// then generates SQL using the given visitor.
func (m *SelectManager) toSQLCore(v nodes.Visitor) (string, error) {
	core := m.CloneCore()
	for _, t := range m.transformers {
		var err error
		core, err = t.TransformSelect(core)
		if err != nil {
			return "", err
		}
	}
	return core.Accept(v), nil
}

// ToSQL applies all registered transformers and generates SQL with parameters.
// Returns SQL string, parameter values (if parameterised), and any error.
// Parameters are collected automatically when the visitor has parameterisation enabled.
func (m *SelectManager) ToSQL(v nodes.Visitor) (string, []any, error) {
	return toSQLParams(v, m.toSQLCore)
}

// ToSQLParams applies transformers and generates parameterized SQL.
//
// Deprecated: Use ToSQL() instead, which now always returns params.
func (m *SelectManager) ToSQLParams(v nodes.Visitor) (string, []any, error) {
	return m.ToSQL(v)
}

// Accept implements the Node interface so that a SelectManager can be
// used as a subquery (e.g., as the right side of a JoinNode).
// It delegates to the underlying SelectCore.
func (m *SelectManager) Accept(v nodes.Visitor) string {
	return m.Core.Accept(v)
}

// As wraps the query's SelectCore in a TableAlias, enabling it to be
// used as a named subquery in FROM or JOIN clauses.
func (m *SelectManager) As(name string) *nodes.TableAlias {
	return &nodes.TableAlias{Relation: m.Core, AliasName: name}
}

// CloneCore returns a shallow copy of the SelectCore so transformers
// don't modify the original.
func (m *SelectManager) CloneCore() *nodes.SelectCore {
	projections := make([]nodes.Node, len(m.Core.Projections))
	copy(projections, m.Core.Projections)

	wheres := make([]nodes.Node, len(m.Core.Wheres))
	copy(wheres, m.Core.Wheres)

	joins := make([]*nodes.JoinNode, len(m.Core.Joins))
	copy(joins, m.Core.Joins)

	groups := make([]nodes.Node, len(m.Core.Groups))
	copy(groups, m.Core.Groups)

	havings := make([]nodes.Node, len(m.Core.Havings))
	copy(havings, m.Core.Havings)

	windows := make([]*nodes.WindowDefinition, len(m.Core.Windows))
	copy(windows, m.Core.Windows)

	orders := make([]nodes.Node, len(m.Core.Orders))
	copy(orders, m.Core.Orders)

	distinctOn := make([]nodes.Node, len(m.Core.DistinctOn))
	copy(distinctOn, m.Core.DistinctOn)

	hints := make([]string, len(m.Core.Hints))
	copy(hints, m.Core.Hints)

	ctes := make([]*nodes.CTENode, len(m.Core.CTEs))
	copy(ctes, m.Core.CTEs)

	return &nodes.SelectCore{
		From:        m.Core.From,
		Projections: projections,
		Wheres:      wheres,
		Joins:       joins,
		Groups:      groups,
		Havings:     havings,
		Windows:     windows,
		Orders:      orders,
		Limit:       m.Core.Limit,
		Offset:      m.Core.Offset,
		Distinct:    m.Core.Distinct,
		DistinctOn:  distinctOn,
		Lock:        m.Core.Lock,
		SkipLocked:  m.Core.SkipLocked,
		Comment:     m.Core.Comment,
		Hints:       hints,
		CTEs:        ctes,
	}
}
