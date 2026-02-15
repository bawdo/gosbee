// Package gosbee provides a fluent SQL query builder for Go.
//
// This package re-exports commonly used types and functions from subpackages
// for convenience. Advanced users can import subpackages directly:
//   - github.com/bawdo/gosbee/managers (query builders)
//   - github.com/bawdo/gosbee/nodes (AST nodes)
//   - github.com/bawdo/gosbee/visitors (SQL generation)
//   - github.com/bawdo/gosbee/plugins (query transformers)
package gosbee

import (
	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/visitors"
)

// --- Manager Types ---

// SelectManager provides a fluent API for building SELECT queries.
type SelectManager = managers.SelectManager

// InsertManager provides a fluent API for building INSERT queries.
type InsertManager = managers.InsertManager

// UpdateManager provides a fluent API for building UPDATE queries.
type UpdateManager = managers.UpdateManager

// DeleteManager provides a fluent API for building DELETE queries.
type DeleteManager = managers.DeleteManager

// --- Manager Constructors ---

// NewSelect creates a new SelectManager with the given table as FROM.
func NewSelect(from nodes.Node) *managers.SelectManager {
	return managers.NewSelectManager(from)
}

// NewInsert creates a new InsertManager for inserting into the given table.
func NewInsert(into nodes.Node) *managers.InsertManager {
	return managers.NewInsertManager(into)
}

// NewUpdate creates a new UpdateManager for updating the given table.
func NewUpdate(table nodes.Node) *managers.UpdateManager {
	return managers.NewUpdateManager(table)
}

// NewDelete creates a new DeleteManager for deleting from the given table.
func NewDelete(from nodes.Node) *managers.DeleteManager {
	return managers.NewDeleteManager(from)
}

// --- Core Node Types ---

// Table represents a SQL table reference.
type Table = nodes.Table

// Attribute represents a column reference (e.g., table.column).
type Attribute = nodes.Attribute

// Node is the base interface all AST nodes implement.
type Node = nodes.Node

// --- Common Node Constructors ---

// NewTable creates a new table reference.
func NewTable(name string) *nodes.Table {
	return nodes.NewTable(name)
}

// Literal creates a SQL literal node (e.g., numbers, strings).
func Literal(value any) nodes.Node {
	return nodes.Literal(value)
}

// BindParam creates a parameterised placeholder (e.g., $1, ?).
func BindParam(value any) *nodes.BindParamNode {
	return nodes.NewBindParam(value)
}

// Star creates an unqualified star (*) for SELECT *.
func Star() *nodes.StarNode {
	return nodes.Star()
}

// --- Aggregate Functions ---

// Count creates a COUNT(expr) aggregate.
func Count(expr nodes.Node) *nodes.AggregateNode {
	return nodes.Count(expr)
}

// Sum creates a SUM(expr) aggregate.
func Sum(expr nodes.Node) *nodes.AggregateNode {
	return nodes.Sum(expr)
}

// Avg creates an AVG(expr) aggregate.
func Avg(expr nodes.Node) *nodes.AggregateNode {
	return nodes.Avg(expr)
}

// Min creates a MIN(expr) aggregate.
func Min(expr nodes.Node) *nodes.AggregateNode {
	return nodes.Min(expr)
}

// Max creates a MAX(expr) aggregate.
func Max(expr nodes.Node) *nodes.AggregateNode {
	return nodes.Max(expr)
}

// CountDistinct creates a COUNT(DISTINCT expr) aggregate.
func CountDistinct(expr nodes.Node) *nodes.AggregateNode {
	return nodes.CountDistinct(expr)
}

// --- Visitor Types ---

// SQLiteVisitor generates SQLite-compatible SQL.
type SQLiteVisitor = visitors.SQLiteVisitor

// PostgresVisitor generates PostgreSQL-compatible SQL.
type PostgresVisitor = visitors.PostgresVisitor

// MySQLVisitor generates MySQL-compatible SQL.
type MySQLVisitor = visitors.MySQLVisitor

// --- Visitor Constructors ---

// NewSQLiteVisitor creates a new SQLite visitor.
func NewSQLiteVisitor(opts ...visitors.Option) *visitors.SQLiteVisitor {
	return visitors.NewSQLiteVisitor(opts...)
}

// NewPostgresVisitor creates a new PostgreSQL visitor.
func NewPostgresVisitor(opts ...visitors.Option) *visitors.PostgresVisitor {
	return visitors.NewPostgresVisitor(opts...)
}

// NewMySQLVisitor creates a new MySQL visitor.
func NewMySQLVisitor(opts ...visitors.Option) *visitors.MySQLVisitor {
	return visitors.NewMySQLVisitor(opts...)
}

// --- Visitor Options ---

// WithParams enables parameterisation mode for visitors.
//
// Note: Parameterisation is now enabled by default. This option is kept
// for backwards compatibility and has no effect.
func WithParams() visitors.Option {
	return visitors.WithParams()
}

// WithoutParams disables parameterised query mode.
//
// ⚠️ WARNING: Disables SQL injection protection. Only use for debugging or when
// you're certain all values are trusted. Production code should NEVER use this option.
func WithoutParams() visitors.Option {
	return visitors.WithoutParams()
}
