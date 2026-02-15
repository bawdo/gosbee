// Package plugins defines the Transformer interface for AST middleware.
package plugins

import "github.com/bawdo/gosbee/nodes"

// Transformer is the interface that AST transformation plugins implement.
// Plugins embed BaseTransformer and override only the methods they need.
type Transformer interface {
	TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error)
	TransformInsert(stmt *nodes.InsertStatement) (*nodes.InsertStatement, error)
	TransformUpdate(stmt *nodes.UpdateStatement) (*nodes.UpdateStatement, error)
	TransformDelete(stmt *nodes.DeleteStatement) (*nodes.DeleteStatement, error)
}

// BaseTransformer provides no-op defaults for all Transformer methods.
// Plugins embed this and override only the methods they care about.
type BaseTransformer struct{}

func (BaseTransformer) TransformSelect(c *nodes.SelectCore) (*nodes.SelectCore, error) {
	return c, nil
}
func (BaseTransformer) TransformInsert(s *nodes.InsertStatement) (*nodes.InsertStatement, error) {
	return s, nil
}
func (BaseTransformer) TransformUpdate(s *nodes.UpdateStatement) (*nodes.UpdateStatement, error) {
	return s, nil
}
func (BaseTransformer) TransformDelete(s *nodes.DeleteStatement) (*nodes.DeleteStatement, error) {
	return s, nil
}
