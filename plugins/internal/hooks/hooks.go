// Package hooks lets package plugins lend unexported walker features to the
// plugins bundled with this module, without adding them to the public API.
package hooks

import "github.com/bawdo/gosbee/nodes"

// RebindAttributes returns n with every Attribute whose Relation is a bare
// *nodes.Table named table pointing at to instead. It copies on write: n and
// every node under it are left untouched, and nested queries are not entered.
// Package plugins sets it during initialisation.
var RebindAttributes func(n nodes.Node, table string, to nodes.Node) nodes.Node
