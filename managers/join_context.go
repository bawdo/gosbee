package managers

import "github.com/bawdo/gosbee/nodes"

// JoinContext is returned by SelectManager.Join() and enforces that
// a join condition is provided via On() before continuing to build
// the query. This prevents incomplete JOINs in the AST.
type JoinContext struct {
	manager *SelectManager
	join    *nodes.JoinNode
}

// On sets the join condition and returns the SelectManager for
// continued method chaining.
func (jc *JoinContext) On(condition nodes.Node) *SelectManager {
	jc.join.On = condition
	return jc.manager
}
