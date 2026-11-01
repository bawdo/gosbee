package managers

import (
	"errors"
	"fmt"

	"github.com/bawdo/gosbee/nodes"
)

// ErrSetTargetTable is returned by ToSQL when a SET target (UPDATE SET, or
// INSERT ... ON CONFLICT DO UPDATE SET) is a column of a table other than the
// one the statement writes. Visitors write a SET target as a bare column name,
// as PostgreSQL and SQLite require, so such a target would otherwise resolve
// silently to a column of the statement's own table.
//
// The check is made by the managers. Rendering a statement node directly with
// Accept bypasses it.
var ErrSetTargetTable = errors.New("gosbee: SET target column belongs to another table")

// checkSetTargets refuses any assignment whose target column is bound to a
// relation other than target. A column with no relation is accepted, and so
// is one whose relation shares target's name (the alias or table name) or its
// underlying table name, so users.Col("x") is accepted against users AS u.
func checkSetTargets(target nodes.Node, assignments []*nodes.AssignmentNode) error {
	for _, a := range assignments {
		attr, ok := a.Left.(*nodes.Attribute)
		if !ok || attr.Relation == nil {
			continue
		}
		if sameRelation(attr.Relation, target) {
			continue
		}
		return fmt.Errorf("%w: %q is a column of %q, not %q", ErrSetTargetTable,
			attr.Name, nodes.RelationName(attr.Relation), nodes.RelationName(target))
	}
	return nil
}

func sameRelation(rel, target nodes.Node) bool {
	if name := nodes.RelationName(target); name != "" && nodes.RelationName(rel) == name {
		return true
	}
	src := nodes.TableSourceName(target)
	return src != "" && nodes.TableSourceName(rel) == src
}
