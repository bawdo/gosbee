package plugins

import "github.com/bawdo/gosbee/nodes"

// TableRef holds a reference to a table relation and its underlying name.
// Relation is the node used to create column references (preserving aliases),
// and Name is the underlying table name (for matching/filtering).
type TableRef struct {
	Relation nodes.Node // *nodes.Table or *nodes.TableAlias
	Name     string     // underlying table name
}

// CollectTables returns all table relations referenced in a SelectCore,
// including the FROM table and all JOIN targets. Subqueries and other
// non-table nodes are skipped.
func CollectTables(core *nodes.SelectCore) []TableRef {
	var refs []TableRef
	if ref, ok := extractTableRef(core.From); ok {
		refs = append(refs, ref)
	}
	for _, j := range core.Joins {
		if ref, ok := extractTableRef(j.Right); ok {
			refs = append(refs, ref)
		}
	}
	return refs
}

func extractTableRef(n nodes.Node) (TableRef, bool) {
	switch r := n.(type) {
	case *nodes.Table:
		return TableRef{Relation: r, Name: r.Name}, true
	case *nodes.TableAlias:
		if tbl, ok := r.Relation.(*nodes.Table); ok {
			return TableRef{Relation: r, Name: tbl.Name}, true
		}
		return TableRef{Relation: r, Name: r.AliasName}, true
	default:
		return TableRef{}, false
	}
}
