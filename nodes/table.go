package nodes

// Table represents a SQL table reference.
type Table struct {
	Name string
}

func NewTable(name string) *Table {
	return &Table{Name: name}
}

func (t *Table) Accept(v Visitor) string { return v.VisitTable(t) }

// Col creates an Attribute (column reference) bound to this table.
func (t *Table) Col(name string) *Attribute {
	return NewAttribute(t, name)
}

// Alias creates an aliased reference to this table.
func (t *Table) Alias(name string) *TableAlias {
	return &TableAlias{Relation: t, AliasName: name}
}

// Star creates a qualified star (table.*) for this table.
func (t *Table) Star() *StarNode {
	return &StarNode{Table: t}
}

// TableAlias represents an aliased reference to a table or subquery.
type TableAlias struct {
	Relation  Node // *Table, *SelectCore, or any Node
	AliasName string
}

func (ta *TableAlias) Accept(v Visitor) string { return v.VisitTableAlias(ta) }

// Col creates an Attribute (column reference) bound to this table alias.
func (ta *TableAlias) Col(name string) *Attribute {
	return NewAttribute(ta, name)
}

// RelationName returns the name associated with a relation node.
// For a Table it returns the table name; for a TableAlias it returns the alias name.
func RelationName(n Node) string {
	switch r := n.(type) {
	case *Table:
		return r.Name
	case *TableAlias:
		return r.AliasName
	default:
		return ""
	}
}

// TableSourceName returns the underlying table name from a relation node.
// For a TableAlias it looks through to the underlying Table if one exists,
// falling back to the alias name.
func TableSourceName(n Node) string {
	switch r := n.(type) {
	case *Table:
		return r.Name
	case *TableAlias:
		if tbl, ok := r.Relation.(*Table); ok {
			return tbl.Name
		}
		return r.AliasName
	default:
		return ""
	}
}
