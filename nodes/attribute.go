package nodes

// Attribute represents a column reference bound to a table or table alias.
type Attribute struct {
	Predications
	Arithmetics
	Combinable
	Name     string
	Relation Node   // *Table or *TableAlias
	TypeName string // SQL type for coercion (e.g. "integer", "text")
}

// NewAttribute creates an Attribute with Predications and Combinable
// properly initialized to reference the new Attribute as self.
func NewAttribute(relation Node, name string) *Attribute {
	a := &Attribute{Name: name, Relation: relation}
	a.Predications.self = a
	a.Arithmetics.self = a
	a.Combinable.self = a
	return a
}

func (a *Attribute) Accept(v Visitor) string { return v.VisitAttribute(a) }

// Typed returns a copy of the Attribute with TypeName set.
// The copy has its own Predications/Arithmetics/Combinable self pointers.
func (a *Attribute) Typed(typeName string) *Attribute {
	c := &Attribute{Name: a.Name, Relation: a.Relation, TypeName: typeName}
	c.Predications.self = c
	c.Arithmetics.self = c
	c.Combinable.self = c
	return c
}

// Coerce wraps val using the attribute's type. If TypeName is set,
// returns a CastedNode; otherwise returns a plain Literal.
func (a *Attribute) Coerce(val any) Node {
	if a.TypeName != "" {
		return NewCasted(val, a.TypeName)
	}
	return Literal(val)
}
