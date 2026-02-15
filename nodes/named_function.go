package nodes

// NamedFunctionNode represents a named SQL function call like COALESCE, LOWER, CAST, etc.
type NamedFunctionNode struct {
	Predications
	Arithmetics
	Combinable
	Name     string
	Args     []Node
	Distinct bool
}

func (n *NamedFunctionNode) Accept(v Visitor) string { return v.VisitNamedFunction(n) }

// NewNamedFunction creates a NamedFunctionNode with properly initialised embedded structs.
func NewNamedFunction(name string, args ...Node) *NamedFunctionNode {
	n := &NamedFunctionNode{Name: name, Args: args}
	n.Predications.self = n
	n.Arithmetics.self = n
	n.Combinable.self = n
	return n
}

// Coalesce creates a COALESCE(args...) function call.
func Coalesce(args ...Node) *NamedFunctionNode {
	return NewNamedFunction("COALESCE", args...)
}

// Lower creates a LOWER(expr) function call.
func Lower(expr Node) *NamedFunctionNode {
	return NewNamedFunction("LOWER", expr)
}

// Upper creates an UPPER(expr) function call.
func Upper(expr Node) *NamedFunctionNode {
	return NewNamedFunction("UPPER", expr)
}

// Substring creates a SUBSTRING(expr, start, len) function call.
func Substring(expr, start, length Node) *NamedFunctionNode {
	return NewNamedFunction("SUBSTRING", expr, start, length)
}

// Cast creates a CAST(expr AS typeName) expression.
// The type name is stored as a SqlLiteral so it renders verbatim.
func Cast(expr Node, typeName string) *NamedFunctionNode {
	return NewNamedFunction("CAST", expr, NewSqlLiteral(typeName))
}

// Over wraps the named function with an inline window definition.
func (n *NamedFunctionNode) Over(def *WindowDefinition) *OverNode {
	o := NewOverNode(n)
	o.Window = def
	return o
}

// OverName wraps the named function with a named window reference.
func (n *NamedFunctionNode) OverName(name string) *OverNode {
	o := NewOverNode(n)
	o.WindowName = name
	return o
}
