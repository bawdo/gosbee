package nodes

// InfixOp identifies the binary math/bitwise/concat operator.
type InfixOp int

const (
	OpPlus InfixOp = iota
	OpMinus
	OpMultiply
	OpDivide
	OpBitwiseAnd
	OpBitwiseOr
	OpBitwiseXor
	OpShiftLeft
	OpShiftRight
	OpConcat
)

// InfixNode represents a binary math, bitwise, or concat expression.
type InfixNode struct {
	Predications
	Arithmetics
	Combinable
	Left  Node
	Right Node
	Op    InfixOp
}

func (n *InfixNode) Accept(v Visitor) string { return v.VisitInfix(n) }

// UnaryMathOp identifies the unary math operator.
type UnaryMathOp int

const (
	OpBitwiseNot UnaryMathOp = iota
)

// UnaryMathNode represents a unary math expression (e.g., bitwise NOT).
type UnaryMathNode struct {
	Predications
	Arithmetics
	Combinable
	Expr Node
	Op   UnaryMathOp
}

func (n *UnaryMathNode) Accept(v Visitor) string { return v.VisitUnaryMath(n) }

// NewInfixNode creates an InfixNode with properly initialised embedded structs.
func NewInfixNode(left, right Node, op InfixOp) *InfixNode {
	n := &InfixNode{Left: left, Right: right, Op: op}
	n.Predications.self = n
	n.Arithmetics.self = n
	n.Combinable.self = n
	return n
}

// NewUnaryMathNode creates a UnaryMathNode with properly initialised embedded structs.
func NewUnaryMathNode(expr Node, op UnaryMathOp) *UnaryMathNode {
	n := &UnaryMathNode{Expr: expr, Op: op}
	n.Predications.self = n
	n.Arithmetics.self = n
	n.Combinable.self = n
	return n
}
