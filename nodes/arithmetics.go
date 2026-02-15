package nodes

// Arithmetics provides math and bitwise methods to types that embed it.
// The self field must be set to the embedding node.
type Arithmetics struct {
	self Node
}

func (a Arithmetics) newInfix(op InfixOp, val any) *InfixNode {
	n := &InfixNode{Left: a.self, Right: Literal(val), Op: op}
	n.Predications.self = n
	n.Arithmetics.self = n
	n.Combinable.self = n
	return n
}

func (a Arithmetics) Plus(val any) *InfixNode       { return a.newInfix(OpPlus, val) }
func (a Arithmetics) Minus(val any) *InfixNode      { return a.newInfix(OpMinus, val) }
func (a Arithmetics) Multiply(val any) *InfixNode   { return a.newInfix(OpMultiply, val) }
func (a Arithmetics) Divide(val any) *InfixNode     { return a.newInfix(OpDivide, val) }
func (a Arithmetics) BitwiseAnd(val any) *InfixNode { return a.newInfix(OpBitwiseAnd, val) }
func (a Arithmetics) BitwiseOr(val any) *InfixNode  { return a.newInfix(OpBitwiseOr, val) }
func (a Arithmetics) BitwiseXor(val any) *InfixNode { return a.newInfix(OpBitwiseXor, val) }
func (a Arithmetics) ShiftLeft(val any) *InfixNode  { return a.newInfix(OpShiftLeft, val) }
func (a Arithmetics) ShiftRight(val any) *InfixNode { return a.newInfix(OpShiftRight, val) }
func (a Arithmetics) Concat(val any) *InfixNode     { return a.newInfix(OpConcat, val) }

func (a Arithmetics) BitwiseNot() *UnaryMathNode {
	n := &UnaryMathNode{Expr: a.self, Op: OpBitwiseNot}
	n.Predications.self = n
	n.Arithmetics.self = n
	n.Combinable.self = n
	return n
}
