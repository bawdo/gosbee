package plugins

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/bawdo/gosbee/nodes"
)

// ErrUnpoliceable is returned when a strict transformer meets SQL it cannot
// inspect, such as a SqlLiteral or a StringJoin. The error never includes the
// raw SQL text.
var ErrUnpoliceable = errors.New("plugins: query contains SQL the transformer cannot inspect")

// Scope describes what is visible to the SelectCore being transformed.
type Scope struct {
	CTENames map[string]bool // CTEs declared on this core or any enclosing core

	// Qualifiers maps each name a column may be qualified with (a table name
	// or a table alias) to the underlying table name, for this core and every
	// enclosing core. Inner names shadow outer ones. Derived-table aliases and
	// CTE names are not tables, so they are absent.
	Qualifiers map[string]string
	memo       map[string]any // shared by every core in one walk
}

// Tables returns the table relations referenced by core, as CollectTables
// does, minus any that name a CTE in scope and minus derived tables (an alias
// over a subquery). A derived table is policed by walking into it, so its
// alias must not be mistaken for a table name.
func (s Scope) Tables(core *nodes.SelectCore) []TableRef {
	var refs []TableRef
	for _, ref := range CollectTables(core) {
		if alias, ok := ref.Relation.(*nodes.TableAlias); ok {
			if _, isTable := alias.Relation.(*nodes.Table); !isTable {
				continue
			}
		}
		if !s.CTENames[ref.Name] {
			refs = append(refs, ref)
		}
	}
	return refs
}

// Memo returns the value cached under key for this walk, calling fn to
// produce it the first time. Errors are not cached. On a zero Scope (one not
// created by a walk) fn is simply called every time.
func (s Scope) Memo(key string, fn func() (any, error)) (any, error) {
	if s.memo == nil {
		return fn()
	}
	if v, ok := s.memo[key]; ok {
		return v, nil
	}
	v, err := fn()
	if err != nil {
		return nil, err
	}
	s.memo[key] = v
	return v, nil
}

// withCTEs returns a copy of s with names added to CTENames. Qualifiers and
// the memo are shared with s.
func (s Scope) withCTEs(names []string) Scope {
	if len(names) == 0 {
		return s
	}
	out := Scope{CTENames: make(map[string]bool, len(s.CTENames)+len(names)), Qualifiers: s.Qualifiers, memo: s.memo}
	for name := range s.CTENames {
		out.CTENames[name] = true
	}
	for _, name := range names {
		out.CTENames[name] = true
	}
	return out
}

// withQualifiers returns a copy of s with the table names and aliases that
// core's FROM and JOINs introduce added to Qualifiers, shadowing any outer
// entry of the same name. A derived-table alias or a CTE reference is not a
// table: with shadow set it removes any outer entry of the same name,
// otherwise it is ignored. s.CTENames must already include core's own CTEs.
// The memo is shared with s.
func (s Scope) withQualifiers(core *nodes.SelectCore, shadow bool) Scope {
	refs := CollectTables(core)
	if len(refs) == 0 {
		return s
	}
	out := s
	out.Qualifiers = make(map[string]string, len(s.Qualifiers)+len(refs))
	maps.Copy(out.Qualifiers, s.Qualifiers)
	for _, ref := range refs {
		name := nodes.RelationName(ref.Relation)
		var table string
		switch rel := ref.Relation.(type) {
		case *nodes.Table:
			table = rel.Name
		case *nodes.TableAlias:
			if tbl, ok := rel.Relation.(*nodes.Table); ok {
				table = tbl.Name
			}
		}
		if name == "" {
			continue
		}
		if table == "" || s.CTENames[table] {
			if shadow {
				delete(out.Qualifiers, name)
			}
			continue
		}
		out.Qualifiers[name] = table
	}
	return out
}

// ScopedTransformer is used instead of TransformSelect when implemented.
type ScopedTransformer interface {
	TransformSelectScoped(core *nodes.SelectCore, scope Scope) (*nodes.SelectCore, error)
}

// StrictTransformer makes the walker refuse SqlLiteral nodes when
// RefusesRawSQL returns true.
type StrictTransformer interface {
	RefusesRawSQL() bool
}

// ApplySelect applies t to core and to every SelectCore nested inside it
// (subqueries, CTE bodies, joined subqueries, set-operation branches).
// Inner cores are transformed before the core that contains them. The input
// tree is never modified: each core is copied before t sees it, and every
// container node on the path to a changed node is copied too.
func ApplySelect(t Transformer, core *nodes.SelectCore) (*nodes.SelectCore, error) {
	if core == nil {
		return nil, nil
	}
	out, err := ApplyNode(t, core)
	if err != nil {
		return nil, err
	}
	return out.(*nodes.SelectCore), nil
}

// ApplyNode is ApplySelect for any node, such as a SetOperationNode or a
// DML statement. Only SelectCores are handed to t; for anything else the
// walker just rewrites the children.
func ApplyNode(t Transformer, n nodes.Node) (nodes.Node, error) {
	if n == nil {
		return nil, nil
	}
	r := &rewriter{t: t, scope: Scope{memo: map[string]any{}}}
	if st, ok := t.(StrictTransformer); ok {
		r.strict = st.RefusesRawSQL()
	}
	out := r.node(n)
	if r.err != nil {
		return nil, r.err
	}
	return out, nil
}

// Attributes returns every Attribute in the tree under n, including those
// inside function calls and nested queries, in traversal order.
func Attributes(n nodes.Node) []*nodes.Attribute {
	if n == nil {
		return nil
	}
	r := &rewriter{collect: true}
	r.node(n)
	return r.attrs
}

// ShallowAttributes is Attributes without descending into nested queries:
// a SelectCore or SetOperationNode below n is skipped. When n is itself a
// SelectCore its own clauses are still collected.
func ShallowAttributes(n nodes.Node) []*nodes.Attribute {
	if n == nil {
		return nil
	}
	r := &rewriter{collect: true, shallow: true, root: n}
	r.node(n)
	return r.attrs
}

// rewriter is a copy-on-write nodes.Visitor. Each Visit method stores its
// result in out and the first error in err, and returns "". A nil t walks
// the tree without transforming anything (used by Attributes).
//
// JoinNode.Left and Attribute.Relation are not walked: neither is rendered,
// and walking them would transform the same subquery more than once.
type rewriter struct {
	t       Transformer
	strict  bool
	scope   Scope
	out     nodes.Node
	err     error
	collect bool
	attrs   []*nodes.Attribute
	shallow bool // collect only: skip nested queries below root
	root    nodes.Node
}

var _ nodes.Visitor = (*rewriter)(nil)

// node rewrites n and returns the result. Once an error is recorded the
// input is returned unchanged.
func (r *rewriter) node(n nodes.Node) nodes.Node {
	if n == nil || r.err != nil {
		return n
	}
	r.out = nil
	n.Accept(r)
	if r.err != nil {
		return n
	}
	return r.out
}

// list rewrites every node in ns. The input slice is returned when nothing
// changed; otherwise a fresh slice is returned and ns is left untouched.
func (r *rewriter) list(ns []nodes.Node) ([]nodes.Node, bool) {
	var out []nodes.Node
	for i, n := range ns {
		m := r.node(n)
		if out == nil && m != n {
			out = slices.Clone(ns)
		}
		if out != nil {
			out[i] = m
		}
	}
	if out == nil {
		return ns, false
	}
	return out, true
}

// listCopy always returns a fresh slice, for the slices owned by a
// SelectCore, so a transformer can append to them without touching the input.
func (r *rewriter) listCopy(ns []nodes.Node) []nodes.Node {
	out, changed := r.list(ns)
	if !changed {
		out = slices.Clone(ns)
	}
	return out
}

func (r *rewriter) assignments(as []*nodes.AssignmentNode) ([]*nodes.AssignmentNode, bool) {
	var out []*nodes.AssignmentNode
	for i, a := range as {
		m := r.assignment(a)
		if out == nil && m != a {
			out = slices.Clone(as)
		}
		if out != nil {
			out[i] = m
		}
	}
	if out == nil {
		return as, false
	}
	return out, true
}

func (r *rewriter) assignment(a *nodes.AssignmentNode) *nodes.AssignmentNode {
	if a == nil {
		return nil
	}
	if out, ok := r.node(a).(*nodes.AssignmentNode); ok {
		return out
	}
	return a
}

// coreJoin rewrites a join of a SelectCore, walking its right side with
// rightScope and its ON condition with onScope, copying it when either
// changed. r.scope is left as onScope.
func (r *rewriter) coreJoin(j *nodes.JoinNode, rightScope, onScope Scope) *nodes.JoinNode {
	if j == nil {
		return nil
	}
	r.scope = rightScope
	right := r.node(j.Right)
	r.scope = onScope
	on := r.node(j.On)
	if right == j.Right && on == j.On {
		return j
	}
	c := *j
	c.Right, c.On = right, on
	return &c
}

func (r *rewriter) cte(c *nodes.CTENode) *nodes.CTENode {
	if c == nil {
		return nil
	}
	if out, ok := r.node(c).(*nodes.CTENode); ok {
		return out
	}
	return c
}

// window rewrites the expressions in a window definition, copying it when
// any of them changed.
func (r *rewriter) window(w *nodes.WindowDefinition) *nodes.WindowDefinition {
	if w == nil {
		return nil
	}
	partition, pc := r.list(w.PartitionBy)
	order, oc := r.list(w.OrderBy)
	frame := r.frame(w.Frame)
	if !pc && !oc && frame == w.Frame {
		return w
	}
	c := *w
	c.PartitionBy, c.OrderBy, c.Frame = partition, order, frame
	return &c
}

func (r *rewriter) frame(f *nodes.WindowFrame) *nodes.WindowFrame {
	if f == nil {
		return nil
	}
	start := r.node(f.Start.Offset)
	end := f.End
	if f.End != nil {
		if off := r.node(f.End.Offset); off != f.End.Offset {
			e := *f.End
			e.Offset = off
			end = &e
		}
	}
	if start == f.Start.Offset && end == f.End {
		return f
	}
	c := *f
	c.Start.Offset, c.End = start, end
	return &c
}

func (r *rewriter) set(n nodes.Node) string {
	r.out = n
	return ""
}

func (r *rewriter) fail(err error) string {
	if r.err == nil {
		r.err = err
	}
	return ""
}

// --- Leaves ---

func (r *rewriter) VisitTable(n *nodes.Table) string             { return r.set(n) }
func (r *rewriter) VisitLiteral(n *nodes.LiteralNode) string     { return r.set(n) }
func (r *rewriter) VisitStar(n *nodes.StarNode) string           { return r.set(n) }
func (r *rewriter) VisitBindParam(n *nodes.BindParamNode) string { return r.set(n) }
func (r *rewriter) VisitCasted(n *nodes.CastedNode) string       { return r.set(n) }

func (r *rewriter) VisitAttribute(n *nodes.Attribute) string {
	if r.collect {
		r.attrs = append(r.attrs, n)
	}
	return r.set(n)
}

func (r *rewriter) VisitSqlLiteral(n *nodes.SqlLiteral) string {
	if r.strict {
		return r.fail(fmt.Errorf("%w: raw SQL fragment", ErrUnpoliceable))
	}
	return r.set(n)
}

// --- SELECT ---

func (r *rewriter) VisitSelectCore(n *nodes.SelectCore) string {
	if r.shallow && nodes.Node(n) != r.root {
		return r.set(n)
	}
	c := *n
	outer := r.scope

	names := make([]string, len(n.CTEs))
	recursive := false
	for i, cte := range n.CTEs {
		names[i] = cte.Name
		recursive = recursive || cte.Recursive
	}

	// A plain WITH body sees only the CTEs declared before it; WITH
	// RECURSIVE (rendered when any CTE is recursive) lets every body see
	// every name in the list.
	c.CTEs = slices.Clone(n.CTEs)
	for i, cte := range n.CTEs {
		visible := names[:i]
		if recursive {
			visible = names
		}
		r.scope = outer.withCTEs(visible)
		c.CTEs[i] = r.cte(cte)
	}

	// A derived table body (non-LATERAL) sees only the enclosing scope, not
	// this core's FROM entries or its own alias. A LATERAL body also sees
	// this core's real tables. Every other clause sees the full own scope,
	// where derived-table aliases shadow outer qualifiers.
	base := outer.withCTEs(names)
	own := base.withQualifiers(n, true)
	lateral := base.withQualifiers(n, false)

	r.scope = base
	c.From = r.node(n.From)
	r.scope = own
	c.Projections = r.listCopy(n.Projections)
	c.Joins = slices.Clone(n.Joins)
	for i, j := range n.Joins {
		right := base
		if j != nil && j.Lateral {
			right = lateral
		}
		c.Joins[i] = r.coreJoin(j, right, own)
	}
	r.scope = own
	c.Wheres = r.listCopy(n.Wheres)
	c.Groups = r.listCopy(n.Groups)
	c.Havings = r.listCopy(n.Havings)
	c.Windows = slices.Clone(n.Windows)
	for i, w := range n.Windows {
		c.Windows[i] = r.window(w)
	}
	c.Orders = r.listCopy(n.Orders)
	c.Limit = r.node(n.Limit)
	c.Offset = r.node(n.Offset)
	c.DistinctOn = r.listCopy(n.DistinctOn)
	c.Hints = slices.Clone(n.Hints)

	defer func() { r.scope = outer }()
	if r.err != nil || r.t == nil {
		return r.set(&c)
	}

	var out *nodes.SelectCore
	var err error
	if st, ok := r.t.(ScopedTransformer); ok {
		out, err = st.TransformSelectScoped(&c, r.scope)
	} else {
		out, err = r.t.TransformSelect(&c)
	}
	if err != nil {
		return r.fail(err)
	}
	return r.set(out)
}

func (r *rewriter) VisitCTE(n *nodes.CTENode) string {
	q := r.node(n.Query)
	if q == n.Query {
		return r.set(n)
	}
	c := *n
	c.Query = q
	return r.set(&c)
}

func (r *rewriter) VisitTableAlias(n *nodes.TableAlias) string {
	rel := r.node(n.Relation)
	if rel == n.Relation {
		return r.set(n)
	}
	c := *n
	c.Relation = rel
	return r.set(&c)
}

func (r *rewriter) VisitJoin(n *nodes.JoinNode) string {
	right, on := r.node(n.Right), r.node(n.On)
	if right == n.Right && on == n.On {
		return r.set(n)
	}
	c := *n
	c.Right, c.On = right, on
	return r.set(&c)
}

func (r *rewriter) VisitSetOperation(n *nodes.SetOperationNode) string {
	if r.shallow && nodes.Node(n) != r.root {
		return r.set(n)
	}
	left, right := r.node(n.Left), r.node(n.Right)
	orders, oc := r.list(n.Orders)
	limit, offset := r.node(n.Limit), r.node(n.Offset)
	if left == n.Left && right == n.Right && !oc && limit == n.Limit && offset == n.Offset {
		return r.set(n)
	}
	c := *n
	c.Left, c.Right, c.Orders, c.Limit, c.Offset = left, right, orders, limit, offset
	return r.set(&c)
}

func (r *rewriter) VisitExists(n *nodes.ExistsNode) string {
	sub := r.node(n.Subquery)
	if sub == n.Subquery {
		return r.set(n)
	}
	if n.Negated {
		return r.set(nodes.NotExists(sub))
	}
	return r.set(nodes.Exists(sub))
}

func (r *rewriter) VisitOrdering(n *nodes.OrderingNode) string {
	expr := r.node(n.Expr)
	if expr == n.Expr {
		return r.set(n)
	}
	c := *n
	c.Expr = expr
	return r.set(&c)
}

func (r *rewriter) VisitGroupingSet(n *nodes.GroupingSetNode) string {
	cols, cc := r.list(n.Columns)
	var sets [][]nodes.Node
	for i, set := range n.Sets {
		s, changed := r.list(set)
		if sets == nil && changed {
			sets = slices.Clone(n.Sets)
		}
		if sets != nil {
			sets[i] = s
		}
	}
	if !cc && sets == nil {
		return r.set(n)
	}
	c := *n
	c.Columns = cols
	if sets != nil {
		c.Sets = sets
	}
	return r.set(&c)
}

// --- Predicates and logic ---

func (r *rewriter) VisitComparison(n *nodes.ComparisonNode) string {
	left, right := r.node(n.Left), r.node(n.Right)
	if left == n.Left && right == n.Right {
		return r.set(n)
	}
	return r.set(nodes.NewComparisonNode(left, right, n.Op))
}

func (r *rewriter) VisitUnary(n *nodes.UnaryNode) string {
	expr := r.node(n.Expr)
	if expr == n.Expr {
		return r.set(n)
	}
	c := *n
	c.Expr = expr
	return r.set(&c)
}

func (r *rewriter) VisitAnd(n *nodes.AndNode) string {
	left, right := r.node(n.Left), r.node(n.Right)
	if left == n.Left && right == n.Right {
		return r.set(n)
	}
	c := *n
	c.Left, c.Right = left, right
	return r.set(&c)
}

func (r *rewriter) VisitOr(n *nodes.OrNode) string {
	left, right := r.node(n.Left), r.node(n.Right)
	if left == n.Left && right == n.Right {
		return r.set(n)
	}
	c := *n
	c.Left, c.Right = left, right
	return r.set(&c)
}

func (r *rewriter) VisitNot(n *nodes.NotNode) string {
	expr := r.node(n.Expr)
	if expr == n.Expr {
		return r.set(n)
	}
	c := *n
	c.Expr = expr
	return r.set(&c)
}

func (r *rewriter) VisitIn(n *nodes.InNode) string {
	expr := r.node(n.Expr)
	vals, vc := r.list(n.Vals)
	if expr == n.Expr && !vc {
		return r.set(n)
	}
	c := *n
	c.Expr, c.Vals = expr, vals
	return r.set(&c)
}

func (r *rewriter) VisitBetween(n *nodes.BetweenNode) string {
	expr, low, high := r.node(n.Expr), r.node(n.Low), r.node(n.High)
	if expr == n.Expr && low == n.Low && high == n.High {
		return r.set(n)
	}
	c := *n
	c.Expr, c.Low, c.High = expr, low, high
	return r.set(&c)
}

func (r *rewriter) VisitGrouping(n *nodes.GroupingNode) string {
	expr := r.node(n.Expr)
	if expr == n.Expr {
		return r.set(n)
	}
	c := *n
	c.Expr = expr
	return r.set(&c)
}

// --- Expressions ---

func (r *rewriter) VisitInfix(n *nodes.InfixNode) string {
	left, right := r.node(n.Left), r.node(n.Right)
	if left == n.Left && right == n.Right {
		return r.set(n)
	}
	return r.set(nodes.NewInfixNode(left, right, n.Op))
}

func (r *rewriter) VisitUnaryMath(n *nodes.UnaryMathNode) string {
	expr := r.node(n.Expr)
	if expr == n.Expr {
		return r.set(n)
	}
	return r.set(nodes.NewUnaryMathNode(expr, n.Op))
}

func (r *rewriter) VisitAggregate(n *nodes.AggregateNode) string {
	expr, filter := r.node(n.Expr), r.node(n.Filter)
	if expr == n.Expr && filter == n.Filter {
		return r.set(n)
	}
	c := nodes.NewAggregateNode(n.Func, expr)
	c.Distinct, c.Filter = n.Distinct, filter
	return r.set(c)
}

func (r *rewriter) VisitExtract(n *nodes.ExtractNode) string {
	expr := r.node(n.Expr)
	if expr == n.Expr {
		return r.set(n)
	}
	return r.set(nodes.NewExtractNode(n.Field, expr))
}

func (r *rewriter) VisitWindowFunction(n *nodes.WindowFuncNode) string {
	args, changed := r.list(n.Args)
	if !changed {
		return r.set(n)
	}
	c := *n
	c.Args = args
	return r.set(&c)
}

func (r *rewriter) VisitOver(n *nodes.OverNode) string {
	expr, window := r.node(n.Expr), r.window(n.Window)
	if expr == n.Expr && window == n.Window {
		return r.set(n)
	}
	c := nodes.NewOverNode(expr)
	c.Window, c.WindowName = window, n.WindowName
	return r.set(c)
}

// VisitNamedFunction skips the type argument of CAST: nodes.Cast stores it as
// a SqlLiteral, and the SQL visitor validates it as a type name when rendering.
func (r *rewriter) VisitNamedFunction(n *nodes.NamedFunctionNode) string {
	var args []nodes.Node
	var changed bool
	if hasCastTypeArg(n) {
		first := r.node(n.Args[0])
		args, changed = []nodes.Node{first, n.Args[1]}, first != n.Args[0]
	} else {
		args, changed = r.list(n.Args)
	}
	if !changed {
		return r.set(n)
	}
	c := nodes.NewNamedFunction(n.Name, args...)
	c.Distinct = n.Distinct
	return r.set(c)
}

// hasCastTypeArg reports whether n is CAST(expr AS type) with the type held
// as a SqlLiteral, as nodes.Cast builds it.
func hasCastTypeArg(n *nodes.NamedFunctionNode) bool {
	if n.Name != "CAST" || len(n.Args) != 2 {
		return false
	}
	_, ok := n.Args[1].(*nodes.SqlLiteral)
	return ok
}

func (r *rewriter) VisitCase(n *nodes.CaseNode) string {
	operand, elseVal := r.node(n.Operand), r.node(n.ElseVal)
	var whens []nodes.CaseWhen
	for i, w := range n.Whens {
		cond, res := r.node(w.Condition), r.node(w.Result)
		if whens == nil && (cond != w.Condition || res != w.Result) {
			whens = slices.Clone(n.Whens)
		}
		if whens != nil {
			whens[i] = nodes.CaseWhen{Condition: cond, Result: res}
		}
	}
	if operand == n.Operand && elseVal == n.ElseVal && whens == nil {
		return r.set(n)
	}
	c := nodes.NewCase()
	c.Operand, c.Whens, c.ElseVal = operand, n.Whens, elseVal
	if whens != nil {
		c.Whens = whens
	}
	return r.set(c)
}

func (r *rewriter) VisitAlias(n *nodes.AliasNode) string {
	expr := r.node(n.Expr)
	if expr == n.Expr {
		return r.set(n)
	}
	return r.set(nodes.NewAliasNode(expr, n.Name))
}

// --- DML ---

func (r *rewriter) VisitInsertStatement(n *nodes.InsertStatement) string {
	into := r.node(n.Into)
	cols, cc := r.list(n.Columns)
	var values [][]nodes.Node
	for i, row := range n.Values {
		vals, changed := r.list(row)
		if values == nil && changed {
			values = slices.Clone(n.Values)
		}
		if values != nil {
			values[i] = vals
		}
	}
	sel := r.node(n.Select)
	returning, rc := r.list(n.Returning)
	onConflict := n.OnConflict
	if onConflict != nil {
		if oc, ok := r.node(onConflict).(*nodes.OnConflictNode); ok {
			onConflict = oc
		}
	}
	if into == n.Into && !cc && values == nil && sel == n.Select && !rc && onConflict == n.OnConflict {
		return r.set(n)
	}
	c := *n
	c.Into, c.Columns, c.Select, c.Returning, c.OnConflict = into, cols, sel, returning, onConflict
	if values != nil {
		c.Values = values
	}
	return r.set(&c)
}

func (r *rewriter) VisitUpdateStatement(n *nodes.UpdateStatement) string {
	table := r.node(n.Table)
	assigns, ac := r.assignments(n.Assignments)
	wheres, wc := r.list(n.Wheres)
	returning, rc := r.list(n.Returning)
	if table == n.Table && !ac && !wc && !rc {
		return r.set(n)
	}
	c := *n
	c.Table, c.Assignments, c.Wheres, c.Returning = table, assigns, wheres, returning
	return r.set(&c)
}

func (r *rewriter) VisitDeleteStatement(n *nodes.DeleteStatement) string {
	from := r.node(n.From)
	wheres, wc := r.list(n.Wheres)
	returning, rc := r.list(n.Returning)
	if from == n.From && !wc && !rc {
		return r.set(n)
	}
	c := *n
	c.From, c.Wheres, c.Returning = from, wheres, returning
	return r.set(&c)
}

func (r *rewriter) VisitAssignment(n *nodes.AssignmentNode) string {
	left, right := r.node(n.Left), r.node(n.Right)
	if left == n.Left && right == n.Right {
		return r.set(n)
	}
	c := *n
	c.Left, c.Right = left, right
	return r.set(&c)
}

func (r *rewriter) VisitOnConflict(n *nodes.OnConflictNode) string {
	cols, cc := r.list(n.Columns)
	assigns, ac := r.assignments(n.Assignments)
	wheres, wc := r.list(n.Wheres)
	if !cc && !ac && !wc {
		return r.set(n)
	}
	c := *n
	c.Columns, c.Assignments, c.Wheres = cols, assigns, wheres
	return r.set(&c)
}
