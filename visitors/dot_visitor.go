package visitors

import (
	"fmt"
	"strings"

	"github.com/bawdo/gosbee/nodes"
)

// Color constants for DOT node categories.
const (
	colorTable      = "#6CA6CD" // blue — tables, aliases
	colorAttribute  = "#B0D4E8" // light blue — attributes, stars
	colorComparison = "#FFB347" // orange — comparisons, predicates
	colorLogical    = "#FFEB80" // yellow — AND, OR, NOT, Grouping, DISTINCT
	colorLiteral    = "#D3D3D3" // grey — literals, values
	colorJoin       = "#77DD77" // green — joins
	colorOrdering   = "#CDA0E0" // purple — ordering
	colorAssignment  = "#FF6961" // red — assignments, DML
	colorArithmetic  = "#98FB98" // mint green — arithmetic, math
	colorFunction    = "#87CEEB" // sky blue — aggregates, functions
)

// dotNode represents a single node in the DOT graph.
type dotNode struct {
	id    string
	label string
	color string
}

// dotEdge represents a directed edge between two nodes in the DOT graph.
type dotEdge struct {
	from  string
	to    string
	label string
}

// pluginCluster groups nodes added by a plugin into a DOT subgraph cluster.
type pluginCluster struct {
	name    string
	color   string
	nodeIDs []string
}

// PluginProvenance tracks which clause indices belong to which plugins.
type PluginProvenance struct {
	entries []provenanceEntry
}

type provenanceEntry struct {
	plugin string
	color  string
	index  int
	clause string // "where" or "join"
}

// NewPluginProvenance creates a new PluginProvenance tracker.
func NewPluginProvenance() *PluginProvenance {
	return &PluginProvenance{}
}

// AddWhere marks a WHERE clause index as belonging to a plugin.
func (pp *PluginProvenance) AddWhere(plugin, color string, index int) {
	pp.entries = append(pp.entries, provenanceEntry{plugin: plugin, color: color, index: index, clause: "where"})
}

// AddJoin marks a JOIN clause index as belonging to a plugin.
func (pp *PluginProvenance) AddJoin(plugin, color string, index int) {
	pp.entries = append(pp.entries, provenanceEntry{plugin: plugin, color: color, index: index, clause: "join"})
}

func (pp *PluginProvenance) pluginForWhere(index int) (string, string, bool) {
	for _, e := range pp.entries {
		if e.clause == "where" && e.index == index {
			return e.plugin, e.color, true
		}
	}
	return "", "", false
}

func (pp *PluginProvenance) pluginForJoin(index int) (string, string, bool) {
	for _, e := range pp.entries {
		if e.clause == "join" && e.index == index {
			return e.plugin, e.color, true
		}
	}
	return "", "", false
}

// DotVisitor walks the AST and produces Graphviz DOT output.
// It implements nodes.Visitor.
type DotVisitor struct {
	nextID     int
	nodes      []dotNode
	edges      []dotEdge
	clusters   []pluginCluster
	parentID   string
	edgeLabel  string
	provenance *PluginProvenance
}

// NewDotVisitor creates a new DotVisitor ready to walk an AST.
func NewDotVisitor() *DotVisitor {
	return &DotVisitor{}
}

// SetProvenance configures plugin provenance tracking for clause attribution.
func (dv *DotVisitor) SetProvenance(p *PluginProvenance) {
	dv.provenance = p
}

// addNode creates a new DOT node with the given label and color, returning its ID.
func (dv *DotVisitor) addNode(label, color string) string {
	id := fmt.Sprintf("n%d", dv.nextID)
	dv.nextID++
	dv.nodes = append(dv.nodes, dotNode{id: id, label: label, color: color})
	return id
}

// addEdge records a directed edge from one node to another.
func (dv *DotVisitor) addEdge(from, to, label string) {
	dv.edges = append(dv.edges, dotEdge{from: from, to: to, label: label})
}

// visitChild saves and restores the parent context, sets the edge label,
// and calls child.Accept to recursively visit the child node.
func (dv *DotVisitor) visitChild(parentID, label string, child nodes.Node) string {
	savedParent := dv.parentID
	savedLabel := dv.edgeLabel
	dv.parentID = parentID
	dv.edgeLabel = label
	result := child.Accept(dv)
	dv.parentID = savedParent
	dv.edgeLabel = savedLabel
	return result
}

// connectToParent adds an edge from the current parentID to nodeID if a parent exists.
func (dv *DotVisitor) connectToParent(nodeID string) {
	if dv.parentID != "" {
		dv.addEdge(dv.parentID, nodeID, dv.edgeLabel)
	}
}

// AddPluginCluster registers a plugin cluster for grouped rendering in the DOT output.
func (dv *DotVisitor) AddPluginCluster(name, color string, nodeIDs []string) {
	if len(nodeIDs) > 0 {
		dv.clusters = append(dv.clusters, pluginCluster{name: name, color: color, nodeIDs: nodeIDs})
	}
}

// NodeCount returns the number of nodes accumulated so far.
func (dv *DotVisitor) NodeCount() int {
	return len(dv.nodes)
}

// NodeIDsSince returns the IDs of nodes added since (and including) the given index.
func (dv *DotVisitor) NodeIDsSince(start int) []string {
	if start >= len(dv.nodes) {
		return nil
	}
	ids := make([]string, len(dv.nodes)-start)
	for i := start; i < len(dv.nodes); i++ {
		ids[i-start] = dv.nodes[i].id
	}
	return ids
}

// visitWheresWithProvenance visits WHERE clauses and collects provenance-tracked
// node IDs into pluginClusters. Returns the accumulated clusters map.
func (dv *DotVisitor) visitWheresWithProvenance(parentID string, wheres []nodes.Node, clusters map[string]*struct {
	color string
	ids   []string
}) {
	for i, w := range wheres {
		snapshot := dv.NodeCount()
		dv.visitChild(parentID, fmt.Sprintf("WHERE[%d]", i), w)
		if dv.provenance != nil {
			if plugin, color, ok := dv.provenance.pluginForWhere(i); ok {
				c, exists := clusters[plugin]
				if !exists {
					c = &struct {
						color string
						ids   []string
					}{color: color}
					clusters[plugin] = c
				}
				c.ids = append(c.ids, dv.NodeIDsSince(snapshot)...)
			}
		}
	}
}

// flushPluginClusters registers all accumulated plugin clusters.
func (dv *DotVisitor) flushPluginClusters(clusters map[string]*struct {
	color string
	ids   []string
}) {
	for name, c := range clusters {
		dv.AddPluginCluster(name, c.color, c.ids)
	}
}

// ToDot generates the complete DOT graph text.
func (dv *DotVisitor) ToDot() string {
	var sb strings.Builder

	sb.WriteString("digraph AST {\n")
	sb.WriteString("  rankdir=TB;\n")
	sb.WriteString("  node [shape=box, style=filled, fontname=\"Helvetica\"];\n")
	sb.WriteString("  edge [fontname=\"Helvetica\", fontsize=10];\n")

	// Collect IDs that belong to clusters so we can exclude them from the main body.
	clustered := make(map[string]bool)
	for _, c := range dv.clusters {
		for _, id := range c.nodeIDs {
			clustered[id] = true
		}
	}

	// Non-clustered nodes.
	for _, n := range dv.nodes {
		if !clustered[n.id] {
			sb.WriteString(fmt.Sprintf("  %s [label=\"%s\", fillcolor=\"%s\"];\n",
				n.id, escapeLabel(n.label), n.color))
		}
	}

	// Clusters.
	for i, c := range dv.clusters {
		sb.WriteString(fmt.Sprintf("  subgraph cluster_%d_%s {\n", i, c.name))
		sb.WriteString(fmt.Sprintf("    label=\"%s\";\n", c.name))
		sb.WriteString("    style=dashed;\n")
		sb.WriteString(fmt.Sprintf("    color=\"%s\";\n", c.color))
		sb.WriteString("    fontname=\"Helvetica\";\n")
		for _, id := range c.nodeIDs {
			// Find the node to get its label and color.
			for _, n := range dv.nodes {
				if n.id == id {
					sb.WriteString(fmt.Sprintf("    %s [label=\"%s\", fillcolor=\"%s\"];\n",
						n.id, escapeLabel(n.label), n.color))
					break
				}
			}
		}
		sb.WriteString("  }\n")
	}

	// Edges.
	for _, e := range dv.edges {
		if e.label != "" {
			sb.WriteString(fmt.Sprintf("  %s -> %s [label=\"%s\"];\n", e.from, e.to, e.label))
		} else {
			sb.WriteString(fmt.Sprintf("  %s -> %s;\n", e.from, e.to))
		}
	}

	sb.WriteString("}\n")
	return sb.String()
}

// escapeLabel escapes double quotes in DOT labels.
// Backslash sequences like \n are intentional DOT line breaks and are preserved.
func escapeLabel(s string) string {
	s = strings.ReplaceAll(s, "\"", "\\\"")
	return s
}

// qualifierName returns the qualifier name for an Attribute's Relation.
func qualifierName(rel nodes.Node) string {
	return nodes.RelationName(rel)
}

// --- Visitor interface implementation ---

func (dv *DotVisitor) VisitTable(n *nodes.Table) string {
	id := dv.addNode("Table\\n"+n.Name, colorTable)
	dv.connectToParent(id)
	return id
}

func (dv *DotVisitor) VisitTableAlias(n *nodes.TableAlias) string {
	id := dv.addNode("TableAlias\\n"+n.AliasName, colorTable)
	dv.connectToParent(id)
	dv.visitChild(id, "RELATION", n.Relation)
	return id
}

func (dv *DotVisitor) VisitAttribute(n *nodes.Attribute) string {
	qualifier := qualifierName(n.Relation)
	label := "Attribute\\n"
	if qualifier != "" {
		label += qualifier + "."
	}
	label += n.Name
	id := dv.addNode(label, colorAttribute)
	dv.connectToParent(id)
	return id
}

func (dv *DotVisitor) VisitLiteral(n *nodes.LiteralNode) string {
	label := fmt.Sprintf("Literal\\n%v", n.Value)
	id := dv.addNode(label, colorLiteral)
	dv.connectToParent(id)
	return id
}

func (dv *DotVisitor) VisitStar(n *nodes.StarNode) string {
	var label string
	if n.Table != nil {
		label = "Star\\n" + n.Table.Name + ".*"
	} else {
		label = "Star\\n*"
	}
	id := dv.addNode(label, colorAttribute)
	dv.connectToParent(id)
	return id
}

func (dv *DotVisitor) VisitSqlLiteral(n *nodes.SqlLiteral) string {
	id := dv.addNode("SqlLiteral\\n"+n.Raw, colorLiteral)
	dv.connectToParent(id)
	return id
}

// Comparison operator display names.
var comparisonOpName = [...]string{
	nodes.OpEq:                "=",
	nodes.OpNotEq:             "!=",
	nodes.OpGt:                ">",
	nodes.OpGtEq:              ">=",
	nodes.OpLt:                "<",
	nodes.OpLtEq:              "<=",
	nodes.OpLike:              "LIKE",
	nodes.OpNotLike:           "NOT LIKE",
	nodes.OpRegexp:            "~",
	nodes.OpNotRegexp:         "!~",
	nodes.OpDistinctFrom:      "IS DISTINCT FROM",
	nodes.OpNotDistinctFrom:   "IS NOT DISTINCT FROM",
	nodes.OpCaseSensitiveEq:   "CASE = (sensitive)",
	nodes.OpCaseInsensitiveEq: "CASE = (insensitive)",
	nodes.OpContains:          "@>",
	nodes.OpOverlaps:          "&&",
}

func (dv *DotVisitor) VisitComparison(n *nodes.ComparisonNode) string {
	id := dv.addNode("Comparison\\n"+comparisonOpName[n.Op], colorComparison)
	dv.connectToParent(id)
	dv.visitChild(id, "LEFT", n.Left)
	dv.visitChild(id, "RIGHT", n.Right)
	return id
}

func (dv *DotVisitor) VisitUnary(n *nodes.UnaryNode) string {
	var label string
	switch n.Op {
	case nodes.OpIsNull:
		label = "Unary\\nIS NULL"
	case nodes.OpIsNotNull:
		label = "Unary\\nIS NOT NULL"
	default:
		label = "Unary"
	}
	id := dv.addNode(label, colorComparison)
	dv.connectToParent(id)
	dv.visitChild(id, "EXPR", n.Expr)
	return id
}

func (dv *DotVisitor) VisitAnd(n *nodes.AndNode) string {
	id := dv.addNode("AND", colorLogical)
	dv.connectToParent(id)
	dv.visitChild(id, "LEFT", n.Left)
	dv.visitChild(id, "RIGHT", n.Right)
	return id
}

func (dv *DotVisitor) VisitOr(n *nodes.OrNode) string {
	id := dv.addNode("OR", colorLogical)
	dv.connectToParent(id)
	dv.visitChild(id, "LEFT", n.Left)
	dv.visitChild(id, "RIGHT", n.Right)
	return id
}

func (dv *DotVisitor) VisitNot(n *nodes.NotNode) string {
	id := dv.addNode("NOT", colorLogical)
	dv.connectToParent(id)
	dv.visitChild(id, "EXPR", n.Expr)
	return id
}

func (dv *DotVisitor) VisitIn(n *nodes.InNode) string {
	label := "IN"
	if n.Negate {
		label = "NOT IN"
	}
	id := dv.addNode(label, colorComparison)
	dv.connectToParent(id)
	dv.visitChild(id, "EXPR", n.Expr)
	for i, v := range n.Vals {
		dv.visitChild(id, fmt.Sprintf("VAL[%d]", i), v)
	}
	return id
}

func (dv *DotVisitor) VisitBetween(n *nodes.BetweenNode) string {
	label := "BETWEEN"
	if n.Negate {
		label = "NOT BETWEEN"
	}
	id := dv.addNode(label, colorComparison)
	dv.connectToParent(id)
	dv.visitChild(id, "EXPR", n.Expr)
	dv.visitChild(id, "LOW", n.Low)
	dv.visitChild(id, "HIGH", n.High)
	return id
}

func (dv *DotVisitor) VisitGrouping(n *nodes.GroupingNode) string {
	id := dv.addNode("Grouping\\n( )", colorLogical)
	dv.connectToParent(id)
	dv.visitChild(id, "EXPR", n.Expr)
	return id
}

func (dv *DotVisitor) VisitJoin(n *nodes.JoinNode) string {
	label := n.Type.String()
	if n.Lateral {
		label += "\\nLATERAL"
	}
	id := dv.addNode("Join\\n"+label, colorJoin)
	dv.connectToParent(id)
	dv.visitChild(id, "RIGHT", n.Right)
	if n.On != nil {
		dv.visitChild(id, "ON", n.On)
	}
	return id
}

func (dv *DotVisitor) VisitOrdering(n *nodes.OrderingNode) string {
	dir := "ASC"
	if n.Direction == nodes.Desc {
		dir = "DESC"
	}
	switch n.Nulls {
	case nodes.NullsFirst:
		dir += "\\nNULLS FIRST"
	case nodes.NullsLast:
		dir += "\\nNULLS LAST"
	}
	id := dv.addNode("Order\\n"+dir, colorOrdering)
	dv.connectToParent(id)
	dv.visitChild(id, "EXPR", n.Expr)
	return id
}

func (dv *DotVisitor) VisitSelectCore(n *nodes.SelectCore) string {
	id := dv.addNode("SelectCore", colorTable)
	dv.connectToParent(id)

	for i, cte := range n.CTEs {
		dv.visitChild(id, fmt.Sprintf("CTE[%d]", i), cte)
	}
	dv.visitDotComment(id, n.Comment)
	dv.visitDotHints(id, n.Hints)
	dv.visitDotDistinct(id, n.Distinct, n.DistinctOn)

	if n.From != nil {
		dv.visitChild(id, "FROM", n.From)
	}

	dv.visitChildList(id, "SELECT", n.Projections)
	dv.visitJoinsWithProvenance(id, n.Joins, n.Wheres)
	dv.visitChildList(id, "GROUP", n.Groups)
	dv.visitChildList(id, "HAVING", n.Havings)
	dv.visitDotWindows(id, n.Windows)
	dv.visitChildList(id, "ORDER", n.Orders)

	if n.Limit != nil {
		dv.visitChild(id, "LIMIT", n.Limit)
	}
	if n.Offset != nil {
		dv.visitChild(id, "OFFSET", n.Offset)
	}

	dv.visitDotLock(id, n.Lock, n.SkipLocked)

	return id
}

// visitChildList visits a slice of nodes as indexed children (e.g. "SELECT[0]", "SELECT[1]").
func (dv *DotVisitor) visitChildList(parentID, prefix string, items []nodes.Node) {
	for i, item := range items {
		dv.visitChild(parentID, fmt.Sprintf("%s[%d]", prefix, i), item)
	}
}

func (dv *DotVisitor) visitDotComment(parentID, comment string) {
	if comment != "" {
		commentID := dv.addNode("Comment\\n"+comment, colorLiteral)
		dv.addEdge(parentID, commentID, "COMMENT")
	}
}

func (dv *DotVisitor) visitDotHints(parentID string, hints []string) {
	for i, h := range hints {
		hintID := dv.addNode("Hint\\n"+h, colorLiteral)
		dv.addEdge(parentID, hintID, fmt.Sprintf("HINT[%d]", i))
	}
}

func (dv *DotVisitor) visitDotDistinct(parentID string, distinct bool, distinctOn []nodes.Node) {
	if len(distinctOn) > 0 {
		distinctOnID := dv.addNode("DISTINCT ON", colorLogical)
		dv.addEdge(parentID, distinctOnID, "DISTINCT ON")
		for i, c := range distinctOn {
			dv.visitChild(distinctOnID, fmt.Sprintf("COL[%d]", i), c)
		}
	} else if distinct {
		distinctID := dv.addNode("DISTINCT", colorLogical)
		dv.addEdge(parentID, distinctID, "DISTINCT")
	}
}

// visitJoinsWithProvenance handles JOINs and WHEREs together for provenance tracking.
func (dv *DotVisitor) visitJoinsWithProvenance(parentID string, joins []*nodes.JoinNode, wheres []nodes.Node) {
	pluginClusters := make(map[string]*struct {
		color string
		ids   []string
	})

	for i, j := range joins {
		snapshot := dv.NodeCount()
		dv.visitChild(parentID, fmt.Sprintf("JOIN[%d]", i), j)
		if dv.provenance != nil {
			if plugin, color, ok := dv.provenance.pluginForJoin(i); ok {
				c, exists := pluginClusters[plugin]
				if !exists {
					c = &struct {
						color string
						ids   []string
					}{color: color}
					pluginClusters[plugin] = c
				}
				c.ids = append(c.ids, dv.NodeIDsSince(snapshot)...)
			}
		}
	}

	dv.visitWheresWithProvenance(parentID, wheres, pluginClusters)
	dv.flushPluginClusters(pluginClusters)
}

func (dv *DotVisitor) visitDotWindows(parentID string, windows []*nodes.WindowDefinition) {
	for i, w := range windows {
		label := fmt.Sprintf("WINDOW\\n%s", w.Name)
		winID := dv.addNode(label, colorFunction)
		dv.addEdge(parentID, winID, fmt.Sprintf("WINDOW[%d]", i))
		for j, p := range w.PartitionBy {
			dv.visitChild(winID, fmt.Sprintf("PARTITION[%d]", j), p)
		}
		for j, o := range w.OrderBy {
			dv.visitChild(winID, fmt.Sprintf("ORDER[%d]", j), o)
		}
		if w.Frame != nil {
			frameLabel := "ROWS"
			if w.Frame.Type == nodes.FrameRange {
				frameLabel = "RANGE"
			}
			frameID := dv.addNode("Frame\\n"+frameLabel, colorFunction)
			dv.addEdge(winID, frameID, "FRAME")
		}
	}
}

func (dv *DotVisitor) visitDotLock(parentID string, lock nodes.LockMode, skipLocked bool) {
	if lock != nodes.NoLock {
		label := lockModeSQL[lock]
		if skipLocked {
			label += "\\nSKIP LOCKED"
		}
		lockID := dv.addNode(label, colorLogical)
		dv.addEdge(parentID, lockID, "LOCK")
	}
}

func (dv *DotVisitor) VisitInsertStatement(n *nodes.InsertStatement) string {
	id := dv.addNode("InsertStatement", colorAssignment)
	dv.connectToParent(id)

	// INTO
	if n.Into != nil {
		dv.visitChild(id, "INTO", n.Into)
	}

	// COLUMNS
	for i, c := range n.Columns {
		dv.visitChild(id, fmt.Sprintf("COLUMN[%d]", i), c)
	}

	// VALUES
	for i, row := range n.Values {
		for j, v := range row {
			dv.visitChild(id, fmt.Sprintf("VALUES[%d][%d]", i, j), v)
		}
	}

	// SELECT (INSERT FROM SELECT)
	if n.Select != nil {
		dv.visitChild(id, "SELECT", n.Select)
	}

	// RETURNING
	for i, r := range n.Returning {
		dv.visitChild(id, fmt.Sprintf("RETURNING[%d]", i), r)
	}

	// ON CONFLICT
	if n.OnConflict != nil {
		dv.visitChild(id, "ON_CONFLICT", n.OnConflict)
	}

	return id
}

func (dv *DotVisitor) VisitUpdateStatement(n *nodes.UpdateStatement) string {
	id := dv.addNode("UpdateStatement", colorAssignment)
	dv.connectToParent(id)

	// TABLE
	if n.Table != nil {
		dv.visitChild(id, "TABLE", n.Table)
	}

	// SET
	for i, a := range n.Assignments {
		dv.visitChild(id, fmt.Sprintf("SET[%d]", i), a)
	}

	// WHERE with provenance tracking
	pluginClusters := make(map[string]*struct {
		color string
		ids   []string
	})
	dv.visitWheresWithProvenance(id, n.Wheres, pluginClusters)
	dv.flushPluginClusters(pluginClusters)

	// RETURNING
	for i, r := range n.Returning {
		dv.visitChild(id, fmt.Sprintf("RETURNING[%d]", i), r)
	}

	return id
}

func (dv *DotVisitor) VisitDeleteStatement(n *nodes.DeleteStatement) string {
	id := dv.addNode("DeleteStatement", colorAssignment)
	dv.connectToParent(id)

	// FROM
	if n.From != nil {
		dv.visitChild(id, "FROM", n.From)
	}

	// WHERE with provenance tracking
	pluginClusters := make(map[string]*struct {
		color string
		ids   []string
	})
	dv.visitWheresWithProvenance(id, n.Wheres, pluginClusters)
	dv.flushPluginClusters(pluginClusters)

	// RETURNING
	for i, r := range n.Returning {
		dv.visitChild(id, fmt.Sprintf("RETURNING[%d]", i), r)
	}

	return id
}

func (dv *DotVisitor) VisitAssignment(n *nodes.AssignmentNode) string {
	id := dv.addNode("Assignment\\n=", colorAssignment)
	dv.connectToParent(id)
	dv.visitChild(id, "COLUMN", n.Left)
	dv.visitChild(id, "VALUE", n.Right)
	return id
}

func (dv *DotVisitor) VisitOnConflict(n *nodes.OnConflictNode) string {
	label := "OnConflict\\nDO NOTHING"
	if n.Action == nodes.DoUpdate {
		label = "OnConflict\\nDO UPDATE"
	}
	id := dv.addNode(label, colorAssignment)
	dv.connectToParent(id)

	// TARGET columns
	for i, c := range n.Columns {
		dv.visitChild(id, fmt.Sprintf("TARGET[%d]", i), c)
	}

	// SET assignments
	for i, a := range n.Assignments {
		dv.visitChild(id, fmt.Sprintf("SET[%d]", i), a)
	}

	// WHERE
	for i, w := range n.Wheres {
		dv.visitChild(id, fmt.Sprintf("WHERE[%d]", i), w)
	}

	return id
}

// Infix operator display names for DOT labels.
var infixOpName = [...]string{
	nodes.OpPlus:       "+",
	nodes.OpMinus:      "-",
	nodes.OpMultiply:   "*",
	nodes.OpDivide:     "/",
	nodes.OpBitwiseAnd: "&",
	nodes.OpBitwiseOr:  "|",
	nodes.OpBitwiseXor: "^",
	nodes.OpShiftLeft:  "<<",
	nodes.OpShiftRight: ">>",
	nodes.OpConcat:     "||",
}

func (dv *DotVisitor) VisitInfix(n *nodes.InfixNode) string {
	id := dv.addNode("Infix\\n"+infixOpName[n.Op], colorArithmetic)
	dv.connectToParent(id)
	dv.visitChild(id, "LEFT", n.Left)
	dv.visitChild(id, "RIGHT", n.Right)
	return id
}

func (dv *DotVisitor) VisitUnaryMath(n *nodes.UnaryMathNode) string {
	id := dv.addNode("UnaryMath\\n~", colorArithmetic)
	dv.connectToParent(id)
	dv.visitChild(id, "EXPR", n.Expr)
	return id
}

// Aggregate function display names for DOT labels.
var aggregateFuncName = [...]string{
	nodes.AggCount: "COUNT",
	nodes.AggSum:   "SUM",
	nodes.AggAvg:   "AVG",
	nodes.AggMin:   "MIN",
	nodes.AggMax:   "MAX",
}

func (dv *DotVisitor) VisitAggregate(n *nodes.AggregateNode) string {
	label := aggregateFuncName[n.Func]
	if n.Distinct {
		label += "\\nDISTINCT"
	}
	id := dv.addNode(label, colorFunction)
	dv.connectToParent(id)
	if n.Expr != nil {
		dv.visitChild(id, "EXPR", n.Expr)
	} else {
		starID := dv.addNode("*", colorAttribute)
		dv.addEdge(id, starID, "EXPR")
	}
	if n.Filter != nil {
		dv.visitChild(id, "FILTER", n.Filter)
	}
	return id
}

// Extract field display names for DOT labels.
var extractFieldName = [...]string{
	nodes.ExtractYear:    "YEAR",
	nodes.ExtractMonth:   "MONTH",
	nodes.ExtractDay:     "DAY",
	nodes.ExtractHour:    "HOUR",
	nodes.ExtractMinute:  "MINUTE",
	nodes.ExtractSecond:  "SECOND",
	nodes.ExtractDow:     "DOW",
	nodes.ExtractDoy:     "DOY",
	nodes.ExtractEpoch:   "EPOCH",
	nodes.ExtractQuarter: "QUARTER",
	nodes.ExtractWeek:    "WEEK",
}

func (dv *DotVisitor) VisitExtract(n *nodes.ExtractNode) string {
	id := dv.addNode("EXTRACT\\n"+extractFieldName[n.Field], colorFunction)
	dv.connectToParent(id)
	dv.visitChild(id, "FROM", n.Expr)
	return id
}

// Window function display names for DOT labels.
var windowFuncName = [...]string{
	nodes.WinRowNumber:   "ROW_NUMBER",
	nodes.WinRank:        "RANK",
	nodes.WinDenseRank:   "DENSE_RANK",
	nodes.WinNtile:       "NTILE",
	nodes.WinLag:         "LAG",
	nodes.WinLead:        "LEAD",
	nodes.WinFirstValue:  "FIRST_VALUE",
	nodes.WinLastValue:   "LAST_VALUE",
	nodes.WinNthValue:    "NTH_VALUE",
	nodes.WinCumeDist:    "CUME_DIST",
	nodes.WinPercentRank: "PERCENT_RANK",
}

func (dv *DotVisitor) VisitWindowFunction(n *nodes.WindowFuncNode) string {
	id := dv.addNode(windowFuncName[n.Func], colorFunction)
	dv.connectToParent(id)
	for i, arg := range n.Args {
		dv.visitChild(id, fmt.Sprintf("ARG[%d]", i), arg)
	}
	return id
}

func (dv *DotVisitor) VisitOver(n *nodes.OverNode) string {
	label := "OVER"
	if n.WindowName != "" {
		label = "OVER\\n" + n.WindowName
	}
	id := dv.addNode(label, colorFunction)
	dv.connectToParent(id)
	dv.visitChild(id, "EXPR", n.Expr)
	if n.Window != nil {
		for i, p := range n.Window.PartitionBy {
			dv.visitChild(id, fmt.Sprintf("PARTITION[%d]", i), p)
		}
		for i, o := range n.Window.OrderBy {
			dv.visitChild(id, fmt.Sprintf("ORDER[%d]", i), o)
		}
		if n.Window.Frame != nil {
			frameLabel := "ROWS"
			if n.Window.Frame.Type == nodes.FrameRange {
				frameLabel = "RANGE"
			}
			frameID := dv.addNode("Frame\\n"+frameLabel, colorFunction)
			dv.addEdge(id, frameID, "FRAME")
		}
	}
	return id
}

func (dv *DotVisitor) VisitExists(n *nodes.ExistsNode) string {
	label := "EXISTS"
	if n.Negated {
		label = "NOT EXISTS"
	}
	id := dv.addNode(label, colorComparison)
	dv.connectToParent(id)
	dv.visitChild(id, "SUBQUERY", n.Subquery)
	return id
}

func (dv *DotVisitor) VisitSetOperation(n *nodes.SetOperationNode) string {
	label := setOpTypeSQL[n.Type]
	id := dv.addNode(label, colorLogical)
	dv.connectToParent(id)
	dv.visitChild(id, "LEFT", n.Left)
	dv.visitChild(id, "RIGHT", n.Right)
	return id
}

func (dv *DotVisitor) VisitCTE(n *nodes.CTENode) string {
	label := "CTE\\n" + n.Name
	if n.Recursive {
		label += "\\n(RECURSIVE)"
	}
	id := dv.addNode(label, colorTable)
	dv.connectToParent(id)
	dv.visitChild(id, "QUERY", n.Query)
	return id
}

func (dv *DotVisitor) VisitNamedFunction(n *nodes.NamedFunctionNode) string {
	label := n.Name
	if n.Distinct {
		label += "\\nDISTINCT"
	}
	id := dv.addNode(label, colorFunction)
	dv.connectToParent(id)
	for i, arg := range n.Args {
		dv.visitChild(id, fmt.Sprintf("ARG[%d]", i), arg)
	}
	return id
}

func (dv *DotVisitor) VisitCase(n *nodes.CaseNode) string {
	id := dv.addNode("CASE", colorLogical)
	dv.connectToParent(id)
	if n.Operand != nil {
		dv.visitChild(id, "OPERAND", n.Operand)
	}
	for i, w := range n.Whens {
		dv.visitChild(id, fmt.Sprintf("WHEN[%d]", i), w.Condition)
		dv.visitChild(id, fmt.Sprintf("THEN[%d]", i), w.Result)
	}
	if n.ElseVal != nil {
		dv.visitChild(id, "ELSE", n.ElseVal)
	}
	return id
}

// Grouping set type display names for DOT labels.
var groupingSetTypeName = [...]string{
	nodes.Cube:         "CUBE",
	nodes.Rollup:       "ROLLUP",
	nodes.GroupingSets: "GROUPING SETS",
}

func (dv *DotVisitor) VisitGroupingSet(n *nodes.GroupingSetNode) string {
	id := dv.addNode(groupingSetTypeName[n.Type], colorFunction)
	dv.connectToParent(id)
	if n.Type == nodes.GroupingSets {
		for i, set := range n.Sets {
			for j, col := range set {
				dv.visitChild(id, fmt.Sprintf("SET[%d][%d]", i, j), col)
			}
		}
	} else {
		for i, col := range n.Columns {
			dv.visitChild(id, fmt.Sprintf("COL[%d]", i), col)
		}
	}
	return id
}

func (dv *DotVisitor) VisitAlias(n *nodes.AliasNode) string {
	id := dv.addNode("Alias\\n"+n.Name, colorAttribute)
	dv.connectToParent(id)
	dv.visitChild(id, "EXPR", n.Expr)
	return id
}

func (dv *DotVisitor) VisitBindParam(n *nodes.BindParamNode) string {
	label := fmt.Sprintf("BindParam\\n%v", n.Value)
	id := dv.addNode(label, colorLiteral)
	dv.connectToParent(id)
	return id
}

func (dv *DotVisitor) VisitCasted(n *nodes.CastedNode) string {
	label := fmt.Sprintf("Casted\\n%v (%s)", n.Value, n.TypeName)
	id := dv.addNode(label, colorLiteral)
	dv.connectToParent(id)
	return id
}
