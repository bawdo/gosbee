package main

import (
	"fmt"
	"strings"

	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// resolveTable looks up an alias first, falling back to ensureTable.
func (s *Session) resolveTable(name string) nodes.Node {
	if a, ok := s.aliases[name]; ok {
		return a
	}
	return s.ensureTable(name)
}

// --- AST display helpers ---

// printASTMetadata prints CTEs, set operations, comments, and hints.
func (s *Session) printASTMetadata(c *nodes.SelectCore) {
	for _, cte := range s.ctes {
		kind := "WITH"
		if cte.recursive {
			kind = "WITH RECURSIVE"
		}
		_, _ = fmt.Fprintf(s.out,"  %s %s\n", kind, cte.name)
	}
	for i, entry := range s.setOps {
		from := "(none)"
		if entry.query.Core.From != nil {
			from = nodeSummary(entry.query.Core.From)
		}
		_, _ = fmt.Fprintf(s.out,"  QUERY[%d]: FROM %s %s\n", i, from, entry.opType)
	}
	if c.Comment != "" {
		_, _ = fmt.Fprintf(s.out,"  COMMENT: %s\n", c.Comment)
	}
	if len(c.Hints) > 0 {
		_, _ = fmt.Fprintf(s.out,"  HINTS: %s\n", strings.Join(c.Hints, ", "))
	}
}

func (s *Session) printASTFrom(c *nodes.SelectCore) {
	if c.From != nil {
		_, _ = fmt.Fprintf(s.out,"  FROM:   %s\n", nodeSummary(c.From))
	}
}

func (s *Session) printASTDistinct(c *nodes.SelectCore) {
	if len(c.DistinctOn) > 0 {
		names := make([]string, len(c.DistinctOn))
		for i, d := range c.DistinctOn {
			names[i] = nodeSummary(d)
		}
		_, _ = fmt.Fprintf(s.out,"  DISTINCT ON: %s\n", strings.Join(names, ", "))
	} else if c.Distinct {
		_, _ = fmt.Fprintln(s.out,"  DISTINCT: true")
	}
}

func (s *Session) printASTProjections(c *nodes.SelectCore) {
	if len(c.Projections) > 0 {
		names := make([]string, len(c.Projections))
		for i, p := range c.Projections {
			names[i] = nodeSummary(p)
		}
		_, _ = fmt.Fprintf(s.out,"  SELECT: %s\n", strings.Join(names, ", "))
	} else {
		_, _ = fmt.Fprintln(s.out,"  SELECT: *")
	}
}

func (s *Session) printASTJoins(c *nodes.SelectCore) {
	for i, j := range c.Joins {
		label := j.Type.String()
		if j.Lateral {
			label = "LATERAL " + label
		}
		_, _ = fmt.Fprintf(s.out,"  JOIN[%d]: %s %s\n", i, label, nodeSummary(j.Right))
	}
}

func (s *Session) printASTWheres(c *nodes.SelectCore) {
	if len(c.Wheres) > 0 {
		_, _ = fmt.Fprintf(s.out,"  WHERE:  %d condition(s)\n", len(c.Wheres))
	}
}

func (s *Session) printASTGroups(c *nodes.SelectCore) {
	if len(c.Groups) > 0 {
		names := make([]string, len(c.Groups))
		for i, g := range c.Groups {
			names[i] = nodeSummary(g)
		}
		_, _ = fmt.Fprintf(s.out,"  GROUP:  %s\n", strings.Join(names, ", "))
	}
}

func (s *Session) printASTHavings(c *nodes.SelectCore) {
	if len(c.Havings) > 0 {
		_, _ = fmt.Fprintf(s.out,"  HAVING: %d condition(s)\n", len(c.Havings))
	}
}

func (s *Session) printASTWindows(c *nodes.SelectCore) {
	if len(c.Windows) > 0 {
		names := make([]string, len(c.Windows))
		for i, w := range c.Windows {
			names[i] = w.Name
		}
		_, _ = fmt.Fprintf(s.out,"  WINDOW: %s\n", strings.Join(names, ", "))
	}
}

func (s *Session) printASTOrders(c *nodes.SelectCore) {
	if len(c.Orders) > 0 {
		names := make([]string, len(c.Orders))
		for i, o := range c.Orders {
			if ord, ok := o.(*nodes.OrderingNode); ok {
				dir := "ASC"
				if ord.Direction == nodes.Desc {
					dir = "DESC"
				}
				switch ord.Nulls {
				case nodes.NullsFirst:
					dir += " NULLS FIRST"
				case nodes.NullsLast:
					dir += " NULLS LAST"
				}
				names[i] = nodeSummary(ord.Expr) + " " + dir
			} else {
				names[i] = fmt.Sprintf("%T", o)
			}
		}
		_, _ = fmt.Fprintf(s.out,"  ORDER:  %s\n", strings.Join(names, ", "))
	}
}

func (s *Session) printASTLimitOffset(c *nodes.SelectCore) {
	if c.Limit != nil {
		_, _ = fmt.Fprintf(s.out,"  LIMIT:  %s\n", nodeSummary(c.Limit))
	}
	if c.Offset != nil {
		_, _ = fmt.Fprintf(s.out,"  OFFSET: %s\n", nodeSummary(c.Offset))
	}
}

func (s *Session) printASTLock(c *nodes.SelectCore) {
	if c.Lock != nodes.NoLock {
		lockName := c.Lock.String()
		if c.SkipLocked {
			lockName += " SKIP LOCKED"
		}
		_, _ = fmt.Fprintf(s.out,"  LOCK:   %s\n", lockName)
	}
}

func (s *Session) printASTFooter() {
	for _, entry := range s.plugins.entries {
		_, _ = fmt.Fprintf(s.out,"  Plugin: %s (%s)\n", entry.name, entry.status())
	}
	if s.parameterize {
		_, _ = fmt.Fprintln(s.out,"  Parameterize: on")
	}
	if s.conn != nil {
		_, _ = fmt.Fprintf(s.out,"  Connected: %s (%s)\n", sanitizeDSN(s.conn.dsn), s.conn.engine)
	}
}

// --- Mode and rebuild helpers ---

// setMode switches the DML mode and clears all query builders.
func (s *Session) setMode(mode dmlMode) {
	s.mode = mode
	s.query = nil
	s.insertQuery = nil
	s.updateQuery = nil
	s.deleteQuery = nil
}

// rebuildQueryWithPlugins rebuilds the current SELECT query from scratch,
// re-applying all enabled plugins. No-op if no query exists.
func (s *Session) rebuildQueryWithPlugins() {
	if s.query == nil {
		return
	}
	core := s.query.Core
	s.query = managers.NewSelectManager(core.From)
	s.rebuildCoreFields(core)
	s.plugins.applyTo(func(t plugins.Transformer) { s.query.Use(t) })
}

// rebuildCoreFields copies all fields from an old core to the current query's
// core. Uses CloneCore() to ensure the field list stays in sync with
// SelectCore's definition, then preserves the current From.
func (s *Session) rebuildCoreFields(old *nodes.SelectCore) {
	from := s.query.Core.From
	tmp := managers.NewSelectManager(old.From)
	tmp.Core = old
	cloned := tmp.CloneCore()
	cloned.From = from
	s.query.Core = cloned
}

// --- Node summary helpers ---

// nodeSummary returns a concise human-readable label for a node.
func nodeSummary(n nodes.Node) string {
	switch v := n.(type) {
	case *nodes.Table:
		return v.Name
	case *nodes.TableAlias:
		return tableAliasSourceName(v) + " AS " + v.AliasName
	case *nodes.Attribute:
		return nodeSummary(v.Relation) + "." + v.Name
	case *nodes.StarNode:
		if v.Table != nil {
			return v.Table.Name + ".*"
		}
		return "*"
	case *nodes.LiteralNode:
		return fmt.Sprintf("%v", v.Value)
	case *nodes.SqlLiteral:
		return v.Raw
	case *nodes.SelectCore:
		return "(subquery)"
	case *nodes.NamedFunctionNode:
		return v.Name + "(...)"
	case *nodes.CaseNode:
		return "CASE...END"
	case *nodes.AliasNode:
		return nodeSummary(v.Expr) + " AS " + v.Name
	case *nodes.GroupingSetNode:
		switch v.Type {
		case nodes.Cube:
			return "CUBE(...)"
		case nodes.Rollup:
			return "ROLLUP(...)"
		default:
			return "GROUPING SETS(...)"
		}
	default:
		return fmt.Sprintf("%T", n)
	}
}

// tableAliasSourceName returns the source name for a TableAlias.
func tableAliasSourceName(ta *nodes.TableAlias) string {
	if name := nodes.TableSourceName(ta); name != "" {
		return name
	}
	return "(subquery)"
}
