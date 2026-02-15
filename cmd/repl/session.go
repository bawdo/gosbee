package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
	"github.com/bawdo/gosbee/visitors"
	"github.com/ergochat/readline"
)

var errNoQuery = errors.New("no query defined (use 'from <table>' first)")

// setOpEntry records a pushed set operation for the REPL stack approach.
type setOpEntry struct {
	opType nodes.SetOpType
	query  *managers.SelectManager
}

// cteEntry records a pushed CTE for the REPL push approach.
type cteEntry struct {
	name      string
	query     *managers.SelectManager
	recursive bool
}

// dmlMode tracks which kind of statement the REPL is currently building.
type dmlMode int

const (
	modeSelect dmlMode = iota
	modeInsert
	modeUpdate
	modeDelete
)

// Session holds the REPL state: registered tables, the current query,
// the active visitor/engine, and any enabled plugins.
type Session struct {
	tables       map[string]*nodes.Table
	aliases      map[string]*nodes.TableAlias
	query        *managers.SelectManager
	engine       string
	visitor      nodes.Visitor
	plugins      pluginRegistry     // enabled plugins
	configurers  []pluginConfigurer // all known plugins
	opaConfig    *opaPluginRef      // OPA server config (nil when not set up)
	parameterize bool
	commands     []commandEntry // command registry (sorted by prefix length desc)
	conn         *dbConn        // nil when disconnected
	lastDSN      string         // remembers the previous DSN for reconnect
	rl           *readline.Instance
	setOps       []setOpEntry // set operation stack
	ctes         []cteEntry   // CTE stack
	mode         dmlMode
	insertQuery  *managers.InsertManager
	updateQuery  *managers.UpdateManager
	deleteQuery  *managers.DeleteManager
	out          io.Writer // destination for REPL output (default os.Stdout)
}

// NewSession creates a session with the given SQL dialect.
func NewSession(engine string, rl *readline.Instance) *Session {
	s := &Session{
		tables:  make(map[string]*nodes.Table),
		aliases: make(map[string]*nodes.TableAlias),
		rl:      rl,
		out:     os.Stdout,
	}
	s.configurers = []pluginConfigurer{
		{name: "softdelete", configure: configureSoftdelete},
		{name: "opa", configure: configureOPA},
	}
	s.setEngine(engine)
	s.initCommands()
	return s
}

// pluginNames returns the names of all known plugins (for tab completion).
func (s *Session) pluginNames() []string {
	names := make([]string, len(s.configurers))
	for i, c := range s.configurers {
		names[i] = c.name
	}
	return names
}

func (s *Session) setEngine(engine string) {
	s.engine = engine
	var opts []visitors.Option
	if !s.parameterize {
		// Params are now enabled by default, so disable them when parameterize is false
		opts = append(opts, visitors.WithoutParams())
	}
	switch engine {
	case "mysql":
		s.visitor = visitors.NewMySQLVisitor(opts...)
	case "sqlite":
		s.visitor = visitors.NewSQLiteVisitor(opts...)
	default:
		s.engine = "postgres"
		s.visitor = visitors.NewPostgresVisitor(opts...)
	}
}

// ensureTable returns the table if registered, otherwise registers it.
func (s *Session) ensureTable(name string) *nodes.Table {
	if t, ok := s.tables[name]; ok {
		return t
	}
	t := nodes.NewTable(name)
	s.tables[name] = t
	return t
}

// resolveTable returns an alias or table by name.
// GenerateSQL produces the SQL string for the current query.
func (s *Session) GenerateSQL() (string, error) {
	var sql string
	var err error

	switch s.mode {
	case modeInsert:
		if s.insertQuery == nil {
			return "", errors.New("no INSERT query defined")
		}
		sql, _, err = s.insertQuery.ToSQL(s.visitor)
	case modeUpdate:
		if s.updateQuery == nil {
			return "", errors.New("no UPDATE query defined")
		}
		sql, _, err = s.updateQuery.ToSQL(s.visitor)
	case modeDelete:
		if s.deleteQuery == nil {
			return "", errors.New("no DELETE query defined")
		}
		sql, _, err = s.deleteQuery.ToSQL(s.visitor)
	default:
		if s.query == nil {
			return "", errNoQuery
		}
		sql, _, err = s.query.ToSQL(s.visitor)
	}

	return sql, err
}

// Execute parses and runs a single REPL command.
func (s *Session) Execute(line string) error {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil
	}
	lower := strings.ToLower(line)

	for _, cmd := range s.commands {
		if strings.HasSuffix(cmd.prefix, " ") {
			if strings.HasPrefix(lower, cmd.prefix) {
				return cmd.handler(line[len(cmd.prefix):])
			}
		} else {
			if lower == cmd.prefix {
				return cmd.handler("")
			}
		}
	}

	word := strings.Fields(line)[0]
	return fmt.Errorf("unknown command: %s (type 'help' for commands)", word)
}

// --- Command handlers ---

func (s *Session) cmdTable(args string) error {
	name := strings.TrimSpace(args)
	if name == "" {
		return errors.New("usage: table <name>")
	}
	s.ensureTable(name)
	_, _ = fmt.Fprintf(s.out, "  Registered table %q\n", name)
	return nil
}

func (s *Session) cmdAlias(args string) error {
	parts := strings.Fields(args)
	if len(parts) != 2 {
		return errors.New("usage: alias <table> <alias_name>")
	}
	tableName, aliasName := parts[0], parts[1]
	table := s.ensureTable(tableName)
	alias := table.Alias(aliasName)
	s.aliases[aliasName] = alias
	_, _ = fmt.Fprintf(s.out, "  Aliased %q as %q\n", tableName, aliasName)
	return nil
}

func (s *Session) cmdFrom(args string) error {
	name := strings.TrimSpace(args)
	if name == "" {
		return errors.New("usage: from <table>")
	}
	from := s.resolveTable(name)
	s.setMode(modeSelect)
	s.query = managers.NewSelectManager(from)
	s.plugins.applyTo(func(t plugins.Transformer) { s.query.Use(t) })
	_, _ = fmt.Fprintf(s.out, "  Query FROM %q\n", name)
	return nil
}

// splitTopLevelCommas splits a string on commas that are at the top level
// (not inside parentheses). This allows function calls with multiple args
// like LAG(t.col, 1, 0) to be kept intact.
func splitTopLevelCommas(s string) []string {
	var parts []string
	var cur strings.Builder
	depth := 0
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '(':
			depth++
			cur.WriteByte(ch)
		case ch == ')':
			depth--
			cur.WriteByte(ch)
		case ch == ',' && depth == 0:
			parts = append(parts, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(ch)
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}
	return parts
}

func (s *Session) cmdSelect(args string) error {
	if s.query == nil {
		return errNoQuery
	}
	parts := splitTopLevelCommas(args)
	var projs []nodes.Node
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if p == "*" {
			projs = append(projs, nodes.Star())
			continue
		}
		if strings.HasSuffix(p, ".*") {
			tableName := strings.TrimSuffix(p, ".*")
			table := s.ensureTable(tableName)
			projs = append(projs, table.Star())
			continue
		}
		// Tokenize the part and try parsing as an arithmetic expression.
		tokens := tokenize(p)
		node, endPos, err := s.parseArithExpr(tokens, 0)
		if err != nil {
			return err
		}
		// Check for AS alias
		if endPos < len(tokens) && strings.ToLower(tokens[endPos]) == "as" {
			endPos++
			if endPos >= len(tokens) {
				return errors.New("expected alias name after AS")
			}
			aliasName := tokens[endPos]
			node = nodes.NewAliasNode(node, aliasName)
			endPos++
		}
		if endPos != len(tokens) {
			return fmt.Errorf("unexpected token %q in projection", tokens[endPos])
		}
		projs = append(projs, node)
	}
	s.query.Select(projs...)
	_, _ = fmt.Fprintf(s.out, "  Projections set (%d columns)\n", len(projs))
	return nil
}

func (s *Session) cmdDistinct() error {
	if s.query == nil {
		return errNoQuery
	}
	s.query.Distinct()
	_, _ = fmt.Fprintln(s.out, "  DISTINCT enabled")
	return nil
}

func (s *Session) cmdWhere(args string) error {
	cond, err := s.parseCondition(strings.TrimSpace(args))
	if err != nil {
		return fmt.Errorf("where: %w", err)
	}
	switch s.mode {
	case modeUpdate:
		if s.updateQuery == nil {
			return errors.New("no UPDATE query defined")
		}
		s.updateQuery.Where(cond)
	case modeDelete:
		if s.deleteQuery == nil {
			return errors.New("no DELETE query defined")
		}
		s.deleteQuery.Where(cond)
	default:
		if s.query == nil {
			return errNoQuery
		}
		s.query.Where(cond)
	}
	_, _ = fmt.Fprintln(s.out, "  WHERE condition added")
	return nil
}

func (s *Session) cmdGroup(args string) error {
	if s.query == nil {
		return errNoQuery
	}
	parts := splitTopLevelCommas(args)
	var groups []nodes.Node
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		lower := strings.ToLower(p)
		if strings.HasPrefix(lower, "cube(") || strings.HasPrefix(lower, "rollup(") ||
			strings.HasPrefix(lower, "grouping sets(") {
			node, err := s.parseGroupingSet(p)
			if err != nil {
				return err
			}
			groups = append(groups, node)
			continue
		}
		col, err := s.resolveColRef(p)
		if err != nil {
			return err
		}
		groups = append(groups, col)
	}
	s.query.Group(groups...)
	_, _ = fmt.Fprintf(s.out, "  GROUP BY set (%d columns)\n", len(groups))
	return nil
}

// parseGroupingSet parses a CUBE(...), ROLLUP(...), or GROUPING SETS(...) expression.
func (s *Session) parseGroupingSet(input string) (nodes.Node, error) {
	tokens := tokenize(input)
	if len(tokens) < 3 {
		return nil, fmt.Errorf("invalid grouping set: %s", input)
	}

	lower0 := strings.ToLower(tokens[0])

	switch lower0 {
	case "cube", "rollup":
		if tokens[1] != "(" {
			return nil, fmt.Errorf("expected ( after %s", tokens[0])
		}
		pos := 2
		var cols []nodes.Node
		for pos < len(tokens) && tokens[pos] != ")" {
			if tokens[pos] == "," {
				pos++
				continue
			}
			col, err := s.resolveColRef(tokens[pos])
			if err != nil {
				return nil, err
			}
			cols = append(cols, col)
			pos++
		}
		if lower0 == "cube" {
			return nodes.NewCube(cols...), nil
		}
		return nodes.NewRollup(cols...), nil

	case "grouping":
		if len(tokens) < 4 || strings.ToLower(tokens[1]) != "sets" || tokens[2] != "(" {
			return nil, errors.New("expected GROUPING SETS(")
		}
		pos := 3
		var sets [][]nodes.Node
		for pos < len(tokens) && tokens[pos] != ")" {
			if tokens[pos] == "," {
				pos++
				continue
			}
			if tokens[pos] == "(" {
				pos++ // skip (
				var group []nodes.Node
				for pos < len(tokens) && tokens[pos] != ")" {
					if tokens[pos] == "," {
						pos++
						continue
					}
					col, err := s.resolveColRef(tokens[pos])
					if err != nil {
						return nil, err
					}
					group = append(group, col)
					pos++
				}
				if pos < len(tokens) {
					pos++ // skip )
				}
				sets = append(sets, group)
				continue
			}
			// bare column ref (single-column group)
			col, err := s.resolveColRef(tokens[pos])
			if err != nil {
				return nil, err
			}
			sets = append(sets, []nodes.Node{col})
			pos++
		}
		return nodes.NewGroupingSets(sets...), nil
	}

	return nil, fmt.Errorf("unknown grouping set type: %s", tokens[0])
}

func (s *Session) cmdHaving(args string) error {
	if s.query == nil {
		return errNoQuery
	}
	cond, err := s.parseCondition(strings.TrimSpace(args))
	if err != nil {
		return fmt.Errorf("having: %w", err)
	}
	s.query.Having(cond)
	_, _ = fmt.Fprintln(s.out, "  HAVING condition added")
	return nil
}

func (s *Session) cmdWindow(args string) error {
	if s.query == nil {
		return errNoQuery
	}
	// Parse: name [partition by cols] [order by cols] [rows/range ...]
	tokens := tokenize(args)
	if len(tokens) == 0 {
		return errors.New("usage: window <name> [partition by <cols>] [order by <cols> [asc|desc]] [rows|range ...]")
	}

	name := tokens[0]
	def, _, err := s.parseWindowDef(tokens, 1)
	if err != nil {
		return fmt.Errorf("window: %w", err)
	}
	def.Name = name
	s.query.Window(def)
	_, _ = fmt.Fprintf(s.out, "  Window %q defined\n", name)
	return nil
}

func (s *Session) cmdOrder(args string) error {
	if s.query == nil {
		return errNoQuery
	}
	parts := strings.Split(args, ",")
	var orderings []nodes.Node
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		fields := strings.Fields(p)
		colRef := fields[0]
		col, err := s.resolveColRef(colRef)
		if err != nil {
			return err
		}
		dir := nodes.Asc
		nulls := nodes.NullsDefault
		i := 1
		if i < len(fields) {
			switch strings.ToLower(fields[i]) {
			case "desc":
				dir = nodes.Desc
				i++
			case "asc":
				i++
			}
		}
		// Parse optional NULLS FIRST/LAST
		if i < len(fields) && strings.ToLower(fields[i]) == "nulls" {
			i++
			if i < len(fields) {
				switch strings.ToLower(fields[i]) {
				case "first":
					nulls = nodes.NullsFirst
				case "last":
					nulls = nodes.NullsLast
				default:
					return fmt.Errorf("expected FIRST or LAST after NULLS, got %q", fields[i])
				}
			} else {
				return errors.New("expected FIRST or LAST after NULLS")
			}
		}
		ordering := &nodes.OrderingNode{Expr: col, Direction: dir, Nulls: nulls}
		orderings = append(orderings, ordering)
	}
	s.query.Order(orderings...)
	_, _ = fmt.Fprintf(s.out, "  ORDER BY set (%d columns)\n", len(orderings))
	return nil
}

func (s *Session) cmdLimit(args string) error {
	if s.query == nil {
		return errNoQuery
	}
	n, err := strconv.Atoi(strings.TrimSpace(args))
	if err != nil {
		return fmt.Errorf("limit requires an integer, got %q", args)
	}
	s.query.Limit(n)
	_, _ = fmt.Fprintf(s.out, "  LIMIT set to %d\n", n)
	return nil
}

func (s *Session) cmdOffset(args string) error {
	if s.query == nil {
		return errNoQuery
	}
	n, err := strconv.Atoi(strings.TrimSpace(args))
	if err != nil {
		return fmt.Errorf("offset requires an integer, got %q", args)
	}
	s.query.Offset(n)
	_, _ = fmt.Fprintf(s.out, "  OFFSET set to %d\n", n)
	return nil
}

func (s *Session) cmdJoin(args string, joinType nodes.JoinType) error {
	if s.query == nil {
		return errNoQuery
	}
	lower := strings.ToLower(args)
	onIdx := strings.Index(lower, " on ")
	if onIdx < 0 {
		return errors.New("expected: <table> on <condition>")
	}

	tableName := strings.TrimSpace(args[:onIdx])
	condStr := strings.TrimSpace(args[onIdx+4:])

	table := s.resolveTable(tableName)
	cond, err := s.parseCondition(condStr)
	if err != nil {
		return fmt.Errorf("join condition: %w", err)
	}
	s.query.Join(table, joinType).On(cond)

	_, _ = fmt.Fprintf(s.out, "  %s %q added\n", joinType, tableName)
	return nil
}

func (s *Session) cmdDistinctOn(args string) error {
	if s.query == nil {
		return errNoQuery
	}
	parts := strings.Split(args, ",")
	var cols []nodes.Node
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		col, err := s.resolveColRef(p)
		if err != nil {
			return err
		}
		cols = append(cols, col)
	}
	s.query.DistinctOn(cols...)
	_, _ = fmt.Fprintf(s.out, "  DISTINCT ON set (%d columns)\n", len(cols))
	return nil
}

func (s *Session) cmdForLock(mode nodes.LockMode) error {
	if s.query == nil {
		return errNoQuery
	}
	s.query.Core.Lock = mode
	_, _ = fmt.Fprintf(s.out, "  %s enabled\n", mode)
	return nil
}

func (s *Session) cmdSkipLocked() error {
	if s.query == nil {
		return errNoQuery
	}
	s.query.SkipLocked()
	_, _ = fmt.Fprintln(s.out, "  SKIP LOCKED enabled")
	return nil
}

func (s *Session) cmdComment(args string) error {
	if s.query == nil {
		return errNoQuery
	}
	text := strings.TrimSpace(args)
	if text == "" {
		return errors.New("usage: comment <text>")
	}
	s.query.Comment(text)
	_, _ = fmt.Fprintf(s.out, "  Comment set: %s\n", text)
	return nil
}

func (s *Session) cmdHint(args string) error {
	if s.query == nil {
		return errNoQuery
	}
	text := strings.TrimSpace(args)
	if text == "" {
		return errors.New("usage: hint <text>")
	}
	s.query.Hint(text)
	_, _ = fmt.Fprintf(s.out, "  Hint added: %s\n", text)
	return nil
}

func (s *Session) cmdLateralJoin(args string, joinType nodes.JoinType) error {
	if s.query == nil {
		return errNoQuery
	}
	lower := strings.ToLower(args)
	onIdx := strings.Index(lower, " on ")
	if onIdx < 0 {
		return errors.New("expected: <table> on <condition>")
	}

	tableName := strings.TrimSpace(args[:onIdx])
	condStr := strings.TrimSpace(args[onIdx+4:])

	table := s.resolveTable(tableName)
	cond, err := s.parseCondition(condStr)
	if err != nil {
		return fmt.Errorf("join condition: %w", err)
	}
	s.query.LateralJoin(table, joinType).On(cond)

	_, _ = fmt.Fprintf(s.out, "  LATERAL %s %q added\n", joinType, tableName)
	return nil
}

func (s *Session) cmdRawJoin(args string) error {
	if s.query == nil {
		return errNoQuery
	}
	raw := strings.TrimSpace(args)
	if raw == "" {
		return errors.New("usage: raw join <SQL text>")
	}
	s.query.StringJoin(raw)
	_, _ = fmt.Fprintln(s.out, "  String join added")
	return nil
}

func (s *Session) cmdSetOp(opType nodes.SetOpType) error {
	if s.query == nil {
		return errNoQuery
	}
	s.setOps = append(s.setOps, setOpEntry{opType: opType, query: s.query})
	s.query = nil
	_, _ = fmt.Fprintf(s.out, "  %s — start a new query with 'from <table>'\n", opType)
	return nil
}

func (s *Session) cmdWith(args string, recursive bool) error {
	if s.query == nil {
		return errNoQuery
	}
	name := strings.TrimSpace(args)
	if name == "" {
		if recursive {
			return errors.New("usage: with recursive <name>")
		}
		return errors.New("usage: with <name>")
	}
	s.ctes = append(s.ctes, cteEntry{name: name, query: s.query, recursive: recursive})
	// Register the CTE name as a table for FROM/JOIN
	s.ensureTable(name)
	s.query = nil
	kind := "CTE"
	if recursive {
		kind = "recursive CTE"
	}
	_, _ = fmt.Fprintf(s.out, "  Pushed %s %q — start a new query with 'from <table>'\n", kind, name)
	return nil
}

func (s *Session) cmdCrossJoin(args string) error {
	if s.query == nil {
		return errNoQuery
	}
	name := strings.TrimSpace(args)
	if name == "" {
		return errors.New("usage: cross join <table>")
	}
	table := s.resolveTable(name)
	s.query.CrossJoin(table)
	_, _ = fmt.Fprintf(s.out, "  CROSS JOIN %q added\n", name)
	return nil
}

// cmdSQL generates and displays the SQL for the current query, handling
// CTEs, set operations, and parameterised output.
func (s *Session) cmdSQL() error {
	// DML modes use simple SQL generation.
	if s.mode != modeSelect {
		sql, err := s.GenerateSQL()
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(s.out, "  %s;\n", sql)
		return nil
	}

	if s.query == nil {
		return errNoQuery
	}

	s.attachCTEs()
	defer s.cleanupCTEs()

	if finalNode := s.buildSetOperationChain(); finalNode != nil {
		s.printSQLNode(finalNode)
		return nil
	}

	return s.printSQLQuery()
}

// attachCTEs attaches pushed CTE entries to the current query's core.
func (s *Session) attachCTEs() {
	for _, cte := range s.ctes {
		if cte.recursive {
			s.query.WithRecursive(cte.name, cte.query.Core)
		} else {
			s.query.With(cte.name, cte.query.Core)
		}
	}
}

// cleanupCTEs removes temporarily attached CTEs from the current query.
func (s *Session) cleanupCTEs() {
	s.query.Core.CTEs = nil
}

// buildSetOperationChain chains set operations left-to-right into a single node.
// Returns nil if there are no set operations.
func (s *Session) buildSetOperationChain() nodes.Node {
	if len(s.setOps) == 0 {
		return nil
	}
	var finalNode nodes.Node = s.setOps[0].query.Core
	for i := 0; i < len(s.setOps); i++ {
		var right nodes.Node
		if i+1 < len(s.setOps) {
			right = s.setOps[i+1].query.Core
		} else {
			right = s.query.Core
		}
		finalNode = &nodes.SetOperationNode{
			Left:  finalNode,
			Right: right,
			Type:  s.setOps[i].opType,
		}
	}
	return finalNode
}

// printSQLNode renders a node to SQL and prints it, including params if enabled.
func (s *Session) printSQLNode(n nodes.Node) {
	if p, ok := s.visitor.(nodes.Parameterizer); ok && s.parameterize {
		p.Reset()
		sql := n.Accept(s.visitor)
		_, _ = fmt.Fprintf(s.out, "  %s;\n", sql)
		params := p.Params()
		if len(params) > 0 {
			_, _ = fmt.Fprintf(s.out, "  Params: %v\n", params)
		}
		return
	}
	sql := n.Accept(s.visitor)
	_, _ = fmt.Fprintf(s.out, "  %s;\n", sql)
}

// printSQLQuery generates SQL from the current query and prints it with params.
func (s *Session) printSQLQuery() error {
	if p, ok := s.visitor.(nodes.Parameterizer); ok && s.parameterize {
		p.Reset()
		sql, err := s.GenerateSQL()
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(s.out, "  %s;\n", sql)
		params := p.Params()
		if len(params) > 0 {
			_, _ = fmt.Fprintf(s.out, "  Params: %v\n", params)
		}
		return nil
	}
	sql, err := s.GenerateSQL()
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(s.out, "  %s;\n", sql)
	return nil
}

func (s *Session) cmdEngine(args string) error {
	name := strings.TrimSpace(strings.ToLower(args))
	if !isValidEngine(name) {
		return fmt.Errorf("unknown engine %q (choose: postgres, mysql, sqlite)", name)
	}
	s.setEngine(name)
	_, _ = fmt.Fprintf(s.out, "  Engine set to %s\n", s.engine)
	return nil
}

// cmdPlugin routes plugin sub-commands: enables a plugin by name, or
// dispatches to cmdPluginOff for disabling.
func (s *Session) cmdPlugin(args string) error {
	parts := strings.Fields(strings.TrimSpace(args))
	if len(parts) == 0 {
		return errors.New("usage: plugin <name> [args] | plugin off [name]")
	}
	name := strings.ToLower(parts[0])
	if name == "off" {
		return s.cmdPluginOff(parts[1:])
	}
	for _, c := range s.configurers {
		if c.name == name {
			return c.configure(s, strings.TrimSpace(args[len(parts[0]):]))
		}
	}
	return fmt.Errorf("unknown plugin: %s", name)
}

func (s *Session) cmdPluginOff(parts []string) error {
	if len(parts) == 0 {
		s.plugins.deregisterAll()
		s.opaConfig = nil
		_, _ = fmt.Fprintln(s.out, "  All plugins disabled")
	} else {
		name := strings.ToLower(parts[0])
		if !s.plugins.deregister(name) {
			return fmt.Errorf("plugin %q is not enabled", name)
		}
		if name == "opa" {
			s.opaConfig = nil
		}
		_, _ = fmt.Fprintf(s.out, "  %s disabled\n", name)
	}
	s.rebuildQueryWithPlugins()
	return nil
}

func (s *Session) cmdPlugins() {
	_, _ = fmt.Fprintln(s.out, "  Available plugins:")
	for _, c := range s.configurers {
		if entry, ok := s.plugins.get(c.name); ok {
			_, _ = fmt.Fprintf(s.out, "    %-14s on   (%s)\n", c.name, entry.status())
		} else {
			_, _ = fmt.Fprintf(s.out, "    %-14s off\n", c.name)
		}
	}
}

func (s *Session) cmdParameterize() error {
	s.parameterize = !s.parameterize
	s.setEngine(s.engine) // recreate visitor with/without parameterization
	if s.parameterize {
		_, _ = fmt.Fprintln(s.out, "  Parameterized queries enabled")
	} else {
		_, _ = fmt.Fprintln(s.out, "  Parameterized queries disabled")
	}
	return nil
}

// cmdExpr evaluates a standalone SQL expression without requiring a full query.
func (s *Session) cmdExpr(args string) error {
	node, err := s.parseExpression(args)
	if err != nil {
		return fmt.Errorf("expr: %w", err)
	}

	if p, ok := s.visitor.(nodes.Parameterizer); ok && s.parameterize {
		p.Reset()
		sql := node.Accept(s.visitor)
		_, _ = fmt.Fprintf(s.out, "  %s\n", sql)
		params := p.Params()
		if len(params) > 0 {
			_, _ = fmt.Fprintf(s.out, "  Params: %v\n", params)
		}
		return nil
	}

	sql := node.Accept(s.visitor)
	_, _ = fmt.Fprintf(s.out, "  %s\n", sql)
	return nil
}

func (s *Session) makeParamVisitor() nodes.Visitor {
	opts := []visitors.Option{visitors.WithParams()}
	switch s.engine {
	case "mysql":
		return visitors.NewMySQLVisitor(opts...)
	case "sqlite":
		return visitors.NewSQLiteVisitor(opts...)
	default:
		return visitors.NewPostgresVisitor(opts...)
	}
}

func (s *Session) cmdConnect(args string) error {
	dsn := strings.TrimSpace(args)

	if s.conn != nil {
		return fmt.Errorf("already connected to %s (use 'disconnect' first)", sanitizeDSN(s.conn.dsn))
	}

	// Direct DSN provided — connect immediately.
	if dsn != "" {
		return s.connectWithDSN(dsn)
	}

	// Interactive: offer reconnect if we have a previous DSN, otherwise wizard.
	if s.lastDSN != "" {
		choice := prompt(s.rl, fmt.Sprintf("Reconnect to %s? (y/n/setup)", sanitizeDSN(s.lastDSN)), "y")
		switch strings.ToLower(choice) {
		case "y", "yes":
			return s.connectWithDSN(s.lastDSN)
		case "s", "setup":
			return s.connectViaWizard()
		default:
			_, _ = fmt.Fprintln(s.out, "  Connect cancelled")
			return nil
		}
	}

	return s.connectViaWizard()
}

func (s *Session) connectWithDSN(dsn string) error {
	conn, err := connect(s.engine, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	s.conn = conn
	s.lastDSN = dsn
	_, _ = fmt.Fprintf(s.out, "  Connected to %s (%s)\n", sanitizeDSN(dsn), s.engine)
	return nil
}

func (s *Session) connectViaWizard() error {
	var dsn string
	switch s.engine {
	case "sqlite":
		dsn = buildSQLiteDSN(s.rl)
	case "mysql":
		dsn = buildMySQLDSN(s.rl)
	default:
		dsn = buildPostgresDSN(s.rl)
	}

	if dsn == "" {
		_, _ = fmt.Fprintln(s.out, "  No connection configured")
		return nil
	}

	_, _ = fmt.Fprintf(s.out, "  DSN: %s\n", sanitizeDSN(dsn))
	return s.connectWithDSN(dsn)
}

func (s *Session) cmdDisconnect() error {
	if s.conn == nil {
		return errors.New("not connected")
	}
	dsn := sanitizeDSN(s.conn.dsn)
	if err := s.conn.close(); err != nil {
		return fmt.Errorf("disconnect: %w", err)
	}
	s.conn = nil
	_, _ = fmt.Fprintf(s.out, "  Disconnected from %s\n", dsn)
	return nil
}

// cmdExec executes the current query against the connected database,
// always using parameterised queries for safety.
func (s *Session) cmdExec() error {
	if s.conn == nil {
		return errors.New("not connected (use 'connect <dsn>' first)")
	}

	if s.conn.engine != s.engine {
		_, _ = fmt.Fprintf(s.out, "  Warning: connected to %s but engine is set to %s\n", s.conn.engine, s.engine)
	}

	pv := s.makeParamVisitor()
	var sqlStr string
	var params []any
	var err error

	switch s.mode {
	case modeInsert:
		if s.insertQuery == nil {
			return errors.New("no INSERT query defined")
		}
		sqlStr, params, err = s.insertQuery.ToSQLParams(pv)
	case modeUpdate:
		if s.updateQuery == nil {
			return errors.New("no UPDATE query defined")
		}
		sqlStr, params, err = s.updateQuery.ToSQLParams(pv)
	case modeDelete:
		if s.deleteQuery == nil {
			return errors.New("no DELETE query defined")
		}
		sqlStr, params, err = s.deleteQuery.ToSQLParams(pv)
	default:
		if s.query == nil {
			return errNoQuery
		}
		sqlStr, params, err = s.query.ToSQLParams(pv)
	}
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(s.out, "  %s;\n", sqlStr)
	if len(params) > 0 {
		_, _ = fmt.Fprintf(s.out, "  Params: %v\n", params)
	}

	result, err := s.conn.execQuery(sqlStr, params)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprint(s.out, result)
	return nil
}

// cmdAST displays a summary of the current query's abstract syntax tree,
// showing each clause (FROM, SELECT, WHERE, etc.) and its contents.
func (s *Session) cmdAST() error {
	switch s.mode {
	case modeInsert:
		return s.cmdASTInsert()
	case modeUpdate:
		return s.cmdASTUpdate()
	case modeDelete:
		return s.cmdASTDelete()
	}

	if s.query == nil {
		return errNoQuery
	}
	c := s.query.Core

	_, _ = fmt.Fprintf(s.out, "  Engine: %s\n", s.engine)
	s.printASTMetadata(c)
	s.printASTFrom(c)
	s.printASTDistinct(c)
	s.printASTProjections(c)
	s.printASTJoins(c)
	s.printASTWheres(c)
	s.printASTGroups(c)
	s.printASTHavings(c)
	s.printASTWindows(c)
	s.printASTOrders(c)
	s.printASTLimitOffset(c)
	s.printASTLock(c)
	s.printASTFooter()
	return nil
}

// cmdDot exports the current query AST as a Graphviz DOT file, applying
// plugins with provenance tracking for colour-coded visualisation.
func (s *Session) cmdDot(args string) error {
	fpath := strings.TrimSpace(args)
	if fpath == "" {
		return fmt.Errorf("usage: dot <filepath>")
	}
	if s.query == nil {
		return errNoQuery
	}

	// Clone and apply plugins incrementally for provenance.
	core := s.query.CloneCore()
	prov := visitors.NewPluginProvenance()
	for _, entry := range s.plugins.entries {
		prevWheres := len(core.Wheres)
		prevJoins := len(core.Joins)
		t := entry.factory()
		var err error
		core, err = t.TransformSelect(core)
		if err != nil {
			return err
		}
		for i := prevWheres; i < len(core.Wheres); i++ {
			prov.AddWhere(entry.name, entry.color, i)
		}
		for i := prevJoins; i < len(core.Joins); i++ {
			prov.AddJoin(entry.name, entry.color, i)
		}
	}

	dv := visitors.NewDotVisitor()
	dv.SetProvenance(prov)
	core.Accept(dv)

	if err := os.WriteFile(fpath, []byte(dv.ToDot()), 0600); err != nil {
		return fmt.Errorf("failed to write DOT file: %w", err)
	}
	_, _ = fmt.Fprintf(s.out, "  Wrote DOT to %s\n", fpath)
	return nil
}

func (s *Session) cmdReset() error {
	s.setMode(modeSelect)
	s.setOps = nil
	s.ctes = nil
	_, _ = fmt.Fprintln(s.out, "  Query cleared")
	return nil
}

func (s *Session) cmdTables() error {
	if len(s.tables) == 0 && len(s.aliases) == 0 {
		_, _ = fmt.Fprintln(s.out, "  No tables registered")
		return nil
	}
	for name := range s.tables {
		_, _ = fmt.Fprintf(s.out, "  table: %s\n", name)
	}
	for name, a := range s.aliases {
		_, _ = fmt.Fprintf(s.out, "  alias: %s -> %s\n", name, tableAliasSourceName(a))
	}
	return nil
}

func (s *Session) cmdHelp() {
	_, _ = fmt.Fprintln(s.out, `
  Query Building:
    from <table>              Start a new query (sets FROM)
    select <cols>             Set projections (table.col, *, table.*)
    project <cols>            Alias for select
    distinct                  Enable DISTINCT modifier
    distinct on <cols>        Enable DISTINCT ON (PostgreSQL, comma-separated)
    where <condition>         Add a WHERE condition
    group <table.col,...>     Add GROUP BY (comma-separated)
    group cube(col,...)       GROUP BY with CUBE
    group rollup(col,...)     GROUP BY with ROLLUP
    group grouping sets(...)  GROUP BY with GROUPING SETS
    having <condition>        Add a HAVING condition
    order <col> [asc|desc] [nulls first|last]  Add ORDER BY
    limit <n>                 Set LIMIT
    offset <n>                Set OFFSET
    take <n>                  Alias for limit
    comment <text>            Add a SQL comment (/* text */)
    hint <text>               Add an optimizer hint (/*+ text */)

  INSERT Builder:
    insert into <table>       Start an INSERT statement
    columns <col1>, <col2>    Set column list
    values <val1>, <val2>     Add a row of values (repeatable)
    on conflict (<cols>) do nothing    UPSERT: DO NOTHING
    on conflict (<cols>) do update set <col> = <val>   UPSERT: DO UPDATE
    returning <cols>          Set RETURNING clause

  UPDATE Builder:
    update <table>            Start an UPDATE statement
    set <col> = <val>         Add a SET assignment (repeatable)
    where <condition>         Add WHERE (shared with SELECT)
    returning <cols>          Set RETURNING clause

  DELETE Builder:
    delete from <table>       Start a DELETE statement
    where <condition>         Add WHERE (shared with SELECT)
    returning <cols>          Set RETURNING clause

  Joins:
    join <t> on <cond>        Add an INNER JOIN
    left join <t> on <cond>   Add a LEFT OUTER JOIN
    right join <t> on <cond>  Add a RIGHT OUTER JOIN
    full join <t> on <cond>   Add a FULL OUTER JOIN
    cross join <table>        Add a CROSS JOIN
    lateral join <t> on <c>   Add a LATERAL INNER JOIN (PostgreSQL)
    lateral left join <t> on <c>  Add a LATERAL LEFT JOIN (PostgreSQL)
    raw join <SQL>            Add a raw SQL join fragment

  Locking:
    for update                Add FOR UPDATE clause
    for share                 Add FOR SHARE clause
    for no key update         Add FOR NO KEY UPDATE clause
    for key share             Add FOR KEY SHARE clause
    skip locked               Add SKIP LOCKED modifier

  Set Operations:
    union                     Push current query, start UNION
    union all                 Push current query, start UNION ALL
    intersect                 Push current query, start INTERSECT
    intersect all             Push current query, start INTERSECT ALL
    except                    Push current query, start EXCEPT
    except all                Push current query, start EXCEPT ALL

  CTEs (Common Table Expressions):
    with <name>               Push current query as CTE, start new query
    with recursive <name>     Push current query as recursive CTE

  Tables:
    table <name>              Register a table
    alias <table> <name>      Create a table alias
    tables                    List registered tables

  Output:
    sql                       Generate and display SQL
    ast                       Show AST summary
    dot <filepath>            Export AST as Graphviz DOT file
    expr <expression>         Evaluate a standalone expression
    exec                      Execute query against connected DB (alias: run)

  Configuration:
    engine <name>             Switch dialect (postgres, mysql, sqlite)
    parameterize              Toggle parameterized queries (alias: params)
    connect [dsn]             Connect (setup wizard, reconnect, or provide DSN)
    disconnect                Close database connection

  Plugins — Soft Delete:
    plugin softdelete [col]                Enable soft-delete (default: deleted_at)
    plugin softdelete <col> on <tables..>  Soft-delete for specific tables
    plugin softdelete <t.col, ...>         Per-table soft-delete columns

  Plugins — OPA:
    opa                       OPA setup wizard
    opa status                Show OPA configuration
    opa off                   Disable OPA plugin
    opa reload                Rebuild plugin with current config
    opa inputs                Re-discover and set input values
    opa explain <table>       Show how OPA translates to SQL conditions
    opa explain <table> verbose  Show with raw OPA response and translation trace
    opa conditions            Show OPA-injected conditions on current query
    opa masks                 Show column masks for current query tables
    opa url <url>             Change OPA server URL
    opa policy <path>         Change OPA policy path
    opa input <key> <value>   Set/update a single input value
    opa input <key>           Remove an input value

  Plugins — General:
    plugins                   List available plugins and status
    plugin off [name]         Disable one or all plugins

  Session:
    edit                      Edit or remove query clauses interactively
    edit <clause>             Edit specific clause (select, where, join, order, group, having, window)
    reset                     Clear the current query
    help                      Show this help
    exit / quit               Exit the REPL

  DSN formats:
    postgres: postgres://user:pass@host:5432/dbname?sslmode=disable
    mysql:    user:pass@tcp(host:3306)/dbname
    sqlite:   path/to/file.db  or  :memory:

  Condition syntax:
    table.col = value         Equality (strings: 'text', nums: 42, bools: true/false)
    table.col != value        Not equal
    table.col > value         Greater than  (also >=, <, <=)
    table.col like 'pattern'  LIKE / NOT LIKE
    table.col is null         IS NULL / IS NOT NULL
    table.col in (1, 2, 3)   IN / NOT IN
    table.col between 1 and 5 BETWEEN / NOT BETWEEN
    table.col regexp 'pat'    REGEXP / NOT REGEXP
    table.col is distinct from value     IS DISTINCT FROM
    table.col is not distinct from value IS NOT DISTINCT FROM
    table.col @> '{1,2}'      Contains (PostgreSQL)
    table.col && '{1,2}'      Overlaps (PostgreSQL)

  Named functions (usable in select, where, having, expr):
    COALESCE(expr, expr, ...)            First non-null value
    LOWER(expr)                          Lowercase
    UPPER(expr)                          Uppercase
    SUBSTRING(expr, start, len)          Substring
    CAST(expr AS type)                   Type cast
    GREATEST(expr, ...)                  Largest value
    LEAST(expr, ...)                     Smallest value
    NULLIF(expr, expr)                   Return NULL if equal
    ABS(expr), LENGTH(expr)              Math/string functions
    TRIM(expr), REPLACE(expr, old, new)  String functions
    CONCAT(expr, ...)                    Concatenation
    Any NAME(args...) pattern            Arbitrary function calls

  CASE expressions (usable in select, where, having, expr):
    CASE WHEN cond THEN result ... [ELSE result] END   Searched CASE
    CASE expr WHEN val THEN result ... [ELSE result] END  Simple CASE

  Column aliasing (in select):
    select table.col AS alias_name       Alias a column or expression

  Aggregate functions (usable in select, where, having, expr):
    COUNT(*)                  Count all rows
    COUNT(table.col)          Count non-null values
    COUNT(DISTINCT table.col) Count distinct values
    SUM(table.col)            Sum of values
    AVG(table.col)            Average of values
    MIN(table.col)            Minimum value
    MAX(table.col)            Maximum value
    SUM(t.col) FILTER (WHERE t.status = 'active')   Filtered aggregate
    EXTRACT(YEAR FROM table.col)   Extract date/time part
    EXTRACT fields: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND,
                    DOW, DOY, EPOCH, QUARTER, WEEK

  Window functions (usable in select):
    ROW_NUMBER() OVER (...)             Row number
    RANK() OVER (...)                   Rank with gaps
    DENSE_RANK() OVER (...)             Rank without gaps
    NTILE(n) OVER (...)                 Divide into n buckets
    LAG(col [, offset [, default]]) OVER (...)   Previous row value
    LEAD(col [, offset [, default]]) OVER (...)  Next row value
    FIRST_VALUE(col) OVER (...)         First value in frame
    LAST_VALUE(col) OVER (...)          Last value in frame
    NTH_VALUE(col, n) OVER (...)        Nth value in frame
    CUME_DIST() OVER (...)              Cumulative distribution
    PERCENT_RANK() OVER (...)           Percent rank
    SUM(col) OVER (...)                 Aggregate as window function

  OVER clause syntax:
    OVER ()                                       Empty window
    OVER (PARTITION BY table.col)                  Partition by column
    OVER (ORDER BY table.col [ASC|DESC])           Order by column
    OVER (PARTITION BY t.a ORDER BY t.b DESC)      Both
    OVER (ORDER BY t.col ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    OVER (ORDER BY t.col RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
    OVER w                                         Named window reference

  Window command:
    window <name> [partition by cols] [order by cols] [rows/range ...]

  Arithmetic operators (usable in where, having, expr, select):
    +   addition              table.col + 5
    -   subtraction           table.col - 1
    *   multiplication        table.col * 100
    /   division              table.col / 2
    &   bitwise AND           table.flags & 255
    |   bitwise OR            table.flags | 4
    ^   bitwise XOR           table.flags ^ 255
    ~   bitwise NOT (unary)   ~table.flags
    <<  shift left            table.col << 2
    >>  shift right           table.col >> 2
    ||  concatenation         table.first || ' ' || table.last

  Expression syntax (for expr and where):
    table.col = value         Equality
    table.col > value         Comparison (also >=, <, <=, !=)
    table.col like 'pattern'  LIKE / NOT LIKE
    table.col is null         IS NULL / IS NOT NULL
    table.col in (1, 2, 3)    IN / NOT IN
    table.col between 1 and 5 BETWEEN / NOT BETWEEN
    table.col regexp 'pat'    REGEXP / NOT REGEXP
    table.col is distinct from value  IS DISTINCT FROM
    table.col @> '{1,2}'      Contains (PostgreSQL)
    table.col && '{1,2}'      Overlaps (PostgreSQL)
    EXISTS(from <table> ...)  EXISTS subquery (via set operation syntax)
    NOT EXISTS(...)           NOT EXISTS subquery
    <cond> and <cond>         Logical AND
    <cond> or <cond>          Logical OR (lower precedence than AND)
    not <cond>                Logical NOT

  Examples:
    table users
    table posts
    from users
    select users.id, users.name
    where users.active = true
    where users.age + 5 > 18
    select users.price * users.qty, users.name
    join posts on users.id = posts.user_id
    order users.name desc nulls last
    for update
    sql
    engine mysql
    sql
    parameterize
    sql
    expr users.age > 18 and users.active = true
    expr users.name = 'alice' or users.name = 'carol'
    expr users.first || ' ' || users.last = 'John Doe'

  Set operation example:
    from users                -- first query
    where users.active = true
    union all                 -- push and start second query
    from users
    where users.role = 'admin'
    sql                       -- generates UNION ALL

  CTE example:
    from users                -- CTE query
    where users.active = true
    with active_users         -- push as CTE
    from active_users         -- main query using CTE
    sql                       -- generates WITH ... AS (...) SELECT ...

  Readline:
    Tab             Auto-complete commands, tables, columns
    Up/Down         Navigate command history
    Ctrl+A/E        Move to start/end of line
    Ctrl+R          Reverse history search
    Ctrl+C          Cancel current line`)
}

// --- Helpers ---

// setMode switches the DML mode and clears all query builders.

// --- Edit support ---

var editClauseOrder = []string{"select", "where", "join", "order", "group", "having", "window"}
var editClauseLabels = map[string]string{
	"select": "SELECT",
	"where":  "WHERE",
	"join":   "JOIN",
	"order":  "ORDER BY",
	"group":  "GROUP BY",
	"having": "HAVING",
	"window": "WINDOW",
}
var validEditClauses = map[string]bool{
	"select": true, "where": true, "join": true,
	"order": true, "group": true, "having": true,
	"window": true,
}

type editEntry struct {
	clauseType string // "select", "where", "join", "order", "group", "having", "window"
	index      int    // index into the corresponding Core slice
	display    string // rendered SQL fragment
}

func (s *Session) buildEditEntries(filter string) []editEntry {
	if s.query == nil {
		return nil
	}
	core := s.query.Core
	v := s.visitor
	var entries []editEntry

	if filter == "" || filter == "select" {
		for i, p := range core.Projections {
			entries = append(entries, editEntry{clauseType: "select", index: i, display: p.Accept(v)})
		}
	}
	if filter == "" || filter == "where" {
		for i, w := range core.Wheres {
			entries = append(entries, editEntry{clauseType: "where", index: i, display: w.Accept(v)})
		}
	}
	if filter == "" || filter == "join" {
		for i, j := range core.Joins {
			label := j.Type.String() + " " + j.Right.Accept(v)
			if j.On != nil {
				label += " ON " + j.On.Accept(v)
			}
			entries = append(entries, editEntry{clauseType: "join", index: i, display: label})
		}
	}
	if filter == "" || filter == "order" {
		for i, o := range core.Orders {
			entries = append(entries, editEntry{clauseType: "order", index: i, display: o.Accept(v)})
		}
	}
	if filter == "" || filter == "group" {
		for i, g := range core.Groups {
			entries = append(entries, editEntry{clauseType: "group", index: i, display: g.Accept(v)})
		}
	}
	if filter == "" || filter == "having" {
		for i, h := range core.Havings {
			entries = append(entries, editEntry{clauseType: "having", index: i, display: h.Accept(v)})
		}
	}
	if filter == "" || filter == "window" {
		for i, w := range core.Windows {
			entries = append(entries, editEntry{clauseType: "window", index: i, display: w.Name})
		}
	}
	return entries
}

func (s *Session) removeEntry(entry editEntry) {
	core := s.query.Core
	switch entry.clauseType {
	case "select":
		core.Projections = append(core.Projections[:entry.index], core.Projections[entry.index+1:]...)
	case "where":
		core.Wheres = append(core.Wheres[:entry.index], core.Wheres[entry.index+1:]...)
	case "join":
		core.Joins = append(core.Joins[:entry.index], core.Joins[entry.index+1:]...)
	case "order":
		core.Orders = append(core.Orders[:entry.index], core.Orders[entry.index+1:]...)
	case "group":
		core.Groups = append(core.Groups[:entry.index], core.Groups[entry.index+1:]...)
	case "having":
		core.Havings = append(core.Havings[:entry.index], core.Havings[entry.index+1:]...)
	case "window":
		core.Windows = append(core.Windows[:entry.index], core.Windows[entry.index+1:]...)
	}
}

func (s *Session) editEntryValue(entry editEntry, newValue string) error {
	core := s.query.Core
	switch entry.clauseType {
	case "select":
		parts := strings.Split(newValue, ",")
		var projs []nodes.Node
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if p == "*" {
				projs = append(projs, nodes.Star())
				continue
			}
			if strings.HasSuffix(p, ".*") {
				tableName := strings.TrimSuffix(p, ".*")
				table := s.ensureTable(tableName)
				projs = append(projs, table.Star())
				continue
			}
			col, err := s.resolveColRef(p)
			if err != nil {
				return err
			}
			projs = append(projs, col)
		}
		idx := entry.index
		tail := append([]nodes.Node{}, core.Projections[idx+1:]...)
		core.Projections = append(core.Projections[:idx], projs...)
		core.Projections = append(core.Projections, tail...)

	case "where":
		cond, err := s.parseCondition(strings.TrimSpace(newValue))
		if err != nil {
			return fmt.Errorf("where: %w", err)
		}
		core.Wheres[entry.index] = cond

	case "having":
		cond, err := s.parseCondition(strings.TrimSpace(newValue))
		if err != nil {
			return fmt.Errorf("having: %w", err)
		}
		core.Havings[entry.index] = cond

	case "join":
		lower := strings.ToLower(newValue)
		onIdx := strings.Index(lower, " on ")
		if onIdx < 0 {
			return errors.New("expected: <table> on <condition>")
		}
		tableName := strings.TrimSpace(newValue[:onIdx])
		condStr := strings.TrimSpace(newValue[onIdx+4:])
		table := s.resolveTable(tableName)
		cond, err := s.parseCondition(condStr)
		if err != nil {
			return fmt.Errorf("join condition: %w", err)
		}
		j := core.Joins[entry.index]
		j.Right = table
		j.On = cond

	case "order":
		fields := strings.Fields(newValue)
		if len(fields) == 0 {
			return errors.New("expected: table.col [asc|desc]")
		}
		col, err := s.resolveColRef(fields[0])
		if err != nil {
			return err
		}
		dir := nodes.Asc
		if len(fields) > 1 {
			switch strings.ToLower(fields[1]) {
			case "desc":
				dir = nodes.Desc
			case "asc":
				// default
			default:
				return fmt.Errorf("expected ASC or DESC, got %q", fields[1])
			}
		}
		core.Orders[entry.index] = &nodes.OrderingNode{Expr: col, Direction: dir}

	case "group":
		col, err := s.resolveColRef(strings.TrimSpace(newValue))
		if err != nil {
			return err
		}
		core.Groups[entry.index] = col
	}
	return nil
}

func displayEditEntries(w io.Writer, entries []editEntry, filtered bool) {
	if len(entries) == 0 {
		return
	}
	if filtered {
		for i, e := range entries {
			if i == 0 {
				_, _ = fmt.Fprintf(w, "    %s:\n", editClauseLabels[e.clauseType])
			}
			_, _ = fmt.Fprintf(w, "      [%d] %s\n", i+1, e.display)
		}
		return
	}
	// Unfiltered: interleave entries and empty clauses in canonical order.
	byType := map[string][]editEntry{}
	for _, e := range entries {
		byType[e.clauseType] = append(byType[e.clauseType], e)
	}
	num := 1
	for _, ct := range editClauseOrder {
		_, _ = fmt.Fprintf(w, "    %s:\n", editClauseLabels[ct])
		if group, ok := byType[ct]; ok {
			for _, e := range group {
				_, _ = fmt.Fprintf(w, "      [%d] %s\n", num, e.display)
				num++
			}
		} else {
			_, _ = fmt.Fprintln(w, "      (empty)")
		}
	}
}

func editPromptLabel(clauseType string) string {
	switch clauseType {
	case "select":
		return "New projection"
	case "where":
		return "New condition"
	case "join":
		return "New join (<table> on <condition>)"
	case "order":
		return "New ordering (table.col [asc|desc])"
	case "group":
		return "New grouping (table.col)"
	case "having":
		return "New condition"
	default:
		return "New value"
	}
}

// cmdEdit provides interactive editing of individual query clauses,
// allowing removal or replacement of specific projections, conditions, joins, etc.
func (s *Session) cmdEdit(args string) error {
	if s.query == nil {
		return errNoQuery
	}

	filter := strings.TrimSpace(strings.ToLower(args))
	if filter != "" && !validEditClauses[filter] {
		return fmt.Errorf("unknown clause %q (choose: select, where, join, order, group, having, window)", filter)
	}

	if s.rl == nil {
		return errors.New("edit requires an interactive session")
	}

	entries := s.buildEditEntries(filter)
	if len(entries) == 0 {
		if filter != "" {
			_, _ = fmt.Fprintf(s.out, "  %s:\n    (empty)\n", editClauseLabels[filter])
		} else {
			_, _ = fmt.Fprintln(s.out, "  Nothing to edit")
		}
		return nil
	}

	for {
		_, _ = fmt.Fprintln(s.out, "  Editable clauses:")
		displayEditEntries(s.out, entries, filter != "")
		_, _ = fmt.Fprintln(s.out)
		action := prompt(s.rl, "Action (remove <n>, edit <n>, or cancel)", "cancel")
		action = strings.TrimSpace(strings.ToLower(action))

		if action == "cancel" || action == "c" || action == "" {
			break
		}

		parts := strings.Fields(action)
		if len(parts) != 2 {
			_, _ = fmt.Fprintln(s.out, "  Usage: remove <n> or edit <n>")
			continue
		}

		verb := parts[0]
		num, err := strconv.Atoi(parts[1])
		if err != nil || num < 1 || num > len(entries) {
			_, _ = fmt.Fprintf(s.out, "  Invalid entry number (1-%d)\n", len(entries))
			continue
		}
		idx := num - 1

		switch verb {
		case "remove", "rm", "delete", "del":
			entry := entries[idx]
			s.removeEntry(entry)
			_, _ = fmt.Fprintf(s.out, "  Removed %s [%d]\n", editClauseLabels[entry.clauseType], num)

		case "edit", "e":
			entry := entries[idx]
			label := editPromptLabel(entry.clauseType)
			for {
				newVal := prompt(s.rl, label, entry.display)
				if newVal == entry.display {
					_, _ = fmt.Fprintln(s.out, "  (unchanged)")
					break
				}
				if err := s.editEntryValue(entry, newVal); err != nil {
					_, _ = fmt.Fprintf(s.out, "  Error: %v\n", err)
					continue
				}
				_, _ = fmt.Fprintf(s.out, "  Updated %s [%d]\n", editClauseLabels[entry.clauseType], num)
				break
			}

		default:
			_, _ = fmt.Fprintln(s.out, "  Usage: remove <n> or edit <n>")
			continue
		}

		// Rebuild entries after modification.
		entries = s.buildEditEntries(filter)
		if len(entries) == 0 {
			_, _ = fmt.Fprintln(s.out, "  No more entries")
			break
		}
	}
	return nil
}
