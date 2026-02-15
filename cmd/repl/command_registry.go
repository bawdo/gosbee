package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/bawdo/gosbee/nodes"
)

// commandEntry maps a REPL prefix to its handler and optional tab-completer.
type commandEntry struct {
	prefix    string
	handler   func(args string) error
	completer func(args string) (completionContext, string) // nil = no arg completion
	hidden    bool                                          // excluded from commandNames()
}

// initCommands builds the command registry and sorts by prefix length descending.
func (s *Session) initCommands() {
	s.commands = []commandEntry{
		// --- no-arg / display commands ---
		{prefix: "sql", handler: func(_ string) error { return s.cmdSQL() }},
		{prefix: "tosql", handler: func(_ string) error { return s.cmdSQL() }},
		{prefix: "ast", handler: func(_ string) error { return s.cmdAST() }},
		{prefix: "dot ", handler: func(a string) error { return s.cmdDot(a) }},
		{prefix: "dot", handler: func(_ string) error { return fmt.Errorf("usage: dot <filepath>") }},
		{prefix: "reset", handler: func(_ string) error { return s.cmdReset() }},
		{prefix: "tables", handler: func(_ string) error { return s.cmdTables() }},
		{prefix: "help", handler: func(_ string) error { s.cmdHelp(); return nil }},

		// --- distinct / locking ---
		{prefix: "distinct on ", handler: func(a string) error { return s.cmdDistinctOn(a) }},
		{prefix: "distinct", handler: func(_ string) error { return s.cmdDistinct() }},
		{prefix: "for no key update", handler: func(_ string) error { return s.cmdForLock(nodes.ForNoKeyUpdate) }},
		{prefix: "for key share", handler: func(_ string) error { return s.cmdForLock(nodes.ForKeyShare) }},
		{prefix: "for update", handler: func(_ string) error { return s.cmdForLock(nodes.ForUpdate) }},
		{prefix: "for share", handler: func(_ string) error { return s.cmdForLock(nodes.ForShare) }},
		{prefix: "skip locked", handler: func(_ string) error { return s.cmdSkipLocked() }},

		// --- comment / hint ---
		{prefix: "comment ", handler: func(a string) error { return s.cmdComment(a) }},
		{prefix: "hint ", handler: func(a string) error { return s.cmdHint(a) }},

		// --- edit ---
		{prefix: "edit ", handler: func(a string) error { return s.cmdEdit(a) }, completer: completeEditArgs},
		{prefix: "edit", handler: func(_ string) error { return s.cmdEdit("") }},

		// --- table registration ---
		{prefix: "table ", handler: func(a string) error { return s.cmdTable(a) }},
		{prefix: "t ", handler: func(a string) error { return s.cmdTable(a) }, hidden: true},
		{prefix: "alias ", handler: func(a string) error { return s.cmdAlias(a) }, completer: completeAliasArgs},

		// --- query building ---
		{prefix: "from ", handler: func(a string) error { return s.cmdFrom(a) }, completer: completeTableArgs},
		{prefix: "select ", handler: func(a string) error { return s.cmdSelect(a) }, completer: completeColumnArgs},
		{prefix: "project ", handler: func(a string) error { return s.cmdSelect(a) }, completer: completeColumnArgs},
		{prefix: "group ", handler: func(a string) error { return s.cmdGroup(a) }, completer: completeColumnArgs},
		{prefix: "having ", handler: func(a string) error { return s.cmdHaving(a) }, completer: completeColumnArgs},
		{prefix: "order ", handler: func(a string) error { return s.cmdOrder(a) }, completer: completeOrderArgs},
		{prefix: "limit ", handler: func(a string) error { return s.cmdLimit(a) }},
		{prefix: "offset ", handler: func(a string) error { return s.cmdOffset(a) }},
		{prefix: "take ", handler: func(a string) error { return s.cmdLimit(a) }},
		{prefix: "where ", handler: func(a string) error { return s.cmdWhere(a) }, completer: completeColumnArgs},
		{prefix: "window ", handler: func(a string) error { return s.cmdWindow(a) }, completer: completeWindowArgs},

		// --- joins (multi-word prefixes) ---
		{prefix: "lateral left join ", handler: func(a string) error { return s.cmdLateralJoin(a, nodes.LeftOuterJoin) }, completer: completeJoinArgs},
		{prefix: "lateral join ", handler: func(a string) error { return s.cmdLateralJoin(a, nodes.InnerJoin) }, completer: completeJoinArgs},
		{prefix: "outer join ", handler: func(a string) error { return s.cmdJoin(a, nodes.LeftOuterJoin) }, completer: completeJoinArgs, hidden: true},
		{prefix: "right join ", handler: func(a string) error { return s.cmdJoin(a, nodes.RightOuterJoin) }, completer: completeJoinArgs},
		{prefix: "cross join ", handler: func(a string) error { return s.cmdCrossJoin(a) }, completer: completeJoinArgs},
		{prefix: "left join ", handler: func(a string) error { return s.cmdJoin(a, nodes.LeftOuterJoin) }, completer: completeJoinArgs},
		{prefix: "full join ", handler: func(a string) error { return s.cmdJoin(a, nodes.FullOuterJoin) }, completer: completeJoinArgs},
		{prefix: "raw join ", handler: func(a string) error { return s.cmdRawJoin(a) }},
		{prefix: "join ", handler: func(a string) error { return s.cmdJoin(a, nodes.InnerJoin) }, completer: completeTableArgs},

		// --- set operations ---
		{prefix: "union all", handler: func(_ string) error { return s.cmdSetOp(nodes.UnionAll) }},
		{prefix: "intersect all", handler: func(_ string) error { return s.cmdSetOp(nodes.IntersectAll) }},
		{prefix: "except all", handler: func(_ string) error { return s.cmdSetOp(nodes.ExceptAll) }},
		{prefix: "union", handler: func(_ string) error { return s.cmdSetOp(nodes.Union) }},
		{prefix: "intersect", handler: func(_ string) error { return s.cmdSetOp(nodes.Intersect) }},
		{prefix: "except", handler: func(_ string) error { return s.cmdSetOp(nodes.Except) }},

		// --- CTEs ---
		{prefix: "with recursive ", handler: func(a string) error { return s.cmdWith(a, true) }},
		{prefix: "with ", handler: func(a string) error { return s.cmdWith(a, false) }},

		// --- DML builders ---
		{prefix: "insert into ", handler: func(a string) error { return s.cmdInsertInto(a) }, completer: completeTableArgs},
		{prefix: "delete from ", handler: func(a string) error { return s.cmdDeleteFrom(a) }, completer: completeTableArgs},
		{prefix: "on conflict ", handler: func(a string) error { return s.cmdOnConflict(a) }},
		{prefix: "returning ", handler: func(a string) error { return s.cmdReturning(a) }, completer: completeColumnArgs},
		{prefix: "columns ", handler: func(a string) error { return s.cmdColumns(a) }, completer: completeColumnArgs},
		{prefix: "values ", handler: func(a string) error { return s.cmdValues(a) }},
		{prefix: "update ", handler: func(a string) error { return s.cmdUpdate(a) }, completer: completeTableArgs},
		{prefix: "set ", handler: func(a string) error { return s.cmdSet(a) }, completer: completeColumnArgs},

		// --- database connectivity ---
		{prefix: "connect ", handler: func(a string) error { return s.cmdConnect(a) }},
		{prefix: "connect", handler: func(_ string) error { return s.cmdConnect("") }},
		{prefix: "disconnect", handler: func(_ string) error { return s.cmdDisconnect() }},
		{prefix: "exec", handler: func(_ string) error { return s.cmdExec() }},
		{prefix: "run", handler: func(_ string) error { return s.cmdExec() }},

		// --- expression evaluation ---
		{prefix: "expr ", handler: func(a string) error { return s.cmdExpr(a) }, completer: completeColumnArgs},

		// --- parameterize toggle ---
		{prefix: "parameterize", handler: func(_ string) error { return s.cmdParameterize() }},
		{prefix: "params", handler: func(_ string) error { return s.cmdParameterize() }},

		// --- OPA commands ---
		{prefix: "opa conditions", handler: func(_ string) error { return s.cmdOPAConditions() }},
		{prefix: "opa explain ", handler: func(a string) error { return s.cmdOPAExplain(strings.TrimSpace(a)) }},
		{prefix: "opa explain", handler: func(_ string) error { return s.cmdOPAExplain("") }},
		{prefix: "opa inputs", handler: func(_ string) error { return s.cmdOPAInputs() }},
		{prefix: "opa input ", handler: func(a string) error { return s.cmdOPAInput(strings.TrimSpace(a)) }},
		{prefix: "opa input", handler: func(_ string) error { return s.cmdOPAInput("") }},
		{prefix: "opa masks", handler: func(_ string) error { return s.cmdOPAMasks() }},
		{prefix: "opa policy ", handler: func(a string) error { return s.cmdOPAPolicy(strings.TrimSpace(a)) }},
		{prefix: "opa policy", handler: func(_ string) error { return s.cmdOPAPolicy("") }},
		{prefix: "opa reload", handler: func(_ string) error { return s.cmdOPAReload() }},
		{prefix: "opa status", handler: func(_ string) error { s.cmdOPAStatus(); return nil }},
		{prefix: "opa url ", handler: func(a string) error { return s.cmdOPAUrl(strings.TrimSpace(a)) }},
		{prefix: "opa url", handler: func(_ string) error { return s.cmdOPAUrl("") }},
		{prefix: "opa off", handler: func(_ string) error { return s.cmdOPAOff() }},
		{prefix: "opa", handler: func(_ string) error { return s.cmdOPASetup() }},

		// --- engine / plugins ---
		{prefix: "set_engine ", handler: func(a string) error { return s.cmdEngine(a) }, completer: completeEngineArgs},
		{prefix: "engine ", handler: func(a string) error { return s.cmdEngine(a) }, completer: completeEngineArgs},
		{prefix: "plugin ", handler: func(a string) error { return s.cmdPlugin(a) }, completer: completePluginArgs},
		{prefix: "plugins", handler: func(_ string) error { s.cmdPlugins(); return nil }},
	}

	// Sort by prefix length descending so longest prefixes match first.
	sort.Slice(s.commands, func(i, j int) bool {
		return len(s.commands[i].prefix) > len(s.commands[j].prefix)
	})
}

// commandNames derives the command name list from the registry for tab completion.
func (s *Session) commandNames() []string {
	seen := make(map[string]bool)
	var names []string
	for _, cmd := range s.commands {
		if cmd.hidden {
			continue
		}
		name := strings.TrimRight(cmd.prefix, " ")
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	// exit/quit are handled by the REPL loop, not Execute().
	for _, extra := range []string{"exit", "quit"} {
		if !seen[extra] {
			names = append(names, extra)
		}
	}
	sort.Strings(names)
	return names
}

// --- Shared completion helpers ---

// completeJoinArgs handles completion for multi-word join prefixes:
// table name → ON clause → column ref → operator.
func completeJoinArgs(args string) (completionContext, string) {
	words := strings.Fields(args)
	if len(words) == 0 {
		return contextTableName, ""
	}
	if strings.Contains(args, " ") {
		last := words[len(words)-1]
		if strings.HasSuffix(args, " ") {
			return contextOperator, ""
		}
		return contextColumnRef, last
	}
	return contextTableName, args
}

// completeTableArgs handles completion for single-word table commands
// (from, join, insert into, update, delete from).
func completeTableArgs(args string) (completionContext, string) {
	arg := strings.TrimSpace(args)
	if !strings.Contains(arg, " ") {
		return contextTableName, arg
	}
	parts := strings.Fields(arg)
	last := parts[len(parts)-1]
	if strings.HasSuffix(args, " ") {
		return contextOperator, ""
	}
	return contextColumnRef, last
}

// completeColumnArgs handles completion for column-ref commands
// (select, where, having, group, expr, columns, returning, set).
func completeColumnArgs(args string) (completionContext, string) {
	last := lastToken(args)
	if strings.HasSuffix(args, " ") {
		prevTokens := strings.Fields(args)
		if len(prevTokens) > 0 {
			prev := strings.ToLower(prevTokens[len(prevTokens)-1])
			if strings.Contains(prev, ".") {
				return contextOperator, ""
			}
		}
		return contextColumnRef, ""
	}
	return contextColumnRef, last
}

// completeOrderArgs handles completion for the order command:
// column refs, then direction (asc/desc/nulls) after a column.
func completeOrderArgs(args string) (completionContext, string) {
	if strings.HasSuffix(args, " ") {
		parts := strings.Fields(args)
		if len(parts) > 0 {
			last := strings.ToLower(parts[len(parts)-1])
			if strings.Contains(last, ".") {
				return contextOrderDir, ""
			}
		}
		return contextColumnRef, ""
	}
	last := lastToken(args)
	lowerLast := strings.ToLower(last)
	if lowerLast == "a" || lowerLast == "as" || lowerLast == "d" || lowerLast == "de" || lowerLast == "des" {
		return contextOrderDir, last
	}
	return contextColumnRef, last
}

// completeWindowArgs handles completion for the window command:
// first arg is the window name (no completion), then column refs.
func completeWindowArgs(args string) (completionContext, string) {
	parts := strings.Fields(args)
	if len(parts) <= 1 && !strings.HasSuffix(args, " ") {
		// Still typing the window name; no completion.
		return contextCommand, ""
	}
	last := lastToken(args)
	if strings.HasSuffix(args, " ") {
		return contextColumnRef, ""
	}
	return contextColumnRef, last
}

// completeEngineArgs handles completion for engine/set_engine commands.
func completeEngineArgs(args string) (completionContext, string) {
	return contextEngine, strings.TrimSpace(args)
}

// completePluginArgs handles completion for the plugin command:
// plugin names, or after "off" the names of enabled plugins.
func completePluginArgs(args string) (completionContext, string) {
	if strings.HasPrefix(strings.ToLower(args), "off ") {
		partial := strings.TrimSpace(args[4:])
		return contextPluginOff, partial
	}
	arg := strings.TrimSpace(args)
	if !strings.Contains(arg, " ") {
		return contextPlugin, arg
	}
	return contextCommand, ""
}

// completeAliasArgs handles completion for the alias command:
// first arg is a registered table name, second is free-form.
func completeAliasArgs(args string) (completionContext, string) {
	arg := strings.TrimSpace(args)
	if !strings.Contains(arg, " ") {
		return contextAliasTable, arg
	}
	return contextCommand, ""
}

// completeEditArgs handles completion for the edit command: clause names.
func completeEditArgs(args string) (completionContext, string) {
	return contextEditClause, strings.TrimSpace(args)
}
