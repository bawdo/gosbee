package main

import (
	"sort"
	"strings"
)

// completionContext describes what kind of completion is appropriate.
type completionContext int

const (
	contextCommand    completionContext = iota // start of line or partial command
	contextTableName                          // after from/join/etc
	contextColumnRef                          // after select/where/having/group/expr
	contextEngine                             // after engine/set_engine
	contextPlugin                             // after plugin
	contextPluginOff                          // after plugin off
	contextOrderDir                           // after a column ref in order context
	contextOperator                           // after a column ref in condition context
	contextAliasTable                         // first arg of alias (table names only)
	contextEditClause                         // after edit
)

var editClauseNames = []string{"group", "having", "join", "order", "select", "where", "window"}
var engineNames = []string{"mysql", "postgres", "sqlite"}
var orderDirs = []string{"asc", "desc", "nulls first", "nulls last"}
var operators = []string{
	"!=", "&", "*", "+", "-", "/", "<", "<<", "<=", "=", ">", ">=", ">>",
	"^", "|", "||", "~",
	"between", "in", "is", "like", "not",
}

var functionNames = []string{
	"ABS(", "AVG(",
	"CASE ", "CAST(", "COALESCE(", "CONCAT(", "COUNT(", "COUNT(DISTINCT ", "CUBE(", "CUME_DIST(",
	"DENSE_RANK(", "EXISTS(", "EXTRACT(",
	"FIRST_VALUE(", "GREATEST(",
	"LAG(", "LAST_VALUE(", "LEAD(", "LEAST(", "LENGTH(", "LOWER(",
	"MAX(", "MIN(", "NOT EXISTS(", "NTH_VALUE(", "NTILE(", "NULLIF(",
	"PERCENT_RANK(", "RANK(", "REPLACE(", "ROLLUP(", "ROUND(", "ROW_NUMBER(",
	"SUBSTRING(", "SUM(",
	"TRIM(", "UPPER(",
}

// replCompleter implements readline's AutoCompleter interface.
type replCompleter struct {
	sess *Session
}

// Do returns completion candidates for the current line/cursor position.
// length is the number of chars from end of line[:pos] that form the prefix being completed.
// newLine contains the suffixes to append for each candidate.
func (c *replCompleter) Do(line []rune, pos int) (newLine [][]rune, length int) {
	lineStr := string(line[:pos])
	ctx, prefix := c.parseContext(lineStr)

	var candidates []string
	switch ctx {
	case contextCommand:
		candidates = c.completeCommands(prefix)
	case contextTableName:
		candidates = c.completeTableNames(prefix)
	case contextColumnRef:
		candidates = c.completeColumnRef(prefix)
	case contextEngine:
		candidates = filterPrefix(engineNames, prefix)
	case contextPlugin:
		candidates = filterPrefix(append([]string{"off"}, c.sess.pluginNames()...), prefix)
	case contextPluginOff:
		candidates = filterPrefix(c.sess.plugins.names(), prefix)
	case contextOrderDir:
		candidates = filterPrefix(orderDirs, prefix)
	case contextOperator:
		candidates = filterPrefix(operators, prefix)
	case contextAliasTable:
		candidates = c.completeRegisteredTables(prefix)
	case contextEditClause:
		candidates = filterPrefix(editClauseNames, prefix)
	}

	for _, cand := range candidates {
		suffix := cand[len(prefix):]
		// Add trailing space for convenience.
		newLine = append(newLine, []rune(suffix+" "))
	}
	length = len([]rune(prefix))
	return
}

// parseContext examines the line up to cursor and determines what kind of
// completion is needed and the current prefix being typed.
func (c *replCompleter) parseContext(line string) (completionContext, string) {
	lower := strings.ToLower(line)

	for _, cmd := range c.sess.commands {
		if !strings.HasSuffix(cmd.prefix, " ") {
			continue // exact-match commands have no arg completion
		}
		if strings.HasPrefix(lower, cmd.prefix) && cmd.completer != nil {
			return cmd.completer(line[len(cmd.prefix):])
		}
	}

	// Default: command completion.
	return contextCommand, strings.TrimSpace(line)
}

// completeCommands returns command names matching the prefix.
func (c *replCompleter) completeCommands(prefix string) []string {
	return filterPrefix(c.sess.commandNames(), prefix)
}

// completeTableNames returns registered + DB table names matching prefix.
func (c *replCompleter) completeTableNames(prefix string) []string {
	var names []string
	// Registered tables.
	for name := range c.sess.tables {
		names = append(names, name)
	}
	// Aliases.
	for name := range c.sess.aliases {
		names = append(names, name)
	}
	// DB tables.
	if c.sess.conn != nil {
		names = append(names, c.sess.conn.schemaTables()...)
	}
	names = dedup(names)
	sort.Strings(names)
	return filterPrefix(names, prefix)
}

// completeRegisteredTables returns only session-registered tables (for alias command).
func (c *replCompleter) completeRegisteredTables(prefix string) []string {
	var names []string
	for name := range c.sess.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return filterPrefix(names, prefix)
}

// completeColumnRef handles both table-name and table.column completion.
func (c *replCompleter) completeColumnRef(prefix string) []string {
	if strings.Contains(prefix, ".") {
		// After the dot: complete column names.
		parts := strings.SplitN(prefix, ".", 2)
		tableName := parts[0]
		colPrefix := parts[1]

		// Check for "table.*" or "table." (no prefix yet).
		if colPrefix == "" || colPrefix == "*" {
			candidates := []string{tableName + ".*"}
			if c.sess.conn != nil {
				for _, col := range c.sess.conn.schemaColumns(tableName) {
					candidates = append(candidates, tableName+"."+col)
				}
			}
			return filterPrefix(candidates, prefix)
		}

		var candidates []string
		if c.sess.conn != nil {
			for _, col := range c.sess.conn.schemaColumns(tableName) {
				candidates = append(candidates, tableName+"."+col)
			}
		}
		// Always include the star option.
		candidates = append(candidates, tableName+".*")
		return filterPrefix(candidates, prefix)
	}

	// Before the dot: complete table names and function names.
	candidates := c.completeTableNames(prefix)
	candidates = append(candidates, filterPrefix(functionNames, prefix)...)
	return candidates
}

// filterPrefix returns items that start with prefix (case-insensitive).
func filterPrefix(items []string, prefix string) []string {
	if prefix == "" {
		result := make([]string, len(items))
		copy(result, items)
		return result
	}
	lowerPrefix := strings.ToLower(prefix)
	var result []string
	for _, item := range items {
		if strings.HasPrefix(strings.ToLower(item), lowerPrefix) {
			result = append(result, item)
		}
	}
	return result
}

// dedup removes duplicate strings.
func dedup(items []string) []string {
	seen := make(map[string]bool, len(items))
	var result []string
	for _, item := range items {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	return result
}

// lastToken returns the last whitespace-separated token, handling commas.
func lastToken(s string) string {
	// Find the last comma or space.
	lastSep := -1
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ' ' || s[i] == ',' || s[i] == '\t' {
			lastSep = i
			break
		}
	}
	if lastSep >= 0 {
		return s[lastSep+1:]
	}
	return s
}
