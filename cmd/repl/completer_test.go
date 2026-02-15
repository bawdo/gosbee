package main

import (
	"sort"
	"strings"
	"testing"
)

func newTestCompleter(tables ...string) *replCompleter {
	sess := NewSession("postgres", nil)
	for _, t := range tables {
		_ = sess.Execute("table " + t)
	}
	return &replCompleter{sess: sess}
}

// --- Command completion ---

func TestCompleteCommandsEmpty(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	candidates := c.completeCommands("")
	names := c.sess.commandNames()
	if len(candidates) != len(names) {
		t.Errorf("expected %d commands, got %d", len(names), len(candidates))
	}
}

func TestCompleteCommandsPrefix(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	candidates := c.completeCommands("sel")
	if len(candidates) != 1 || candidates[0] != "select" {
		t.Errorf("expected [select], got %v", candidates)
	}
}

func TestCompleteCommandsMultiMatch(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	candidates := c.completeCommands("s")
	// Should include: select, set_engine, sql
	found := map[string]bool{}
	for _, c := range candidates {
		found[c] = true
	}
	for _, want := range []string{"select", "set_engine", "sql"} {
		if !found[want] {
			t.Errorf("expected %q in candidates: %v", want, candidates)
		}
	}
}

// --- Table name completion ---

func TestCompleteTableNames(t *testing.T) {
	t.Parallel()
	c := newTestCompleter("users", "posts", "comments")
	candidates := c.completeTableNames("u")
	if len(candidates) != 1 || candidates[0] != "users" {
		t.Errorf("expected [users], got %v", candidates)
	}
}

func TestCompleteTableNamesAll(t *testing.T) {
	t.Parallel()
	c := newTestCompleter("users", "posts")
	candidates := c.completeTableNames("")
	sort.Strings(candidates)
	if len(candidates) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(candidates), candidates)
	}
}

func TestCompleteTableNamesWithAlias(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("alias users u")
	c := &replCompleter{sess: sess}

	candidates := c.completeTableNames("")
	found := map[string]bool{}
	for _, name := range candidates {
		found[name] = true
	}
	if !found["users"] || !found["u"] {
		t.Errorf("expected both 'users' and 'u', got %v", candidates)
	}
}

// --- Engine completion ---

func TestCompleteEngines(t *testing.T) {
	t.Parallel()
	candidates := filterPrefix(engineNames, "p")
	if len(candidates) != 1 || candidates[0] != "postgres" {
		t.Errorf("expected [postgres], got %v", candidates)
	}
}

func TestCompleteEnginesAll(t *testing.T) {
	t.Parallel()
	candidates := filterPrefix(engineNames, "")
	if len(candidates) != 3 {
		t.Errorf("expected 3 engines, got %d", len(candidates))
	}
}

// --- Plugin completion ---

func TestCompletePlugins(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	allNames := append([]string{"off"}, sess.pluginNames()...)
	candidates := filterPrefix(allNames, "s")
	if len(candidates) != 1 || candidates[0] != "softdelete" {
		t.Errorf("expected [softdelete], got %v", candidates)
	}
}

func TestCompletePluginsOff(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	allNames := append([]string{"off"}, sess.pluginNames()...)
	candidates := filterPrefix(allNames, "of")
	if len(candidates) != 1 || candidates[0] != "off" {
		t.Errorf("expected [off], got %v", candidates)
	}
}

// --- parseContext ---

func TestParseContextCommandEmpty(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("")
	if ctx != contextCommand || prefix != "" {
		t.Errorf("expected contextCommand/'', got %v/%q", ctx, prefix)
	}
}

func TestParseContextCommandPartial(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("sel")
	if ctx != contextCommand || prefix != "sel" {
		t.Errorf("expected contextCommand/'sel', got %v/%q", ctx, prefix)
	}
}

func TestParseContextTableNameFrom(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("from ")
	if ctx != contextTableName || prefix != "" {
		t.Errorf("expected contextTableName/'', got %v/%q", ctx, prefix)
	}
}

func TestParseContextTableNameFromPartial(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("from us")
	if ctx != contextTableName || prefix != "us" {
		t.Errorf("expected contextTableName/'us', got %v/%q", ctx, prefix)
	}
}

func TestParseContextTableNameJoin(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("join ")
	if ctx != contextTableName || prefix != "" {
		t.Errorf("expected contextTableName/'', got %v/%q", ctx, prefix)
	}
}

func TestParseContextTableNameLeftJoin(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("left join ")
	if ctx != contextTableName || prefix != "" {
		t.Errorf("expected contextTableName/'', got %v/%q", ctx, prefix)
	}
}

func TestParseContextColumnRefWhere(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("where ")
	if ctx != contextColumnRef || prefix != "" {
		t.Errorf("expected contextColumnRef/'', got %v/%q", ctx, prefix)
	}
}

func TestParseContextColumnRefWherePartial(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("where users.")
	if ctx != contextColumnRef || prefix != "users." {
		t.Errorf("expected contextColumnRef/'users.', got %v/%q", ctx, prefix)
	}
}

func TestParseContextEngine(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("engine ")
	if ctx != contextEngine || prefix != "" {
		t.Errorf("expected contextEngine/'', got %v/%q", ctx, prefix)
	}
}

func TestParseContextEnginePartial(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("engine my")
	if ctx != contextEngine || prefix != "my" {
		t.Errorf("expected contextEngine/'my', got %v/%q", ctx, prefix)
	}
}

func TestParseContextPlugin(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("plugin ")
	if ctx != contextPlugin || prefix != "" {
		t.Errorf("expected contextPlugin/'', got %v/%q", ctx, prefix)
	}
}

func TestParseContextOrderDir(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("order users.name ")
	if ctx != contextOrderDir {
		t.Errorf("expected contextOrderDir, got %v (prefix=%q)", ctx, prefix)
	}
}

func TestParseContextAliasTable(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("alias ")
	if ctx != contextAliasTable || prefix != "" {
		t.Errorf("expected contextAliasTable/'', got %v/%q", ctx, prefix)
	}
}

func TestParseContextOperatorAfterColumnRef(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("where users.age ")
	if ctx != contextOperator || prefix != "" {
		t.Errorf("expected contextOperator/'', got %v/%q", ctx, prefix)
	}
}

// --- Column ref with dot ---

func TestCompleteColumnRefBeforeDot(t *testing.T) {
	t.Parallel()
	c := newTestCompleter("users", "posts")
	candidates := c.completeColumnRef("us")
	if len(candidates) != 1 || candidates[0] != "users" {
		t.Errorf("expected [users], got %v", candidates)
	}
}

func TestCompleteColumnRefAfterDot(t *testing.T) {
	t.Parallel()
	c := newTestCompleter("users")
	candidates := c.completeColumnRef("users.")
	// Without a DB connection, should at least include users.*
	found := false
	for _, cand := range candidates {
		if cand == "users.*" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected users.* in candidates, got %v", candidates)
	}
}

// --- Do() integration ---

func TestDoReturnsCompletions(t *testing.T) {
	t.Parallel()
	c := newTestCompleter("users")
	line := []rune("from u")
	newLine, length := c.Do(line, len(line))
	if length != 1 { // "u" is 1 char
		t.Errorf("expected length 1, got %d", length)
	}
	if len(newLine) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(newLine))
	}
	suffix := string(newLine[0])
	if !strings.HasPrefix(suffix, "sers") {
		t.Errorf("expected suffix starting with 'sers', got %q", suffix)
	}
}

func TestDoEmptyLine(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	line := []rune("")
	newLine, length := c.Do(line, 0)
	if length != 0 {
		t.Errorf("expected length 0, got %d", length)
	}
	// Should return all commands.
	names := c.sess.commandNames()
	if len(newLine) != len(names) {
		t.Errorf("expected %d commands, got %d", len(names), len(newLine))
	}
}

// --- Dedup ---

func TestDedup(t *testing.T) {
	t.Parallel()
	items := []string{"a", "b", "a", "c", "b"}
	result := dedup(items)
	if len(result) != 3 {
		t.Errorf("expected 3 unique items, got %d: %v", len(result), result)
	}
}

// --- filterPrefix ---

func TestFilterPrefixCaseInsensitive(t *testing.T) {
	t.Parallel()
	items := []string{"Select", "SQL", "select"}
	result := filterPrefix(items, "sel")
	if len(result) != 2 {
		t.Errorf("expected 2 matches, got %d: %v", len(result), result)
	}
}

func TestFilterPrefixEmpty(t *testing.T) {
	t.Parallel()
	items := []string{"a", "b", "c"}
	result := filterPrefix(items, "")
	if len(result) != 3 {
		t.Errorf("expected 3 items, got %d", len(result))
	}
}

// --- plugins command completion ---

func TestCompletePluginsInCommands(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	candidates := c.completeCommands("plugins")
	if len(candidates) != 1 || candidates[0] != "plugins" {
		t.Errorf("expected [plugins], got %v", candidates)
	}
}

func TestParseContextPluginOff(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("plugin off ")
	if ctx != contextPluginOff || prefix != "" {
		t.Errorf("expected contextPluginOff/'', got %v/%q", ctx, prefix)
	}
}

func TestParseContextPluginOffPartial(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, prefix := c.parseContext("plugin off soft")
	if ctx != contextPluginOff || prefix != "soft" {
		t.Errorf("expected contextPluginOff/'soft', got %v/%q", ctx, prefix)
	}
}

func TestCompletePluginOffNames(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("plugin softdelete")
	candidates := filterPrefix(sess.plugins.names(), "s")
	if len(candidates) != 1 || candidates[0] != "softdelete" {
		t.Errorf("expected [softdelete], got %v", candidates)
	}
}

// --- OPA tab completion (Task 10) ---

func TestCompleteOPACommands(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	candidates := c.completeCommands("opa")
	found := map[string]bool{}
	for _, cand := range candidates {
		found[cand] = true
	}
	if !found["opa"] || !found["opa off"] || !found["opa status"] {
		t.Errorf("expected opa, opa off, opa status, got %v", candidates)
	}
}

func TestParseContextOPA(t *testing.T) {
	t.Parallel()
	c := newTestCompleter()
	ctx, _ := c.parseContext("opa")
	if ctx != contextCommand {
		t.Errorf("expected contextCommand, got %v", ctx)
	}
}

func TestCompletePluginOffOPA(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{url: "http://localhost:8181", policy: "data.authz.allow"}
	_ = configureOPA(sess, "")
	candidates := filterPrefix(sess.plugins.names(), "o")
	found := false
	for _, c := range candidates {
		if c == "opa" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'opa' in enabled plugin names, got %v", candidates)
	}
}

func TestCompleterIncludesDot(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	found := false
	for _, name := range sess.commandNames() {
		if name == "dot" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'dot' in commandNames")
	}
}
