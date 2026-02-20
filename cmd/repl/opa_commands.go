package main

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/bawdo/gosbee/plugins"
	"github.com/bawdo/gosbee/plugins/opa"
)

// opaPluginRef holds OPA server configuration.
type opaPluginRef struct {
	url        string
	policy     string
	input      map[string]any
	dataTables []string // selected tables for this session
}

func (s *Session) cmdOPAOff() error {
	if s.opaConfig == nil {
		return errors.New("OPA is not enabled")
	}
	s.plugins.deregister("opa")
	s.opaConfig = nil
	_, _ = fmt.Fprintln(s.out, "  OPA disabled")
	s.rebuildQueryWithPlugins()
	return nil
}

func (s *Session) cmdOPAStatus() {
	if s.opaConfig == nil {
		_, _ = fmt.Fprintln(s.out, "  OPA: off")
		return
	}
	_, _ = fmt.Fprintln(s.out, "  OPA: on")
	_, _ = fmt.Fprintf(s.out, "    Server: %s\n", s.opaConfig.url)
	_, _ = fmt.Fprintf(s.out, "    Policy: %s\n", s.opaConfig.policy)
	if len(s.opaConfig.dataTables) > 0 {
		_, _ = fmt.Fprintf(s.out, "    Tables: %s\n", strings.Join(s.opaConfig.dataTables, ", "))
	}
	if len(s.opaConfig.input) > 0 {
		_, _ = fmt.Fprintln(s.out, "    Inputs:")
		s.printInputMap(s.opaConfig.input, "      ")
	} else {
		_, _ = fmt.Fprintln(s.out, "    Inputs: (none)")
	}
	if s.query != nil {
		client := opa.NewClient(s.opaConfig.url, s.opaConfig.policy, s.opaConfig.input)
		masks, err := client.FetchMasks()
		if err == nil && len(masks) > 0 {
			maskCount := 0
			for _, cols := range masks {
				maskCount += len(cols)
			}
			_, _ = fmt.Fprintf(s.out, "    Masks: %d column(s) masked\n", maskCount)
		} else {
			_, _ = fmt.Fprintln(s.out, "    Masks: none")
		}
	}
}

func (s *Session) printInputMap(m map[string]any, indent string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := m[k]
		if nested, ok := v.(map[string]any); ok {
			_, _ = fmt.Fprintf(s.out, "%s%s:\n", indent, k)
			s.printInputMap(nested, indent+"  ")
		} else {
			_, _ = fmt.Fprintf(s.out, "%s%s: %v\n", indent, k, v)
		}
	}
}

func (s *Session) printMasks(masks map[string]map[string]opa.MaskAction) {
	if len(masks) == 0 {
		return
	}
	_, _ = fmt.Fprintln(s.out, "    Masks:")
	tables := make([]string, 0, len(masks))
	for tbl := range masks {
		tables = append(tables, tbl)
	}
	sort.Strings(tables)
	for _, tbl := range tables {
		cols := masks[tbl]
		colNames := make([]string, 0, len(cols))
		for col := range cols {
			colNames = append(colNames, col)
		}
		sort.Strings(colNames)
		for _, col := range colNames {
			action := cols[col]
			if action.Replace != nil {
				_, _ = fmt.Fprintf(s.out, "      %s.%s → replace: '%s'\n", tbl, col, action.Replace.Value)
			}
		}
	}
}

// columnResolver returns an opa.ColumnResolver that uses the DB schema
// to look up column names for a table. Returns an error if no DB connection
// or no schema is available for the table.
func (s *Session) columnResolver() opa.ColumnResolver {
	return func(tableName string) ([]string, error) {
		if s.conn == nil {
			return nil, fmt.Errorf("no database connection (required for column masking)")
		}
		cols := s.conn.schemaColumns(tableName)
		if cols == nil {
			return nil, fmt.Errorf("no schema for table %q", tableName)
		}
		return cols, nil
	}
}

// configureOPA registers the OPA plugin in the registry using the current
// opaConfig. The args parameter is ignored (OPA is configured via opa setup).
func configureOPA(s *Session, _ string) error {
	if s.opaConfig == nil {
		return errors.New("OPA not configured - run 'opa setup' first")
	}
	cfg := s.opaConfig
	s.plugins.register(pluginEntry{
		name: "opa",
		factory: func() plugins.Transformer {
			return opa.NewFromServer(cfg.url, cfg.policy, cfg.input,
				opa.WithColumnResolver(s.columnResolver()))
		},
		status: func() string { return fmt.Sprintf("policy: %s", cfg.policy) },
		color:  "#9B59B6",
	})
	return nil
}

func (s *Session) opaReload() {
	_ = configureOPA(s, "")
	s.rebuildQueryWithPlugins()
}

func (s *Session) cmdOPAReload() error {
	if s.opaConfig == nil {
		return errors.New("OPA is not enabled")
	}
	s.opaPromptRediscover()
	return nil
}

// opaPromptRediscover asks "Re-discover inputs from server? (y/n)".
// If yes and interactive, runs the inputs flow. Otherwise just reloads.
func (s *Session) opaPromptRediscover() {
	if s.rl != nil {
		answer := prompt(s.rl, "Re-discover inputs from server? (y/n)", "n")
		if strings.ToLower(answer) == "y" || strings.ToLower(answer) == "yes" {
			_ = s.cmdOPAInputs()
			return
		}
	}
	s.opaReload()
	_, _ = fmt.Fprintln(s.out, "  OPA plugin reloaded.")
}

func (s *Session) cmdOPAUrl(args string) error {
	if s.opaConfig == nil {
		return errors.New("OPA is not enabled")
	}
	url := strings.TrimSpace(args)
	if url == "" {
		return errors.New("usage: opa url <url>")
	}
	s.opaConfig.url = url
	_, _ = fmt.Fprintf(s.out, "  OPA server URL set to %s\n", url)
	s.opaPromptRediscover()
	return nil
}

func (s *Session) cmdOPAPolicy(args string) error {
	if s.opaConfig == nil {
		return errors.New("OPA is not enabled")
	}
	policy := strings.TrimSpace(args)
	if policy == "" {
		return errors.New("usage: opa policy <path>")
	}
	s.opaConfig.policy = policy
	_, _ = fmt.Fprintf(s.out, "  OPA policy path set to %s\n", policy)
	s.opaPromptRediscover()
	return nil
}

func (s *Session) cmdOPAInput(args string) error {
	if s.opaConfig == nil {
		return errors.New("OPA is not enabled")
	}
	parts := strings.Fields(args)
	if len(parts) == 0 {
		return errors.New("usage: opa input <key> [value]")
	}
	key := parts[0]
	if len(parts) == 1 {
		// Remove the key.
		deleteNestedValue(s.opaConfig.input, key)
		_, _ = fmt.Fprintf(s.out, "  Removed input %s\n", key)
	} else {
		// Set/update the key.
		valStr := strings.Join(parts[1:], " ")
		val := parseOPAValue(valStr)
		setNestedValue(s.opaConfig.input, key, val)
		_, _ = fmt.Fprintf(s.out, "  Set input %s = %v\n", key, val)
	}
	s.opaPromptRediscover()
	return nil
}

func (s *Session) cmdOPAInputs() error {
	if s.opaConfig == nil {
		return errors.New("OPA is not enabled")
	}
	if s.rl == nil {
		return errors.New("opa inputs requires an interactive session")
	}
	client := opa.NewClient(s.opaConfig.url, s.opaConfig.policy, nil)
	var dataUnknowns []string
	for _, t := range s.opaConfig.dataTables {
		dataUnknowns = append(dataUnknowns, "data."+t)
	}
	inputPaths, err := client.DiscoverInputs(dataUnknowns...)
	if err != nil {
		return fmt.Errorf("OPA: cannot reach server at %s: %w", s.opaConfig.url, err)
	}
	input := map[string]any{}
	if len(inputPaths) > 0 {
		_, _ = fmt.Fprintf(s.out, "  Policy requires %d input(s):\n", len(inputPaths))
		for _, path := range inputPaths {
			current := getNestedValue(s.opaConfig.input, path)
			defaultVal := ""
			if current != nil {
				defaultVal = fmt.Sprintf("%v", current)
			}
			val := prompt(s.rl, path, defaultVal)
			if val != "" {
				setNestedValue(input, path, parseOPAValue(val))
			}
		}
	} else {
		_, _ = fmt.Fprintln(s.out, "  No inputs required by policy")
	}
	s.opaConfig.input = input
	s.opaReload()
	_, _ = fmt.Fprintln(s.out, "  OPA inputs updated and reloaded.")
	return nil
}

// cmdOPAExplain shows how OPA translates policy decisions to SQL conditions,
// with optional verbose mode showing raw request/response and translation trace.
func (s *Session) cmdOPAExplain(args string) error {
	if s.opaConfig == nil {
		return errors.New("OPA is not enabled")
	}
	parts := strings.Fields(args)
	if len(parts) == 0 {
		return errors.New("usage: opa explain <table> [verbose]")
	}
	tableName := parts[0]
	verbose := len(parts) > 1 && strings.ToLower(parts[1]) == "verbose"

	client := opa.NewClient(s.opaConfig.url, s.opaConfig.policy, s.opaConfig.input)
	result, err := client.Explain(tableName)
	if err != nil {
		return fmt.Errorf("OPA explain: %w", err)
	}

	_, _ = fmt.Fprintf(s.out, "  OPA explain for table %q:\n", tableName)

	if result.AccessDenied {
		_, _ = fmt.Fprintln(s.out, "    Access denied (no matching rules)")
		if verbose {
			_, _ = fmt.Fprintf(s.out, "    Request:\n      %s\n", result.RequestJSON)
			_, _ = fmt.Fprintf(s.out, "    Response:\n      %s\n", result.RawJSON)
		} else {
			_, _ = fmt.Fprintln(s.out, "    (use 'opa explain <table> verbose' to see request/response)")
		}
		return nil
	}

	if result.UnconditionalAllow {
		_, _ = fmt.Fprintln(s.out, "    Unconditional allow (no conditions)")
		s.printMasks(result.Masks)
		return nil
	}

	if verbose {
		_, _ = fmt.Fprintf(s.out, "    Request:\n      %s\n", result.RequestJSON)
		_, _ = fmt.Fprintf(s.out, "    Response:\n      %s\n", result.RawJSON)
		_, _ = fmt.Fprintln(s.out, "    Translation:")
		for i, tr := range result.Translations {
			_, _ = fmt.Fprintf(s.out, "      [%d] %s(data.%s.%s, %v) → %s\n",
				i+1, tr.Operator, tableName, tr.Column, tr.Value, tr.SQL)
		}
	}

	_, _ = fmt.Fprintf(s.out, "    %d query(ies), %d expression(s)\n", result.QueryCount, result.ExpressionCount)
	_, _ = fmt.Fprintln(s.out, "    Conditions:")
	v := s.visitor
	for _, cond := range result.Conditions {
		_, _ = fmt.Fprintf(s.out, "      %s\n", cond.Accept(v))
	}
	s.printMasks(result.Masks)
	return nil
}

func (s *Session) cmdOPAConditions() error {
	if s.opaConfig == nil {
		return errors.New("OPA is not enabled")
	}
	if s.query == nil {
		return errNoQuery
	}
	client := opa.NewClient(s.opaConfig.url, s.opaConfig.policy, s.opaConfig.input)
	refs := plugins.CollectTables(s.query.Core)
	if len(refs) == 0 {
		_, _ = fmt.Fprintln(s.out, "  No tables in query")
		return nil
	}
	_, _ = fmt.Fprintln(s.out, "  OPA conditions:")
	v := s.visitor
	for _, ref := range refs {
		conditions, err := client.Compile(ref.Name)
		if err != nil {
			_, _ = fmt.Fprintf(s.out, "    %s: %v\n", ref.Name, err)
			continue
		}
		if len(conditions) == 0 {
			_, _ = fmt.Fprintf(s.out, "    %s: (unconditional allow)\n", ref.Name)
			continue
		}
		parts := make([]string, len(conditions))
		for i, c := range conditions {
			parts[i] = c.Accept(v)
		}
		_, _ = fmt.Fprintf(s.out, "    %s: %s\n", ref.Name, strings.Join(parts, " AND "))
	}
	return nil
}

func (s *Session) cmdOPAMasks() error {
	if s.opaConfig == nil {
		return errors.New("OPA is not enabled")
	}
	if s.query == nil {
		return errNoQuery
	}
	client := opa.NewClient(s.opaConfig.url, s.opaConfig.policy, s.opaConfig.input)
	masks, err := client.FetchMasks()
	if err != nil {
		return fmt.Errorf("OPA masks: %w", err)
	}
	if len(masks) == 0 {
		_, _ = fmt.Fprintln(s.out, "  No masks active.")
		return nil
	}
	tables := make([]string, 0, len(masks))
	for tbl := range masks {
		tables = append(tables, tbl)
	}
	sort.Strings(tables)
	for _, tbl := range tables {
		tableMasks := masks[tbl]
		if len(tableMasks) == 0 {
			continue
		}
		_, _ = fmt.Fprintf(s.out, "  Masks for %s:\n", tbl)
		cols := make([]string, 0, len(tableMasks))
		for col := range tableMasks {
			cols = append(cols, col)
		}
		sort.Strings(cols)
		for _, col := range cols {
			action := tableMasks[col]
			if action.Replace != nil {
				_, _ = fmt.Fprintf(s.out, "    %s → replace: '%s'\n", col, action.Replace.Value)
			}
		}
	}
	return nil
}

// cmdOPASetup runs the interactive OPA setup wizard: prompts for server URL,
// discovers available policies for selection (with manual fallback), prompts
// for a table name, then discovers and prompts for input fields.
func (s *Session) cmdOPASetup() error {
	if s.rl == nil {
		return errors.New("opa setup requires an interactive session")
	}
	_, _ = fmt.Fprintln(s.out, "  OPA setup:")
	url := prompt(s.rl, "OPA server URL", "http://localhost:8181")

	policyPath, err := s.selectOPAPolicy(url)
	if err != nil {
		return err
	}

	dataTables, err := s.selectOPATables(url, policyPath)
	if err != nil {
		return err
	}

	client := opa.NewClient(url, policyPath, nil)
	var dataUnknowns []string
	for _, t := range dataTables {
		dataUnknowns = append(dataUnknowns, "data."+t)
	}
	inputPaths, err := client.DiscoverInputs(dataUnknowns...)
	if err != nil {
		return fmt.Errorf("OPA: cannot reach server at %s: %w", url, err)
	}
	input := map[string]any{}
	if len(inputPaths) > 0 {
		_, _ = fmt.Fprintf(s.out, "  Policy requires %d input(s):\n", len(inputPaths))
		for _, path := range inputPaths {
			val := prompt(s.rl, path, "")
			if val != "" {
				setNestedValue(input, path, parseOPAValue(val))
			}
		}
	}
	s.opaConfig = &opaPluginRef{url: url, policy: policyPath, input: input, dataTables: dataTables}
	_ = configureOPA(s, "")
	s.rebuildQueryWithPlugins()
	_, _ = fmt.Fprintf(s.out, "  OPA enabled — policy: %s\n", policyPath)
	if len(dataTables) > 0 {
		_, _ = fmt.Fprintf(s.out, "  Tables: %s\n", strings.Join(dataTables, ", "))
	}
	return nil
}

// selectOPAPolicy discovers available policies from the OPA server and prompts
// the user to select one. Falls back to a manual text prompt if discovery
// fails or returns no matching policies.
func (s *Session) selectOPAPolicy(url string) (string, error) {
	client := opa.NewClient(url, "data.placeholder", nil)
	_, _ = fmt.Fprintln(s.out, "  Searching for policies...")
	policies, err := client.DiscoverPolicies()
	if err != nil {
		_, _ = fmt.Fprintf(s.out, "  (policy discovery failed: %v — enter path manually)\n", err)
	} else if len(policies) == 0 {
		_, _ = fmt.Fprintln(s.out, "  (no matching policies found — enter path manually)")
	}

	if err != nil || len(policies) == 0 {
		policyPath := prompt(s.rl, "Policy path (e.g. data.authz.allow)", "")
		if policyPath == "" {
			return "", errors.New("policy path is required")
		}
		return policyPath, nil
	}

	paths := make([]string, len(policies))
	for i, p := range policies {
		paths[i] = p.FullPath
	}
	idx, err := s.selectOne("policy", paths)
	if err != nil {
		return "", err
	}
	return policies[idx].FullPath, nil
}

// selectOPATables discovers table names from the selected policy and prompts
// the user to select one or more. Falls back to a manual text prompt if
// discovery returns no tables.
func (s *Session) selectOPATables(url, policyPath string) ([]string, error) {
	client := opa.NewClient(url, policyPath, nil)
	_, _ = fmt.Fprintln(s.out, "  Inspecting policy for tables...")
	discovered, err := client.DiscoverTables()
	if err != nil {
		_, _ = fmt.Fprintf(s.out, "  (table discovery failed: %v — enter table manually)\n", err)
	} else if len(discovered) == 0 {
		_, _ = fmt.Fprintln(s.out, "  (no tables found — enter table manually)")
	}

	if err != nil || len(discovered) == 0 {
		tableName := prompt(s.rl, "Table name (for data discovery)", "")
		if tableName == "" {
			return nil, nil
		}
		return []string{tableName}, nil
	}

	indices, err := s.selectMany("table", discovered)
	if err != nil {
		return nil, err
	}
	tables := make([]string, len(indices))
	for i, idx := range indices {
		tables[i] = discovered[idx]
	}
	return tables, nil
}

// selectMany displays a numbered list of items and prompts the user to pick
// zero or more by comma-separated number. Returns the 0-based indices of the
// selected items. An empty input is valid and returns an empty slice.
func (s *Session) selectMany(label string, items []string) ([]int, error) {
	_, _ = fmt.Fprintf(s.out, "  Found %d %s(s):\n", len(items), label)
	for i, item := range items {
		_, _ = fmt.Fprintf(s.out, "    [%d] %s\n", i+1, item)
	}
	raw := prompt(s.rl, fmt.Sprintf("Select %s(s) (e.g. 1,3 or leave blank for none)", label), "")
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var indices []int
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil || n < 1 || n > len(items) {
			return nil, fmt.Errorf("invalid selection %q (choose 1-%d)", part, len(items))
		}
		indices = append(indices, n-1)
	}
	return indices, nil
}

// selectOne displays a numbered list of items and prompts the user to pick
// one by number. Returns the 0-based index of the selected item.
func (s *Session) selectOne(label string, items []string) (int, error) {
	_, _ = fmt.Fprintf(s.out, "  Found %d matching %s(s):\n", len(items), label)
	for i, item := range items {
		_, _ = fmt.Fprintf(s.out, "    [%d] %s\n", i+1, item)
	}
	raw := prompt(s.rl, fmt.Sprintf("Select %s (1-%d)", label, len(items)), "1")
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 1 || n > len(items) {
		return 0, fmt.Errorf("invalid selection %q (choose 1-%d)", raw, len(items))
	}
	return n - 1, nil
}

func parseOPAValue(s string) any {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	switch strings.ToLower(s) {
	case "true":
		return true
	case "false":
		return false
	}
	return s
}

func setNestedValue(m map[string]any, path string, val any) {
	parts := strings.Split(path, ".")
	current := m
	for i := 0; i < len(parts)-1; i++ {
		next, ok := current[parts[i]].(map[string]any)
		if !ok {
			next = map[string]any{}
			current[parts[i]] = next
		}
		current = next
	}
	current[parts[len(parts)-1]] = val
}

func deleteNestedValue(m map[string]any, path string) {
	parts := strings.Split(path, ".")
	current := m
	for i := 0; i < len(parts)-1; i++ {
		next, ok := current[parts[i]].(map[string]any)
		if !ok {
			return
		}
		current = next
	}
	delete(current, parts[len(parts)-1])
}

func getNestedValue(m map[string]any, path string) any {
	parts := strings.Split(path, ".")
	current := m
	for i := 0; i < len(parts)-1; i++ {
		next, ok := current[parts[i]].(map[string]any)
		if !ok {
			return nil
		}
		current = next
	}
	return current[parts[len(parts)-1]]
}
