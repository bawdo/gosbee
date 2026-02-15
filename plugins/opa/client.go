package opa

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bawdo/gosbee/internal/quoting"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/visitors"
)

// Client communicates with an OPA server's Compile API.
type Client struct {
	baseURL    string
	policyPath string
	input      map[string]any
	httpClient *http.Client
}

// NewClient creates an OPA Client with the given base URL, policy path, and input.
// The policy path is normalized to include the "data." prefix if not already present.
//
// SECURITY: The baseURL is used as-is for HTTP requests. In production, use HTTPS
// to prevent policy decisions and input data from being transmitted in plain text.
func NewClient(baseURL, policyPath string, input map[string]any) *Client {
	if !strings.HasPrefix(policyPath, "data.") {
		policyPath = "data." + policyPath
	}
	return &Client{
		baseURL:    baseURL,
		policyPath: policyPath,
		input:      input,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

// postJSON sends a POST request with JSON body to the given path and returns
// the response body. Returns an error if the request fails or returns a
// non-200 status code.
func (c *Client) postJSON(path string, reqBody []byte) ([]byte, error) {
	resp, err := c.httpClient.Post(c.baseURL+path, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}

// --- Compile API response types ---

type compileResponse struct {
	Result compileResult `json:"result"`
}

type compileResult struct {
	Queries [][]compileExpression            `json:"queries"`
	Masks   map[string]map[string]MaskAction `json:"masks"`
}

type compileExpression struct {
	Index int           `json:"index"`
	Terms []compileTerm `json:"terms"`
}

type compileTerm struct {
	Type  string `json:"type"`
	Value any    // string, int, float64, bool, or []compileTerm (for ref)
}

// MaskAction describes how to mask a single column.
type MaskAction struct {
	Replace *ReplaceAction `json:"replace"`
}

// ReplaceAction replaces the column value with a literal string.
type ReplaceAction struct {
	Value string `json:"value"`
}

// UnmarshalJSON handles polymorphic deserialization of compileTerm.Value
// based on the Type field.
func (ct *compileTerm) UnmarshalJSON(data []byte) error {
	var raw struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	ct.Type = raw.Type

	switch raw.Type {
	case "string", "var":
		var s string
		if err := json.Unmarshal(raw.Value, &s); err != nil {
			return fmt.Errorf("opa: failed to unmarshal %s value: %w", raw.Type, err)
		}
		ct.Value = s
	case "number":
		var f float64
		if err := json.Unmarshal(raw.Value, &f); err != nil {
			return fmt.Errorf("opa: failed to unmarshal number value: %w", err)
		}
		// Store whole numbers as int.
		if f == math.Trunc(f) && !math.IsInf(f, 0) && !math.IsNaN(f) {
			ct.Value = int(f)
		} else {
			ct.Value = f
		}
	case "boolean":
		var b bool
		if err := json.Unmarshal(raw.Value, &b); err != nil {
			return fmt.Errorf("opa: failed to unmarshal boolean value: %w", err)
		}
		ct.Value = b
	case "ref":
		var terms []compileTerm
		if err := json.Unmarshal(raw.Value, &terms); err != nil {
			return fmt.Errorf("opa: failed to unmarshal ref value: %w", err)
		}
		ct.Value = terms
	default:
		return fmt.Errorf("opa: unknown term type %q", raw.Type)
	}
	return nil
}

// parseCompileResponse parses a raw JSON body from the OPA Compile API.
func parseCompileResponse(data []byte) (*compileResponse, error) {
	var resp compileResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("opa: failed to parse compile response: %w", err)
	}
	return &resp, nil
}

// --- Expression translation ---

// extractOperator pulls the operator name from the first term of an expression,
// which is expected to be a ref containing a single var.
func extractOperator(term compileTerm) (string, error) {
	if term.Type != "ref" {
		return "", fmt.Errorf("opa: operator term must be ref, got %s", term.Type)
	}
	parts, ok := term.Value.([]compileTerm)
	if !ok || len(parts) == 0 {
		return "", errors.New("opa: operator ref has no parts")
	}
	if parts[0].Type != "var" {
		return "", fmt.Errorf("opa: operator ref[0] must be var, got %s", parts[0].Type)
	}
	name, ok := parts[0].Value.(string)
	if !ok {
		return "", errors.New("opa: operator var value is not a string")
	}
	return name, nil
}

// extractColumnName pulls the column name from a data ref term.
// The column name is the last string-typed element in the ref.
func extractColumnName(term compileTerm) (string, error) {
	if term.Type != "ref" {
		return "", fmt.Errorf("opa: column term must be ref, got %s", term.Type)
	}
	parts, ok := term.Value.([]compileTerm)
	if !ok || len(parts) == 0 {
		return "", errors.New("opa: column ref has no parts")
	}
	// Walk backwards to find the last string-typed element.
	for i := len(parts) - 1; i >= 0; i-- {
		if parts[i].Type == "string" {
			s, ok := parts[i].Value.(string)
			if !ok {
				return "", errors.New("opa: column ref string value is not a string")
			}
			return s, nil
		}
	}
	return "", errors.New("opa: column ref has no string-typed element")
}

// isDataRef returns true if the term is a ref starting with var "data".
func isDataRef(term compileTerm) bool {
	if term.Type != "ref" {
		return false
	}
	parts, ok := term.Value.([]compileTerm)
	if !ok || len(parts) == 0 {
		return false
	}
	if parts[0].Type != "var" {
		return false
	}
	name, ok := parts[0].Value.(string)
	return ok && name == "data"
}

// translateExpression converts an OPA compile expression into an AST node
// using the given table for column references. OPA does not guarantee operand
// order, so we identify the data ref and value term by type rather than position.
func translateExpression(expr compileExpression, table *nodes.Table) (nodes.Node, error) {
	if len(expr.Terms) < 3 {
		return nil, fmt.Errorf("opa: expression has %d terms, need at least 3", len(expr.Terms))
	}

	op, err := extractOperator(expr.Terms[0])
	if err != nil {
		return nil, err
	}

	// Determine which term is the column ref and which is the value.
	var colTerm, valTerm compileTerm
	switch {
	case isDataRef(expr.Terms[1]):
		colTerm, valTerm = expr.Terms[1], expr.Terms[2]
	case isDataRef(expr.Terms[2]):
		colTerm, valTerm = expr.Terms[2], expr.Terms[1]
	default:
		return nil, errors.New("opa: expression has no data ref term")
	}

	colName, err := extractColumnName(colTerm)
	if err != nil {
		return nil, err
	}

	attr := table.Col(colName)
	val := valTerm.Value

	switch op {
	case "eq", "equal":
		return attr.Eq(val), nil
	case "neq":
		return attr.NotEq(val), nil
	case "lt":
		return attr.Lt(val), nil
	case "lte":
		return attr.LtEq(val), nil
	case "gt":
		return attr.Gt(val), nil
	case "gte":
		return attr.GtEq(val), nil
	case "startswith":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("opa: startswith requires string value, got %T", val)
		}
		return attr.Like(quoting.EscapeLikePattern(s) + "%"), nil
	case "endswith":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("opa: endswith requires string value, got %T", val)
		}
		return attr.Like("%" + quoting.EscapeLikePattern(s)), nil
	case "contains":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("opa: contains requires string value, got %T", val)
		}
		return attr.Like("%" + quoting.EscapeLikePattern(s) + "%"), nil
	default:
		return nil, fmt.Errorf("opa: unsupported operator %q", op)
	}
}

// --- Query set translation ---

// translateQueries converts the full query set from an OPA Compile response
// into AST condition nodes suitable for injection into a WHERE clause.
//
// Semantics:
//   - nil or empty queries = access denied (error)
//   - [[]] = unconditional allow (nil conditions, no error)
//   - Single query with expressions = each expression returned separately (AND'd by SelectCore)
//   - Multiple queries = each query AND'd internally, then OR'd together
func translateQueries(queries [][]compileExpression, table *nodes.Table) ([]nodes.Node, error) {
	if len(queries) == 0 {
		return nil, errors.New("opa: access denied")
	}

	// Check for unconditional allow: single empty query.
	if len(queries) == 1 && len(queries[0]) == 0 {
		return nil, nil
	}

	// Single query: return each expression as a separate condition.
	if len(queries) == 1 {
		var conditions []nodes.Node
		for _, expr := range queries[0] {
			node, err := translateExpression(expr, table)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, node)
		}
		return conditions, nil
	}

	// Multiple queries: each query AND'd internally, then OR'd together.
	groups := make([]nodes.Node, len(queries))
	for i, query := range queries {
		if len(query) == 0 {
			// An empty query in a multi-query set means unconditional allow
			// for that branch. Since it's OR'd, the entire result is allow.
			return nil, nil
		}
		first, err := translateExpression(query[0], table)
		if err != nil {
			return nil, err
		}
		group := first
		for j := 1; j < len(query); j++ {
			node, err := translateExpression(query[j], table)
			if err != nil {
				return nil, err
			}
			group = group.(interface {
				And(nodes.Node) *nodes.AndNode
			}).And(node)
		}
		groups[i] = group
	}

	// OR all groups together.
	result := groups[0]
	for i := 1; i < len(groups); i++ {
		result = result.(interface {
			Or(nodes.Node) *nodes.GroupingNode
		}).Or(groups[i])
	}
	return []nodes.Node{result}, nil
}

// --- Compile API request ---

type compileRequest struct {
	Query    string   `json:"query"`
	Input    any      `json:"input,omitempty"`
	Unknowns []string `json:"unknowns"`
}

// Compile calls the OPA Compile API for the given table and returns AST
// condition nodes that can be injected into a WHERE clause.
func (c *Client) Compile(tableName string) ([]nodes.Node, error) {
	reqBody := compileRequest{
		Query:    c.policyPath + " == true",
		Input:    c.input,
		Unknowns: []string{"data." + tableName},
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("opa: failed to marshal compile request: %w", err)
	}

	body, err := c.postJSON("/v1/compile", data)
	if err != nil {
		return nil, fmt.Errorf("opa: compile request failed: %w", err)
	}

	parsed, err := parseCompileResponse(body)
	if err != nil {
		return nil, err
	}

	return translateQueries(parsed.Result.Queries, nodes.NewTable(tableName))
}

// --- CompileWithMasks ---

// CompileResult holds both the row-filtering conditions and column masks.
type CompileResult struct {
	Conditions []nodes.Node
	Masks      map[string]map[string]MaskAction
}

// CompileWithMasks calls the OPA Compile API for the given table and fetches
// masks from the OPA Data API. Returns a CompileResult containing both
// row-filtering conditions and column masks.
func (c *Client) CompileWithMasks(tableName string) (*CompileResult, error) {
	conditions, err := c.Compile(tableName)
	if err != nil {
		return nil, err
	}

	masks, err := c.FetchMasks()
	if err != nil {
		return nil, err
	}

	return &CompileResult{Conditions: conditions, Masks: masks}, nil
}

// --- Data API: masks ---

// masksDataPath returns the Data API URL path for the masks rule.
// Given policyPath "data.policies.filtering.platform.consignment.include",
// it returns "policies/filtering/platform/consignment/masks".
func (c *Client) masksDataPath() string {
	path := strings.TrimPrefix(c.policyPath, "data.")
	// Strip the rule name (last segment after final dot).
	if idx := strings.LastIndex(path, "."); idx >= 0 {
		path = path[:idx]
	}
	return strings.ReplaceAll(path, ".", "/") + "/masks"
}

// FetchMasks queries the OPA Data API to evaluate the masks rule for the
// current policy. Returns nil if no masks are defined or all mask values
// are non-string (meaning "no mask").
func (c *Client) FetchMasks() (map[string]map[string]MaskAction, error) {
	type dataRequest struct {
		Input any `json:"input,omitempty"`
	}
	reqBody := dataRequest{Input: c.input}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("opa: failed to marshal data request: %w", err)
	}

	body, err := c.postJSON("/v1/data/"+c.masksDataPath(), data)
	if err != nil {
		return nil, fmt.Errorf("opa: masks request failed: %w", err)
	}

	return parseMasksResponse(body)
}

// parseMasksResponse parses the Data API response for masks.
// The response shape is: {"result": {"table": {"column": {"replace": {"value": "<MASKED>"}}}}}
// When value is {} (empty object) or non-string, the column is not masked.
func parseMasksResponse(data []byte) (map[string]map[string]MaskAction, error) {
	var resp struct {
		Result map[string]map[string]map[string]any `json:"result"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("opa: failed to parse masks response: %w", err)
	}

	if len(resp.Result) == 0 {
		return nil, nil
	}

	var masks map[string]map[string]MaskAction
	for table, columns := range resp.Result {
		for column, action := range columns {
			replaceRaw, ok := action["replace"]
			if !ok {
				continue
			}
			replaceObj, ok := replaceRaw.(map[string]any)
			if !ok {
				continue
			}
			value, ok := replaceObj["value"]
			if !ok {
				continue
			}
			// Only string values mean "mask this column".
			// Non-string values (like {} empty object) mean "no mask".
			valueStr, ok := value.(string)
			if !ok {
				continue
			}
			if masks == nil {
				masks = make(map[string]map[string]MaskAction)
			}
			if masks[table] == nil {
				masks[table] = make(map[string]MaskAction)
			}
			masks[table][column] = MaskAction{Replace: &ReplaceAction{Value: valueStr}}
		}
	}
	return masks, nil
}

// --- Explain ---

// ExplainTranslation records how a single OPA expression was translated.
type ExplainTranslation struct {
	Operator string // OPA operator (eq, neq, lt, etc.)
	Column   string // column name from data ref
	Value    any    // literal value
	SQL      string // resulting SQL fragment
}

// ExplainResult holds the diagnostic output from an Explain call.
type ExplainResult struct {
	RequestJSON        string
	RawJSON            string
	QueryCount         int
	ExpressionCount    int
	Translations       []ExplainTranslation
	Conditions         []nodes.Node
	Masks              map[string]map[string]MaskAction
	UnconditionalAllow bool
	AccessDenied       bool
}

// Explain calls the OPA Compile API and returns diagnostic information
// about how the response translates to SQL conditions.
func (c *Client) Explain(tableName string) (*ExplainResult, error) {
	reqBody := compileRequest{
		Query:    c.policyPath + " == true",
		Input:    c.input,
		Unknowns: []string{"data." + tableName},
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("opa: failed to marshal compile request: %w", err)
	}

	body, err := c.postJSON("/v1/compile", data)
	if err != nil {
		return nil, fmt.Errorf("opa: compile request failed: %w", err)
	}

	parsed, err := parseCompileResponse(body)
	if err != nil {
		return nil, err
	}

	masks, _ := c.FetchMasks() // Best-effort for diagnostics.

	result := &ExplainResult{
		RequestJSON: string(data),
		RawJSON:     string(body),
		QueryCount:  len(parsed.Result.Queries),
		Masks:       masks,
	}

	// Count total expressions.
	for _, query := range parsed.Result.Queries {
		result.ExpressionCount += len(query)
	}

	// Check for unconditional allow.
	if len(parsed.Result.Queries) == 1 && len(parsed.Result.Queries[0]) == 0 {
		result.UnconditionalAllow = true
		return result, nil
	}

	// Check for access denied — return result with raw JSON for diagnostics.
	if len(parsed.Result.Queries) == 0 {
		result.AccessDenied = true
		return result, nil
	}

	table := nodes.NewTable(tableName)

	// Build translations for each expression.
	for _, query := range parsed.Result.Queries {
		for _, expr := range query {
			op, opErr := extractOperator(expr.Terms[0])
			colName := ""
			var val any
			if len(expr.Terms) >= 3 {
				if isDataRef(expr.Terms[1]) {
					colName, _ = extractColumnName(expr.Terms[1])
					val = expr.Terms[2].Value
				} else if isDataRef(expr.Terms[2]) {
					colName, _ = extractColumnName(expr.Terms[2])
					val = expr.Terms[1].Value
				}
			}
			node, nodeErr := translateExpression(expr, table)
			sqlStr := ""
			if nodeErr == nil && node != nil {
				v := visitors.NewPostgresVisitor()
				sqlStr = node.Accept(v)
			}
			tr := ExplainTranslation{
				Column: colName,
				Value:  val,
				SQL:    sqlStr,
			}
			if opErr == nil {
				tr.Operator = op
			}
			result.Translations = append(result.Translations, tr)
		}
	}

	// Get the full translated conditions.
	conditions, err := translateQueries(parsed.Result.Queries, table)
	if err != nil {
		return nil, err
	}
	result.Conditions = conditions

	return result, nil
}

// --- Input discovery ---

// inputRefPath checks if a term is a ref starting with var "input" and returns
// the dot-joined path of remaining string elements (e.g., "subject.role").
func inputRefPath(term compileTerm) (string, bool) {
	if term.Type != "ref" {
		return "", false
	}
	parts, ok := term.Value.([]compileTerm)
	if !ok || len(parts) < 2 {
		return "", false
	}
	// First element must be var "input".
	if parts[0].Type != "var" {
		return "", false
	}
	name, ok := parts[0].Value.(string)
	if !ok || name != "input" {
		return "", false
	}
	// Collect remaining string elements into a dot-separated path.
	var segments []string
	for _, p := range parts[1:] {
		if p.Type != "string" {
			return "", false
		}
		s, ok := p.Value.(string)
		if !ok {
			return "", false
		}
		segments = append(segments, s)
	}
	if len(segments) == 0 {
		return "", false
	}
	return strings.Join(segments, "."), true
}

// extractInputPaths scans all terms in a compile response for refs starting
// with "input" and returns sorted unique dot-paths.
func extractInputPaths(resp *compileResponse) []string {
	seen := map[string]bool{}
	for _, query := range resp.Result.Queries {
		for _, expr := range query {
			for _, term := range expr.Terms {
				if path, ok := inputRefPath(term); ok {
					seen[path] = true
				}
			}
		}
	}
	paths := make([]string, 0, len(seen))
	for p := range seen {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	return paths
}

// DiscoverInputs calls the OPA Compile API with unknowns=["input"] to discover
// which input fields a policy references. Additional data paths (e.g.
// "data.consignments") can be passed so that rules referencing those paths
// also produce residuals, exposing all required input fields.
func (c *Client) DiscoverInputs(dataUnknowns ...string) ([]string, error) {
	unknowns := []string{"input"}
	unknowns = append(unknowns, dataUnknowns...)
	reqBody := compileRequest{
		Query:    c.policyPath + " == true",
		Input:    map[string]any{},
		Unknowns: unknowns,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("opa: failed to marshal compile request: %w", err)
	}

	body, err := c.postJSON("/v1/compile", data)
	if err != nil {
		return nil, fmt.Errorf("opa: compile request failed: %w", err)
	}

	parsed, err := parseCompileResponse(body)
	if err != nil {
		return nil, err
	}

	return extractInputPaths(parsed), nil
}
