package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/bawdo/gosbee/nodes"
)

// tokenize splits input into tokens, respecting single-quoted strings
// and recognising multi-char operators (!=, <>, >=, <=) and punctuation.
func tokenize(input string) []string {
	var tokens []string
	var cur strings.Builder
	inQuote := false

	flush := func() {
		if cur.Len() > 0 {
			tokens = append(tokens, cur.String())
			cur.Reset()
		}
	}

	for i := 0; i < len(input); i++ {
		ch := input[i]

		if inQuote {
			cur.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(input) && input[i+1] == '\'' {
					cur.WriteByte('\'')
					i++
				} else {
					inQuote = false
					flush()
				}
			}
			continue
		}

		switch {
		case ch == '\'':
			flush()
			cur.WriteByte(ch)
			inQuote = true

		case ch == '(' || ch == ')' || ch == ',':
			flush()
			tokens = append(tokens, string(ch))

		case ch == '!' && i+1 < len(input) && input[i+1] == '~':
			flush()
			tokens = append(tokens, "!~")
			i++
		case ch == '!' && i+1 < len(input) && input[i+1] == '=':
			flush()
			tokens = append(tokens, "!=")
			i++
		case ch == '@' && i+1 < len(input) && input[i+1] == '>':
			flush()
			tokens = append(tokens, "@>")
			i++
		case ch == '&' && i+1 < len(input) && input[i+1] == '&':
			flush()
			tokens = append(tokens, "&&")
			i++
		case ch == '|' && i+1 < len(input) && input[i+1] == '|':
			flush()
			tokens = append(tokens, "||")
			i++
		case ch == '<' && i+1 < len(input) && input[i+1] == '>':
			flush()
			tokens = append(tokens, "<>")
			i++
		case ch == '<' && i+1 < len(input) && input[i+1] == '=':
			flush()
			tokens = append(tokens, "<=")
			i++
		case ch == '<' && i+1 < len(input) && input[i+1] == '<':
			flush()
			tokens = append(tokens, "<<")
			i++
		case ch == '>' && i+1 < len(input) && input[i+1] == '=':
			flush()
			tokens = append(tokens, ">=")
			i++
		case ch == '>' && i+1 < len(input) && input[i+1] == '>':
			flush()
			tokens = append(tokens, ">>")
			i++
		case ch == '=' || ch == '>' || ch == '<':
			flush()
			tokens = append(tokens, string(ch))
		case ch == '+' || ch == '-' || ch == '*' || ch == '/' || ch == '&' || ch == '|' || ch == '^' || ch == '~':
			flush()
			tokens = append(tokens, string(ch))

		case ch == ' ' || ch == '\t':
			flush()

		default:
			cur.WriteByte(ch)
		}
	}
	flush()
	return tokens
}

// parseValue converts a token string to a Go value suitable for Literal().
func parseValue(token string) (any, error) {
	lower := strings.ToLower(token)
	if lower == "true" {
		return true, nil
	}
	if lower == "false" {
		return false, nil
	}
	if lower == "null" {
		return nil, nil
	}
	if strings.HasPrefix(token, "'") && strings.HasSuffix(token, "'") && len(token) >= 2 {
		inner := token[1 : len(token)-1]
		return strings.ReplaceAll(inner, "''", "'"), nil
	}
	if i, err := strconv.Atoi(token); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(token, 64); err == nil {
		return f, nil
	}
	return nil, fmt.Errorf("cannot parse value: %s", token)
}

// isArithOp returns true if the token is a binary arithmetic operator.
func isArithOp(token string) bool {
	switch token {
	case "+", "-", "*", "/", "&", "|", "^", "<<", ">>", "||":
		return true
	}
	return false
}

// arithOp maps a token string to a nodes.InfixOp.
func arithOp(token string) nodes.InfixOp {
	switch token {
	case "+":
		return nodes.OpPlus
	case "-":
		return nodes.OpMinus
	case "*":
		return nodes.OpMultiply
	case "/":
		return nodes.OpDivide
	case "&":
		return nodes.OpBitwiseAnd
	case "|":
		return nodes.OpBitwiseOr
	case "^":
		return nodes.OpBitwiseXor
	case "<<":
		return nodes.OpShiftLeft
	case ">>":
		return nodes.OpShiftRight
	case "||":
		return nodes.OpConcat
	default:
		panic("unreachable: invalid arithmetic operator: " + token)
	}
}

// aggregateFunc maps a lowercase function name to its AggregateFunc enum.
func aggregateFunc(name string) (nodes.AggregateFunc, bool) {
	switch name {
	case "count":
		return nodes.AggCount, true
	case "sum":
		return nodes.AggSum, true
	case "avg":
		return nodes.AggAvg, true
	case "min":
		return nodes.AggMin, true
	case "max":
		return nodes.AggMax, true
	default:
		return 0, false
	}
}

// extractField maps a lowercase field name to its ExtractField enum.
func extractField(name string) (nodes.ExtractField, bool) {
	switch strings.ToLower(name) {
	case "year":
		return nodes.ExtractYear, true
	case "month":
		return nodes.ExtractMonth, true
	case "day":
		return nodes.ExtractDay, true
	case "hour":
		return nodes.ExtractHour, true
	case "minute":
		return nodes.ExtractMinute, true
	case "second":
		return nodes.ExtractSecond, true
	case "dow":
		return nodes.ExtractDow, true
	case "doy":
		return nodes.ExtractDoy, true
	case "epoch":
		return nodes.ExtractEpoch, true
	case "quarter":
		return nodes.ExtractQuarter, true
	case "week":
		return nodes.ExtractWeek, true
	default:
		return 0, false
	}
}

// windowFunc maps a lowercase function name to its WindowFunc enum.
func windowFunc(name string) (nodes.WindowFunc, bool) {
	switch name {
	case "row_number":
		return nodes.WinRowNumber, true
	case "rank":
		return nodes.WinRank, true
	case "dense_rank":
		return nodes.WinDenseRank, true
	case "ntile":
		return nodes.WinNtile, true
	case "lag":
		return nodes.WinLag, true
	case "lead":
		return nodes.WinLead, true
	case "first_value":
		return nodes.WinFirstValue, true
	case "last_value":
		return nodes.WinLastValue, true
	case "nth_value":
		return nodes.WinNthValue, true
	case "cume_dist":
		return nodes.WinCumeDist, true
	case "percent_rank":
		return nodes.WinPercentRank, true
	default:
		return 0, false
	}
}

// isNamedFunc returns true if the lowercase token is a known named function.
func isNamedFunc(name string) bool {
	switch name {
	case "coalesce", "lower", "upper", "substring", "cast",
		"greatest", "least", "nullif", "abs", "length",
		"trim", "replace", "concat", "left", "right",
		"round", "ceil", "floor", "now", "current_date",
		"current_timestamp":
		return true
	}
	return false
}

// parseAtom parses a single atom: function call, column reference, or literal value.
func (s *Session) parseAtom(tokens []string, pos int) (nodes.Node, int, error) {
	if pos >= len(tokens) {
		return nil, pos, errors.New("expected expression")
	}

	token := tokens[pos]
	lower := strings.ToLower(token)

	// CASE expression
	if lower == "case" {
		return s.parseCaseExpr(tokens, pos)
	}

	// Check for function call: name followed by (
	if pos+1 < len(tokens) && tokens[pos+1] == "(" {
		if fn, ok := aggregateFunc(lower); ok {
			return s.parseAggregateCall(tokens, pos, fn)
		}
		if lower == "extract" {
			return s.parseExtractCall(tokens, pos)
		}
		if _, ok := windowFunc(lower); ok {
			return s.parseWindowFuncCall(tokens, pos)
		}
		// Named functions (known or arbitrary name( pattern)
		if isNamedFunc(lower) || isIdentifier(token) {
			return s.parseNamedFuncCall(tokens, pos)
		}
	}

	// Column reference.
	if strings.Contains(token, ".") && !strings.HasPrefix(token, "'") {
		col, err := s.resolveColRef(token)
		if err != nil {
			return nil, pos, err
		}
		return col, pos + 1, nil
	}

	// Literal value.
	val, err := parseValue(token)
	if err != nil {
		return nil, pos, err
	}
	return nodes.Literal(val), pos + 1, nil
}

// isIdentifier returns true if the token looks like a SQL identifier (starts with letter/underscore).
func isIdentifier(token string) bool {
	if len(token) == 0 {
		return false
	}
	ch := token[0]
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

// parseNamedFuncCall parses NAME(args) with special handling for CAST(expr AS type).
func (s *Session) parseNamedFuncCall(tokens []string, pos int) (nodes.Node, int, error) {
	funcName := tokens[pos]
	upper := strings.ToUpper(funcName)
	pos++ // skip function name
	if pos >= len(tokens) || tokens[pos] != "(" {
		return nil, pos, fmt.Errorf("expected ( after %s", funcName)
	}
	pos++ // skip (

	// Special handling for CAST(expr AS type)
	if upper == "CAST" {
		expr, nextPos, err := s.parseArithExpr(tokens, pos)
		if err != nil {
			return nil, pos, err
		}
		pos = nextPos
		if pos >= len(tokens) || strings.ToLower(tokens[pos]) != "as" {
			return nil, pos, errors.New("expected AS in CAST expression")
		}
		pos++ // skip AS
		// Collect type tokens until closing )
		var typeParts []string
		depth := 0
	TypeLoop:
		for pos < len(tokens) {
			switch tokens[pos] {
			case "(":
				depth++
				typeParts = append(typeParts, tokens[pos])
				pos++
			case ")":
				if depth == 0 {
					break TypeLoop
				}
				depth--
				typeParts = append(typeParts, tokens[pos])
				pos++
			default:
				typeParts = append(typeParts, tokens[pos])
				pos++
			}
		}
		if pos >= len(tokens) || tokens[pos] != ")" {
			return nil, pos, errors.New("expected ) after CAST type")
		}
		pos++ // skip )
		typeName := strings.Join(typeParts, " ")
		fn := nodes.Cast(expr, typeName)

		// Check for OVER clause
		if pos < len(tokens) && strings.ToLower(tokens[pos]) == "over" {
			return s.parseOverClause(fn, tokens, pos)
		}
		return fn, pos, nil
	}

	// General named function: NAME([DISTINCT] arg1, arg2, ...)
	distinct := false
	if pos < len(tokens) && strings.ToLower(tokens[pos]) == "distinct" {
		distinct = true
		pos++
	}

	var args []nodes.Node
	for pos < len(tokens) && tokens[pos] != ")" {
		if tokens[pos] == "," {
			pos++
			continue
		}
		arg, nextPos, err := s.parseArithExpr(tokens, pos)
		if err != nil {
			return nil, pos, err
		}
		args = append(args, arg)
		pos = nextPos
	}

	if pos >= len(tokens) || tokens[pos] != ")" {
		return nil, pos, fmt.Errorf("expected ) after %s arguments", funcName)
	}
	pos++ // skip )

	fn := nodes.NewNamedFunction(upper, args...)
	fn.Distinct = distinct

	// Check for OVER clause
	if pos < len(tokens) && strings.ToLower(tokens[pos]) == "over" {
		return s.parseOverClause(fn, tokens, pos)
	}

	return fn, pos, nil
}

// scanUntilKeyword scans tokens starting at pos, tracking parenthesis depth,
// and returns the collected tokens and the position of the first top-level
// keyword match. If no keyword is found, pos will be at len(tokens).
func scanUntilKeyword(tokens []string, pos int, keywords ...string) ([]string, int) {
	start := pos
	depth := 0
	for pos < len(tokens) {
		switch {
		case tokens[pos] == "(":
			depth++
		case tokens[pos] == ")":
			depth--
		case depth == 0:
			lower := strings.ToLower(tokens[pos])
			for _, kw := range keywords {
				if lower == kw {
					return tokens[start:pos], pos
				}
			}
		}
		pos++
	}
	return tokens[start:pos], pos
}

// parseCaseExpr parses CASE [operand] WHEN cond THEN result ... [ELSE result] END.
func (s *Session) parseCaseExpr(tokens []string, pos int) (nodes.Node, int, error) {
	pos++ // skip CASE

	if pos >= len(tokens) {
		return nil, pos, errors.New("expected WHEN or operand after CASE")
	}

	var caseNode *nodes.CaseNode

	// Check if next token is WHEN (searched CASE) or an operand (simple CASE)
	if strings.ToLower(tokens[pos]) == "when" {
		caseNode = nodes.NewCase()
	} else {
		// Parse operand expression — simple tokens up to WHEN
		operand, nextPos, err := s.parseArithExpr(tokens, pos)
		if err != nil {
			return nil, pos, err
		}
		caseNode = nodes.NewCase(operand)
		pos = nextPos
	}

	// Parse WHEN ... THEN ... pairs
	for pos < len(tokens) && strings.ToLower(tokens[pos]) == "when" {
		pos++ // skip WHEN

		condTokens, nextPos := scanUntilKeyword(tokens, pos, "then")
		if nextPos >= len(tokens) || strings.ToLower(tokens[nextPos]) != "then" {
			return nil, nextPos, errors.New("expected THEN in CASE expression")
		}
		pos = nextPos + 1 // skip THEN

		resultTokens, nextPos := scanUntilKeyword(tokens, pos, "when", "else", "end")
		pos = nextPos

		cond, err := s.parseConditionOrExpr(condTokens)
		if err != nil {
			return nil, pos, fmt.Errorf("CASE WHEN condition: %w", err)
		}
		result, _, err := s.parseArithExpr(resultTokens, 0)
		if err != nil {
			return nil, pos, fmt.Errorf("CASE THEN result: %w", err)
		}
		caseNode.When(cond, result)
	}

	// ELSE
	if pos < len(tokens) && strings.ToLower(tokens[pos]) == "else" {
		pos++ // skip ELSE
		elseTokens, nextPos := scanUntilKeyword(tokens, pos, "end")
		pos = nextPos
		elseVal, _, err := s.parseArithExpr(elseTokens, 0)
		if err != nil {
			return nil, pos, fmt.Errorf("CASE ELSE: %w", err)
		}
		caseNode.Else(elseVal)
	}

	// END
	if pos >= len(tokens) || strings.ToLower(tokens[pos]) != "end" {
		return nil, pos, errors.New("expected END in CASE expression")
	}
	pos++ // skip END

	return caseNode, pos, nil
}

// parseConditionOrExpr tries to parse tokens as a condition (with operators like =, >, etc.)
// or falls back to a simple arithmetic expression (for simple CASE operand matching).
func (s *Session) parseConditionOrExpr(tokens []string) (nodes.Node, error) {
	if len(tokens) == 0 {
		return nil, errors.New("empty expression")
	}
	// Try as condition first (has comparison operators).
	node, err := s.parseConditionFromTokens(tokens)
	if err == nil {
		return node, nil
	}
	// Fall back to arithmetic expression.
	n, _, err2 := s.parseArithExpr(tokens, 0)
	if err2 != nil {
		return nil, err // return original error
	}
	return n, nil
}

// parseAggregateCall parses COUNT(...), SUM(...), etc. including optional
// DISTINCT and FILTER (WHERE ...) clauses.
func (s *Session) parseAggregateCall(tokens []string, pos int, fn nodes.AggregateFunc) (nodes.Node, int, error) {
	funcName := tokens[pos]
	pos++ // skip function name
	if pos >= len(tokens) || tokens[pos] != "(" {
		return nil, pos, fmt.Errorf("expected ( after %s", funcName)
	}
	pos++ // skip (

	distinct := false
	if pos < len(tokens) && strings.ToLower(tokens[pos]) == "distinct" {
		distinct = true
		pos++
	}

	var expr nodes.Node
	if pos < len(tokens) && tokens[pos] == "*" {
		// COUNT(*) — expr stays nil
		pos++
	} else if pos < len(tokens) && tokens[pos] != ")" {
		var err error
		expr, pos, err = s.parseArithExpr(tokens, pos)
		if err != nil {
			return nil, pos, err
		}
	}

	if pos >= len(tokens) || tokens[pos] != ")" {
		return nil, pos, fmt.Errorf("expected ) after %s arguments", funcName)
	}
	pos++ // skip )

	n := nodes.NewAggregateNode(fn, expr)
	n.Distinct = distinct

	// Check for FILTER (WHERE ...)
	if pos < len(tokens) && strings.ToLower(tokens[pos]) == "filter" {
		pos++
		if pos >= len(tokens) || tokens[pos] != "(" {
			return nil, pos, errors.New("expected ( after FILTER")
		}
		pos++ // skip (
		if pos >= len(tokens) || strings.ToLower(tokens[pos]) != "where" {
			return nil, pos, errors.New("expected WHERE after FILTER (")
		}
		pos++ // skip WHERE

		// Collect tokens until matching closing ).
		depth := 1
		start := pos
		for pos < len(tokens) {
			if tokens[pos] == "(" {
				depth++
			}
			if tokens[pos] == ")" {
				depth--
				if depth == 0 {
					break
				}
			}
			pos++
		}
		if depth != 0 {
			return nil, pos, errors.New("unmatched ( in FILTER clause")
		}
		filterTokens := tokens[start:pos]
		filterNode, err := s.parseConditionFromTokens(filterTokens)
		if err != nil {
			return nil, pos, fmt.Errorf("FILTER condition: %w", err)
		}
		n = n.WithFilter(filterNode)
		pos++ // skip )
	}

	// Check for OVER clause (aggregate as window function)
	if pos < len(tokens) && strings.ToLower(tokens[pos]) == "over" {
		return s.parseOverClause(n, tokens, pos)
	}

	return n, pos, nil
}

// parseExtractCall parses EXTRACT(field FROM expr).
func (s *Session) parseExtractCall(tokens []string, pos int) (nodes.Node, int, error) {
	pos++ // skip "extract"
	if pos >= len(tokens) || tokens[pos] != "(" {
		return nil, pos, errors.New("expected ( after EXTRACT")
	}
	pos++ // skip (

	if pos >= len(tokens) {
		return nil, pos, errors.New("expected field name in EXTRACT")
	}
	field, ok := extractField(tokens[pos])
	if !ok {
		return nil, pos, fmt.Errorf("unknown EXTRACT field: %s (expected YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DOW, DOY, EPOCH, QUARTER, WEEK)", tokens[pos])
	}
	pos++

	if pos >= len(tokens) || strings.ToLower(tokens[pos]) != "from" {
		return nil, pos, errors.New("expected FROM after EXTRACT field")
	}
	pos++ // skip FROM

	expr, pos, err := s.parseArithExpr(tokens, pos)
	if err != nil {
		return nil, pos, err
	}

	if pos >= len(tokens) || tokens[pos] != ")" {
		return nil, pos, errors.New("expected ) after EXTRACT expression")
	}
	pos++ // skip )

	return nodes.NewExtractNode(field, expr), pos, nil
}

// parseArithExpr parses a chain of arithmetic operations from tokens starting
// at pos. Returns the resulting node, the next position, and any error.
func (s *Session) parseArithExpr(tokens []string, pos int) (nodes.Node, int, error) {
	if pos >= len(tokens) {
		return nil, pos, errors.New("expected expression")
	}

	// Check for ~ (unary bitwise NOT) prefix.
	unary := false
	if tokens[pos] == "~" {
		unary = true
		pos++
		if pos >= len(tokens) {
			return nil, pos, errors.New("expected expression after ~")
		}
	}

	// Parse atom: function call, column reference, or literal value.
	atom, pos, err := s.parseAtom(tokens, pos)
	if err != nil {
		return nil, pos, err
	}

	// Apply unary ~ if present.
	if unary {
		atom = nodes.NewUnaryMathNode(atom, nodes.OpBitwiseNot)
	}

	// Loop: while next token is an arithmetic operator, chain operations.
	for pos < len(tokens) && isArithOp(tokens[pos]) {
		opToken := tokens[pos]
		pos++

		if pos >= len(tokens) {
			return nil, pos, fmt.Errorf("expected expression after %s", opToken)
		}

		// Check for ~ prefix on next atom.
		nextUnary := false
		if tokens[pos] == "~" {
			nextUnary = true
			pos++
			if pos >= len(tokens) {
				return nil, pos, errors.New("expected expression after ~")
			}
		}

		// Parse next atom.
		nextAtom, nextPos, err := s.parseAtom(tokens, pos)
		if err != nil {
			return nil, pos, err
		}
		pos = nextPos

		// Apply unary ~ if present.
		if nextUnary {
			nextAtom = nodes.NewUnaryMathNode(nextAtom, nodes.OpBitwiseNot)
		}

		// Build InfixNode.
		atom = nodes.NewInfixNode(atom, nextAtom, arithOp(opToken))
	}

	return atom, pos, nil
}

// resolveColRef resolves "table.column" into an *Attribute using registered
// tables and aliases in the session.
func (s *Session) resolveColRef(ref string) (*nodes.Attribute, error) {
	if strings.ContainsAny(ref, ", \t") {
		return nil, fmt.Errorf("expected table.column, got %q (use commas to separate multiple columns)", ref)
	}
	parts := strings.SplitN(ref, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("expected table.column, got %q", ref)
	}
	name := parts[0]
	col := parts[1]

	if a, ok := s.aliases[name]; ok {
		return a.Col(col), nil
	}
	if t, ok := s.tables[name]; ok {
		return t.Col(col), nil
	}
	return nil, fmt.Errorf("unknown table or alias %q (register with 'table %s' first)", name, name)
}

// comparisonOp maps a comparison operator token to a nodes.ComparisonOp.
// Returns the op and true if valid, or false if not a comparison operator.
func comparisonOp(token string) (nodes.ComparisonOp, bool) {
	switch token {
	case "=":
		return nodes.OpEq, true
	case "!=", "<>":
		return nodes.OpNotEq, true
	case ">":
		return nodes.OpGt, true
	case ">=":
		return nodes.OpGtEq, true
	case "<":
		return nodes.OpLt, true
	case "<=":
		return nodes.OpLtEq, true
	case "like":
		return nodes.OpLike, true
	case "regexp":
		return nodes.OpRegexp, true
	case "@>":
		return nodes.OpContains, true
	case "&&":
		return nodes.OpOverlaps, true
	default:
		return 0, false
	}
}

// parseCondition parses a condition string like "users.age > 18" or
// "users.age + 5 > 10" into a Node.
func (s *Session) parseCondition(input string) (nodes.Node, error) {
	tokens := tokenize(input)
	return s.parseConditionFromTokens(tokens)
}

// requireAttribute asserts that node is an *Attribute. Returns an error
// mentioning the operator if not.
func requireAttribute(n nodes.Node, op string) (*nodes.Attribute, error) {
	if col, ok := n.(*nodes.Attribute); ok {
		return col, nil
	}
	return nil, fmt.Errorf("%s requires a simple column reference (table.column), not an arithmetic expression", op)
}

func parseIsCondition(col *nodes.Attribute, tokens []string) (nodes.Node, error) {
	if len(tokens) == 0 {
		return nil, errors.New("expected NULL, NOT NULL, DISTINCT FROM, or NOT DISTINCT FROM after IS")
	}
	t0 := strings.ToLower(tokens[0])
	if t0 == "null" {
		return col.IsNull(), nil
	}
	// IS DISTINCT FROM <value>
	if t0 == "distinct" && len(tokens) >= 3 && strings.ToLower(tokens[1]) == "from" {
		val, err := parseValue(tokens[2])
		if err != nil {
			return nil, err
		}
		return col.IsDistinctFrom(val), nil
	}
	if t0 == "not" && len(tokens) > 1 {
		t1 := strings.ToLower(tokens[1])
		if t1 == "null" {
			return col.IsNotNull(), nil
		}
		// IS NOT DISTINCT FROM <value>
		if t1 == "distinct" && len(tokens) >= 4 && strings.ToLower(tokens[2]) == "from" {
			val, err := parseValue(tokens[3])
			if err != nil {
				return nil, err
			}
			return col.IsNotDistinctFrom(val), nil
		}
	}
	return nil, errors.New("expected NULL, NOT NULL, DISTINCT FROM, or NOT DISTINCT FROM after IS")
}

func (s *Session) parseNotCondition(col *nodes.Attribute, tokens []string) (nodes.Node, error) {
	if len(tokens) == 0 {
		return nil, errors.New("expected IN, LIKE, BETWEEN, or REGEXP after NOT")
	}
	switch strings.ToLower(tokens[0]) {
	case "in":
		return parseInCondition(col, tokens[1:], true)
	case "like":
		if len(tokens) < 2 {
			return nil, errors.New("missing value after NOT LIKE")
		}
		val, err := parseValue(tokens[1])
		if err != nil {
			return nil, err
		}
		return col.NotLike(val), nil
	case "between":
		return parseNotBetweenCondition(col, tokens[1:])
	case "regexp":
		if len(tokens) < 2 {
			return nil, errors.New("missing value after NOT REGEXP")
		}
		val, err := parseValue(tokens[1])
		if err != nil {
			return nil, err
		}
		return col.DoesNotMatchRegexp(val), nil
	default:
		return nil, fmt.Errorf("expected IN, LIKE, BETWEEN, or REGEXP after NOT, got %s", tokens[0])
	}
}

func parseInCondition(col *nodes.Attribute, tokens []string, negate bool) (nodes.Node, error) {
	var vals []any
	for _, t := range tokens {
		if t == "(" || t == ")" || t == "," {
			continue
		}
		val, err := parseValue(t)
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	if len(vals) == 0 {
		return nil, errors.New("IN requires at least one value")
	}
	if negate {
		return col.NotIn(vals...), nil
	}
	return col.In(vals...), nil
}

func parseBetweenCondition(col *nodes.Attribute, tokens []string) (nodes.Node, error) {
	if len(tokens) < 3 {
		return nil, errors.New("expected: BETWEEN <low> AND <high>")
	}
	low, err := parseValue(tokens[0])
	if err != nil {
		return nil, err
	}
	if strings.ToLower(tokens[1]) != "and" {
		return nil, errors.New("expected AND between BETWEEN values")
	}
	high, err := parseValue(tokens[2])
	if err != nil {
		return nil, err
	}
	return col.Between(low, high), nil
}

func parseNotBetweenCondition(col *nodes.Attribute, tokens []string) (nodes.Node, error) {
	if len(tokens) < 3 {
		return nil, errors.New("expected: NOT BETWEEN <low> AND <high>")
	}
	low, err := parseValue(tokens[0])
	if err != nil {
		return nil, err
	}
	if strings.ToLower(tokens[1]) != "and" {
		return nil, errors.New("expected AND between NOT BETWEEN values")
	}
	high, err := parseValue(tokens[2])
	if err != nil {
		return nil, err
	}
	return col.NotBetween(low, high), nil
}

// exprPart holds a segment of tokens forming a single condition, plus the
// combinator keyword ("and" or "or") that follows it. The last part has
// an empty combinator.
type exprPart struct {
	tokens    []string
	combinator string // "and", "or", or ""
}

// splitExpressionParts splits tokens on top-level AND/OR keywords, respecting
// parenthesised groups and BETWEEN ... AND ... ranges.
func splitExpressionParts(tokens []string) []exprPart {
	var parts []exprPart
	var cur []string
	depth := 0
	inBetween := false

	for i := 0; i < len(tokens); i++ {
		lower := strings.ToLower(tokens[i])

		if lower == "(" {
			depth++
			cur = append(cur, tokens[i])
			continue
		}
		if lower == ")" {
			depth--
			cur = append(cur, tokens[i])
			continue
		}

		if depth > 0 {
			cur = append(cur, tokens[i])
			continue
		}

		if lower == "between" {
			inBetween = true
			cur = append(cur, tokens[i])
			continue
		}

		// "NOT BETWEEN" also has a BETWEEN-style AND that should not split.
		if lower == "not" && i+1 < len(tokens) && strings.ToLower(tokens[i+1]) == "between" {
			cur = append(cur, tokens[i])
			i++
			inBetween = true
			cur = append(cur, tokens[i])
			continue
		}

		if lower == "and" && inBetween {
			// This AND belongs to BETWEEN ... AND ..., not a combinator.
			inBetween = false
			cur = append(cur, tokens[i])
			continue
		}

		if (lower == "and" || lower == "or") && depth == 0 {
			parts = append(parts, exprPart{tokens: cur, combinator: lower})
			cur = nil
			continue
		}

		cur = append(cur, tokens[i])
	}

	if len(cur) > 0 {
		parts = append(parts, exprPart{tokens: cur})
	}
	return parts
}

// parseExpression parses an expression with AND/OR combinators and optional
// NOT prefix. AND binds tighter than OR (standard SQL precedence).
func (s *Session) parseExpression(input string) (nodes.Node, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, errors.New("empty expression")
	}

	tokens := tokenize(input)
	parts := splitExpressionParts(tokens)

	if len(parts) == 0 {
		return nil, errors.New("empty expression")
	}

	// Group by OR: collect runs of AND-connected parts, then OR them together.
	// Each "OR group" is a slice of parts that are AND-connected.
	type orGroup struct {
		parts []exprPart
	}
	var groups []orGroup
	var curGroup []exprPart

	for _, p := range parts {
		curGroup = append(curGroup, p)
		if p.combinator == "or" || p.combinator == "" {
			groups = append(groups, orGroup{parts: curGroup})
			curGroup = nil
		}
		// If combinator is "and", keep accumulating into curGroup.
	}

	// Parse each group: within a group, parts are AND-connected.
	var orNodes []nodes.Node
	for _, g := range groups {
		var andNode nodes.Node
		for _, p := range g.parts {
			cond, err := s.parseSingleCondition(p.tokens)
			if err != nil {
				return nil, err
			}
			if andNode == nil {
				andNode = cond
			} else {
				andNode = andNode.(interface{ And(nodes.Node) *nodes.AndNode }).And(cond)
			}
		}
		orNodes = append(orNodes, andNode)
	}

	// Chain OR groups.
	result := orNodes[0]
	for i := 1; i < len(orNodes); i++ {
		result = result.(interface{ Or(nodes.Node) *nodes.GroupingNode }).Or(orNodes[i])
	}

	return result, nil
}

// parseSingleCondition handles an optional NOT prefix then delegates to parseConditionFromTokens.
func (s *Session) parseSingleCondition(tokens []string) (nodes.Node, error) {
	if len(tokens) == 0 {
		return nil, errors.New("empty condition")
	}

	if strings.ToLower(tokens[0]) == "not" {
		inner, err := s.parseSingleCondition(tokens[1:])
		if err != nil {
			return nil, err
		}
		return inner.(interface{ Not() *nodes.NotNode }).Not(), nil
	}

	return s.parseConditionFromTokens(tokens)
}

// parseConditionFromTokens parses a single condition from pre-tokenized input.
// It supports arithmetic expressions on the left side (and right side for
// binary comparison operators like =, !=, >, >=, <, <=, like).
func (s *Session) parseConditionFromTokens(tokens []string) (nodes.Node, error) {
	if len(tokens) < 2 {
		return nil, errors.New("expected: <table.column> <operator> <value>")
	}

	// Parse left side as an arithmetic expression.
	leftNode, pos, err := s.parseArithExpr(tokens, 0)
	if err != nil {
		return nil, err
	}

	if pos >= len(tokens) {
		return nil, errors.New("expected operator after expression")
	}

	op := strings.ToLower(tokens[pos])

	// Binary comparison operators: both sides support arithmetic.
	if cmpOp, ok := comparisonOp(op); ok {
		pos++
		if pos >= len(tokens) {
			return nil, errors.New("missing value after operator")
		}
		rightNode, _, err := s.parseArithExpr(tokens, pos)
		if err != nil {
			return nil, err
		}
		return nodes.NewComparisonNode(leftNode, rightNode, cmpOp), nil
	}

	// Operators that require a simple column reference on the left side.
	switch op {
	case "is":
		col, err := requireAttribute(leftNode, "IS")
		if err != nil {
			return nil, err
		}
		return parseIsCondition(col, tokens[pos+1:])
	case "not":
		col, err := requireAttribute(leftNode, "NOT")
		if err != nil {
			return nil, err
		}
		return s.parseNotCondition(col, tokens[pos+1:])
	case "in":
		col, err := requireAttribute(leftNode, "IN")
		if err != nil {
			return nil, err
		}
		return parseInCondition(col, tokens[pos+1:], false)
	case "between":
		col, err := requireAttribute(leftNode, "BETWEEN")
		if err != nil {
			return nil, err
		}
		return parseBetweenCondition(col, tokens[pos+1:])
	default:
		return nil, fmt.Errorf("unknown operator: %s", op)
	}
}

// parseWindowFuncCall parses a window function call like ROW_NUMBER(), NTILE(4),
// LAG(table.col, 1, 0), etc., and then expects an OVER clause.
func (s *Session) parseWindowFuncCall(tokens []string, pos int) (nodes.Node, int, error) {
	funcName := tokens[pos]
	fn, _ := windowFunc(strings.ToLower(funcName))
	pos++ // skip function name
	if pos >= len(tokens) || tokens[pos] != "(" {
		return nil, pos, fmt.Errorf("expected ( after %s", funcName)
	}
	pos++ // skip (

	// Parse arguments (may be empty for ROW_NUMBER, RANK, etc.)
	var args []nodes.Node
	for pos < len(tokens) && tokens[pos] != ")" {
		if tokens[pos] == "," {
			pos++
			continue
		}
		arg, nextPos, err := s.parseArithExpr(tokens, pos)
		if err != nil {
			return nil, pos, err
		}
		args = append(args, arg)
		pos = nextPos
	}

	if pos >= len(tokens) || tokens[pos] != ")" {
		return nil, pos, fmt.Errorf("expected ) after %s arguments", funcName)
	}
	pos++ // skip )

	wfn := &nodes.WindowFuncNode{Func: fn, Args: args}

	// Window functions require OVER clause.
	if pos >= len(tokens) || strings.ToLower(tokens[pos]) != "over" {
		return nil, pos, fmt.Errorf("window function %s requires OVER clause", funcName)
	}

	return s.parseOverClause(wfn, tokens, pos)
}

// overNode creates a properly initialized OverNode by dispatching to the
// appropriate Over/OverName method on the expression type.
func overNode(expr nodes.Node, def *nodes.WindowDefinition, name string) *nodes.OverNode {
	if name != "" {
		switch e := expr.(type) {
		case *nodes.WindowFuncNode:
			return e.OverName(name)
		case *nodes.AggregateNode:
			return e.OverName(name)
		case *nodes.NamedFunctionNode:
			return e.OverName(name)
		}
	}
	switch e := expr.(type) {
	case *nodes.WindowFuncNode:
		return e.Over(def)
	case *nodes.AggregateNode:
		return e.Over(def)
	case *nodes.NamedFunctionNode:
		return e.Over(def)
	}
	// Fallback: should not happen with correct usage.
	return nil
}

// parseOverClause parses OVER (...) or OVER name after any expression.
// The expr parameter is the expression (WindowFuncNode or AggregateNode) being wrapped.
func (s *Session) parseOverClause(expr nodes.Node, tokens []string, pos int) (nodes.Node, int, error) {
	pos++ // skip OVER

	if pos >= len(tokens) {
		return nil, pos, errors.New("expected window name or ( after OVER")
	}

	// OVER name (named window reference)
	if tokens[pos] != "(" {
		name := tokens[pos]
		pos++
		over := overNode(expr, nil, name)
		if over == nil {
			return nil, pos, errors.New("OVER requires a window function or aggregate")
		}
		return over, pos, nil
	}

	// OVER (...) — parse inline window definition
	pos++ // skip (
	def, nextPos, err := s.parseWindowDef(tokens, pos)
	if err != nil {
		return nil, pos, err
	}
	pos = nextPos

	if pos >= len(tokens) || tokens[pos] != ")" {
		return nil, pos, errors.New("expected ) after OVER clause")
	}
	pos++ // skip )

	over := overNode(expr, def, "")
	if over == nil {
		return nil, pos, errors.New("OVER requires a window function or aggregate")
	}
	return over, pos, nil
}

// parseWindowDef parses the contents inside OVER (...):
// [PARTITION BY cols] [ORDER BY cols] [ROWS/RANGE frame]
func (s *Session) parseWindowDef(tokens []string, pos int) (*nodes.WindowDefinition, int, error) {
	def := nodes.NewWindowDef()

	// PARTITION BY
	if pos < len(tokens) && strings.ToLower(tokens[pos]) == "partition" {
		pos++
		if pos >= len(tokens) || strings.ToLower(tokens[pos]) != "by" {
			return nil, pos, errors.New("expected BY after PARTITION")
		}
		pos++ // skip BY

		for pos < len(tokens) {
			lower := strings.ToLower(tokens[pos])
			if lower == "order" || lower == "rows" || lower == "range" || tokens[pos] == ")" {
				break
			}
			if tokens[pos] == "," {
				pos++
				continue
			}
			col, nextPos, err := s.parseArithExpr(tokens, pos)
			if err != nil {
				return nil, pos, err
			}
			def.PartitionBy = append(def.PartitionBy, col)
			pos = nextPos
		}
	}

	// ORDER BY
	if pos < len(tokens) && strings.ToLower(tokens[pos]) == "order" {
		pos++
		if pos >= len(tokens) || strings.ToLower(tokens[pos]) != "by" {
			return nil, pos, errors.New("expected BY after ORDER")
		}
		pos++ // skip BY

		for pos < len(tokens) {
			lower := strings.ToLower(tokens[pos])
			if lower == "rows" || lower == "range" || tokens[pos] == ")" {
				break
			}
			if tokens[pos] == "," {
				pos++
				continue
			}
			expr, nextPos, err := s.parseArithExpr(tokens, pos)
			if err != nil {
				return nil, pos, err
			}
			pos = nextPos

			// Check for ASC/DESC
			dir := nodes.Asc
			if pos < len(tokens) {
				switch strings.ToLower(tokens[pos]) {
				case "asc":
					pos++
				case "desc":
					dir = nodes.Desc
					pos++
				}
			}

			if dir == nodes.Desc {
				def.OrderBy = append(def.OrderBy, &nodes.OrderingNode{Expr: expr, Direction: nodes.Desc})
			} else {
				def.OrderBy = append(def.OrderBy, &nodes.OrderingNode{Expr: expr, Direction: nodes.Asc})
			}
		}
	}

	// ROWS / RANGE frame
	if pos < len(tokens) {
		lower := strings.ToLower(tokens[pos])
		if lower == "rows" || lower == "range" {
			frame, nextPos, err := s.parseFrameSpec(tokens, pos)
			if err != nil {
				return nil, pos, err
			}
			def.Frame = frame
			pos = nextPos
		}
	}

	return def, pos, nil
}

// parseFrameSpec parses ROWS/RANGE [BETWEEN bound AND bound | bound].
func (s *Session) parseFrameSpec(tokens []string, pos int) (*nodes.WindowFrame, int, error) {
	frameType := nodes.FrameRows
	if strings.ToLower(tokens[pos]) == "range" {
		frameType = nodes.FrameRange
	}
	pos++ // skip ROWS/RANGE

	if pos >= len(tokens) {
		return nil, pos, errors.New("expected frame bound after ROWS/RANGE")
	}

	if strings.ToLower(tokens[pos]) == "between" {
		pos++ // skip BETWEEN
		start, nextPos, err := s.parseFrameBound(tokens, pos)
		if err != nil {
			return nil, pos, err
		}
		pos = nextPos

		if pos >= len(tokens) || strings.ToLower(tokens[pos]) != "and" {
			return nil, pos, errors.New("expected AND in frame BETWEEN clause")
		}
		pos++ // skip AND

		end, nextPos, err := s.parseFrameBound(tokens, pos)
		if err != nil {
			return nil, pos, err
		}
		pos = nextPos

		return &nodes.WindowFrame{Type: frameType, Start: start, End: &end}, pos, nil
	}

	// Single bound (no BETWEEN)
	start, nextPos, err := s.parseFrameBound(tokens, pos)
	if err != nil {
		return nil, pos, err
	}
	return &nodes.WindowFrame{Type: frameType, Start: start}, nextPos, nil
}

// parseFrameBound parses a single frame bound:
// UNBOUNDED PRECEDING, N PRECEDING, CURRENT ROW, N FOLLOWING, UNBOUNDED FOLLOWING.
func (s *Session) parseFrameBound(tokens []string, pos int) (nodes.FrameBound, int, error) {
	if pos >= len(tokens) {
		return nodes.FrameBound{}, pos, errors.New("expected frame bound")
	}

	lower := strings.ToLower(tokens[pos])

	if lower == "unbounded" {
		pos++
		if pos >= len(tokens) {
			return nodes.FrameBound{}, pos, errors.New("expected PRECEDING or FOLLOWING after UNBOUNDED")
		}
		dir := strings.ToLower(tokens[pos])
		pos++
		if dir == "preceding" {
			return nodes.UnboundedPreceding(), pos, nil
		}
		if dir == "following" {
			return nodes.UnboundedFollowing(), pos, nil
		}
		return nodes.FrameBound{}, pos, fmt.Errorf("expected PRECEDING or FOLLOWING after UNBOUNDED, got %s", dir)
	}

	if lower == "current" {
		pos++
		if pos >= len(tokens) || strings.ToLower(tokens[pos]) != "row" {
			return nodes.FrameBound{}, pos, errors.New("expected ROW after CURRENT")
		}
		pos++ // skip ROW
		return nodes.CurrentRow(), pos, nil
	}

	// N PRECEDING or N FOLLOWING
	val, err := parseValue(tokens[pos])
	if err != nil {
		return nodes.FrameBound{}, pos, fmt.Errorf("expected frame bound value: %w", err)
	}
	pos++
	if pos >= len(tokens) {
		return nodes.FrameBound{}, pos, errors.New("expected PRECEDING or FOLLOWING after offset")
	}
	dir := strings.ToLower(tokens[pos])
	pos++
	if dir == "preceding" {
		return nodes.Preceding(nodes.Literal(val)), pos, nil
	}
	if dir == "following" {
		return nodes.Following(nodes.Literal(val)), pos, nil
	}
	return nodes.FrameBound{}, pos, fmt.Errorf("expected PRECEDING or FOLLOWING, got %s", dir)
}
