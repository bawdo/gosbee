// Package opa provides a Transformer that enforces Open Policy Agent
// policies on queries by injecting policy-derived WHERE conditions.
//
// You supply a [PolicyFunc] that is called once per table referenced in
// the query (FROM and JOINs). The function inspects the table name and
// returns zero or more AST condition nodes to append to the WHERE
// clause. If the function returns an error the query is rejected
// entirely — useful for hard "access denied" rules.
//
// # Basic usage
//
//	policy := func(table string) ([]nodes.Node, error) {
//	    if table == "secrets" {
//	        return nil, errors.New("access denied")
//	    }
//	    // Restrict "users" to tenant_id = 42
//	    if table == "users" {
//	        t := nodes.NewTable(table)
//	        cond := t.Col("tenant_id").Eq(42)
//	        return []nodes.Node{cond}, nil
//	    }
//	    return nil, nil // no extra conditions
//	}
//
//	o := opa.New(policy)
//	query := managers.NewSelectManager(table)
//	query.Use(o)
//	// SELECT * FROM "users" WHERE "users"."tenant_id" = 42
//
// # Combining with other plugins
//
// OPA composes with any other Transformer. Register multiple plugins
// with successive query.Use() calls and they are applied in order:
//
//	query.Use(softdelete.New())
//	query.Use(opa.New(policy))
//
// # Design notes
//
// The OPA plugin is code-only — it requires a [PolicyFunc] at
// construction time, so it is not exposed in the interactive REPL.
// For REPL-friendly plugins see the softdelete package.
//
// # Nested queries and raw SQL
//
// Via a manager, the policy is applied inside subqueries, CTE bodies and
// set-operation branches, and is evaluated once per table per query. A name
// that refers to a CTE is not a table and is skipped. Queries containing raw
// SQL (SqlLiteral, StringJoin) are refused with plugins.ErrUnpoliceable.
package opa

import (
	"errors"
	"fmt"

	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// ErrMaskedColumnUse is returned when a masked column is used anywhere other
// than as a bare projection, where it cannot be replaced by its mask.
var ErrMaskedColumnUse = errors.New("opa: masked column used outside a plain projection")

// PolicyFunc evaluates a policy for the given table name and returns
// conditions to inject into the query's WHERE clause. Returning a non-nil
// error rejects the query entirely (e.g., "access denied").
type PolicyFunc func(tableName string) ([]nodes.Node, error)

// ColumnResolver returns the column names for a given table. It is required
// when masks are returned by the OPA server and the query uses star projections,
// because the star must be expanded into explicit column references to allow
// individual columns to be replaced with their mask values.
type ColumnResolver func(tableName string) ([]string, error)

// Option configures an OPA transformer.
type Option func(*OPA)

// WithColumnResolver sets the column resolver used to expand star projections
// when column masks are present. Without a resolver, star projections with
// masks will produce an error.
func WithColumnResolver(resolver ColumnResolver) Option {
	return func(o *OPA) {
		o.columnResolver = resolver
	}
}

// OPA is a Transformer that evaluates a policy function against every table
// in the query and injects the resulting conditions. It supports two modes:
//   - PolicyFunc mode (via [New]): calls a Go function to evaluate policy
//   - Server mode (via [NewFromServer]): calls an OPA server's Compile API
type OPA struct {
	plugins.BaseTransformer
	evalPolicy     PolicyFunc
	client         *Client
	columnResolver ColumnResolver

	// masks overrides the masks fetched from the server. It is set only by
	// tests, so masking can be exercised without an OPA server.
	masks map[string]map[string]MaskAction
}

// New creates an OPA transformer with the given policy function.
func New(policy PolicyFunc) *OPA {
	return &OPA{evalPolicy: policy}
}

// NewFromServer creates an OPA transformer that calls an OPA server's
// Compile API to evaluate policies. The url is the base URL of the OPA
// server (e.g., "http://localhost:8181"), policyPath is the Rego policy
// path (e.g., "data.authz.allow"), and input is the input document to
// send with each request. Optional Option values can configure additional
// behavior such as column resolvers for masking.
func NewFromServer(url, policyPath string, input map[string]any, opts ...Option) *OPA {
	o := &OPA{client: NewClient(url, policyPath, input)}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// TransformSelect is TransformSelectScoped with no CTE scope and no memo, so
// every table is compiled afresh. Managers use TransformSelectScoped via the
// plugin walker.
func (o *OPA) TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error) {
	return o.TransformSelectScoped(core, plugins.Scope{})
}

// TransformSelectScoped evaluates the policy for each table referenced in the
// query (FROM and JOINs) and appends any returned conditions to the WHERE
// clause. Names that refer to a CTE, and derived-table aliases, are not
// tables and are skipped. If the policy returns an error for any table, the
// query is rejected.
//
// Policy results are memoised per table for the duration of one walk, so a
// table referenced in several nested queries is compiled once. In server mode
// masks are fetched once per walk from the OPA Data API and applied to the
// projections. In PolicyFunc mode the user-supplied function is called
// directly (no masking support).
func (o *OPA) TransformSelectScoped(core *nodes.SelectCore, scope plugins.Scope) (*nodes.SelectCore, error) {
	allMasks := o.masks

	// Fetch masks once per walk (server mode only).
	if allMasks == nil && o.client != nil {
		v, err := scope.Memo("opa:masks", func() (any, error) {
			return o.client.FetchMasks()
		})
		if err != nil {
			return nil, err
		}
		allMasks = v.(map[string]map[string]MaskAction)
	}

	// Conditions the policy injects may use masked columns; only the
	// query's own conditions are checked for masked column use.
	queryWheres := len(core.Wheres)

	for _, ref := range scope.Tables(core) {
		name := ref.Name
		v, err := scope.Memo("opa:compile:"+name, func() (any, error) {
			if o.client != nil {
				return o.client.Compile(name)
			}
			return o.evalPolicy(name)
		})
		if err != nil {
			return nil, err
		}
		core.Wheres = append(core.Wheres, v.([]nodes.Node)...)
	}

	if len(allMasks) > 0 {
		var err error
		core, err = o.applyMasks(core, allMasks, queryWheres)
		if err != nil {
			return nil, err
		}
	}

	return core, nil
}

// RefusesRawSQL always reports true: a raw SQL fragment (SqlLiteral,
// StringJoin) may read from a table the policy has not been asked about, so
// the walker refuses it with an error wrapping plugins.ErrUnpoliceable.
func (o *OPA) RefusesRawSQL() bool { return true }

// applyMasks replaces each masked column projected bare (directly, not
// inside an alias or expression) with its mask value as a bind parameter
// aliased to the column name. A star projection is first expanded through the
// column resolver. Any other use of a masked column in the core (in an
// expression, WHERE, HAVING, GROUP BY, ORDER BY, DISTINCT ON, a join
// condition or a window definition) cannot be masked and is refused with
// ErrMaskedColumnUse. Conditions injected by the policy, which follow the
// first queryWheres conditions, are trusted and not checked.
func (o *OPA) applyMasks(core *nodes.SelectCore, masks map[string]map[string]MaskAction, queryWheres int) (*nodes.SelectCore, error) {
	refs := plugins.CollectTables(core)

	// Determine if this is a star projection (empty projections or contains StarNode).
	isStar := len(core.Projections) == 0
	if !isStar {
		for _, p := range core.Projections {
			if _, ok := p.(*nodes.StarNode); ok {
				isStar = true
				break
			}
		}
	}

	if isStar {
		// Star projection: expand to explicit columns, masking as needed.
		if o.columnResolver == nil {
			return nil, fmt.Errorf("opa: column resolver required to apply masks to star projection")
		}

		var expanded []nodes.Node
		for _, ref := range refs {
			tableMasks := masks[ref.Name]

			cols, err := o.columnResolver(ref.Name)
			if err != nil {
				return nil, fmt.Errorf("opa: column resolver: %w", err)
			}

			for _, colName := range cols {
				if action, masked := tableMasks[colName]; masked && action.Replace != nil {
					expanded = append(expanded, maskedProjection(action.Replace.Value, colName))
				} else {
					expanded = append(expanded, nodes.NewAttribute(ref.Relation, colName))
				}
			}
		}
		core.Projections = expanded
	} else {
		// Explicit projections: replace bare masked Attribute nodes.
		for i, proj := range core.Projections {
			attr, ok := proj.(*nodes.Attribute)
			if !ok {
				continue
			}
			if action, masked := maskFor(masks, attr); masked {
				core.Projections[i] = maskedProjection(action.Replace.Value, attr.Name)
			}
		}
	}

	if err := refuseMaskedUse(core, masks, queryWheres); err != nil {
		return nil, err
	}
	return core, nil
}

// maskedProjection renders as <value> AS <colName>, with the value as a bind
// parameter so each dialect quotes or parameterises it safely.
func maskedProjection(value, colName string) *nodes.AliasNode {
	return nodes.NewAliasNode(nodes.NewBindParam(value), colName)
}

// maskFor returns the replace mask for attr, if its table has one.
func maskFor(masks map[string]map[string]MaskAction, attr *nodes.Attribute) (MaskAction, bool) {
	tableName := tableNameFromRelation(attr.Relation)
	if tableName == "" {
		return MaskAction{}, false
	}
	action, masked := masks[tableName][attr.Name]
	if !masked || action.Replace == nil {
		return MaskAction{}, false
	}
	return action, true
}

// refuseMaskedUse returns ErrMaskedColumnUse if a masked column appears
// anywhere in core once bare projections have been replaced. Nested queries
// have already been masked by the walker, so any masked column still found
// inside them is an outer reference used in an expression. Only the first
// queryWheres conditions are checked: the rest were injected by the policy.
func refuseMaskedUse(core *nodes.SelectCore, masks map[string]map[string]MaskAction, queryWheres int) error {
	var exprs []nodes.Node
	exprs = append(exprs, core.Projections...)
	exprs = append(exprs, core.Wheres[:queryWheres]...)
	exprs = append(exprs, core.Havings...)
	exprs = append(exprs, core.Groups...)
	exprs = append(exprs, core.Orders...)
	exprs = append(exprs, core.DistinctOn...)
	for _, j := range core.Joins {
		if j != nil {
			exprs = append(exprs, j.On)
		}
	}
	for _, w := range core.Windows {
		if w == nil {
			continue
		}
		exprs = append(exprs, w.PartitionBy...)
		exprs = append(exprs, w.OrderBy...)
		if w.Frame != nil {
			exprs = append(exprs, w.Frame.Start.Offset)
			if w.Frame.End != nil {
				exprs = append(exprs, w.Frame.End.Offset)
			}
		}
	}

	for _, expr := range exprs {
		for _, attr := range plugins.Attributes(expr) {
			if _, masked := maskFor(masks, attr); masked {
				return fmt.Errorf("%w: %s.%s", ErrMaskedColumnUse, tableNameFromRelation(attr.Relation), attr.Name)
			}
		}
	}
	return nil
}

// tableNameFromRelation extracts the underlying table name from a relation node.
func tableNameFromRelation(rel nodes.Node) string {
	return nodes.TableSourceName(rel)
}
