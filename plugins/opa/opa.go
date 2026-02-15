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
package opa

import (
	"fmt"
	"strings"

	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// PolicyFunc evaluates a policy for the given table name and returns
// conditions to inject into the query's WHERE clause. Returning a non-nil
// error rejects the query entirely (e.g., "access denied").
type PolicyFunc func(tableName string) ([]nodes.Node, error)

// ColumnResolver returns the column names for a given table. It is required
// when masks are returned by the OPA server and the query uses star projections,
// because the star must be expanded into explicit column references to allow
// individual columns to be replaced with masked literals.
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

// TransformSelect evaluates the policy for each table referenced in the query
// (FROM and JOINs) and appends any returned conditions to the WHERE clause.
// If the policy returns an error for any table, the query is rejected.
// In server mode masks are fetched once from the OPA Data API and applied
// to the projections. In PolicyFunc mode the user-supplied function is
// called directly (no masking support).
func (o *OPA) TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error) {
	var allMasks map[string]map[string]MaskAction

	// Fetch masks once (server mode only).
	if o.client != nil {
		masks, err := o.client.FetchMasks()
		if err != nil {
			return nil, err
		}
		allMasks = masks
	}

	for _, ref := range plugins.CollectTables(core) {
		if o.client != nil {
			conditions, err := o.client.Compile(ref.Name)
			if err != nil {
				return nil, err
			}
			core.Wheres = append(core.Wheres, conditions...)
		} else {
			conditions, err := o.evalPolicy(ref.Name)
			if err != nil {
				return nil, err
			}
			core.Wheres = append(core.Wheres, conditions...)
		}
	}

	if len(allMasks) > 0 {
		var err error
		core, err = o.applyMasks(core, allMasks)
		if err != nil {
			return nil, err
		}
	}

	return core, nil
}

// applyMasks rewrites the projections of a SelectCore to replace masked
// columns with SqlLiteral nodes containing the replacement value.
func (o *OPA) applyMasks(core *nodes.SelectCore, masks map[string]map[string]MaskAction) (*nodes.SelectCore, error) {
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
					expanded = append(expanded, maskLiteral(action.Replace.Value, colName))
				} else {
					expanded = append(expanded, nodes.NewAttribute(ref.Relation, colName))
				}
			}
		}
		core.Projections = expanded
	} else {
		// Explicit projections: replace matching Attribute nodes.
		for i, proj := range core.Projections {
			attr, ok := proj.(*nodes.Attribute)
			if !ok {
				continue
			}
			tableName := tableNameFromRelation(attr.Relation)
			if tableName == "" {
				continue
			}
			tableMasks, hasMasks := masks[tableName]
			if !hasMasks {
				continue
			}
			action, masked := tableMasks[attr.Name]
			if !masked || action.Replace == nil {
				continue
			}
			core.Projections[i] = maskLiteral(action.Replace.Value, attr.Name)
		}
	}

	return core, nil
}

// maskLiteral creates a SqlLiteral that renders as '<value>' AS "colName".
// The value is escaped against SQL injection by doubling single quotes.
func maskLiteral(value, colName string) *nodes.SqlLiteral {
	escaped := strings.ReplaceAll(value, "'", "''")
	raw := fmt.Sprintf("'%s' AS \"%s\"", escaped, strings.ReplaceAll(colName, "\"", "\"\""))
	return nodes.NewSqlLiteral(raw)
}

// tableNameFromRelation extracts the underlying table name from a relation node.
func tableNameFromRelation(rel nodes.Node) string {
	return nodes.TableSourceName(rel)
}
