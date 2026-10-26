package plugins_test

import (
	"errors"
	"maps"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
	"github.com/bawdo/gosbee/visitors"
)

// recordedCall captures what the walker showed the transformer for one core.
type recordedCall struct {
	tables     []string
	cteNames   []string
	qualifiers map[string]string
}

// recordingTransformer appends "<table>"."seen" IS NULL for every table in
// scope and records each call, so tests can see which cores were visited and
// in what order.
type recordingTransformer struct {
	plugins.BaseTransformer
	strict bool
	calls  []recordedCall
}

func (r *recordingTransformer) TransformSelectScoped(core *nodes.SelectCore, scope plugins.Scope) (*nodes.SelectCore, error) {
	call := recordedCall{qualifiers: maps.Clone(scope.Qualifiers)}
	for name := range scope.CTENames {
		call.cteNames = append(call.cteNames, name)
	}
	sort.Strings(call.cteNames)
	for _, ref := range scope.Tables(core) {
		call.tables = append(call.tables, ref.Name)
		core.Wheres = append(core.Wheres, nodes.NewAttribute(ref.Relation, "seen").IsNull())
	}
	r.calls = append(r.calls, call)
	return core, nil
}

func (r *recordingTransformer) RefusesRawSQL() bool { return r.strict }

// plainTransformer implements only TransformSelect, not ScopedTransformer.
type plainTransformer struct {
	plugins.BaseTransformer
	count int
}

func (p *plainTransformer) TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error) {
	p.count++
	return core, nil
}

// memoTransformer calls scope.Memo with the same key on every core.
type memoTransformer struct {
	plugins.BaseTransformer
	runs int
}

func (m *memoTransformer) TransformSelectScoped(core *nodes.SelectCore, scope plugins.Scope) (*nodes.SelectCore, error) {
	_, err := scope.Memo("k", func() (any, error) {
		m.runs++
		return "v", nil
	})
	return core, err
}

func render(n nodes.Node) string {
	return n.Accept(visitors.NewPostgresVisitor(visitors.WithoutParams()))
}

func applySelect(t *testing.T, tr plugins.Transformer, m *managers.SelectManager) string {
	t.Helper()
	out, err := plugins.ApplySelect(tr, m.Core)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return render(out)
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Errorf("expected SQL to contain %q\n got: %s", want, got)
	}
}

func TestApplySelectTransformsInSubquery(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	sub := managers.NewSelectManager(posts).Select(posts.Col("user_id"))
	m := managers.NewSelectManager(users).Where(users.Col("id").In(sub))

	got := applySelect(t, &recordingTransformer{}, m)
	want := `SELECT * FROM "users" WHERE "users"."id" IN (SELECT "posts"."user_id" FROM "posts" WHERE "posts"."seen" IS NULL) AND "users"."seen" IS NULL`
	if got != want {
		t.Errorf("\nwant: %s\n got: %s", want, got)
	}
}

func TestApplySelectTransformsExistsJoinedSubqueryAndCTE(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	newSub := func() *managers.SelectManager {
		return managers.NewSelectManager(posts).Select(posts.Col("user_id"))
	}

	t.Run("exists", func(t *testing.T) {
		t.Parallel()
		m := managers.NewSelectManager(users).Where(nodes.Exists(newSub()))
		got := applySelect(t, &recordingTransformer{}, m)
		assertContains(t, got, `EXISTS (SELECT "posts"."user_id" FROM "posts" WHERE "posts"."seen" IS NULL)`)
		assertContains(t, got, `"users"."seen" IS NULL`)
	})

	t.Run("joined subquery", func(t *testing.T) {
		t.Parallel()
		p := newSub().As("p")
		m := managers.NewSelectManager(users).Join(p).On(users.Col("id").Eq(p.Col("user_id")))
		got := applySelect(t, &recordingTransformer{}, m)
		want := `SELECT * FROM "users" INNER JOIN (SELECT "posts"."user_id" FROM "posts" WHERE "posts"."seen" IS NULL) AS "p" ON "users"."id" = "p"."user_id" WHERE "users"."seen" IS NULL`
		if got != want {
			t.Errorf("\nwant: %s\n got: %s", want, got)
		}
	})

	t.Run("cte body", func(t *testing.T) {
		t.Parallel()
		m := managers.NewSelectManager(users).With("c", newSub())
		got := applySelect(t, &recordingTransformer{}, m)
		assertContains(t, got, `WITH "c" AS (SELECT "posts"."user_id" FROM "posts" WHERE "posts"."seen" IS NULL)`)
		assertContains(t, got, `"users"."seen" IS NULL`)
	})
}

func TestScopeTablesSkipsDerivedTableAlias(t *testing.T) {
	t.Parallel()
	accounts := nodes.NewTable("accounts")
	posts := nodes.NewTable("posts")
	// The derived table is aliased like a policed table; it must not be
	// reported as that table, since it is policed by walking into it.
	derived := managers.NewSelectManager(posts).Select(posts.Col("user_id")).As("users")
	m := managers.NewSelectManager(managers.NewSelectManager(posts).As("d")).
		Join(accounts).On(accounts.Col("id").Eq(nodes.NewTable("d").Col("id"))).
		Join(derived).On(accounts.Col("id").Eq(derived.Col("user_id")))

	tr := &recordingTransformer{}
	got := applySelect(t, tr, m)

	outerCall := tr.calls[len(tr.calls)-1]
	if !slices.Equal(outerCall.tables, []string{"accounts"}) {
		t.Errorf("outer scope.Tables: want [accounts], got %v", outerCall.tables)
	}
	if strings.Contains(got, `"users"."seen"`) || strings.Contains(got, `"d"."seen"`) {
		t.Errorf("derived table alias was filtered as a table: %s", got)
	}
}

func TestApplyNodeTransformsSetOperationBranches(t *testing.T) {
	t.Parallel()
	users := managers.NewSelectManager(nodes.NewTable("users"))
	posts := managers.NewSelectManager(nodes.NewTable("posts"))

	out, err := plugins.ApplyNode(&recordingTransformer{}, users.Union(posts))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := render(out)
	want := `(SELECT * FROM "users" WHERE "users"."seen" IS NULL) UNION (SELECT * FROM "posts" WHERE "posts"."seen" IS NULL)`
	if got != want {
		t.Errorf("\nwant: %s\n got: %s", want, got)
	}
}

func TestApplyNodeTransformsSubqueryInDelete(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	sub := managers.NewSelectManager(posts).Select(posts.Col("user_id"))
	stmt := &nodes.DeleteStatement{From: users, Wheres: []nodes.Node{users.Col("id").In(sub)}}

	out, err := plugins.ApplyNode(&recordingTransformer{}, stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `DELETE FROM "users" WHERE "users"."id" IN (SELECT "posts"."user_id" FROM "posts" WHERE "posts"."seen" IS NULL)`
	if got := render(out); got != want {
		t.Errorf("\nwant: %s\n got: %s", want, got)
	}
	if got := render(stmt); strings.Contains(got, "seen") {
		t.Errorf("input statement was modified: %s", got)
	}
}

func TestApplySelectIsPostOrder(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	sub := managers.NewSelectManager(posts).Select(posts.Col("user_id"))
	m := managers.NewSelectManager(users).Where(users.Col("id").In(sub))

	tr := &recordingTransformer{}
	applySelect(t, tr, m)

	var order []string
	for _, c := range tr.calls {
		order = append(order, strings.Join(c.tables, ","))
	}
	if want := []string{"posts", "users"}; !slices.Equal(order, want) {
		t.Errorf("call order: want %v, got %v", want, order)
	}
}

func TestApplySelectDoesNotMutateInput(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	sub := managers.NewSelectManager(posts).Select(posts.Col("user_id"))
	joined := managers.NewSelectManager(posts).As("p")
	m := managers.NewSelectManager(users).
		With("c", managers.NewSelectManager(posts)).
		Where(users.Col("id").In(sub), nodes.Exists(sub)).
		Join(joined).On(users.Col("id").Eq(joined.Col("user_id")))

	beforeOuter := render(m.Core)
	beforeSub := render(sub)

	tr := &recordingTransformer{}
	if _, err := plugins.ApplySelect(tr, m.Core); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// A second walk must see exactly the same input and give the same output.
	first := applySelect(t, &recordingTransformer{}, m)
	second := applySelect(t, &recordingTransformer{}, m)

	if got := render(m.Core); got != beforeOuter {
		t.Errorf("outer core changed\nbefore: %s\n after: %s", beforeOuter, got)
	}
	if got := render(sub); got != beforeSub {
		t.Errorf("subquery manager changed\nbefore: %s\n after: %s", beforeSub, got)
	}
	if first != second {
		t.Errorf("repeated walks differ\nfirst:  %s\nsecond: %s", first, second)
	}
}

func TestApplySelectScopeIncludesAncestorCTEs(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	c := nodes.NewTable("c")
	inner := managers.NewSelectManager(c).Select(c.Col("id"))
	m := managers.NewSelectManager(users).
		With("c", managers.NewSelectManager(posts)).
		Where(users.Col("id").In(inner))

	tr := &recordingTransformer{}
	got := applySelect(t, tr, m)

	if len(tr.calls) != 3 {
		t.Fatalf("expected 3 transformer calls, got %d", len(tr.calls))
	}
	// Post-order: the CTE body, then the IN subquery, then the outer core.
	innerCall := tr.calls[1]
	if len(innerCall.tables) != 0 {
		t.Errorf("expected no tables in scope for the inner core, got %v", innerCall.tables)
	}
	if !slices.Equal(innerCall.cteNames, []string{"c"}) {
		t.Errorf("expected inner core to see CTE c, got %v", innerCall.cteNames)
	}
	if strings.Contains(got, `"c"."seen"`) {
		t.Errorf("CTE reference was filtered as a table: %s", got)
	}
	assertContains(t, got, `"posts"."seen" IS NULL`)
	assertContains(t, got, `"users"."seen" IS NULL`)
}

func TestApplySelectCTEBodyDoesNotSeeItsOwnName(t *testing.T) {
	t.Parallel()
	// In a plain WITH, the body's "users" is the real table, not the CTE
	// being defined, so it must still be filtered.
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users).With("users", managers.NewSelectManager(users))

	got := applySelect(t, &recordingTransformer{}, m)
	want := `WITH "users" AS (SELECT * FROM "users" WHERE "users"."seen" IS NULL) SELECT * FROM "users"`
	if got != want {
		t.Errorf("\nwant: %s\n got: %s", want, got)
	}
}

func TestApplySelectCTEBodySeesEarlierSiblings(t *testing.T) {
	t.Parallel()
	posts := nodes.NewTable("posts")
	a := nodes.NewTable("a")
	m := managers.NewSelectManager(nodes.NewTable("b")).
		With("a", managers.NewSelectManager(posts)).
		With("b", managers.NewSelectManager(a))

	got := applySelect(t, &recordingTransformer{}, m)
	want := `WITH "a" AS (SELECT * FROM "posts" WHERE "posts"."seen" IS NULL), "b" AS (SELECT * FROM "a") SELECT * FROM "b"`
	if got != want {
		t.Errorf("\nwant: %s\n got: %s", want, got)
	}
}

func TestApplySelectRecursiveCTEBodySeesItsOwnName(t *testing.T) {
	t.Parallel()
	tree := nodes.NewTable("tree")
	body := managers.NewSelectManager(tree)
	m := managers.NewSelectManager(tree).WithRecursive("tree", body)

	got := applySelect(t, &recordingTransformer{}, m)
	if strings.Contains(got, "seen") {
		t.Errorf("recursive CTE self reference was filtered as a table: %s", got)
	}
}

func TestApplySelectStrictRefusesSqlLiteral(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")

	cases := map[string]*managers.SelectManager{
		"where":       managers.NewSelectManager(users).Where(nodes.NewSqlLiteral("1 = 1")),
		"string join": managers.NewSelectManager(users).StringJoin("JOIN x ON true"),
		"nested": managers.NewSelectManager(users).Where(users.Col("id").In(
			managers.NewSelectManager(posts).Where(nodes.NewSqlLiteral("1 = 1")))),
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			out, err := plugins.ApplySelect(&recordingTransformer{strict: true}, m.Core)
			if !errors.Is(err, plugins.ErrUnpoliceable) {
				t.Fatalf("expected ErrUnpoliceable, got %v", err)
			}
			if out != nil {
				t.Errorf("expected nil core on error, got %s", render(out))
			}
			if msg := err.Error(); strings.Contains(msg, "1 = 1") || strings.Contains(msg, "JOIN x") {
				t.Errorf("error leaks raw SQL: %s", msg)
			}
		})
	}
}

func TestApplySelectNonStrictAllowsSqlLiteral(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users).Where(nodes.NewSqlLiteral("1 = 1"))

	got := applySelect(t, &recordingTransformer{}, m)
	want := `SELECT * FROM "users" WHERE 1 = 1 AND "users"."seen" IS NULL`
	if got != want {
		t.Errorf("\nwant: %s\n got: %s", want, got)
	}
}

func TestApplySelectStrictAllowsCastTypeName(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users).Select(nodes.Cast(users.Col("age"), "VARCHAR"))

	got := applySelect(t, &recordingTransformer{strict: true}, m)
	want := `SELECT CAST("users"."age" AS VARCHAR) FROM "users" WHERE "users"."seen" IS NULL`
	if got != want {
		t.Errorf("\nwant: %s\n got: %s", want, got)
	}
}

func TestApplySelectStrictRefusesSqlLiteralInCastExpr(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := managers.NewSelectManager(users).Select(nodes.Cast(nodes.NewSqlLiteral("1 = 1"), "VARCHAR"))

	_, err := plugins.ApplySelect(&recordingTransformer{strict: true}, m.Core)
	if !errors.Is(err, plugins.ErrUnpoliceable) {
		t.Fatalf("expected ErrUnpoliceable, got %v", err)
	}
}

func TestApplySelectPlainTransformerFallsBack(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	m := managers.NewSelectManager(users).
		With("c", managers.NewSelectManager(posts)).
		Where(nodes.Exists(managers.NewSelectManager(posts)))

	tr := &plainTransformer{}
	applySelect(t, tr, m)
	if tr.count != 3 {
		t.Errorf("expected TransformSelect on 3 cores, got %d", tr.count)
	}
}

func TestScopeMemoSharedAcrossWalk(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	m := managers.NewSelectManager(users).
		Where(users.Col("id").In(managers.NewSelectManager(posts).Select(posts.Col("user_id")))).
		Where(nodes.Exists(managers.NewSelectManager(posts)))

	tr := &memoTransformer{}
	applySelect(t, tr, m)
	if tr.runs != 1 {
		t.Errorf("expected memo fn to run once, ran %d times", tr.runs)
	}
}

func TestScopeMemoOnZeroScopeCallsFn(t *testing.T) {
	t.Parallel()
	var s plugins.Scope
	runs := 0
	for range 2 {
		v, err := s.Memo("k", func() (any, error) {
			runs++
			return "v", nil
		})
		if err != nil || v != "v" {
			t.Fatalf("unexpected result %v, %v", v, err)
		}
	}
	if runs != 2 {
		t.Errorf("expected fn to run on every call without a memo, ran %d times", runs)
	}
}

func TestAttributesFindsNested(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	where := users.Col("active")
	lowered := users.Col("email")
	nested := posts.Col("user_id")
	m := managers.NewSelectManager(users).
		Where(where.Eq(true)).
		Where(nodes.Lower(lowered).Eq("x")).
		Where(nodes.Exists(managers.NewSelectManager(posts).Where(nested.IsNotNull())))

	got := plugins.Attributes(m.Core)
	for _, want := range []*nodes.Attribute{where, lowered, nested} {
		if !slices.Contains(got, want) {
			t.Errorf("expected %s.%s in result", nodes.RelationName(want.Relation), want.Name)
		}
	}
	if len(got) != 3 {
		t.Errorf("expected 3 attributes, got %d", len(got))
	}
}

func TestApplyNodeReachesSubqueriesInEveryContainer(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	sub := func() nodes.Node { return managers.NewSelectManager(posts) }
	col := users.Col("id")
	win := func() *nodes.WindowDefinition { return nodes.NewWindowDef().Partition(sub()) }

	cases := map[string]nodes.Node{
		"comparison":   col.Eq(sub()),
		"and":          col.Eq(1).And(nodes.Exists(sub())),
		"or":           col.Eq(1).Or(nodes.Exists(sub())),
		"not":          nodes.Exists(sub()).Not(),
		"not exists":   nodes.NotExists(sub()),
		"unary":        &nodes.UnaryNode{Expr: sub(), Op: nodes.OpIsNull},
		"between":      col.Between(sub(), sub()),
		"not in":       col.NotIn(sub()),
		"infix":        col.Plus(sub()),
		"unary math":   nodes.NewUnaryMathNode(sub(), nodes.OpBitwiseNot),
		"aggregate":    nodes.Sum(sub()),
		"agg filter":   nodes.Count(nil).WithFilter(nodes.Exists(sub())),
		"extract":      nodes.Extract(nodes.ExtractYear, sub()),
		"function":     nodes.Coalesce(sub(), nodes.Literal(0)),
		"cast expr":    nodes.Cast(sub(), "INTEGER"),
		"case operand": nodes.NewCase(sub()).When(nodes.Literal(1), nodes.Literal(2)),
		"case when":    nodes.NewCase().When(nodes.Exists(sub()), nodes.Literal(1)),
		"case then":    nodes.NewCase().When(col.Eq(1), sub()),
		"case else":    nodes.NewCase().When(col.Eq(1), nodes.Literal(1)).Else(sub()),
		"alias":        nodes.NewAliasNode(sub(), "x"),
		"window func":  &nodes.WindowFuncNode{Func: nodes.WinLag, Args: []nodes.Node{sub()}},
		"over":         nodes.RowNumber().Over(win()),
		"over order":   nodes.RowNumber().Over(nodes.NewWindowDef().Order(nodes.NewAliasNode(sub(), "x"))),
		"over frame":   nodes.RowNumber().Over(nodes.NewWindowDef().Rows(nodes.Preceding(sub()), nodes.Following(sub()))),
		"ordering":     &nodes.OrderingNode{Expr: sub(), Direction: nodes.Desc},
		"cube":         nodes.NewCube(sub()),
		"grouping set": nodes.NewGroupingSets([]nodes.Node{col}, []nodes.Node{sub()}),
		"table alias":  managers.NewSelectManager(posts).As("p"),
		"set op order": &nodes.SetOperationNode{
			Left: sub(), Right: sub(), Type: nodes.UnionAll,
			Orders: []nodes.Node{&nodes.OrderingNode{Expr: sub()}}, Limit: sub(), Offset: sub(),
		},
		"core clauses": func() nodes.Node {
			m := managers.NewSelectManager(users).
				DistinctOn(sub()).
				Group(sub()).
				Having(nodes.Exists(sub())).
				Window(nodes.NewWindowDef("w").Partition(sub())).
				Order(&nodes.OrderingNode{Expr: sub()})
			m.Core.Limit = sub()
			m.Core.Offset = sub()
			return m.Core
		}(),
		"insert select": &nodes.InsertStatement{Into: users, Columns: []nodes.Node{col}, Select: sub()},
		"insert values": &nodes.InsertStatement{Into: users, Columns: []nodes.Node{col}, Values: [][]nodes.Node{{nodes.Literal(1)}, {sub()}}},
		"insert returning": &nodes.InsertStatement{
			Into: users, Columns: []nodes.Node{col}, Values: [][]nodes.Node{{nodes.Literal(1)}},
			Returning: []nodes.Node{sub()},
		},
		"on conflict": &nodes.InsertStatement{
			Into: users, Columns: []nodes.Node{col}, Values: [][]nodes.Node{{nodes.Literal(1)}},
			OnConflict: &nodes.OnConflictNode{
				Columns: []nodes.Node{col}, Action: nodes.DoUpdate,
				Assignments: []*nodes.AssignmentNode{{Left: col, Right: nodes.Literal(2)}},
				Wheres:      []nodes.Node{nodes.Exists(sub())},
			},
		},
		"update": &nodes.UpdateStatement{
			Table:       users,
			Assignments: []*nodes.AssignmentNode{{Left: col, Right: sub()}},
			Wheres:      []nodes.Node{col.Eq(1)},
		},
		"update returning": &nodes.UpdateStatement{
			Table:       users,
			Assignments: []*nodes.AssignmentNode{{Left: col, Right: nodes.Literal(1)}},
			Returning:   []nodes.Node{sub()},
		},
		"delete returning": &nodes.DeleteStatement{From: users, Returning: []nodes.Node{sub()}},
	}
	for name, n := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			before := render(n)
			out, err := plugins.ApplyNode(&recordingTransformer{}, n)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertContains(t, render(out), `FROM "posts" WHERE "posts"."seen" IS NULL`)
			if strings.Contains(before, "seen") {
				t.Fatalf("test input already contains the filter: %s", before)
			}
			if after := render(n); after != before {
				t.Errorf("input changed\nbefore: %s\n after: %s", before, after)
			}
		})
	}
}

func TestScopeQualifiersOwnCore(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	u := users.Alias("u")
	derived := managers.NewSelectManager(nodes.NewTable("orders")).As("d")
	c := nodes.NewTable("c")
	m := managers.NewSelectManager(u).
		With("c", managers.NewSelectManager(nodes.NewTable("tags")))
	m.Join(posts).On(u.Col("id").Eq(posts.Col("user_id")))
	m.Join(derived).On(u.Col("id").Eq(derived.Col("user_id")))
	m.Join(c).On(u.Col("id").Eq(c.Col("user_id")))

	tr := &recordingTransformer{}
	applySelect(t, tr, m)

	outer := tr.calls[len(tr.calls)-1]
	want := map[string]string{"u": "users", "posts": "posts"}
	if !maps.Equal(outer.qualifiers, want) {
		t.Errorf("expected qualifiers %v, got %v", want, outer.qualifiers)
	}
}

func TestScopeQualifiersIncludeEnclosingCore(t *testing.T) {
	t.Parallel()
	u := nodes.NewTable("users").Alias("u")
	posts := nodes.NewTable("posts")
	inner := managers.NewSelectManager(posts).Where(posts.Col("user_id").Eq(u.Col("id")))
	m := managers.NewSelectManager(u).Where(nodes.Exists(inner))

	tr := &recordingTransformer{}
	applySelect(t, tr, m)

	if len(tr.calls) != 2 {
		t.Fatalf("expected 2 transformer calls, got %d", len(tr.calls))
	}
	if want := map[string]string{"u": "users", "posts": "posts"}; !maps.Equal(tr.calls[0].qualifiers, want) {
		t.Errorf("expected inner qualifiers %v, got %v", want, tr.calls[0].qualifiers)
	}
	if want := map[string]string{"u": "users"}; !maps.Equal(tr.calls[1].qualifiers, want) {
		t.Errorf("expected outer qualifiers %v, got %v", want, tr.calls[1].qualifiers)
	}
}

func TestScopeQualifiersInnerNamesShadowOuter(t *testing.T) {
	t.Parallel()
	x := nodes.NewTable("users").Alias("x")
	d := nodes.NewTable("accounts").Alias("d")
	innerX := nodes.NewTable("posts").Alias("x")
	derived := managers.NewSelectManager(nodes.NewTable("orders")).As("d")
	inner := managers.NewSelectManager(innerX)
	inner.Join(derived).On(innerX.Col("id").Eq(derived.Col("post_id")))
	m := managers.NewSelectManager(x).Where(nodes.Exists(inner))
	m.Join(d).On(x.Col("id").Eq(d.Col("user_id")))

	tr := &recordingTransformer{}
	applySelect(t, tr, m)

	// Post-order: the derived table body, the EXISTS core, then the outer core.
	if len(tr.calls) != 3 {
		t.Fatalf("expected 3 transformer calls, got %d", len(tr.calls))
	}
	want := map[string]string{"x": "posts"}
	if got := tr.calls[1].qualifiers; !maps.Equal(got, want) {
		t.Errorf("expected inner qualifiers %v, got %v", want, got)
	}
	if want := map[string]string{"x": "users", "d": "accounts"}; !maps.Equal(tr.calls[2].qualifiers, want) {
		t.Errorf("expected outer qualifiers %v, got %v", want, tr.calls[2].qualifiers)
	}
}

func TestShallowAttributesSkipsNestedQueries(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	where := users.Col("active")
	lowered := users.Col("email")
	nested := posts.Col("user_id")
	branch := posts.Col("author_id")
	union := managers.NewSelectManager(posts).Select(nested).
		Union(managers.NewSelectManager(posts).Select(branch))
	m := managers.NewSelectManager(users).
		Where(where.Eq(true)).
		Where(nodes.Lower(lowered).Eq("x")).
		Where(nodes.Exists(managers.NewSelectManager(posts).Where(nested.IsNotNull()))).
		Where(users.Col("id").In(union))

	got := plugins.ShallowAttributes(m.Core)
	for _, want := range []*nodes.Attribute{where, lowered} {
		if !slices.Contains(got, want) {
			t.Errorf("expected %s.%s in result", nodes.RelationName(want.Relation), want.Name)
		}
	}
	for _, unwanted := range []*nodes.Attribute{nested, branch} {
		if slices.Contains(got, unwanted) {
			t.Errorf("did not expect nested %s.%s in result", nodes.RelationName(unwanted.Relation), unwanted.Name)
		}
	}
	if len(got) != 3 {
		t.Errorf("expected 3 attributes (active, email, id), got %d", len(got))
	}
}

func TestScopeQualifiersSurviveInnerCTEs(t *testing.T) {
	t.Parallel()
	u := nodes.NewTable("users").Alias("u")
	posts := nodes.NewTable("posts")
	inner := managers.NewSelectManager(posts).
		With("c", managers.NewSelectManager(nodes.NewTable("tags")))
	m := managers.NewSelectManager(u).Where(nodes.Exists(inner))

	tr := &recordingTransformer{}
	applySelect(t, tr, m)

	// Post-order: the CTE body, the EXISTS core, then the outer core.
	if len(tr.calls) != 3 {
		t.Fatalf("expected 3 transformer calls, got %d", len(tr.calls))
	}
	if want := map[string]string{"u": "users", "posts": "posts"}; !maps.Equal(tr.calls[1].qualifiers, want) {
		t.Errorf("expected inner qualifiers %v, got %v", want, tr.calls[1].qualifiers)
	}
}
