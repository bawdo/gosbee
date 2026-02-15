package managers

import (
	"errors"
	"testing"

	"github.com/bawdo/gosbee/internal/testutil"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// --- NewSelectManager ---

func TestNewSelectManagerSetsFrom(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	if m.Core.From != users {
		t.Error("expected From to be the users table")
	}
	if len(m.Core.Projections) != 0 {
		t.Error("expected empty projections")
	}
	if len(m.Core.Wheres) != 0 {
		t.Error("expected empty wheres")
	}
	if len(m.Core.Joins) != 0 {
		t.Error("expected empty joins")
	}
}

func TestNewSelectManagerNilFrom(t *testing.T) {
	t.Parallel()
	m := NewSelectManager(nil)
	if m.Core.From != nil {
		t.Error("expected nil From")
	}
}

// --- Select / Project ---

func TestSelectSetsProjections(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Select(users.Col("id"), users.Col("name"))

	if len(m.Core.Projections) != 2 {
		t.Fatalf("expected 2 projections, got %d", len(m.Core.Projections))
	}
}

func TestSelectReplacesProjections(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Select(users.Col("id"))
	m.Select(users.Col("name"), users.Col("email"))

	if len(m.Core.Projections) != 2 {
		t.Fatalf("expected 2 projections after replacement, got %d", len(m.Core.Projections))
	}
}

func TestProjectIsAliasForSelect(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Project(users.Col("id"))

	if len(m.Core.Projections) != 1 {
		t.Fatalf("expected 1 projection via Project, got %d", len(m.Core.Projections))
	}
}

func TestSelectWithStar(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Select(nodes.Star())

	if len(m.Core.Projections) != 1 {
		t.Fatalf("expected 1 projection, got %d", len(m.Core.Projections))
	}
	if _, ok := m.Core.Projections[0].(*nodes.StarNode); !ok {
		t.Errorf("expected *StarNode, got %T", m.Core.Projections[0])
	}
}

// --- Where ---

func TestWhereAppendsConditions(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Where(users.Col("active").Eq(true))
	m.Where(users.Col("age").Gt(18))

	if len(m.Core.Wheres) != 2 {
		t.Fatalf("expected 2 wheres, got %d", len(m.Core.Wheres))
	}
}

func TestWhereMultipleConditionsInOneCall(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Where(
		users.Col("active").Eq(true),
		users.Col("age").Gt(18),
	)

	if len(m.Core.Wheres) != 2 {
		t.Fatalf("expected 2 wheres, got %d", len(m.Core.Wheres))
	}
}

// --- From ---

func TestFromChangesSource(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	m := NewSelectManager(users)

	m.From(posts)

	if m.Core.From != posts {
		t.Error("expected From to be changed to posts")
	}
}

// --- Join ---

func TestJoinDefaultsToInnerJoin(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	m := NewSelectManager(users)

	m.Join(posts).On(users.Col("id").Eq(posts.Col("user_id")))

	if len(m.Core.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(m.Core.Joins))
	}
	join := m.Core.Joins[0]
	if join.Type != nodes.InnerJoin {
		t.Errorf("expected InnerJoin, got %d", join.Type)
	}
	if join.Left != users {
		t.Error("expected join Left to be users table")
	}
	if join.Right != posts {
		t.Error("expected join Right to be posts table")
	}
	if join.On == nil {
		t.Error("expected join On to be set")
	}
}

func TestJoinWithExplicitType(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	m := NewSelectManager(users)

	m.Join(posts, nodes.LeftOuterJoin).On(users.Col("id").Eq(posts.Col("user_id")))

	if m.Core.Joins[0].Type != nodes.LeftOuterJoin {
		t.Errorf("expected LeftOuterJoin, got %d", m.Core.Joins[0].Type)
	}
}

func TestOuterJoinConvenience(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	m := NewSelectManager(users)

	m.OuterJoin(posts).On(users.Col("id").Eq(posts.Col("user_id")))

	if m.Core.Joins[0].Type != nodes.LeftOuterJoin {
		t.Errorf("expected LeftOuterJoin, got %d", m.Core.Joins[0].Type)
	}
}

func TestCrossJoinNoOnClause(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	colors := nodes.NewTable("colors")
	m := NewSelectManager(users)

	m.CrossJoin(colors)

	if len(m.Core.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(m.Core.Joins))
	}
	if m.Core.Joins[0].Type != nodes.CrossJoin {
		t.Errorf("expected CrossJoin, got %d", m.Core.Joins[0].Type)
	}
	if m.Core.Joins[0].On != nil {
		t.Error("expected CrossJoin to have nil On")
	}
}

func TestMultipleJoins(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")
	comments := nodes.NewTable("comments")
	m := NewSelectManager(users)

	m.Join(posts).On(users.Col("id").Eq(posts.Col("user_id")))
	m.Join(comments, nodes.LeftOuterJoin).On(posts.Col("id").Eq(comments.Col("post_id")))

	if len(m.Core.Joins) != 2 {
		t.Fatalf("expected 2 joins, got %d", len(m.Core.Joins))
	}
	if m.Core.Joins[0].Type != nodes.InnerJoin {
		t.Errorf("expected first join to be InnerJoin")
	}
	if m.Core.Joins[1].Type != nodes.LeftOuterJoin {
		t.Errorf("expected second join to be LeftOuterJoin")
	}
}

// --- Group ---

func TestGroupAppendsColumns(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Group(users.Col("status"))
	m.Group(users.Col("role"))

	if len(m.Core.Groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(m.Core.Groups))
	}
}

func TestGroupMultipleInOneCall(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Group(users.Col("status"), users.Col("role"))

	if len(m.Core.Groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(m.Core.Groups))
	}
}

// --- Having ---

func TestHavingAppendsConditions(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Having(nodes.NewSqlLiteral("COUNT(*)").Gt(5))

	if len(m.Core.Havings) != 1 {
		t.Fatalf("expected 1 having, got %d", len(m.Core.Havings))
	}
}

func TestHavingMultipleCalls(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Having(nodes.NewSqlLiteral("COUNT(*)").Gt(5))
	m.Having(nodes.NewSqlLiteral("SUM(amount)").Lt(1000))

	if len(m.Core.Havings) != 2 {
		t.Fatalf("expected 2 havings, got %d", len(m.Core.Havings))
	}
}

func TestCloneCorePreservesGroupsAndHavings(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	ct := &countingTransformer{}
	m := NewSelectManager(users).
		Group(users.Col("status")).
		Having(nodes.NewSqlLiteral("COUNT(*)").Gt(5))
	m.Use(ct)

	_, _, _ = m.ToSQL(testutil.StubVisitor{})

	if len(m.Core.Groups) != 1 {
		t.Errorf("expected original to keep 1 group, got %d", len(m.Core.Groups))
	}
	if len(m.Core.Havings) != 1 {
		t.Errorf("expected original to keep 1 having, got %d", len(m.Core.Havings))
	}
}

// --- Order ---

func TestOrderAppendsOrderings(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Order(users.Col("name").Asc())
	m.Order(users.Col("id").Desc())

	if len(m.Core.Orders) != 2 {
		t.Fatalf("expected 2 orders, got %d", len(m.Core.Orders))
	}
}

func TestOrderMultipleInOneCall(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Order(users.Col("name").Asc(), users.Col("id").Desc())

	if len(m.Core.Orders) != 2 {
		t.Fatalf("expected 2 orders, got %d", len(m.Core.Orders))
	}
}

// --- Limit / Offset / Take ---

func TestLimitSetsLimit(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Limit(10)

	if m.Core.Limit == nil {
		t.Fatal("expected limit to be set")
	}
	lit, ok := m.Core.Limit.(*nodes.LiteralNode)
	if !ok {
		t.Fatalf("expected *LiteralNode, got %T", m.Core.Limit)
	}
	if lit.Value != 10 {
		t.Errorf("expected 10, got %v", lit.Value)
	}
}

func TestOffsetSetsOffset(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Offset(20)

	if m.Core.Offset == nil {
		t.Fatal("expected offset to be set")
	}
	lit := m.Core.Offset.(*nodes.LiteralNode)
	if lit.Value != 20 {
		t.Errorf("expected 20, got %v", lit.Value)
	}
}

func TestTakeIsAliasForLimit(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Take(5)

	if m.Core.Limit == nil {
		t.Fatal("expected limit to be set via Take")
	}
	lit := m.Core.Limit.(*nodes.LiteralNode)
	if lit.Value != 5 {
		t.Errorf("expected 5, got %v", lit.Value)
	}
}

func TestCloneCorePreservesOrderLimitOffset(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	ct := &countingTransformer{}
	m := NewSelectManager(users).
		Order(users.Col("name").Asc()).
		Limit(10).
		Offset(5)
	m.Use(ct)

	_, _, _ = m.ToSQL(testutil.StubVisitor{})

	// Original should be unchanged
	if len(m.Core.Orders) != 1 {
		t.Errorf("expected original to keep 1 order, got %d", len(m.Core.Orders))
	}
	if m.Core.Limit == nil || m.Core.Offset == nil {
		t.Error("expected original to keep limit and offset")
	}
}

// --- Distinct ---

func TestDistinctEnables(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Distinct()

	if !m.Core.Distinct {
		t.Error("expected Distinct to be true")
	}
}

func TestDistinctExplicitTrue(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Distinct(true)

	if !m.Core.Distinct {
		t.Error("expected Distinct to be true")
	}
}

func TestDistinctExplicitFalse(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	m.Distinct()
	m.Distinct(false)

	if m.Core.Distinct {
		t.Error("expected Distinct to be false after Distinct(false)")
	}
}

func TestCloneCorePreservesDistinct(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	ct := &countingTransformer{}
	m := NewSelectManager(users).Distinct()
	m.Use(ct)

	_, _, _ = m.ToSQL(testutil.StubVisitor{})

	if !m.Core.Distinct {
		t.Error("expected original to keep Distinct true")
	}
}

// --- Fluent chaining ---

func TestFluentChaining(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")

	m := NewSelectManager(users).
		Select(users.Col("name"), users.Col("email")).
		Where(users.Col("active").Eq(true))

	m.Join(posts).On(users.Col("id").Eq(posts.Col("user_id")))

	if len(m.Core.Projections) != 2 {
		t.Errorf("expected 2 projections, got %d", len(m.Core.Projections))
	}
	if len(m.Core.Wheres) != 1 {
		t.Errorf("expected 1 where, got %d", len(m.Core.Wheres))
	}
	if len(m.Core.Joins) != 1 {
		t.Errorf("expected 1 join, got %d", len(m.Core.Joins))
	}
}

// --- Accept (Node interface for subqueries) ---

func TestSelectManagerImplementsNode(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	// Verify it implements nodes.Node
	var n nodes.Node = m
	result := n.Accept(testutil.StubVisitor{})

	if result != "select_core" {
		t.Errorf("expected 'select_core' from stub visitor, got %q", result)
	}
}

func TestSelectManagerAsSubquery(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	posts := nodes.NewTable("posts")

	subquery := NewSelectManager(posts).
		Select(nodes.Star()).
		Where(posts.Col("created_at").Gt("2025-01-01"))

	m := NewSelectManager(users)
	m.Join(subquery).On(users.Col("id").Eq(posts.Col("author_id")))

	// The join's Right should be the subquery SelectManager
	if m.Core.Joins[0].Right != subquery {
		t.Error("expected join Right to be the subquery SelectManager")
	}
}

// --- ToSQL ---

func TestToSQLDelegatesToVisitor(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	sql, _, err := m.ToSQL(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "select_core" {
		t.Errorf("expected 'select_core', got %q", sql)
	}
}

// --- Transformer plugin support ---

// countingTransformer appends a where clause and counts invocations.
type countingTransformer struct {
	plugins.BaseTransformer
	called int
}

func (ct *countingTransformer) TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error) {
	ct.called++
	col := nodes.NewAttribute(core.From, "injected")
	core.Wheres = append(core.Wheres, col.Eq("by_plugin"))
	return core, nil
}

func (ct *countingTransformer) TransformInsert(stmt *nodes.InsertStatement) (*nodes.InsertStatement, error) {
	ct.called++
	return stmt, nil
}

func (ct *countingTransformer) TransformUpdate(stmt *nodes.UpdateStatement) (*nodes.UpdateStatement, error) {
	ct.called++
	return stmt, nil
}

func (ct *countingTransformer) TransformDelete(stmt *nodes.DeleteStatement) (*nodes.DeleteStatement, error) {
	ct.called++
	col := nodes.NewAttribute(stmt.From, "injected")
	stmt.Wheres = append(stmt.Wheres, col.Eq("by_plugin"))
	return stmt, nil
}

func TestUseRegistersTransformer(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	ct := &countingTransformer{}
	m := NewSelectManager(users)
	m.Use(ct)

	_, _, err := m.ToSQL(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ct.called != 1 {
		t.Errorf("expected transformer called once, got %d", ct.called)
	}
}

func TestTransformerDoesNotModifyOriginalCore(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	ct := &countingTransformer{}
	m := NewSelectManager(users).
		Where(users.Col("active").Eq(true))
	m.Use(ct)

	_, _, _ = m.ToSQL(testutil.StubVisitor{})

	// The original core should still have only 1 where
	if len(m.Core.Wheres) != 1 {
		t.Errorf("expected original core to have 1 where, got %d", len(m.Core.Wheres))
	}
}

func TestMultipleTransformersRunInOrder(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	ct1 := &countingTransformer{}
	ct2 := &countingTransformer{}
	m := NewSelectManager(users)
	m.Use(ct1).Use(ct2)

	_, _, err := m.ToSQL(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ct1.called != 1 || ct2.called != 1 {
		t.Error("expected both transformers to be called once")
	}
}

// failingTransformer returns an error.
type failingTransformer struct {
	plugins.BaseTransformer
}

func (ft failingTransformer) TransformSelect(core *nodes.SelectCore) (*nodes.SelectCore, error) {
	return nil, errors.New("policy violation: access denied")
}

func (ft failingTransformer) TransformInsert(stmt *nodes.InsertStatement) (*nodes.InsertStatement, error) {
	return nil, errors.New("policy violation: access denied")
}

func (ft failingTransformer) TransformUpdate(stmt *nodes.UpdateStatement) (*nodes.UpdateStatement, error) {
	return nil, errors.New("policy violation: access denied")
}

func (ft failingTransformer) TransformDelete(stmt *nodes.DeleteStatement) (*nodes.DeleteStatement, error) {
	return nil, errors.New("policy violation: access denied")
}

func TestTransformerErrorStopsGeneration(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)
	m.Use(failingTransformer{})

	sql, _, err := m.ToSQL(testutil.StubVisitor{})
	if err == nil {
		t.Fatal("expected error from failing transformer")
	}
	if sql != "" {
		t.Errorf("expected empty SQL on error, got %q", sql)
	}
	if err.Error() != "policy violation: access denied" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestTransformerErrorShortCircuits(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	ct := &countingTransformer{}
	m := NewSelectManager(users)
	m.Use(failingTransformer{}).Use(ct)

	_, _, _ = m.ToSQL(testutil.StubVisitor{})

	// Second transformer should not have been called
	if ct.called != 0 {
		t.Error("expected second transformer to not be called after first failed")
	}
}

// --- Use chaining ---

func TestUseReturnsSelf(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	result := m.Use(&countingTransformer{})
	if result != m {
		t.Error("expected Use to return the same SelectManager")
	}
}

// --- ToSQLParams ---

func TestToSQLParamsWithParameterizer(t *testing.T) {
	t.Parallel()
	sv := &testutil.StubParamVisitor{}
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	sql, params, err := m.ToSQLParams(sv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "select_core" {
		t.Errorf("expected 'select_core', got %q", sql)
	}
	// After reset + accept of stub, params should be empty (reset clears)
	if len(params) != 0 {
		t.Errorf("expected empty params after reset, got %v", params)
	}
}

func TestToSQLParamsWithoutParameterizer(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	sql, params, err := m.ToSQLParams(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "select_core" {
		t.Errorf("expected 'select_core', got %q", sql)
	}
	if params != nil {
		t.Errorf("expected nil params for non-parameterizer, got %v", params)
	}
}

func TestToSQLParamsTransformerError(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)
	m.Use(failingTransformer{})

	sql, params, err := m.ToSQLParams(testutil.StubVisitor{})
	if err == nil {
		t.Fatal("expected error from failing transformer")
	}
	if sql != "" {
		t.Errorf("expected empty SQL on error, got %q", sql)
	}
	if params != nil {
		t.Errorf("expected nil params on error, got %v", params)
	}
}

// --- Window support ---

func TestWindowAppendsDefinitions(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)
	w := nodes.NewWindowDef("w").Order(users.Col("salary").Asc())
	m.Window(w)

	if len(m.Core.Windows) != 1 {
		t.Fatalf("expected 1 window, got %d", len(m.Core.Windows))
	}
	if m.Core.Windows[0].Name != "w" {
		t.Errorf("expected window name %q, got %q", "w", m.Core.Windows[0].Name)
	}
}

func TestWindowMultiple(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)
	w1 := nodes.NewWindowDef("w1").Order(users.Col("salary").Asc())
	w2 := nodes.NewWindowDef("w2").Partition(users.Col("dept"))
	m.Window(w1, w2)

	if len(m.Core.Windows) != 2 {
		t.Fatalf("expected 2 windows, got %d", len(m.Core.Windows))
	}
}

func TestWindowChaining(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users).
		Select(nodes.RowNumber().OverName("w")).
		Window(nodes.NewWindowDef("w").Order(users.Col("salary").Asc()))

	if len(m.Core.Windows) != 1 {
		t.Fatalf("expected 1 window, got %d", len(m.Core.Windows))
	}
}

func TestCloneCorePreservesWindows(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)
	w := nodes.NewWindowDef("w").Order(users.Col("salary").Asc())
	m.Window(w)

	clone := m.CloneCore()
	if len(clone.Windows) != 1 {
		t.Fatalf("expected 1 window in clone, got %d", len(clone.Windows))
	}

	// Modifying clone should not affect original
	clone.Windows = append(clone.Windows, nodes.NewWindowDef("w2"))
	if len(m.Core.Windows) != 1 {
		t.Error("modifying clone affected original Windows")
	}
}

// --- DISTINCT ON ---

func TestDistinctOnSetsColumns(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)
	m.DistinctOn(users.Col("email"), users.Col("dept"))
	if len(m.Core.DistinctOn) != 2 {
		t.Fatalf("expected 2 distinct on cols, got %d", len(m.Core.DistinctOn))
	}
}

// --- FOR UPDATE / FOR SHARE ---

func TestForUpdateSetsLock(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users).ForUpdate()
	if m.Core.Lock != nodes.ForUpdate {
		t.Errorf("expected ForUpdate, got %d", m.Core.Lock)
	}
}

func TestForShareSetsLock(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users).ForShare()
	if m.Core.Lock != nodes.ForShare {
		t.Errorf("expected ForShare, got %d", m.Core.Lock)
	}
}

func TestSkipLockedSetsFlag(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users).ForUpdate().SkipLocked()
	if !m.Core.SkipLocked {
		t.Error("expected SkipLocked to be true")
	}
}

func TestForNoKeyUpdateSetsLock(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users).ForNoKeyUpdate()
	if m.Core.Lock != nodes.ForNoKeyUpdate {
		t.Errorf("expected ForNoKeyUpdate, got %d", m.Core.Lock)
	}
}

func TestForKeyShareSetsLock(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users).ForKeyShare()
	if m.Core.Lock != nodes.ForKeyShare {
		t.Errorf("expected ForKeyShare, got %d", m.Core.Lock)
	}
}

// --- Comment ---

func TestCommentSetsText(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users).Comment("load users")
	if m.Core.Comment != "load users" {
		t.Errorf("expected 'load users', got %q", m.Core.Comment)
	}
}

// --- Hints ---

func TestHintAppendsHint(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users).Hint("SeqScan(users)").Hint("Parallel(users 4)")
	if len(m.Core.Hints) != 2 {
		t.Fatalf("expected 2 hints, got %d", len(m.Core.Hints))
	}
}

// --- LATERAL JOIN ---

func TestLateralJoinSetsFlag(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	orders := nodes.NewTable("orders")
	m := NewSelectManager(users)
	m.LateralJoin(orders).On(users.Col("id").Eq(orders.Col("user_id")))
	if len(m.Core.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(m.Core.Joins))
	}
	if !m.Core.Joins[0].Lateral {
		t.Error("expected Lateral to be true")
	}
}

// --- String JOIN ---

func TestStringJoinAddsRawSQL(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)
	m.StringJoin("INNER JOIN orders ON orders.user_id = users.id")
	if len(m.Core.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(m.Core.Joins))
	}
	if m.Core.Joins[0].Type != nodes.StringJoin {
		t.Error("expected StringJoin type")
	}
}

// --- Set Operations ---

func TestUnionCreatesSetOp(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	admins := nodes.NewTable("admins")
	m1 := NewSelectManager(users)
	m2 := NewSelectManager(admins)
	op := m1.Union(m2)
	if op.Type != nodes.Union {
		t.Errorf("expected Union, got %d", op.Type)
	}
}

func TestUnionAllCreatesSetOp(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	admins := nodes.NewTable("admins")
	m1 := NewSelectManager(users)
	m2 := NewSelectManager(admins)
	op := m1.UnionAll(m2)
	if op.Type != nodes.UnionAll {
		t.Errorf("expected UnionAll, got %d", op.Type)
	}
}

func TestIntersectCreatesSetOp(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	admins := nodes.NewTable("admins")
	m1 := NewSelectManager(users)
	m2 := NewSelectManager(admins)
	op := m1.Intersect(m2)
	if op.Type != nodes.Intersect {
		t.Errorf("expected Intersect, got %d", op.Type)
	}
}

func TestExceptCreatesSetOp(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	admins := nodes.NewTable("admins")
	m1 := NewSelectManager(users)
	m2 := NewSelectManager(admins)
	op := m1.Except(m2)
	if op.Type != nodes.Except {
		t.Errorf("expected Except, got %d", op.Type)
	}
}

func TestIntersectAllCreatesSetOp(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	admins := nodes.NewTable("admins")
	m1 := NewSelectManager(users)
	m2 := NewSelectManager(admins)
	op := m1.IntersectAll(m2)
	if op.Type != nodes.IntersectAll {
		t.Errorf("expected IntersectAll, got %d", op.Type)
	}
}

func TestExceptAllCreatesSetOp(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	admins := nodes.NewTable("admins")
	m1 := NewSelectManager(users)
	m2 := NewSelectManager(admins)
	op := m1.ExceptAll(m2)
	if op.Type != nodes.ExceptAll {
		t.Errorf("expected ExceptAll, got %d", op.Type)
	}
}

// --- CTE ---

func TestWithAddsCTE(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sub := NewSelectManager(nodes.NewTable("orders"))
	m := NewSelectManager(users)
	m.With("recent_orders", sub.Core)
	if len(m.Core.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(m.Core.CTEs))
	}
	if m.Core.CTEs[0].Name != "recent_orders" {
		t.Errorf("expected name 'recent_orders', got %q", m.Core.CTEs[0].Name)
	}
	if m.Core.CTEs[0].Recursive {
		t.Error("expected non-recursive")
	}
}

func TestWithRecursiveAddsCTE(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	sub := NewSelectManager(nodes.NewTable("categories"))
	m := NewSelectManager(users)
	m.WithRecursive("tree", sub.Core)
	if len(m.Core.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(m.Core.CTEs))
	}
	if !m.Core.CTEs[0].Recursive {
		t.Error("expected recursive")
	}
}

// --- CloneCore copies new fields ---

func TestCloneCoreCopiesnewFields(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)
	m.DistinctOn(users.Col("email"))
	m.ForUpdate().SkipLocked()
	m.Comment("test")
	m.Hint("SeqScan")
	m.With("cte1", &nodes.SelectCore{From: nodes.NewTable("t")})

	clone := m.CloneCore()

	// Verify fields are copied
	if len(clone.DistinctOn) != 1 {
		t.Error("DistinctOn not cloned")
	}
	if clone.Lock != nodes.ForUpdate {
		t.Error("Lock not cloned")
	}
	if !clone.SkipLocked {
		t.Error("SkipLocked not cloned")
	}
	if clone.Comment != "test" {
		t.Error("Comment not cloned")
	}
	if len(clone.Hints) != 1 {
		t.Error("Hints not cloned")
	}
	if len(clone.CTEs) != 1 {
		t.Error("CTEs not cloned")
	}

	// Verify independence
	clone.DistinctOn = append(clone.DistinctOn, users.Col("dept"))
	if len(m.Core.DistinctOn) != 1 {
		t.Error("modifying clone affected original DistinctOn")
	}
	clone.Hints = append(clone.Hints, "extra")
	if len(m.Core.Hints) != 1 {
		t.Error("modifying clone affected original Hints")
	}
	clone.CTEs = append(clone.CTEs, &nodes.CTENode{Name: "cte2"})
	if len(m.Core.CTEs) != 1 {
		t.Error("modifying clone affected original CTEs")
	}
}

func TestSelectManagerAs(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users).Select(users.Col("id"))
	alias := m.As("sub")

	if alias.AliasName != "sub" {
		t.Errorf("expected alias name %q, got %q", "sub", alias.AliasName)
	}
	if alias.Relation != m.Core {
		t.Error("expected Relation to be the SelectCore")
	}
	// The alias should work as a column source.
	col := alias.Col("id")
	if col.Relation != alias {
		t.Error("expected column Relation to be the alias")
	}
}

// --- Transformers ---

func TestTransformers(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	m := NewSelectManager(users)

	// Add a transformer
	transformer := &mockTransformer{}
	m.Use(transformer)

	// Get transformers
	transformers := m.Transformers()
	if len(transformers) != 1 {
		t.Errorf("expected 1 transformer, got %d", len(transformers))
	}
	if transformers[0] != transformer {
		t.Error("expected transformer to match")
	}
}

type mockTransformer struct {
	plugins.BaseTransformer
}

// --- LateralJoin edge cases ---

func TestLateralJoinWithComplexSubquery(t *testing.T) {
	t.Parallel()
	users := nodes.NewTable("users")
	orders := nodes.NewTable("orders")

	// Create a subquery
	subquery := NewSelectManager(orders).
		Select(orders.Col("total")).
		Where(orders.Col("user_id").Eq(users.Col("id"))).
		Limit(1)

	subAlias := subquery.As("latest_order")
	trueCondition := users.Col("id").Eq(users.Col("id")) // Always true condition
	m := NewSelectManager(users).
		Select(users.Col("id")).
		LateralJoin(subAlias).
		On(trueCondition)

	// Verify the LATERAL flag is set
	if len(m.Core.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(m.Core.Joins))
	}
	if !m.Core.Joins[0].Lateral {
		t.Error("expected Lateral flag to be true")
	}
	// Verify it produces SQL without error
	sql, _, err := m.ToSQL(testutil.StubVisitor{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Error("expected non-empty SQL")
	}
}
