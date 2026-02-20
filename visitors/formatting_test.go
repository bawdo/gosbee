package visitors

import (
	"testing"

	"github.com/bawdo/gosbee/internal/testutil"
	"github.com/bawdo/gosbee/nodes"
)

// fmtPG returns a FormattingVisitor wrapping a non-parameterised PostgresVisitor.
// Used throughout formatting tests for concise setup.
func fmtPG() *FormattingVisitor {
	return NewFormattingVisitor(NewPostgresVisitor(WithoutParams()))
}

// fmtMySQL returns a FormattingVisitor wrapping a non-parameterised MySQLVisitor.
func fmtMySQL() *FormattingVisitor {
	return NewFormattingVisitor(NewMySQLVisitor(WithoutParams()))
}

func TestFormattingVisitorDelegatesLeafNodes(t *testing.T) {
	t.Parallel()
	fv := fmtPG()
	users := nodes.NewTable("users")

	// VisitTable
	testutil.AssertSQL(t, fv, users, `"users"`)
	// VisitAttribute
	testutil.AssertSQL(t, fv, users.Col("id"), `"users"."id"`)
	// VisitLiteral
	testutil.AssertSQL(t, fv, nodes.Literal("alice"), `'alice'`)
	testutil.AssertSQL(t, fv, nodes.Literal(42), `42`)
	// VisitStar
	testutil.AssertSQL(t, fv, nodes.Star(), `*`)
}

func TestFormattingVisitorDelegatesMySQLQuoting(t *testing.T) {
	t.Parallel()
	fv := fmtMySQL()
	users := nodes.NewTable("users")
	testutil.AssertSQL(t, fv, users, "`users`")
	testutil.AssertSQL(t, fv, users.Col("id"), "`users`.`id`")
}

func TestFormattingVisitorParamsForwardedToInner(t *testing.T) {
	t.Parallel()
	inner := NewPostgresVisitor() // parameterised (default)
	fv := NewFormattingVisitor(inner)

	// FormattingVisitor must implement Parameterizer
	p, ok := nodes.Visitor(fv).(nodes.Parameterizer)
	if !ok {
		t.Fatal("FormattingVisitor does not implement Parameterizer")
	}

	p.Reset()
	_ = nodes.Literal("hello").Accept(fv)
	params := p.Params()
	if len(params) != 1 || params[0] != "hello" {
		t.Errorf("expected params [hello], got %v", params)
	}

	// Verify Reset clears the accumulated params
	p.Reset()
	if got := p.Params(); got != nil {
		t.Errorf("expected nil params after Reset, got %v", got)
	}
}

func TestFormattingVisitorParamsNilWhenInnerNotParameterizer(t *testing.T) {
	t.Parallel()
	fv := fmtPG() // WithoutParams — inner does not collect params
	p, ok := nodes.Visitor(fv).(nodes.Parameterizer)
	if !ok {
		t.Fatal("FormattingVisitor does not implement Parameterizer")
	}
	if got := p.Params(); got != nil {
		t.Errorf("expected nil params, got %v", got)
	}
}
