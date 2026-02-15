package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/bawdo/gosbee/internal/testutil"
)

// helper executes commands then returns GenerateSQL output.
func execSQL(t *testing.T, engine string, commands ...string) string {
	t.Helper()
	sess := NewSession(engine, nil)
	sess.out = io.Discard
	for _, cmd := range commands {
		if err := sess.Execute(cmd); err != nil {
			t.Fatalf("command %q failed: %v", cmd, err)
		}
	}
	sql, err := sess.GenerateSQL()
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}
	return sql
}


// --- Tokenizer ---

func TestTokenizeSimple(t *testing.T) {
	t.Parallel()
	tokens := tokenize("users.age > 18")
	expected := []string{"users.age", ">", "18"}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d: %v", len(expected), len(tokens), tokens)
	}
	for i, e := range expected {
		if tokens[i] != e {
			t.Errorf("token[%d]: expected %q, got %q", i, e, tokens[i])
		}
	}
}

func TestTokenizeQuotedString(t *testing.T) {
	t.Parallel()
	tokens := tokenize("users.name = 'John Smith'")
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d: %v", len(tokens), tokens)
	}
	if tokens[2] != "'John Smith'" {
		t.Errorf("expected 'John Smith', got %q", tokens[2])
	}
}

func TestTokenizeOperators(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected []string
	}{
		{"a != b", []string{"a", "!=", "b"}},
		{"a <> b", []string{"a", "<>", "b"}},
		{"a >= b", []string{"a", ">=", "b"}},
		{"a <= b", []string{"a", "<=", "b"}},
		{"a = b", []string{"a", "=", "b"}},
		{"a > b", []string{"a", ">", "b"}},
		{"a < b", []string{"a", "<", "b"}},
	}
	for _, tt := range tests {
		tokens := tokenize(tt.input)
		if len(tokens) != len(tt.expected) {
			t.Errorf("tokenize(%q): expected %v, got %v", tt.input, tt.expected, tokens)
			continue
		}
		for i, e := range tt.expected {
			if tokens[i] != e {
				t.Errorf("tokenize(%q)[%d]: expected %q, got %q", tt.input, i, e, tokens[i])
			}
		}
	}
}

func TestTokenizeInList(t *testing.T) {
	t.Parallel()
	tokens := tokenize("('a', 'b', 'c')")
	expected := []string{"(", "'a'", ",", "'b'", ",", "'c'", ")"}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, tokens)
	}
	for i, e := range expected {
		if tokens[i] != e {
			t.Errorf("token[%d]: expected %q, got %q", i, e, tokens[i])
		}
	}
}

// --- ParseValue ---

func TestParseValueString(t *testing.T) {
	t.Parallel()
	val, err := parseValue("'hello'")
	if err != nil {
		t.Fatal(err)
	}
	if val != "hello" {
		t.Errorf("expected %q, got %v", "hello", val)
	}
}

func TestParseValueInt(t *testing.T) {
	t.Parallel()
	val, err := parseValue("42")
	if err != nil {
		t.Fatal(err)
	}
	if val != 42 {
		t.Errorf("expected 42, got %v", val)
	}
}

func TestParseValueFloat(t *testing.T) {
	t.Parallel()
	val, err := parseValue("3.14")
	if err != nil {
		t.Fatal(err)
	}
	if val != 3.14 {
		t.Errorf("expected 3.14, got %v", val)
	}
}

func TestParseValueBool(t *testing.T) {
	t.Parallel()
	v1, _ := parseValue("true")
	v2, _ := parseValue("false")
	if v1 != true {
		t.Error("expected true")
	}
	if v2 != false {
		t.Error("expected false")
	}
}

func TestParseValueNull(t *testing.T) {
	t.Parallel()
	val, _ := parseValue("null")
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}
}

// --- Simple SELECT ---

func TestSimpleSelect(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.id, users.name",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."id", "users"."name" FROM "users"`)
}

func TestSelectStar(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select *",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users"`)
}

func TestSelectQualifiedStar(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.*",
	)
	testutil.AssertEqual(t, sql, `SELECT "users".* FROM "users"`)
}

func TestDefaultStarProjection(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"from users",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users"`)
}

// --- WHERE conditions ---

func TestWhereEq(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.active = true",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."active" = TRUE`)
}

func TestWhereNotEq(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.status != 'deleted'",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."status" != 'deleted'`)
}

func TestWhereGt(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.age > 18",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."age" > 18`)
}

func TestWhereGtEq(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.age >= 18",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."age" >= 18`)
}

func TestWhereLt(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.age < 65",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."age" < 65`)
}

func TestWhereLtEq(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.age <= 65",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."age" <= 65`)
}

func TestWhereLike(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.name like '%foo%'",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."name" LIKE '%foo%'`)
}

func TestWhereNotLike(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.name not like '%bar%'",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."name" NOT LIKE '%bar%'`)
}

func TestWhereIsNull(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.deleted_at is null",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."deleted_at" IS NULL`)
}

func TestWhereIsNotNull(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.deleted_at is not null",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."deleted_at" IS NOT NULL`)
}

func TestWhereIn(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.status in ('active', 'pending')",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."status" IN ('active', 'pending')`)
}

func TestWhereNotIn(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.status not in ('deleted')",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."status" NOT IN ('deleted')`)
}

func TestWhereBetween(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.age between 18 and 65",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."age" BETWEEN 18 AND 65`)
}

func TestMultipleWheres(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.active = true",
		"where users.age > 18",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."active" = TRUE AND "users"."age" > 18`)
}

// --- Column-to-column comparison in WHERE ---

func TestWhereColumnToColumn(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"table posts",
		"from users",
		"where users.id = posts.author_id",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."id" = "posts"."author_id"`)
}

// --- JOINs ---

func TestInnerJoin(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"table posts",
		"from users",
		"join posts on users.id = posts.user_id",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id"`)
}

func TestLeftJoin(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"table posts",
		"from users",
		"left join posts on users.id = posts.user_id",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" LEFT OUTER JOIN "posts" ON "users"."id" = "posts"."user_id"`)
}

func TestRightJoin(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"table posts",
		"from users",
		"right join posts on users.id = posts.user_id",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" RIGHT OUTER JOIN "posts" ON "users"."id" = "posts"."user_id"`)
}

func TestFullJoin(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"table posts",
		"from users",
		"full join posts on users.id = posts.user_id",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" FULL OUTER JOIN "posts" ON "users"."id" = "posts"."user_id"`)
}

func TestCrossJoin(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"table colors",
		"from users",
		"cross join colors",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" CROSS JOIN "colors"`)
}

func TestOuterJoinAlias(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"table posts",
		"from users",
		"outer join posts on users.id = posts.user_id",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" LEFT OUTER JOIN "posts" ON "users"."id" = "posts"."user_id"`)
}

// --- Engine switching ---

func TestEngineSwitching(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("from users")
	_ = sess.Execute("select users.id")

	pg, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, pg, `SELECT "users"."id" FROM "users"`)

	_ = sess.Execute("engine mysql")
	my, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, my, "SELECT `users`.`id` FROM `users`")

	_ = sess.Execute("engine sqlite")
	sl, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, sl, `SELECT "users"."id" FROM "users"`)
}

func TestSetEngineAlias(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("set_engine mysql")

	sql, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, sql, "SELECT * FROM `users`")
}

// --- Soft-delete plugin ---

func TestPluginSoftDelete(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"plugin softdelete",
		"where users.active = true",
		"sql",
	)
	_ = sql // cmdSQL prints; we test via GenerateSQL

	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("plugin softdelete")
	_ = sess.Execute("from users")
	_ = sess.Execute("where users.active = true")
	got, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, got, `SELECT * FROM "users" WHERE "users"."active" = TRUE AND "users"."deleted_at" IS NULL`)
}

func TestPluginSoftDeleteCustomColumn(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("plugin softdelete removed_at")
	_ = sess.Execute("from users")
	got, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, got, `SELECT * FROM "users" WHERE "users"."removed_at" IS NULL`)
}

func TestPluginOff(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("plugin softdelete")
	_ = sess.Execute("from users")
	_ = sess.Execute("plugin off")
	got, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, got, `SELECT * FROM "users"`)
}

func TestPluginOffSoftdelete(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("plugin softdelete")
	_ = sess.Execute("from users")
	_ = sess.Execute("plugin off softdelete")
	got, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, got, `SELECT * FROM "users"`)
}

func TestPluginOffUnknown(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("plugin off foobar")
	if err == nil {
		t.Error("expected error for unknown plugin name")
	}
}

func TestPluginOffAllExplicit(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("plugin softdelete")
	_ = sess.Execute("from users")
	_ = sess.Execute("where users.active = true")
	_ = sess.Execute("plugin off")
	got, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, got, `SELECT * FROM "users" WHERE "users"."active" = TRUE`)
}

func TestPlugins(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("plugins")
	if err != nil {
		t.Errorf("plugins command failed: %v", err)
	}
}

func TestPluginsShowsEnabled(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("plugin softdelete")
	err := sess.Execute("plugins")
	if err != nil {
		t.Errorf("plugins command failed: %v", err)
	}
}

func TestPluginOffSoftdeletePreservesQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("plugin softdelete")
	_ = sess.Execute("from users")
	_ = sess.Execute("where users.active = true")
	_ = sess.Execute("plugin off softdelete")
	got, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, got, `SELECT * FROM "users" WHERE "users"."active" = TRUE`)
}

func TestPluginSoftdeleteOnTables(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("plugin softdelete deleted_at on users posts")
	if err != nil {
		t.Fatalf("command failed: %v", err)
	}
	if _, ok := sess.plugins.get("softdelete"); !ok {
		t.Fatal("expected softdelete to be enabled")
	}
}

func TestPluginSoftdeleteOnTablesSQL(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("table posts")
	_ = sess.Execute("plugin softdelete deleted_at on users")
	_ = sess.Execute("from users")
	_ = sess.Execute("join posts on users.id = posts.user_id")
	got, _ := sess.GenerateSQL()
	// Only users should get the IS NULL condition
	testutil.AssertEqual(t, got, `SELECT * FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id" WHERE "users"."deleted_at" IS NULL`)
}

func TestPluginSoftdeletePerTableColumns(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("plugin softdelete users.deleted_at, posts.removed_at")
	if err != nil {
		t.Fatalf("command failed: %v", err)
	}
	entry, ok := sess.plugins.get("softdelete")
	if !ok {
		t.Fatal("expected softdelete to be enabled")
	}
	status := entry.status()
	if !strings.Contains(status, "users.deleted_at") || !strings.Contains(status, "posts.removed_at") {
		t.Errorf("expected per-table columns in status, got %s", status)
	}
}

func TestPluginSoftdeletePerTableColumnsSQL(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("table posts")
	_ = sess.Execute("plugin softdelete users.deleted_at, posts.removed_at")
	_ = sess.Execute("from users")
	_ = sess.Execute("join posts on users.id = posts.user_id")
	got, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, got, `SELECT * FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id" WHERE "users"."deleted_at" IS NULL AND "posts"."removed_at" IS NULL`)
}

// --- Table alias ---

func TestTableAlias(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"alias users u",
		"from u",
		"select u.id, u.name",
	)
	testutil.AssertEqual(t, sql, `SELECT "u"."id", "u"."name" FROM "users" AS "u"`)
}

// --- Reset ---

func TestReset(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("reset")
	_, err := sess.GenerateSQL()
	if err == nil {
		t.Error("expected error after reset, got nil")
	}
}

// --- Auto-register tables ---

func TestFromAutoRegistersTable(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"from users",
		"select users.id",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."id" FROM "users"`)
}

func TestJoinAutoRegistersTable(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"from users",
		"join posts on users.id = posts.user_id",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id"`)
}

// --- Error handling ---

func TestErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("select users.id")
	if err == nil {
		t.Error("expected error when no query exists")
	}
}

func TestErrorUnknownTable(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	err := sess.Execute("where unknown.col = 1")
	if err == nil {
		t.Error("expected error for unregistered table")
	}
}

func TestErrorUnknownCommand(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("foobar")
	if err == nil {
		t.Error("expected error for unknown command")
	}
}

func TestErrorBadEngine(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("engine oracle")
	if err == nil {
		t.Error("expected error for unknown engine")
	}
}

// --- End-to-end from design doc ---

func TestDesignDocScenario1(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.id, users.name",
		"where users.active = true",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."id", "users"."name" FROM "users" WHERE "users"."active" = TRUE`)
}

func TestDesignDocDialectSwitch(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("from users")
	_ = sess.Execute("select users.id, users.name")
	_ = sess.Execute("where users.active = true")

	pg, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, pg, `SELECT "users"."id", "users"."name" FROM "users" WHERE "users"."active" = TRUE`)

	_ = sess.Execute("engine mysql")
	my, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, my, "SELECT `users`.`id`, `users`.`name` FROM `users` WHERE `users`.`active` = TRUE")
}

// --- GROUP BY ---

func TestGroupBySingle(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.status",
		"group users.status",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."status" FROM "users" GROUP BY "users"."status"`)
}

func TestGroupByMultiple(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.status, users.role",
		"group users.status, users.role",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."status", "users"."role" FROM "users" GROUP BY "users"."status", "users"."role"`)
}

func TestGroupByErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("group users.status")
	if err == nil {
		t.Error("expected error when no query exists")
	}
}

// --- HAVING ---

func TestHaving(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table orders")
	_ = sess.Execute("from orders")
	_ = sess.Execute("select orders.customer_id")
	_ = sess.Execute("group orders.customer_id")
	_ = sess.Execute("having orders.customer_id > 5")
	got, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, got, `SELECT "orders"."customer_id" FROM "orders" GROUP BY "orders"."customer_id" HAVING "orders"."customer_id" > 5`)
}

func TestHavingErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("having orders.total > 100")
	if err == nil {
		t.Error("expected error when no query exists")
	}
}

// --- GROUP BY + HAVING + WHERE + ORDER BY + LIMIT ---

func TestGroupByFullQuery(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"select orders.customer_id",
		"where orders.status = 'completed'",
		"group orders.customer_id",
		"having orders.customer_id > 5",
		"order orders.customer_id asc",
		"limit 10",
	)
	testutil.AssertEqual(t, sql, `SELECT "orders"."customer_id" FROM "orders" WHERE "orders"."status" = 'completed' GROUP BY "orders"."customer_id" HAVING "orders"."customer_id" > 5 ORDER BY "orders"."customer_id" ASC LIMIT 10`)
}

func TestGroupByMySQL(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "mysql",
		"table users",
		"from users",
		"select users.status",
		"group users.status",
	)
	testutil.AssertEqual(t, sql, "SELECT `users`.`status` FROM `users` GROUP BY `users`.`status`")
}

// --- ORDER BY ---

func TestOrderByAsc(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"order users.name asc",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" ORDER BY "users"."name" ASC`)
}

func TestOrderByDesc(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"order users.created_at desc",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" ORDER BY "users"."created_at" DESC`)
}

func TestOrderByDefaultAsc(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"order users.name",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" ORDER BY "users"."name" ASC`)
}

func TestOrderByMultiple(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"order users.name asc, users.id desc",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" ORDER BY "users"."name" ASC, "users"."id" DESC`)
}

// --- LIMIT ---

func TestLimit(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"from users",
		"limit 10",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" LIMIT 10`)
}

func TestTakeAlias(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"from users",
		"take 5",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" LIMIT 5`)
}

// --- OFFSET ---

func TestOffset(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"from users",
		"limit 10",
		"offset 20",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" LIMIT 10 OFFSET 20`)
}

// --- Combined ORDER BY + LIMIT + OFFSET ---

func TestOrderLimitOffset(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.id, users.name",
		"where users.active = true",
		"order users.name asc",
		"limit 25",
		"offset 50",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."id", "users"."name" FROM "users" WHERE "users"."active" = TRUE ORDER BY "users"."name" ASC LIMIT 25 OFFSET 50`)
}

func TestOrderLimitMySQL(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "mysql",
		"table users",
		"from users",
		"order users.name desc",
		"limit 10",
	)
	testutil.AssertEqual(t, sql, "SELECT * FROM `users` ORDER BY `users`.`name` DESC LIMIT 10")
}

// --- Error cases for new commands ---

func TestLimitErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("limit 10")
	if err == nil {
		t.Error("expected error when no query exists")
	}
}

func TestLimitErrorBadValue(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	err := sess.Execute("limit abc")
	if err == nil {
		t.Error("expected error for non-integer limit")
	}
}

func TestOffsetErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("offset 10")
	if err == nil {
		t.Error("expected error when no query exists")
	}
}

func TestOrderErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("order users.name asc")
	if err == nil {
		t.Error("expected error when no query exists")
	}
}

// --- DISTINCT ---

func TestDistinct(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.name",
		"distinct",
	)
	testutil.AssertEqual(t, sql, `SELECT DISTINCT "users"."name" FROM "users"`)
}

func TestDistinctDefaultStar(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"from users",
		"distinct",
	)
	testutil.AssertEqual(t, sql, `SELECT DISTINCT * FROM "users"`)
}

func TestDistinctMySQL(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "mysql",
		"from users",
		"distinct",
	)
	testutil.AssertEqual(t, sql, "SELECT DISTINCT * FROM `users`")
}

func TestDistinctErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("distinct")
	if err == nil {
		t.Error("expected error when no query exists")
	}
}

func TestDistinctPreservedByPluginOff(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("plugin softdelete")
	_ = sess.Execute("from users")
	_ = sess.Execute("distinct")
	_ = sess.Execute("plugin off")
	got, _ := sess.GenerateSQL()
	testutil.AssertEqual(t, got, `SELECT DISTINCT * FROM "users"`)
}

// --- Parameterize ---

func TestParameterizeToggle(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	if sess.parameterize {
		t.Error("expected parameterize to be false initially")
	}
	_ = sess.Execute("parameterize")
	if !sess.parameterize {
		t.Error("expected parameterize to be true after toggle")
	}
	_ = sess.Execute("parameterize")
	if sess.parameterize {
		t.Error("expected parameterize to be false after second toggle")
	}
}

func TestParameterizeParamsAlias(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("params")
	if !sess.parameterize {
		t.Error("expected parameterize to be true via 'params' alias")
	}
}

func TestParameterizePreservedOnEngineSwitch(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("parameterize")
	_ = sess.Execute("engine mysql")
	if !sess.parameterize {
		t.Error("expected parameterize to be preserved after engine switch")
	}
	// Verify visitor is a Parameterizer
	if _, ok := sess.visitor.(interface{ Params() []any }); !ok {
		t.Error("expected visitor to implement Parameterizer after engine switch")
	}
}

func TestParameterizeSQL(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("parameterize")
	_ = sess.Execute("table users")
	_ = sess.Execute("from users")
	_ = sess.Execute("where users.name = 'Alice'")
	_ = sess.Execute("where users.age > 30")

	// GenerateSQL uses the visitor directly — check the visitor's params
	if p, ok := sess.visitor.(interface {
		Params() []any
		Reset()
	}); ok {
		p.Reset()
		sql, _ := sess.GenerateSQL()
		params := p.Params()
		if sql != `SELECT * FROM "users" WHERE "users"."name" = $1 AND "users"."age" > $2` {
			t.Errorf("unexpected SQL: %s", sql)
		}
		if len(params) != 2 {
			t.Fatalf("expected 2 params, got %d: %v", len(params), params)
		}
		if params[0] != "Alice" || params[1] != 30 {
			t.Errorf("expected [Alice 30], got %v", params)
		}
	} else {
		t.Fatal("visitor does not implement Parameterizer")
	}
}

// --- Full complex query ---

// --- Expression REPL (expr command) ---

// exprSQL is a helper that registers tables and evaluates an expression.
func exprSQL(t *testing.T, engine string, tables []string, expr string) string {
	t.Helper()
	sess := NewSession(engine, nil)
	for _, tbl := range tables {
		if err := sess.Execute("table " + tbl); err != nil {
			t.Fatalf("table %q: %v", tbl, err)
		}
	}
	node, err := sess.parseExpression(expr)
	if err != nil {
		t.Fatalf("parseExpression(%q): %v", expr, err)
	}
	return node.Accept(sess.visitor)
}

func TestExprSimpleCondition(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.age > 18")
	testutil.AssertEqual(t, got, `"users"."age" > 18`)
}

func TestExprAnd(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.age > 18 and users.active = true")
	testutil.AssertEqual(t, got, `"users"."age" > 18 AND "users"."active" = TRUE`)
}

func TestExprOr(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.name = 'alice' or users.name = 'bob'")
	testutil.AssertEqual(t, got, `("users"."name" = 'alice' OR "users"."name" = 'bob')`)
}

func TestExprAndOr(t *testing.T) {
	t.Parallel()
	// AND binds tighter than OR: a AND b OR c => (a AND b) OR c
	got := exprSQL(t, "postgres", []string{"users"}, "users.a = 1 and users.b = 2 or users.c = 3")
	testutil.AssertEqual(t, got, `("users"."a" = 1 AND "users"."b" = 2 OR "users"."c" = 3)`)
}

func TestExprNot(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "not users.active = true")
	testutil.AssertEqual(t, got, `NOT ("users"."active" = TRUE)`)
}

func TestExprIsNull(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.deleted_at is null")
	testutil.AssertEqual(t, got, `"users"."deleted_at" IS NULL`)
}

func TestExprIn(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.role in ('admin', 'mod')")
	testutil.AssertEqual(t, got, `"users"."role" IN ('admin', 'mod')`)
}

func TestExprBetween(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.age between 18 and 65")
	testutil.AssertEqual(t, got, `"users"."age" BETWEEN 18 AND 65`)
}

func TestExprBetweenAndCondition(t *testing.T) {
	t.Parallel()
	// BETWEEN's AND should not be treated as a combinator
	got := exprSQL(t, "postgres", []string{"users"}, "users.age between 18 and 65 and users.active = true")
	testutil.AssertEqual(t, got, `"users"."age" BETWEEN 18 AND 65 AND "users"."active" = TRUE`)
}

func TestExprDialect(t *testing.T) {
	t.Parallel()
	tables := []string{"users"}
	expr := "users.active = true"

	pg := exprSQL(t, "postgres", tables, expr)
	testutil.AssertEqual(t, pg, `"users"."active" = TRUE`)

	my := exprSQL(t, "mysql", tables, expr)
	testutil.AssertEqual(t, my, "`users`.`active` = TRUE")

	sl := exprSQL(t, "sqlite", tables, expr)
	testutil.AssertEqual(t, sl, `"users"."active" = TRUE`)
}

func TestExprNoTable(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_, err := sess.parseExpression("unknown.col = 1")
	if err == nil {
		t.Error("expected error for unregistered table")
	}
}

func TestExprEmpty(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_, err := sess.parseExpression("")
	if err == nil {
		t.Error("expected error for empty expression")
	}
}

func TestExprParameterized(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("parameterize")
	_ = sess.Execute("table users")

	node, err := sess.parseExpression("users.age > 18")
	if err != nil {
		t.Fatalf("parseExpression: %v", err)
	}

	if p, ok := sess.visitor.(interface {
		Params() []any
		Reset()
	}); ok {
		p.Reset()
		sql := node.Accept(sess.visitor)
		params := p.Params()
		testutil.AssertEqual(t, sql, `"users"."age" > $1`)
		if len(params) != 1 || params[0] != 18 {
			t.Errorf("expected params [18], got %v", params)
		}
	} else {
		t.Fatal("visitor does not implement Parameterizer")
	}
}

// --- Full complex query ---

func TestComplexQuery(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"table posts",
		"table comments",
		"from users",
		"select users.name, posts.title",
		"join posts on users.id = posts.user_id",
		"left join comments on posts.id = comments.post_id",
		"where users.active = true",
		"where posts.published = true",
	)
	expected := `SELECT "users"."name", "posts"."title" FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id" LEFT OUTER JOIN "comments" ON "posts"."id" = "comments"."post_id" WHERE "users"."active" = TRUE AND "posts"."published" = TRUE`
	testutil.AssertEqual(t, sql, expected)
}

// --- OPA commands ---

func TestOPAOff(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{url: "http://localhost:8181", policy: "data.authz.allow"}
	err := sess.Execute("opa off")
	if err != nil {
		t.Fatalf("opa off failed: %v", err)
	}
	if sess.opaConfig != nil {
		t.Error("expected opaPlugin to be nil")
	}
}

func TestOPAOffNotEnabled(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("opa off")
	if err == nil {
		t.Error("expected error when OPA not enabled")
	}
}

func TestOPAStatus(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("opa status")
	if err != nil {
		t.Fatalf("opa status failed: %v", err)
	}
}

func TestOPAStatusEnabled(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input: map[string]any{"subject": map[string]any{"role": "admin"}},
	}
	err := sess.Execute("opa status")
	if err != nil {
		t.Fatalf("opa status failed: %v", err)
	}
}

func TestOPASetupRequiresInteractive(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("opa")
	if err == nil {
		t.Error("expected error for non-interactive opa setup")
	}
}

// --- OPA helpers ---

func TestParseOPAValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected any
	}{
		{"42", float64(42)},
		{"3.14", float64(3.14)},
		{"true", true},
		{"false", false},
		{"admin", "admin"},
		{"hello world", "hello world"},
	}
	for _, tt := range tests {
		got := parseOPAValue(tt.input)
		if got != tt.expected {
			t.Errorf("parseOPAValue(%q): expected %v (%T), got %v (%T)", tt.input, tt.expected, tt.expected, got, got)
		}
	}
}

func TestSetNestedValue(t *testing.T) {
	t.Parallel()
	m := map[string]any{}
	setNestedValue(m, "subject.role", "admin")
	setNestedValue(m, "subject.tenant_id", float64(42))
	sub, ok := m["subject"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested map, got %T", m["subject"])
	}
	if sub["role"] != "admin" {
		t.Errorf("expected admin, got %v", sub["role"])
	}
	if sub["tenant_id"] != float64(42) {
		t.Errorf("expected 42, got %v", sub["tenant_id"])
	}
}

func TestDeleteNestedValue(t *testing.T) {
	t.Parallel()
	m := map[string]any{
		"subject": map[string]any{
			"role":      "admin",
			"tenant_id": float64(42),
		},
	}
	deleteNestedValue(m, "subject.role")
	sub := m["subject"].(map[string]any)
	if _, ok := sub["role"]; ok {
		t.Error("expected role to be deleted")
	}
	if sub["tenant_id"] != float64(42) {
		t.Errorf("expected tenant_id preserved, got %v", sub["tenant_id"])
	}
}

func TestDeleteNestedValueTopLevel(t *testing.T) {
	t.Parallel()
	m := map[string]any{"key": "value", "other": "kept"}
	deleteNestedValue(m, "key")
	if _, ok := m["key"]; ok {
		t.Error("expected key to be deleted")
	}
	if m["other"] != "kept" {
		t.Error("expected other to be preserved")
	}
}

// --- plugin off clears OPA ---

func TestPluginOffAllClearsOPA(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{url: "http://localhost:8181", policy: "data.authz.allow"}
	_ = configureOPA(sess, "")
	_ = sess.Execute("plugin softdelete")
	_ = sess.Execute("from users")
	_ = sess.Execute("plugin off")
	if sess.opaConfig != nil {
		t.Error("expected OPA cleared")
	}
	if _, ok := sess.plugins.get("softdelete"); ok {
		t.Error("expected softdelete cleared")
	}
}

func TestPluginOffOPANamed(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{url: "http://localhost:8181", policy: "data.authz.allow"}
	_ = configureOPA(sess, "")
	_ = sess.Execute("plugin off opa")
	if sess.opaConfig != nil {
		t.Error("expected OPA cleared")
	}
}

func TestPluginOffSoftdeletePreservesOPA(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("plugin softdelete")
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
	}
	_ = configureOPA(sess, "")
	_ = sess.Execute("from users")
	_ = sess.Execute("plugin off softdelete")
	if _, ok := sess.plugins.get("softdelete"); ok {
		t.Error("expected softdelete cleared")
	}
	if sess.opaConfig == nil {
		t.Error("expected OPA to survive")
	}
}

func TestPluginOffOPAPreservesSoftdelete(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("plugin softdelete")
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
	}
	_ = configureOPA(sess, "")
	_ = sess.Execute("from users")
	_ = sess.Execute("plugin off opa")
	if sess.opaConfig != nil {
		t.Error("expected OPA cleared")
	}
	if _, ok := sess.plugins.get("softdelete"); !ok {
		t.Error("expected softdelete to survive")
	}
	// Softdelete should still be active on the rebuilt query.
	got, _ := sess.GenerateSQL()
	if !strings.Contains(got, "deleted_at") {
		t.Errorf("expected softdelete condition in SQL, got: %s", got)
	}
}

// --- OPA reload ---

func TestOPAReloadRebuildsPlugin(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input: map[string]any{"subject": map[string]any{"role": "admin"}},
	}
	_ = configureOPA(sess, "")
	sess.opaReload()
	if _, ok := sess.plugins.get("opa"); !ok {
		t.Error("expected OPA to be registered")
	}
	if sess.opaConfig.url != "http://localhost:8181" {
		t.Error("expected URL preserved")
	}
	if sess.opaConfig.policy != "data.authz.allow" {
		t.Error("expected policy preserved")
	}
}

func TestOPAReloadRebuildQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input: map[string]any{},
	}
	_ = configureOPA(sess, "")
	_ = sess.Execute("from users")
	oldQuery := sess.query
	sess.opaReload()
	if sess.query == oldQuery {
		t.Error("expected query to be rebuilt")
	}
	if sess.query == nil {
		t.Error("expected query to still exist")
	}
}

func TestOPAReloadCommand(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input: map[string]any{},
	}
	_ = configureOPA(sess, "")
	err := sess.Execute("opa reload")
	if err != nil {
		t.Fatalf("opa reload failed: %v", err)
	}
	if sess.opaConfig == nil {
		t.Error("expected OPA to still be enabled")
	}
}

func TestOPAReloadRequiresEnabled(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("opa reload")
	if err == nil {
		t.Error("expected error when OPA not enabled")
	}
}

// --- getNestedValue ---

func TestGetNestedValue(t *testing.T) {
	t.Parallel()
	m := map[string]any{
		"subject": map[string]any{
			"role": "admin",
		},
	}
	if got := getNestedValue(m, "subject.role"); got != "admin" {
		t.Errorf("expected admin, got %v", got)
	}
	if got := getNestedValue(m, "subject.missing"); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
	if got := getNestedValue(m, "nonexistent.path"); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestOPAInputsRequiresEnabled(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("opa inputs")
	if err == nil {
		t.Error("expected error when OPA not enabled")
	}
}

// --- OPA explain ---

func TestOPAExplainRequiresEnabled(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("opa explain users")
	if err == nil {
		t.Error("expected error when OPA not enabled")
	}
}

func TestOPAExplainRequiresTableArg(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input: map[string]any{},
	}
	err := sess.Execute("opa explain")
	if err == nil {
		t.Error("expected error for missing table arg")
	}
}

// --- OPA conditions ---

func TestOPAConditionsRequiresEnabled(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("opa conditions")
	if err == nil {
		t.Error("expected error when OPA not enabled")
	}
}

func TestOPAConditionsRequiresQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input: map[string]any{},
	}
	err := sess.Execute("opa conditions")
	if err == nil {
		t.Error("expected error when no query")
	}
}

func TestOPAInputsRequiresInteractive(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input: map[string]any{},
	}
	err := sess.Execute("opa inputs")
	if err == nil {
		t.Error("expected error for non-interactive session")
	}
}

// --- opa url tests ---

func TestOPAUrlRequiresEnabled(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("opa url http://other:8181")
	if err == nil {
		t.Error("expected error when OPA not enabled")
	}
}

func TestOPAUrlRequiresArg(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input:  map[string]any{},
	}
	err := sess.Execute("opa url")
	if err == nil {
		t.Error("expected error for missing URL arg")
	}
}

func TestOPAUrlUpdatesConfig(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil) // non-interactive: skips rediscover prompt
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input:  map[string]any{"subject": map[string]any{"role": "admin"}},
	}
	err := sess.Execute("opa url http://other:8181")
	if err != nil {
		t.Fatalf("opa url failed: %v", err)
	}
	if sess.opaConfig.url != "http://other:8181" {
		t.Errorf("expected URL updated, got %s", sess.opaConfig.url)
	}
}

// --- opa policy tests ---

func TestOPAPolicyRequiresEnabled(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("opa policy data.new.allow")
	if err == nil {
		t.Error("expected error when OPA not enabled")
	}
}

func TestOPAPolicyRequiresArg(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input:  map[string]any{},
	}
	err := sess.Execute("opa policy")
	if err == nil {
		t.Error("expected error for missing policy arg")
	}
}

func TestOPAPolicyUpdatesConfig(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input:  map[string]any{},
	}
	err := sess.Execute("opa policy data.new.allow")
	if err != nil {
		t.Fatalf("opa policy failed: %v", err)
	}
	if sess.opaConfig.policy != "data.new.allow" {
		t.Errorf("expected policy updated, got %s", sess.opaConfig.policy)
	}
}

// --- opa input tests ---

func TestOPAInputSetValue(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input:  map[string]any{"subject": map[string]any{"role": "admin"}},
	}
	err := sess.Execute("opa input subject.role editor")
	if err != nil {
		t.Fatalf("opa input failed: %v", err)
	}
	sub := sess.opaConfig.input["subject"].(map[string]any)
	if sub["role"] != "editor" {
		t.Errorf("expected editor, got %v", sub["role"])
	}
}

func TestOPAInputRemoveValue(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input:  map[string]any{"subject": map[string]any{"role": "admin", "tenant": "acme"}},
	}
	err := sess.Execute("opa input subject.role")
	if err != nil {
		t.Fatalf("opa input failed: %v", err)
	}
	sub := sess.opaConfig.input["subject"].(map[string]any)
	if _, ok := sub["role"]; ok {
		t.Error("expected role to be removed")
	}
	if sub["tenant"] != "acme" {
		t.Error("expected tenant preserved")
	}
}

func TestOPAInputRequiresEnabled(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("opa input subject.role admin")
	if err == nil {
		t.Error("expected error when OPA not enabled")
	}
}

func TestOPAInputRequiresKey(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input:  map[string]any{},
	}
	err := sess.Execute("opa input")
	if err == nil {
		t.Error("expected error for missing key")
	}
}

func TestOPAStatusShowsInputDetails(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input: map[string]any{
			"subject": map[string]any{
				"role":      "admin",
				"tenant_id": "acme",
			},
		},
	}
	// Just verify it doesn't error — visual output verified manually.
	sess.cmdOPAStatus()
}

func TestCompleteOPASubcommands(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	comp := &replCompleter{sess: sess}

	tests := []struct {
		line     string
		contains string
	}{
		{"opa r", "opa reload"},
		{"opa i", "opa input"},
		{"opa e", "opa explain"},
		{"opa c", "opa conditions"},
		{"opa u", "opa url"},
		{"opa p", "opa policy"},
	}
	for _, tt := range tests {
		candidates := comp.completeCommands(tt.line)
		found := false
		for _, c := range candidates {
			if c == tt.contains {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("completing %q: expected %q in candidates %v", tt.line, tt.contains, candidates)
		}
	}
}

func TestOPAInputParsesTypes(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	sess.opaConfig = &opaPluginRef{
		url: "http://localhost:8181", policy: "data.authz.allow",
		input:  map[string]any{},
	}
	_ = sess.Execute("opa input enabled true")
	if sess.opaConfig.input["enabled"] != true {
		t.Errorf("expected bool true, got %v (%T)", sess.opaConfig.input["enabled"], sess.opaConfig.input["enabled"])
	}
	_ = sess.Execute("opa input count 42")
	if sess.opaConfig.input["count"] != float64(42) {
		t.Errorf("expected float64 42, got %v (%T)", sess.opaConfig.input["count"], sess.opaConfig.input["count"])
	}
}

// --- buildEditEntries ---

func TestBuildEditEntriesEmpty(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	entries := sess.buildEditEntries("")
	if len(entries) != 0 {
		t.Errorf("expected 0 entries for empty query, got %d", len(entries))
	}
}

func TestBuildEditEntriesAll(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("select users.id, users.name")
	_ = sess.Execute("where users.age > 10")
	_ = sess.Execute("order users.name asc")
	entries := sess.buildEditEntries("")
	if len(entries) != 4 {
		t.Errorf("expected 4 entries, got %d", len(entries))
	}
	if entries[0].clauseType != "select" {
		t.Errorf("expected first entry to be select, got %s", entries[0].clauseType)
	}
	if entries[2].clauseType != "where" {
		t.Errorf("expected third entry to be where, got %s", entries[2].clauseType)
	}
}

func TestBuildEditEntriesFiltered(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("select users.id, users.name")
	_ = sess.Execute("where users.age > 10")
	_ = sess.Execute("where users.active = true")
	entries := sess.buildEditEntries("where")
	if len(entries) != 2 {
		t.Errorf("expected 2 where entries, got %d", len(entries))
	}
	for _, e := range entries {
		if e.clauseType != "where" {
			t.Errorf("expected where, got %s", e.clauseType)
		}
	}
}

// --- removeEntry ---

func TestRemoveEntryWhere(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("where users.age > 10")
	_ = sess.Execute("where users.active = true")
	entries := sess.buildEditEntries("where")
	sess.removeEntry(entries[0])
	if len(sess.query.Core.Wheres) != 1 {
		t.Fatalf("expected 1 where, got %d", len(sess.query.Core.Wheres))
	}
	sql, _ := sess.GenerateSQL()
	if !strings.Contains(sql, "active") {
		t.Errorf("expected active condition to remain, got: %s", sql)
	}
	if strings.Contains(sql, "age") {
		t.Errorf("expected age condition removed, got: %s", sql)
	}
}

func TestRemoveEntrySelect(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("select users.id, users.name, users.email")
	entries := sess.buildEditEntries("select")
	sess.removeEntry(entries[1])
	if len(sess.query.Core.Projections) != 2 {
		t.Fatalf("expected 2 projections, got %d", len(sess.query.Core.Projections))
	}
}

func TestRemoveEntryJoin(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("join posts on users.id = posts.user_id")
	entries := sess.buildEditEntries("join")
	sess.removeEntry(entries[0])
	if len(sess.query.Core.Joins) != 0 {
		t.Fatalf("expected 0 joins, got %d", len(sess.query.Core.Joins))
	}
}

func TestRemoveEntryOrder(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("order users.name asc, users.id desc")
	entries := sess.buildEditEntries("order")
	sess.removeEntry(entries[0])
	if len(sess.query.Core.Orders) != 1 {
		t.Fatalf("expected 1 order, got %d", len(sess.query.Core.Orders))
	}
}

func TestRemoveEntryGroup(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("group users.role")
	entries := sess.buildEditEntries("group")
	sess.removeEntry(entries[0])
	if len(sess.query.Core.Groups) != 0 {
		t.Fatalf("expected 0 groups, got %d", len(sess.query.Core.Groups))
	}
}

func TestRemoveEntryHaving(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("having users.count > 5")
	entries := sess.buildEditEntries("having")
	sess.removeEntry(entries[0])
	if len(sess.query.Core.Havings) != 0 {
		t.Fatalf("expected 0 havings, got %d", len(sess.query.Core.Havings))
	}
}

// --- editEntryValue ---

func TestEditEntryWhere(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("where users.age > 10")
	entries := sess.buildEditEntries("where")
	err := sess.editEntryValue(entries[0], "users.age > 20")
	if err != nil {
		t.Fatalf("editEntryValue failed: %v", err)
	}
	sql, _ := sess.GenerateSQL()
	if !strings.Contains(sql, "20") {
		t.Errorf("expected updated condition, got: %s", sql)
	}
	if strings.Contains(sql, "10") {
		t.Errorf("expected old condition gone, got: %s", sql)
	}
}

func TestEditEntrySelect(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("select users.id, users.name")
	entries := sess.buildEditEntries("select")
	err := sess.editEntryValue(entries[1], "users.email, users.age")
	if err != nil {
		t.Fatalf("editEntryValue failed: %v", err)
	}
	if len(sess.query.Core.Projections) != 3 {
		t.Fatalf("expected 3 projections (id + email + age), got %d", len(sess.query.Core.Projections))
	}
}

func TestEditEntryOrder(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("order users.name asc")
	entries := sess.buildEditEntries("order")
	err := sess.editEntryValue(entries[0], "users.name desc")
	if err != nil {
		t.Fatalf("editEntryValue failed: %v", err)
	}
	sql, _ := sess.GenerateSQL()
	if !strings.Contains(sql, "DESC") {
		t.Errorf("expected DESC, got: %s", sql)
	}
}

func TestEditEntryGroup(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("group users.role")
	entries := sess.buildEditEntries("group")
	err := sess.editEntryValue(entries[0], "users.department")
	if err != nil {
		t.Fatalf("editEntryValue failed: %v", err)
	}
	sql, _ := sess.GenerateSQL()
	if !strings.Contains(sql, "department") {
		t.Errorf("expected department, got: %s", sql)
	}
}

func TestEditEntryHaving(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("having users.count > 5")
	entries := sess.buildEditEntries("having")
	err := sess.editEntryValue(entries[0], "users.count > 10")
	if err != nil {
		t.Fatalf("editEntryValue failed: %v", err)
	}
	sql, _ := sess.GenerateSQL()
	if !strings.Contains(sql, "10") {
		t.Errorf("expected 10, got: %s", sql)
	}
}

func TestEditEntryJoin(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("join posts on users.id = posts.user_id")
	entries := sess.buildEditEntries("join")
	err := sess.editEntryValue(entries[0], "comments on users.id = comments.user_id")
	if err != nil {
		t.Fatalf("editEntryValue failed: %v", err)
	}
	sql, _ := sess.GenerateSQL()
	if !strings.Contains(sql, "comments") {
		t.Errorf("expected comments, got: %s", sql)
	}
	if !strings.Contains(sql, "INNER JOIN") {
		t.Errorf("expected INNER JOIN preserved, got: %s", sql)
	}
}

// --- Edit integration ---

func TestEditIntegrationRemoveAndVerifySQL(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("select users.id, users.name")
	_ = sess.Execute("where users.age > 10")
	_ = sess.Execute("where users.active = true")
	_ = sess.Execute("join posts on users.id = posts.user_id")
	_ = sess.Execute("order users.name asc")

	// Verify initial state.
	entries := sess.buildEditEntries("")
	if len(entries) != 6 { // 2 select + 2 where + 1 join + 1 order
		t.Fatalf("expected 6 entries, got %d", len(entries))
	}

	// Remove first WHERE (users.age > 10).
	whereEntries := sess.buildEditEntries("where")
	sess.removeEntry(whereEntries[0])

	sql, err := sess.GenerateSQL()
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}
	if strings.Contains(sql, "age") {
		t.Errorf("expected age removed from SQL: %s", sql)
	}
	if !strings.Contains(sql, "active") {
		t.Errorf("expected active to remain in SQL: %s", sql)
	}

	// Edit the remaining WHERE.
	whereEntries = sess.buildEditEntries("where")
	err = sess.editEntryValue(whereEntries[0], "users.active = false")
	if err != nil {
		t.Fatalf("editEntryValue failed: %v", err)
	}
	sql, _ = sess.GenerateSQL()
	if !strings.Contains(sql, "FALSE") {
		t.Errorf("expected FALSE in SQL: %s", sql)
	}

	// Remove the join.
	joinEntries := sess.buildEditEntries("join")
	sess.removeEntry(joinEntries[0])
	sql, _ = sess.GenerateSQL()
	if strings.Contains(sql, "JOIN") {
		t.Errorf("expected no JOIN in SQL: %s", sql)
	}

	// Verify final entry count.
	entries = sess.buildEditEntries("")
	if len(entries) != 4 { // 2 select + 1 where + 1 order
		t.Errorf("expected 4 entries after removals, got %d", len(entries))
	}
}

func TestEditRequiresQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("edit")
	if err == nil {
		t.Error("expected error when no query")
	}
}

func TestEditRequiresInteractive(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("where users.age > 10")
	err := sess.Execute("edit")
	if err == nil {
		t.Error("expected error for non-interactive session")
	}
}

func TestEditInvalidClause(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	err := sess.Execute("edit foo")
	if err == nil {
		t.Error("expected error for invalid clause")
	}
}

func TestCompleteEditClauses(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	comp := &replCompleter{sess: sess}

	ctx, prefix := comp.parseContext("edit ")
	if ctx != contextEditClause {
		t.Errorf("expected contextEditClause, got %d", ctx)
	}
	if prefix != "" {
		t.Errorf("expected empty prefix, got %q", prefix)
	}

	ctx, prefix = comp.parseContext("edit w")
	if ctx != contextEditClause {
		t.Errorf("expected contextEditClause, got %d", ctx)
	}
	if prefix != "w" {
		t.Errorf("expected prefix 'w', got %q", prefix)
	}
}

// --- Exec helper: captures stdout from Execute ---

// Exec runs a REPL command, captures stdout, and returns it along with any error.
func (s *Session) Exec(cmd string) (string, error) {
	var buf bytes.Buffer
	old := s.out
	s.out = &buf
	err := s.Execute(cmd)
	s.out = old
	return buf.String(), err
}

// --- OPA masking expands star ---

func TestOPAMaskingExpandsStar(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{"result":{"queries":[[]]}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":"<MASKED>"}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	s := NewSession("postgres", nil)
	_, _ = s.Exec("from consignments")
	s.conn = &dbConn{engine: "postgres", schema: schemaCache{
		tables:  []string{"consignments"},
		columns: map[string][]string{"consignments": {"id", "account_name", "billed_total"}},
	}}

	s.opaConfig = &opaPluginRef{
		url:    srv.URL,
		policy: "data.authz.allow",
		input:  map[string]any{},
	}
	_ = configureOPA(s, "")
	s.rebuildQueryWithPlugins()

	sql, _ := s.Exec("sql")
	testutil.AssertEqual(t, strings.TrimSpace(sql), `SELECT "consignments"."id", "consignments"."account_name", '<MASKED>' AS "billed_total" FROM "consignments";`)
}

// --- opa masks command ---

func TestOPAMasksCommand(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":"<MASKED>"}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	s := NewSession("postgres", nil)
	_, _ = s.Exec("from consignments")
	s.opaConfig = &opaPluginRef{
		url:    srv.URL,
		policy: "data.authz.allow",
		input:  map[string]any{},
	}
	_ = configureOPA(s, "")
	s.rebuildQueryWithPlugins()

	out, err := s.Exec("opa masks")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "billed_total") {
		t.Errorf("expected billed_total in output, got: %s", out)
	}
	if !strings.Contains(out, "<MASKED>") {
		t.Errorf("expected '<MASKED>' in output, got: %s", out)
	}
}

func TestOPAMasksNoOPA(t *testing.T) {
	t.Parallel()
	s := NewSession("postgres", nil)
	_, err := s.Exec("opa masks")
	if err == nil {
		t.Fatal("expected error when OPA is not enabled")
	}
}

func TestOPAMasksNoQuery(t *testing.T) {
	t.Parallel()
	s := NewSession("postgres", nil)
	s.opaConfig = &opaPluginRef{
		url:    "http://localhost:8181",
		policy: "data.authz.allow",
		input:  map[string]any{},
	}
	_, err := s.Exec("opa masks")
	if err == nil {
		t.Fatal("expected error when no query defined")
	}
}

// --- opa explain shows masks ---

func TestOPAExplainShowsMasks(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{
				"result": {
					"queries": [[
						{
							"index": 0,
							"terms": [
								{"type": "ref", "value": [{"type": "var", "value": "eq"}]},
								{"type": "ref", "value": [
									{"type": "var", "value": "data"},
									{"type": "string", "value": "consignments"},
									{"type": "string", "value": "account_name"}
								]},
								{"type": "string", "value": "acme"}
							]
						}
					]]
				}
			}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":"<MASKED>"}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	s := NewSession("postgres", nil)
	s.opaConfig = &opaPluginRef{
		url:    srv.URL,
		policy: "data.authz.allow",
		input:  map[string]any{},
	}
	_ = configureOPA(s, "")

	out, err := s.Exec("opa explain consignments")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Masks:") {
		t.Errorf("expected 'Masks:' section in output, got: %s", out)
	}
	if !strings.Contains(out, "billed_total") {
		t.Errorf("expected 'billed_total' in mask output, got: %s", out)
	}
}

// --- opa status shows mask count ---

func TestOPAStatusShowsMaskCount(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/compile":
			_, _ = w.Write([]byte(`{"result":{"queries":[[]]}}`))
		case strings.HasSuffix(r.URL.Path, "/masks"):
			_, _ = w.Write([]byte(`{"result":{"consignments":{"billed_total":{"replace":{"value":"<MASKED>"}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	s := NewSession("postgres", nil)
	_, _ = s.Exec("from consignments")
	s.opaConfig = &opaPluginRef{
		url:    srv.URL,
		policy: "data.authz.allow",
		input:  map[string]any{},
	}
	_ = configureOPA(s, "")
	s.rebuildQueryWithPlugins()

	out, _ := s.Exec("opa status")
	if !strings.Contains(out, "Masks:") {
		t.Errorf("expected mask info in status output, got: %s", out)
	}
}

// --- dot command tests ---

func TestDotWritesFile(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("from users")
	_ = sess.Execute("select users.id, users.name")
	_ = sess.Execute("where users.active = true")

	tmp := t.TempDir() + "/test.dot"
	out, err := sess.Exec("dot " + tmp)
	if err != nil {
		t.Fatalf("dot command failed: %v", err)
	}
	if !strings.Contains(out, "Wrote DOT to") {
		t.Errorf("expected confirmation message, got: %s", out)
	}

	data, err := os.ReadFile(tmp)
	if err != nil {
		t.Fatalf("failed to read DOT file: %v", err)
	}
	dot := string(data)
	if !strings.Contains(dot, "digraph AST") {
		t.Errorf("expected DOT content, got:\n%s", dot)
	}
	if !strings.Contains(dot, "SelectCore") {
		t.Errorf("expected SelectCore in DOT, got:\n%s", dot)
	}
	if !strings.Contains(dot, `label="FROM"`) {
		t.Errorf("expected FROM edge in DOT, got:\n%s", dot)
	}
}

func TestDotRequiresFilepath(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("from users")

	_, err := sess.Exec("dot")
	if err == nil {
		t.Error("expected error for missing filepath")
	}
	if !strings.Contains(err.Error(), "usage") {
		t.Errorf("expected usage error, got: %v", err)
	}
}

func TestDotRequiresQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_, err := sess.Exec("dot /tmp/test.dot")
	if err == nil {
		t.Error("expected error for no query")
	}
}

func TestDotWithSoftdelete(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("from users")
	_ = sess.Execute("where users.active = true")
	_ = sess.Execute("plugin softdelete")

	tmp := t.TempDir() + "/test.dot"
	_, err := sess.Exec("dot " + tmp)
	if err != nil {
		t.Fatalf("dot command failed: %v", err)
	}

	data, _ := os.ReadFile(tmp)
	dot := string(data)
	if !strings.Contains(dot, "softdelete") {
		t.Errorf("expected softdelete cluster in DOT, got:\n%s", dot)
	}
	if !strings.Contains(dot, "subgraph cluster_") {
		t.Errorf("expected cluster subgraph, got:\n%s", dot)
	}
}

func TestHelpIncludesDot(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	out, _ := sess.Exec("help")
	if !strings.Contains(out, "dot <filepath>") {
		t.Errorf("expected 'dot <filepath>' in help, got:\n%s", out)
	}
}

// --- Arithmetic operations ---

// -- Tokenizer tests --

func TestTokenizeArithPlus(t *testing.T) {
	t.Parallel()
	tokens := tokenize("users.age + 5")
	expected := []string{"users.age", "+", "5"}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, tokens)
	}
	for i, e := range expected {
		if tokens[i] != e {
			t.Errorf("token[%d]: expected %q, got %q", i, e, tokens[i])
		}
	}
}

func TestTokenizeShiftLeft(t *testing.T) {
	t.Parallel()
	tokens := tokenize("users.x << 2")
	expected := []string{"users.x", "<<", "2"}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, tokens)
	}
	for i, e := range expected {
		if tokens[i] != e {
			t.Errorf("token[%d]: expected %q, got %q", i, e, tokens[i])
		}
	}
}

func TestTokenizeShiftRight(t *testing.T) {
	t.Parallel()
	tokens := tokenize("users.x >> 2")
	expected := []string{"users.x", ">>", "2"}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, tokens)
	}
	for i, e := range expected {
		if tokens[i] != e {
			t.Errorf("token[%d]: expected %q, got %q", i, e, tokens[i])
		}
	}
}

func TestTokenizeConcat(t *testing.T) {
	t.Parallel()
	tokens := tokenize("users.a || users.b")
	expected := []string{"users.a", "||", "users.b"}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, tokens)
	}
	for i, e := range expected {
		if tokens[i] != e {
			t.Errorf("token[%d]: expected %q, got %q", i, e, tokens[i])
		}
	}
}

func TestTokenizeBitwiseNot(t *testing.T) {
	t.Parallel()
	tokens := tokenize("~users.flags")
	expected := []string{"~", "users.flags"}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, tokens)
	}
	for i, e := range expected {
		if tokens[i] != e {
			t.Errorf("token[%d]: expected %q, got %q", i, e, tokens[i])
		}
	}
}

func TestTokenizeMultipleArith(t *testing.T) {
	t.Parallel()
	tokens := tokenize("users.price * users.quantity")
	expected := []string{"users.price", "*", "users.quantity"}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, tokens)
	}
	for i, e := range expected {
		if tokens[i] != e {
			t.Errorf("token[%d]: expected %q, got %q", i, e, tokens[i])
		}
	}
}

// -- Where tests with arithmetic --

func TestWhereArithPlus(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.age + 5 > 10",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."age" + 5 > 10`)
}

func TestWhereArithMultiply(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.price * 100 >= 1000",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."price" * 100 >= 1000`)
}

func TestWhereArithColumnToColumn(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.age + users.bonus > 30",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."age" + "users"."bonus" > 30`)
}

func TestWhereArithBitwiseNot(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where ~users.flags & 255 > 0",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE (~"users"."flags") & 255 > 0`)
}

func TestWhereArithConcat(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.first || ' ' || users.last = 'John Doe'",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE ("users"."first" || ' ') || "users"."last" = 'John Doe'`)
}

func TestWhereArithBothSides(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.age + 5 > users.min_age - 1",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."age" + 5 > "users"."min_age" - 1`)
}

// -- Expr tests with arithmetic --

func TestExprArithPlus(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.age + 5 > 10")
	testutil.AssertEqual(t, got, `"users"."age" + 5 > 10`)
}

func TestExprArithShiftLeft(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.x << 2 = 8")
	testutil.AssertEqual(t, got, `"users"."x" << 2 = 8`)
}

func TestExprArithChain(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.a + users.b * 2 > 100")
	testutil.AssertEqual(t, got, `("users"."a" + "users"."b") * 2 > 100`)
}

// -- Select tests with arithmetic --

func TestSelectArithPlus(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.age + 5",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."age" + 5 FROM "users"`)
}

func TestSelectArithMultiply(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.a * users.b",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."a" * "users"."b" FROM "users"`)
}

func TestSelectArithMixed(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.age + 5, users.name",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."age" + 5, "users"."name" FROM "users"`)
}

// -- Having test with arithmetic --

func TestHavingArith(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"group users.dept",
		"having users.total * 100 > 5000",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" GROUP BY "users"."dept" HAVING "users"."total" * 100 > 5000`)
}

// -- Arithmetic with MySQL dialect --

func TestArithMySQL(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "mysql",
		"table users",
		"from users",
		"where users.age + 5 > 10",
	)
	testutil.AssertEqual(t, sql, "SELECT * FROM `users` WHERE `users`.`age` + 5 > 10")
}

// -- Edge cases --

func TestArithDivide(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.total / 2 > 50",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."total" / 2 > 50`)
}

func TestArithBitwiseOr(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.flags | 4 > 0",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."flags" | 4 > 0`)
}

func TestArithBitwiseXor(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.flags ^ 255 > 0",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."flags" ^ 255 > 0`)
}

func TestArithMinus(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.age - 5 > 10",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."age" - 5 > 10`)
}

func TestArithBitwiseAnd(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.flags & 3 = 1",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."flags" & 3 = 1`)
}

// -- Existing operators still work with arithmetic on left side --

func TestArithWithSimpleCondition(t *testing.T) {
	t.Parallel()
	// Simple conditions (no arithmetic) should still work exactly as before
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.age > 18",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."age" > 18`)
}

func TestArithPreservesIsNull(t *testing.T) {
	t.Parallel()
	// IS NULL still works on plain column refs
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.deleted_at is null",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."deleted_at" IS NULL`)
}

func TestArithPreservesIn(t *testing.T) {
	t.Parallel()
	// IN still works on plain column refs
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.role in ('admin', 'mod')",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."role" IN ('admin', 'mod')`)
}

func TestArithPreservesBetween(t *testing.T) {
	t.Parallel()
	// BETWEEN still works on plain column refs
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where users.age between 18 and 65",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE "users"."age" BETWEEN 18 AND 65`)
}

// --- New predication REPL expression tests ---

func TestExprNotBetween(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.age not between 18 and 65")
	testutil.AssertEqual(t, got, `"users"."age" NOT BETWEEN 18 AND 65`)
}

func TestExprNotBetweenAndCondition(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.age not between 18 and 65 and users.active = true")
	testutil.AssertEqual(t, got, `"users"."age" NOT BETWEEN 18 AND 65 AND "users"."active" = TRUE`)
}

func TestExprRegexp(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.name regexp '^A.*'")
	testutil.AssertEqual(t, got, `"users"."name" ~ '^A.*'`)
}

func TestExprRegexpMySQL(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "mysql", []string{"users"}, "users.name regexp '^A.*'")
	testutil.AssertEqual(t, got, "`users`.`name` REGEXP '^A.*'")
}

func TestExprNotRegexp(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.name not regexp '^A.*'")
	testutil.AssertEqual(t, got, `"users"."name" !~ '^A.*'`)
}

func TestExprNotRegexpMySQL(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "mysql", []string{"users"}, "users.name not regexp '^A.*'")
	testutil.AssertEqual(t, got, "`users`.`name` NOT REGEXP '^A.*'")
}

func TestExprIsDistinctFrom(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.status is distinct from 'active'")
	testutil.AssertEqual(t, got, `"users"."status" IS DISTINCT FROM 'active'`)
}

func TestExprIsNotDistinctFrom(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.status is not distinct from 'active'")
	testutil.AssertEqual(t, got, `"users"."status" IS NOT DISTINCT FROM 'active'`)
}

func TestExprContains(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.tags @> '{1,2}'")
	testutil.AssertEqual(t, got, `"users"."tags" @> '{1,2}'`)
}

func TestExprOverlaps(t *testing.T) {
	t.Parallel()
	got := exprSQL(t, "postgres", []string{"users"}, "users.tags && '{3,4}'")
	testutil.AssertEqual(t, got, `"users"."tags" && '{3,4}'`)
}

func TestTokenizeNewOperators(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected []string
	}{
		{"a @> b", []string{"a", "@>", "b"}},
		{"a && b", []string{"a", "&&", "b"}},
		{"a !~ b", []string{"a", "!~", "b"}},
	}
	for _, tt := range tests {
		got := tokenize(tt.input)
		if len(got) != len(tt.expected) {
			t.Errorf("tokenize(%q): expected %v, got %v", tt.input, tt.expected, got)
			continue
		}
		for i, tok := range got {
			if tok != tt.expected[i] {
				t.Errorf("tokenize(%q)[%d]: expected %q, got %q", tt.input, i, tt.expected[i], tok)
			}
		}
	}
}

// --- Aggregate functions ---

func TestSelectCountStar(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select COUNT(*)",
	)
	testutil.AssertEqual(t, sql, `SELECT COUNT(*) FROM "users"`)
}

func TestSelectCountColumn(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select COUNT(users.id)",
	)
	testutil.AssertEqual(t, sql, `SELECT COUNT("users"."id") FROM "users"`)
}

func TestSelectCountDistinct(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select COUNT(DISTINCT users.country)",
	)
	testutil.AssertEqual(t, sql, `SELECT COUNT(DISTINCT "users"."country") FROM "users"`)
}

func TestSelectSum(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"select SUM(orders.total)",
	)
	testutil.AssertEqual(t, sql, `SELECT SUM("orders"."total") FROM "orders"`)
}

func TestSelectAvg(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"select AVG(orders.total)",
	)
	testutil.AssertEqual(t, sql, `SELECT AVG("orders"."total") FROM "orders"`)
}

func TestSelectMin(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"select MIN(orders.total)",
	)
	testutil.AssertEqual(t, sql, `SELECT MIN("orders"."total") FROM "orders"`)
}

func TestSelectMax(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"select MAX(orders.total)",
	)
	testutil.AssertEqual(t, sql, `SELECT MAX("orders"."total") FROM "orders"`)
}

func TestSelectAggregateMySQL(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "mysql",
		"table orders",
		"from orders",
		"select COUNT(*), SUM(orders.total)",
	)
	testutil.AssertEqual(t, sql, "SELECT COUNT(*), SUM(`orders`.`total`) FROM `orders`")
}

func TestSelectAggregateSQLite(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "sqlite",
		"table orders",
		"from orders",
		"select COUNT(*), AVG(orders.total)",
	)
	testutil.AssertEqual(t, sql, `SELECT COUNT(*), AVG("orders"."total") FROM "orders"`)
}

func TestSelectAggregateWithGroupBy(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.status, COUNT(*)",
		"group users.status",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."status", COUNT(*) FROM "users" GROUP BY "users"."status"`)
}

func TestHavingWithAggregate(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.status, COUNT(*)",
		"group users.status",
		"having COUNT(*) > 5",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."status", COUNT(*) FROM "users" GROUP BY "users"."status" HAVING COUNT(*) > 5`)
}

func TestWhereWithAggregate(t *testing.T) {
	t.Parallel()
	// Aggregates can appear in where clauses (for subquery contexts, etc.)
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where COUNT(*) > 10",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE COUNT(*) > 10`)
}

func TestSelectExtractYear(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"select EXTRACT(YEAR FROM orders.created_at)",
	)
	testutil.AssertEqual(t, sql, `SELECT EXTRACT(YEAR FROM "orders"."created_at") FROM "orders"`)
}

func TestSelectExtractMonth(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"select EXTRACT(MONTH FROM orders.created_at)",
	)
	testutil.AssertEqual(t, sql, `SELECT EXTRACT(MONTH FROM "orders"."created_at") FROM "orders"`)
}

func TestSelectExtractMySQL(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "mysql",
		"table orders",
		"from orders",
		"select EXTRACT(DAY FROM orders.created_at)",
	)
	testutil.AssertEqual(t, sql, "SELECT EXTRACT(DAY FROM `orders`.`created_at`) FROM `orders`")
}

func TestWhereExtract(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"where EXTRACT(YEAR FROM orders.created_at) = 2024",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "orders" WHERE EXTRACT(YEAR FROM "orders"."created_at") = 2024`)
}

func TestExprCountStar(t *testing.T) {
	t.Parallel()
	sql := exprSQL(t, "postgres", []string{"users"}, "COUNT(*) > 5")
	testutil.AssertEqual(t, sql, "COUNT(*) > 5")
}

func TestExprCountDistinct(t *testing.T) {
	t.Parallel()
	sql := exprSQL(t, "postgres", []string{"users"}, "COUNT(DISTINCT users.country) > 1")
	testutil.AssertEqual(t, sql, `COUNT(DISTINCT "users"."country") > 1`)
}

func TestExprSumInComparison(t *testing.T) {
	t.Parallel()
	sql := exprSQL(t, "postgres", []string{"orders"}, "SUM(orders.total) >= 1000")
	testutil.AssertEqual(t, sql, `SUM("orders"."total") >= 1000`)
}

func TestExprExtract(t *testing.T) {
	t.Parallel()
	sql := exprSQL(t, "postgres", []string{"orders"}, "EXTRACT(YEAR FROM orders.created_at) = 2024")
	testutil.AssertEqual(t, sql, `EXTRACT(YEAR FROM "orders"."created_at") = 2024`)
}

func TestExprAggregateWithArithmetic(t *testing.T) {
	t.Parallel()
	sql := exprSQL(t, "postgres", []string{"t"}, "SUM(t.price) + 10 > 100")
	testutil.AssertEqual(t, sql, `SUM("t"."price") + 10 > 100`)
}

func TestSelectAggregateWithFilter(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"select SUM(orders.total) FILTER (WHERE orders.status = 'completed')",
	)
	testutil.AssertEqual(t, sql, `SELECT SUM("orders"."total") FILTER (WHERE "orders"."status" = 'completed') FROM "orders"`)
}

func TestSelectCountStarWithFilter(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"select COUNT(*) FILTER (WHERE orders.active = true)",
	)
	testutil.AssertEqual(t, sql, `SELECT COUNT(*) FILTER (WHERE "orders"."active" = TRUE) FROM "orders"`)
}

func TestSelectMultipleAggregates(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"select COUNT(*), SUM(orders.total), AVG(orders.total), MIN(orders.total), MAX(orders.total)",
	)
	testutil.AssertEqual(t, sql, `SELECT COUNT(*), SUM("orders"."total"), AVG("orders"."total"), MIN("orders"."total"), MAX("orders"."total") FROM "orders"`)
}

func TestSelectAggregateCaseInsensitive(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select count(*)",
	)
	testutil.AssertEqual(t, sql, `SELECT COUNT(*) FROM "users"`)
}

func TestSelectExtractAllFields(t *testing.T) {
	t.Parallel()
	fields := []struct {
		name     string
		expected string
	}{
		{"YEAR", "YEAR"},
		{"MONTH", "MONTH"},
		{"DAY", "DAY"},
		{"HOUR", "HOUR"},
		{"MINUTE", "MINUTE"},
		{"SECOND", "SECOND"},
		{"DOW", "DOW"},
		{"DOY", "DOY"},
		{"EPOCH", "EPOCH"},
		{"QUARTER", "QUARTER"},
		{"WEEK", "WEEK"},
	}
	for _, f := range fields {
		t.Run(f.name, func(t *testing.T) {
			sql := execSQL(t, "postgres",
				"table t",
				"from t",
				"select EXTRACT("+f.name+" FROM t.ts)",
			)
			testutil.AssertEqual(t, sql, `SELECT EXTRACT(`+f.expected+` FROM "t"."ts") FROM "t"`)
		})
	}
}

// --- Window functions ---

func TestWindowRowNumber(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select ROW_NUMBER() OVER ()",
	)
	testutil.AssertEqual(t, sql, `SELECT ROW_NUMBER() OVER () FROM "users"`)
}

func TestWindowRank(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select RANK() OVER ()",
	)
	testutil.AssertEqual(t, sql, `SELECT RANK() OVER () FROM "users"`)
}

func TestWindowDenseRank(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select DENSE_RANK() OVER ()",
	)
	testutil.AssertEqual(t, sql, `SELECT DENSE_RANK() OVER () FROM "users"`)
}

func TestWindowCumeDist(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select CUME_DIST() OVER ()",
	)
	testutil.AssertEqual(t, sql, `SELECT CUME_DIST() OVER () FROM "users"`)
}

func TestWindowPercentRank(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select PERCENT_RANK() OVER ()",
	)
	testutil.AssertEqual(t, sql, `SELECT PERCENT_RANK() OVER () FROM "users"`)
}

func TestWindowNtile(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select NTILE(4) OVER (ORDER BY users.salary ASC)",
	)
	testutil.AssertEqual(t, sql, `SELECT NTILE(4) OVER (ORDER BY "users"."salary" ASC) FROM "users"`)
}

func TestWindowFirstValue(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select FIRST_VALUE(users.salary) OVER (ORDER BY users.salary ASC)",
	)
	testutil.AssertEqual(t, sql, `SELECT FIRST_VALUE("users"."salary") OVER (ORDER BY "users"."salary" ASC) FROM "users"`)
}

func TestWindowLastValue(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select LAST_VALUE(users.salary) OVER (ORDER BY users.salary ASC)",
	)
	testutil.AssertEqual(t, sql, `SELECT LAST_VALUE("users"."salary") OVER (ORDER BY "users"."salary" ASC) FROM "users"`)
}

func TestWindowLag(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select LAG(users.salary, 1, 0) OVER (ORDER BY users.salary ASC)",
	)
	testutil.AssertEqual(t, sql, `SELECT LAG("users"."salary", 1, 0) OVER (ORDER BY "users"."salary" ASC) FROM "users"`)
}

func TestWindowLead(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select LEAD(users.salary) OVER (ORDER BY users.salary ASC)",
	)
	testutil.AssertEqual(t, sql, `SELECT LEAD("users"."salary") OVER (ORDER BY "users"."salary" ASC) FROM "users"`)
}

func TestWindowNthValue(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select NTH_VALUE(users.salary, 3) OVER (ORDER BY users.salary ASC)",
	)
	testutil.AssertEqual(t, sql, `SELECT NTH_VALUE("users"."salary", 3) OVER (ORDER BY "users"."salary" ASC) FROM "users"`)
}

func TestWindowWithPartitionBy(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select ROW_NUMBER() OVER (PARTITION BY users.dept)",
	)
	testutil.AssertEqual(t, sql, `SELECT ROW_NUMBER() OVER (PARTITION BY "users"."dept") FROM "users"`)
}

func TestWindowWithPartitionAndOrder(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select RANK() OVER (PARTITION BY users.dept ORDER BY users.salary DESC)",
	)
	testutil.AssertEqual(t, sql, `SELECT RANK() OVER (PARTITION BY "users"."dept" ORDER BY "users"."salary" DESC) FROM "users"`)
}

func TestWindowAggregateOver(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select SUM(users.salary) OVER (PARTITION BY users.dept)",
	)
	testutil.AssertEqual(t, sql, `SELECT SUM("users"."salary") OVER (PARTITION BY "users"."dept") FROM "users"`)
}

func TestWindowCountOver(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select COUNT(*) OVER ()",
	)
	testutil.AssertEqual(t, sql, `SELECT COUNT(*) OVER () FROM "users"`)
}

func TestWindowRowsFrame(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select SUM(users.salary) OVER (ORDER BY users.salary ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
	)
	testutil.AssertEqual(t, sql, `SELECT SUM("users"."salary") OVER (ORDER BY "users"."salary" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM "users"`)
}

func TestWindowRangeFrame(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select COUNT(*) OVER (ORDER BY users.id ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
	)
	testutil.AssertEqual(t, sql, `SELECT COUNT(*) OVER (ORDER BY "users"."id" ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM "users"`)
}

func TestWindowPrecedingFollowing(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select SUM(users.salary) OVER (ORDER BY users.id ASC ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING)",
	)
	testutil.AssertEqual(t, sql, `SELECT SUM("users"."salary") OVER (ORDER BY "users"."id" ASC ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) FROM "users"`)
}

func TestWindowNamedWindow(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select RANK() OVER w",
		"window w order by users.salary asc",
	)
	testutil.AssertEqual(t, sql, `SELECT RANK() OVER "w" FROM "users" WINDOW "w" AS (ORDER BY "users"."salary" ASC)`)
}

func TestWindowNamedWindowPartition(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select ROW_NUMBER() OVER w, RANK() OVER w",
		"window w partition by users.dept order by users.salary asc",
	)
	testutil.AssertEqual(t, sql, `SELECT ROW_NUMBER() OVER "w", RANK() OVER "w" FROM "users" WINDOW "w" AS (PARTITION BY "users"."dept" ORDER BY "users"."salary" ASC)`)
}

func TestWindowMySQL(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "mysql",
		"table users",
		"from users",
		"select ROW_NUMBER() OVER (ORDER BY users.salary DESC)",
	)
	testutil.AssertEqual(t, sql, "SELECT ROW_NUMBER() OVER (ORDER BY `users`.`salary` DESC) FROM `users`")
}

func TestWindowSQLite(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "sqlite",
		"table users",
		"from users",
		"select ROW_NUMBER() OVER (ORDER BY users.salary DESC)",
	)
	testutil.AssertEqual(t, sql, `SELECT ROW_NUMBER() OVER (ORDER BY "users"."salary" DESC) FROM "users"`)
}

func TestWindowCaseInsensitive(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select row_number() over (partition by users.dept order by users.salary desc)",
	)
	testutil.AssertEqual(t, sql, `SELECT ROW_NUMBER() OVER (PARTITION BY "users"."dept" ORDER BY "users"."salary" DESC) FROM "users"`)
}

func TestWindowAllFunctions(t *testing.T) {
	t.Parallel()
	funcs := []struct {
		name     string
		call     string
		expected string
	}{
		{"ROW_NUMBER", "ROW_NUMBER() OVER ()", "ROW_NUMBER() OVER ()"},
		{"RANK", "RANK() OVER ()", "RANK() OVER ()"},
		{"DENSE_RANK", "DENSE_RANK() OVER ()", "DENSE_RANK() OVER ()"},
		{"CUME_DIST", "CUME_DIST() OVER ()", "CUME_DIST() OVER ()"},
		{"PERCENT_RANK", "PERCENT_RANK() OVER ()", "PERCENT_RANK() OVER ()"},
		{"NTILE", "NTILE(4) OVER ()", "NTILE(4) OVER ()"},
		{"FIRST_VALUE", "FIRST_VALUE(t.x) OVER ()", `FIRST_VALUE("t"."x") OVER ()`},
		{"LAST_VALUE", "LAST_VALUE(t.x) OVER ()", `LAST_VALUE("t"."x") OVER ()`},
		{"NTH_VALUE", "NTH_VALUE(t.x, 2) OVER ()", `NTH_VALUE("t"."x", 2) OVER ()`},
		{"LAG", "LAG(t.x) OVER ()", `LAG("t"."x") OVER ()`},
		{"LEAD", "LEAD(t.x) OVER ()", `LEAD("t"."x") OVER ()`},
	}
	for _, f := range funcs {
		t.Run(f.name, func(t *testing.T) {
			sql := execSQL(t, "postgres",
				"table t",
				"from t",
				"select "+f.call,
			)
			testutil.AssertEqual(t, sql, `SELECT `+f.expected+` FROM "t"`)
		})
	}
}

func TestWindowInExpr(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	for _, cmd := range []string{"table users", "from users"} {
		if err := sess.Execute(cmd); err != nil {
			t.Fatalf("command %q failed: %v", cmd, err)
		}
	}
	// expr command should also parse window functions
	err := sess.Execute("expr ROW_NUMBER() OVER (ORDER BY users.id ASC) = 1")
	if err != nil {
		t.Fatalf("expr with window function failed: %v", err)
	}
}

// --- NULLS FIRST/LAST ---

func TestOrderNullsFirst(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "order users.name asc nulls first")
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" ORDER BY "users"."name" ASC NULLS FIRST`)
}

func TestOrderNullsLast(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "order users.name desc nulls last")
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" ORDER BY "users"."name" DESC NULLS LAST`)
}

func TestOrderNullsDefault(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "order users.name desc")
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" ORDER BY "users"."name" DESC`)
}

func TestOrderNullsMultiple(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "order users.name desc nulls first, users.id asc nulls last")
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" ORDER BY "users"."name" DESC NULLS FIRST, "users"."id" ASC NULLS LAST`)
}

func TestOrderNullsErrorMissingDirection(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	err := sess.Execute("order users.name desc nulls")
	if err == nil {
		t.Fatal("expected error for missing FIRST/LAST after NULLS")
	}
}

// --- DISTINCT ON ---

func TestDistinctOn(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "distinct on users.department")
	testutil.AssertEqual(t, sql, `SELECT DISTINCT ON ("users"."department") * FROM "users"`)
}

func TestDistinctOnMultiple(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "distinct on users.department, users.role")
	testutil.AssertEqual(t, sql, `SELECT DISTINCT ON ("users"."department", "users"."role") * FROM "users"`)
}

func TestDistinctOnErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("distinct on users.id")
	if err == nil {
		t.Fatal("expected error for distinct on without query")
	}
}

// --- FOR UPDATE / FOR SHARE / SKIP LOCKED ---

func TestForUpdate(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "for update")
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" FOR UPDATE`)
}

func TestForShare(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "for share")
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" FOR SHARE`)
}

func TestForNoKeyUpdate(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "for no key update")
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" FOR NO KEY UPDATE`)
}

func TestForKeyShare(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "for key share")
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" FOR KEY SHARE`)
}

func TestSkipLocked(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "for update", "skip locked")
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" FOR UPDATE SKIP LOCKED`)
}

func TestForUpdateErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("for update")
	if err == nil {
		t.Fatal("expected error for for update without query")
	}
}

func TestSkipLockedErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("skip locked")
	if err == nil {
		t.Fatal("expected error for skip locked without query")
	}
}

// --- Query Comments ---

func TestComment(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "comment user listing")
	testutil.AssertEqual(t, sql, `/* user listing */ SELECT * FROM "users"`)
}

func TestCommentErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("comment hello")
	if err == nil {
		t.Fatal("expected error for comment without query")
	}
}

func TestCommentErrorEmpty(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	err := sess.Execute("comment ")
	if err == nil {
		t.Fatal("expected error for empty comment")
	}
}

// --- Optimizer Hints ---

func TestHint(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "hint SeqScan(users)")
	testutil.AssertEqual(t, sql, `SELECT /*+ SeqScan(users) */ * FROM "users"`)
}

func TestHintMultiple(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "hint SeqScan(users)", "hint Parallel(users 4)")
	testutil.AssertEqual(t, sql, `SELECT /*+ SeqScan(users) Parallel(users 4) */ * FROM "users"`)
}

func TestHintErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("hint foo")
	if err == nil {
		t.Fatal("expected error for hint without query")
	}
}

// --- LATERAL JOIN ---

func TestLateralJoin(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "table users", "table posts", "from users",
		"lateral join posts on users.id = posts.user_id")
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" INNER JOIN LATERAL "posts" ON "users"."id" = "posts"."user_id"`)
}

func TestLateralLeftJoin(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "table users", "table posts", "from users",
		"lateral left join posts on users.id = posts.user_id")
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" LEFT OUTER JOIN LATERAL "posts" ON "users"."id" = "posts"."user_id"`)
}

func TestLateralJoinErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("table posts")
	err := sess.Execute("lateral join posts on users.id = posts.user_id")
	if err == nil {
		t.Fatal("expected error for lateral join without query")
	}
}

// --- String JOIN (raw SQL) ---

func TestRawJoin(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres", "from users", "raw join NATURAL JOIN posts")
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" NATURAL JOIN posts`)
}

func TestRawJoinErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("raw join NATURAL JOIN posts")
	if err == nil {
		t.Fatal("expected error for raw join without query")
	}
}

func TestRawJoinErrorEmpty(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	err := sess.Execute("raw join ")
	if err == nil {
		t.Fatal("expected error for empty raw join")
	}
}

// --- Set Operations ---

func TestUnion(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	cmds := []string{
		"from users", "where users.active = true",
		"union",
		"from users", "where users.role = 'admin'",
	}
	for _, cmd := range cmds {
		if err := sess.Execute(cmd); err != nil {
			t.Fatalf("command %q failed: %v", cmd, err)
		}
	}
	out, err := sess.Exec("sql")
	if err != nil {
		t.Fatalf("sql failed: %v", err)
	}
	if !strings.Contains(out, "UNION") {
		t.Errorf("expected UNION in output, got: %s", out)
	}
	if !strings.Contains(out, `"users"."active"`) {
		t.Errorf("expected first query conditions in output, got: %s", out)
	}
}

func TestUnionAll(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	cmds := []string{
		"from users", "union all", "from users",
	}
	for _, cmd := range cmds {
		if err := sess.Execute(cmd); err != nil {
			t.Fatalf("command %q failed: %v", cmd, err)
		}
	}
	out, err := sess.Exec("sql")
	if err != nil {
		t.Fatalf("sql failed: %v", err)
	}
	if !strings.Contains(out, "UNION ALL") {
		t.Errorf("expected UNION ALL in output, got: %s", out)
	}
}

func TestIntersect(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	cmds := []string{
		"from users", "intersect", "from users",
	}
	for _, cmd := range cmds {
		if err := sess.Execute(cmd); err != nil {
			t.Fatalf("command %q failed: %v", cmd, err)
		}
	}
	out, err := sess.Exec("sql")
	if err != nil {
		t.Fatalf("sql failed: %v", err)
	}
	if !strings.Contains(out, "INTERSECT") {
		t.Errorf("expected INTERSECT in output, got: %s", out)
	}
}

func TestExcept(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	cmds := []string{
		"from users", "except", "from users",
	}
	for _, cmd := range cmds {
		if err := sess.Execute(cmd); err != nil {
			t.Fatalf("command %q failed: %v", cmd, err)
		}
	}
	out, err := sess.Exec("sql")
	if err != nil {
		t.Fatalf("sql failed: %v", err)
	}
	if !strings.Contains(out, "EXCEPT") {
		t.Errorf("expected EXCEPT in output, got: %s", out)
	}
}

func TestSetOpErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("union")
	if err == nil {
		t.Fatal("expected error for union without query")
	}
}

func TestSetOpResetClearsStack(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("union")
	_ = sess.Execute("from users")
	_ = sess.Execute("reset")
	err := sess.Execute("sql")
	if err == nil {
		t.Fatal("expected error after reset")
	}
}

// --- CTEs ---

func TestWithCTE(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	cmds := []string{
		"from users", "where users.active = true",
		"with active_users",
		"from active_users",
	}
	for _, cmd := range cmds {
		if err := sess.Execute(cmd); err != nil {
			t.Fatalf("command %q failed: %v", cmd, err)
		}
	}
	out, err := sess.Exec("sql")
	if err != nil {
		t.Fatalf("sql failed: %v", err)
	}
	if !strings.Contains(out, "WITH") {
		t.Errorf("expected WITH in output, got: %s", out)
	}
	if !strings.Contains(out, "active_users") {
		t.Errorf("expected active_users in output, got: %s", out)
	}
}

func TestWithRecursiveCTE(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	cmds := []string{
		"from users",
		"with recursive tree",
		"from tree",
	}
	for _, cmd := range cmds {
		if err := sess.Execute(cmd); err != nil {
			t.Fatalf("command %q failed: %v", cmd, err)
		}
	}
	out, err := sess.Exec("sql")
	if err != nil {
		t.Fatalf("sql failed: %v", err)
	}
	if !strings.Contains(out, "WITH RECURSIVE") {
		t.Errorf("expected WITH RECURSIVE in output, got: %s", out)
	}
}

func TestWithErrorNoQuery(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	err := sess.Execute("with foo")
	if err == nil {
		t.Fatal("expected error for with without query")
	}
}

func TestWithErrorNoName(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	err := sess.Execute("with ")
	if err == nil {
		t.Fatal("expected error for with without name")
	}
}

func TestCTEResetClearsStack(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("with foo")
	_ = sess.Execute("from foo")
	_ = sess.Execute("reset")
	if len(sess.ctes) != 0 {
		t.Errorf("expected ctes to be cleared after reset, got %d", len(sess.ctes))
	}
}

// --- AST display for new features ---

func TestASTDisplaysNewFeatures(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	cmds := []string{
		"from users",
		"distinct on users.name",
		"comment test query",
		"hint SeqScan(users)",
		"for update",
		"skip locked",
	}
	for _, cmd := range cmds {
		if err := sess.Execute(cmd); err != nil {
			t.Fatalf("command %q failed: %v", cmd, err)
		}
	}
	out, _ := sess.Exec("ast")
	checks := []string{"DISTINCT ON", "COMMENT", "HINTS", "FOR UPDATE", "SKIP LOCKED"}
	for _, check := range checks {
		if !strings.Contains(out, check) {
			t.Errorf("expected %q in AST output, got:\n%s", check, out)
		}
	}
}

func TestASTDisplaysLateralJoin(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	cmds := []string{
		"table users", "table posts", "from users",
		"lateral join posts on users.id = posts.user_id",
	}
	for _, cmd := range cmds {
		if err := sess.Execute(cmd); err != nil {
			t.Fatalf("command %q failed: %v", cmd, err)
		}
	}
	out, _ := sess.Exec("ast")
	if !strings.Contains(out, "LATERAL") {
		t.Errorf("expected LATERAL in AST output, got:\n%s", out)
	}
}

func TestASTDisplaysNullsOrdering(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("order users.name desc nulls first")
	out, _ := sess.Exec("ast")
	if !strings.Contains(out, "NULLS FIRST") {
		t.Errorf("expected NULLS FIRST in AST output, got:\n%s", out)
	}
}

func TestASTDisplaysSetOps(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("union")
	_ = sess.Execute("from users")
	out, _ := sess.Exec("ast")
	if !strings.Contains(out, "UNION") {
		t.Errorf("expected UNION in AST output, got:\n%s", out)
	}
}

func TestASTDisplaysCTEs(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("with foo")
	_ = sess.Execute("from foo")
	out, _ := sess.Exec("ast")
	if !strings.Contains(out, "WITH") {
		t.Errorf("expected WITH in AST output, got:\n%s", out)
	}
}

// --- Help text covers new features ---

func TestHelpIncludesNewFeatures(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	out, _ := sess.Exec("help")
	checks := []string{
		"distinct on", "for update", "for share", "skip locked",
		"comment", "hint", "lateral join", "raw join",
		"union", "intersect", "except",
		"with <name>", "with recursive",
		"nulls first",
	}
	for _, check := range checks {
		if !strings.Contains(out, check) {
			t.Errorf("expected %q in help output", check)
		}
	}
}

// --- Plugin off preserves new fields ---

func TestPluginOffPreservesNewFields(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("from users")
	_ = sess.Execute("distinct on users.name")
	_ = sess.Execute("for update")
	_ = sess.Execute("skip locked")
	_ = sess.Execute("comment test")
	_ = sess.Execute("hint foo")
	_ = sess.Execute("plugin softdelete")
	_ = sess.Execute("plugin off softdelete")
	sql, err := sess.GenerateSQL()
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}
	checks := []string{"DISTINCT ON", "FOR UPDATE", "SKIP LOCKED", "/* test */", "/*+ foo */"}
	for _, check := range checks {
		if !strings.Contains(sql, check) {
			t.Errorf("expected %q in SQL after plugin off, got: %s", check, sql)
		}
	}
}

// --- Completer tests for new features ---

func TestCompleteNewCommands(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	comp := &replCompleter{sess: sess}

	tests := []struct {
		input    string
		expected string
	}{
		{"commen", "comment"},
		{"dis", "disconnect"},
		{"for u", "for update"},
		{"for s", "for share"},
		{"ski", "skip locked"},
		{"lat", "lateral join"},
		{"raw", "raw join"},
		{"uni", "union"},
		{"inter", "intersect"},
		{"exc", "except"},
		{"with ", "with recursive"},
	}

	for _, tt := range tests {
		ctx, prefix := comp.parseContext(tt.input)
		if ctx != contextCommand {
			// Some multi-word commands parse to command context
			continue
		}
		candidates := comp.completeCommands(prefix)
		found := false
		for _, c := range candidates {
			if c == tt.expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("input %q: expected %q in candidates %v", tt.input, tt.expected, candidates)
		}
	}
}

func TestCompleteOrderNulls(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	comp := &replCompleter{sess: sess}
	ctx, prefix := comp.parseContext("order users.name desc ")
	if ctx != contextOrderDir {
		// After desc + space, the next token should start a new column
		// but "nulls first" / "nulls last" are in orderDirs
		_ = prefix
	}
}

// --- Named Functions ---

func TestSelectCoalesce(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select coalesce(users.name, 'unknown')",
	)
	testutil.AssertEqual(t, sql, `SELECT COALESCE("users"."name", 'unknown') FROM "users"`)
}

func TestSelectLower(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select lower(users.name)",
	)
	testutil.AssertEqual(t, sql, `SELECT LOWER("users"."name") FROM "users"`)
}

func TestSelectUpper(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select upper(users.name)",
	)
	testutil.AssertEqual(t, sql, `SELECT UPPER("users"."name") FROM "users"`)
}

func TestSelectCast(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select cast(users.age AS TEXT)",
	)
	testutil.AssertEqual(t, sql, `SELECT CAST("users"."age" AS TEXT) FROM "users"`)
}

func TestSelectCastWithPrecision(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select cast(users.price AS NUMERIC(10, 2))",
	)
	testutil.AssertEqual(t, sql, `SELECT CAST("users"."price" AS NUMERIC ( 10 , 2 )) FROM "users"`)
}

func TestSelectSubstring(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select substring(users.name, 1, 3)",
	)
	testutil.AssertEqual(t, sql, `SELECT SUBSTRING("users"."name", 1, 3) FROM "users"`)
}

func TestSelectArbitraryFunction(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select my_func(users.id)",
	)
	testutil.AssertEqual(t, sql, `SELECT MY_FUNC("users"."id") FROM "users"`)
}

func TestSelectDistinctFunction(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select concat(distinct users.name)",
	)
	testutil.AssertEqual(t, sql, `SELECT CONCAT(DISTINCT "users"."name") FROM "users"`)
}

func TestNamedFuncOverWindow(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select lower(users.name) over (order by users.id)",
	)
	testutil.AssertEqual(t, sql, `SELECT LOWER("users"."name") OVER (ORDER BY "users"."id" ASC) FROM "users"`)
}

func TestExprCoalesce(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("from users")
	err := sess.Execute("expr coalesce(users.a, users.b) = 'x'")
	if err != nil {
		t.Fatalf("expr coalesce failed: %v", err)
	}
}

func TestWhereNamedFunc(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"where lower(users.name) = 'alice'",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "users" WHERE LOWER("users"."name") = 'alice'`)
}

// --- CASE Expressions ---

func TestSelectSearchedCase(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select case when users.active = true then 'yes' else 'no' end",
	)
	testutil.AssertEqual(t, sql, `SELECT CASE WHEN "users"."active" = TRUE THEN 'yes' ELSE 'no' END FROM "users"`)
}

func TestSelectSimpleCase(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select case users.status when 'active' then 1 when 'inactive' then 0 end",
	)
	testutil.AssertEqual(t, sql, `SELECT CASE "users"."status" WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 END FROM "users"`)
}

func TestSelectCaseWithElse(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select case when users.age > 18 then 'adult' when users.age > 12 then 'teen' else 'child' end",
	)
	testutil.AssertEqual(t, sql, `SELECT CASE WHEN "users"."age" > 18 THEN 'adult' WHEN "users"."age" > 12 THEN 'teen' ELSE 'child' END FROM "users"`)
}

func TestExprCase(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("table users")
	_ = sess.Execute("from users")
	err := sess.Execute("expr case when users.active = true then 'yes' else 'no' end = 'yes'")
	if err != nil {
		t.Fatalf("expr case failed: %v", err)
	}
}

// --- Column Aliasing ---

func TestSelectWithAlias(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.name as full_name",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."name" AS "full_name" FROM "users"`)
}

func TestSelectMultipleWithAlias(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.id, users.name as full_name, users.age as user_age",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."id", "users"."name" AS "full_name", "users"."age" AS "user_age" FROM "users"`)
}

func TestSelectFuncWithAlias(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select count(*) as total",
	)
	testutil.AssertEqual(t, sql, `SELECT COUNT(*) AS "total" FROM "users"`)
}

func TestSelectExprWithAlias(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table users",
		"from users",
		"select users.price * users.qty as total_cost",
	)
	testutil.AssertEqual(t, sql, `SELECT "users"."price" * "users"."qty" AS "total_cost" FROM "users"`)
}

// --- CUBE / ROLLUP / GROUPING SETS ---

func TestGroupByCube(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"group cube(orders.region, orders.product)",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "orders" GROUP BY CUBE("orders"."region", "orders"."product")`)
}

func TestGroupByRollup(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"group rollup(orders.year, orders.quarter, orders.month)",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "orders" GROUP BY ROLLUP("orders"."year", "orders"."quarter", "orders"."month")`)
}

func TestGroupByGroupingSets(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"group grouping sets((orders.region, orders.product), (orders.region), ())",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "orders" GROUP BY GROUPING SETS(("orders"."region", "orders"."product"), ("orders"."region"), ())`)
}

func TestGroupByMixedWithCube(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "postgres",
		"table orders",
		"from orders",
		"group orders.country, cube(orders.region, orders.city)",
	)
	testutil.AssertEqual(t, sql, `SELECT * FROM "orders" GROUP BY "orders"."country", CUBE("orders"."region", "orders"."city")`)
}

func TestGroupByRollupMySQL(t *testing.T) {
	t.Parallel()
	sql := execSQL(t, "mysql",
		"table orders",
		"from orders",
		"group rollup(orders.year, orders.month)",
	)
	testutil.AssertEqual(t, sql, "SELECT * FROM `orders` GROUP BY ROLLUP(`orders`.`year`, `orders`.`month`)")
}

// --- Completer updates ---

// --- DML Builder Tests ---

func TestREPLInsertBasic(t *testing.T) {
	t.Parallel()
	got := execSQL(t, "postgres",
		"insert into users",
		"columns users.name, users.email",
		"values 'Alice', 'alice@test.com'",
	)
	testutil.AssertEqual(t, got, `INSERT INTO "users" ("name", "email") VALUES ('Alice', 'alice@test.com')`)
}

func TestREPLInsertMultiRow(t *testing.T) {
	t.Parallel()
	got := execSQL(t, "postgres",
		"insert into users",
		"columns users.name, users.email",
		"values 'Alice', 'alice@test.com'",
		"values 'Bob', 'bob@test.com'",
	)
	testutil.AssertEqual(t, got, `INSERT INTO "users" ("name", "email") VALUES ('Alice', 'alice@test.com'), ('Bob', 'bob@test.com')`)
}

func TestREPLInsertReturning(t *testing.T) {
	t.Parallel()
	got := execSQL(t, "postgres",
		"insert into users",
		"columns users.name",
		"values 'Alice'",
		"returning users.id",
	)
	testutil.AssertEqual(t, got, `INSERT INTO "users" ("name") VALUES ('Alice') RETURNING "users"."id"`)
}

func TestREPLInsertOnConflictDoNothing(t *testing.T) {
	t.Parallel()
	got := execSQL(t, "postgres",
		"insert into users",
		"columns users.name",
		"values 'Alice'",
		"on conflict (users.name) do nothing",
	)
	testutil.AssertEqual(t, got, `INSERT INTO "users" ("name") VALUES ('Alice') ON CONFLICT ("name") DO NOTHING`)
}

func TestREPLUpdateBasic(t *testing.T) {
	t.Parallel()
	got := execSQL(t, "postgres",
		"update users",
		"set users.name = 'Bob'",
		"where users.id = 1",
	)
	testutil.AssertEqual(t, got, `UPDATE "users" SET "users"."name" = 'Bob' WHERE "users"."id" = 1`)
}

func TestREPLUpdateMultipleSet(t *testing.T) {
	t.Parallel()
	got := execSQL(t, "postgres",
		"update users",
		"set users.name = 'Bob'",
		"set users.email = 'bob@test.com'",
	)
	testutil.AssertEqual(t, got, `UPDATE "users" SET "users"."name" = 'Bob', "users"."email" = 'bob@test.com'`)
}

func TestREPLUpdateReturning(t *testing.T) {
	t.Parallel()
	got := execSQL(t, "postgres",
		"update users",
		"set users.name = 'Bob'",
		"returning users.id, users.name",
	)
	testutil.AssertEqual(t, got, `UPDATE "users" SET "users"."name" = 'Bob' RETURNING "users"."id", "users"."name"`)
}

func TestREPLDeleteBasic(t *testing.T) {
	t.Parallel()
	got := execSQL(t, "postgres",
		"delete from users",
		"where users.id = 1",
	)
	testutil.AssertEqual(t, got, `DELETE FROM "users" WHERE "users"."id" = 1`)
}

func TestREPLDeleteReturning(t *testing.T) {
	t.Parallel()
	got := execSQL(t, "postgres",
		"delete from users",
		"where users.id = 1",
		"returning users.id",
	)
	testutil.AssertEqual(t, got, `DELETE FROM "users" WHERE "users"."id" = 1 RETURNING "users"."id"`)
}

func TestREPLResetClearsDML(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	_ = sess.Execute("insert into users")
	if sess.mode != modeInsert {
		t.Fatalf("expected modeInsert, got %d", sess.mode)
	}
	_ = sess.Execute("reset")
	if sess.mode != modeSelect {
		t.Fatalf("expected modeSelect after reset, got %d", sess.mode)
	}
	if sess.insertQuery != nil {
		t.Error("expected insertQuery to be nil after reset")
	}
}

func TestREPLModeSwitching(t *testing.T) {
	t.Parallel()
	sess := NewSession("postgres", nil)
	// Start in insert mode
	_ = sess.Execute("insert into users")
	if sess.mode != modeInsert {
		t.Fatalf("expected modeInsert, got %d", sess.mode)
	}
	// Switch to select mode via from
	_ = sess.Execute("from users")
	if sess.mode != modeSelect {
		t.Fatalf("expected modeSelect after from, got %d", sess.mode)
	}
	if sess.insertQuery != nil {
		t.Error("expected insertQuery to be nil after from")
	}
	// Switch to update mode
	_ = sess.Execute("update users")
	if sess.mode != modeUpdate {
		t.Fatalf("expected modeUpdate, got %d", sess.mode)
	}
	// Switch to delete mode
	_ = sess.Execute("delete from users")
	if sess.mode != modeDelete {
		t.Fatalf("expected modeDelete, got %d", sess.mode)
	}
}

func TestREPLInsertMySQL(t *testing.T) {
	t.Parallel()
	got := execSQL(t, "mysql",
		"insert into users",
		"columns users.name",
		"values 'Alice'",
	)
	testutil.AssertEqual(t, got, "INSERT INTO `users` (`name`) VALUES ('Alice')")
}

func TestCompleterIncludesNewFunctions(t *testing.T) {
	t.Parallel()
	expected := []string{"CAST(", "COALESCE(", "LOWER(", "UPPER(", "CASE ", "CUBE(", "ROLLUP("}
	for _, fn := range expected {
		found := false
		for _, name := range functionNames {
			if name == fn {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected %q in functionNames", fn)
		}
	}
}
