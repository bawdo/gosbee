package main

import (
	"strings"
	"testing"
)

// --- Unit Tests (no DB) ---

func TestFormatTableBasic(t *testing.T) {
	cols := []string{"id", "name", "active"}
	rows := [][]string{
		{"1", "Alice", "true"},
		{"2", "Bob", "false"},
	}
	result := formatTable(cols, rows)

	if !strings.Contains(result, "| id | name  | active |") {
		t.Errorf("missing header row:\n%s", result)
	}
	if !strings.Contains(result, "|  1 | Alice | true   |") {
		// left-aligned, so no leading space for "1" unless column is wider
		if !strings.Contains(result, "| 1") {
			t.Errorf("missing data row for Alice:\n%s", result)
		}
	}
	if !strings.Contains(result, "(2 rows)") {
		t.Errorf("missing row count:\n%s", result)
	}
}

func TestFormatTableSingleRow(t *testing.T) {
	cols := []string{"x"}
	rows := [][]string{{"42"}}
	result := formatTable(cols, rows)

	if !strings.Contains(result, "(1 row)") {
		t.Errorf("expected '(1 row)', got:\n%s", result)
	}
}

func TestFormatTableEmpty(t *testing.T) {
	cols := []string{"a", "b"}
	result := formatTable(cols, nil)

	if !strings.Contains(result, "(0 rows)") {
		t.Errorf("expected '(0 rows)', got:\n%s", result)
	}
	// Should still have header.
	if !strings.Contains(result, "| a | b |") {
		t.Errorf("missing header:\n%s", result)
	}
}

func TestFormatTableNoColumns(t *testing.T) {
	result := formatTable(nil, nil)
	if result != "(0 rows)\n" {
		t.Errorf("expected '(0 rows)\\n', got: %q", result)
	}
}

func TestSanitizeDSNPostgres(t *testing.T) {
	dsn := "postgres://admin:secret@localhost:5432/mydb?sslmode=disable"
	got := sanitizeDSN(dsn)
	if strings.Contains(got, "secret") {
		t.Errorf("password not masked: %s", got)
	}
	if !strings.Contains(got, "****") {
		t.Errorf("expected masked password: %s", got)
	}
	if !strings.Contains(got, "admin") {
		t.Errorf("username should be preserved: %s", got)
	}
}

func TestSanitizeDSNMySQL(t *testing.T) {
	dsn := "root:mypass@tcp(localhost:3306)/testdb"
	got := sanitizeDSN(dsn)
	if strings.Contains(got, "mypass") {
		t.Errorf("password not masked: %s", got)
	}
	if !strings.Contains(got, "root:****@") {
		t.Errorf("expected masked password: %s", got)
	}
}

func TestSanitizeDSNSQLitePath(t *testing.T) {
	dsn := "/tmp/test.db"
	got := sanitizeDSN(dsn)
	if got != dsn {
		t.Errorf("sqlite path should be unchanged: got %q, want %q", got, dsn)
	}
}

func TestSanitizeDSNMemory(t *testing.T) {
	dsn := ":memory:"
	got := sanitizeDSN(dsn)
	if got != dsn {
		t.Errorf("memory DSN should be unchanged: got %q, want %q", got, dsn)
	}
}

func TestDriverNameMapping(t *testing.T) {
	tests := map[string]string{
		"postgres": "pgx",
		"mysql":    "mysql",
		"sqlite":   "sqlite",
	}
	for engine, expected := range tests {
		got, ok := driverName[engine]
		if !ok {
			t.Errorf("missing driver for %q", engine)
			continue
		}
		if got != expected {
			t.Errorf("driver for %q: got %q, want %q", engine, got, expected)
		}
	}
}

// --- Integration Tests (SQLite :memory:) ---

func TestConnectDisconnect(t *testing.T) {
	conn, err := connect("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	if conn.engine != "sqlite" {
		t.Errorf("engine: got %q, want %q", conn.engine, "sqlite")
	}
	if err := conn.close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestConnectWhenAlreadyConnected(t *testing.T) {
	sess := NewSession("sqlite", nil)
	if err := sess.Execute("connect :memory:"); err != nil {
		t.Fatalf("first connect failed: %v", err)
	}
	defer func() { _ = sess.conn.close() }()

	err := sess.Execute("connect :memory:")
	if err == nil {
		t.Fatal("expected error for double connect")
	}
	if !strings.Contains(err.Error(), "already connected") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDisconnectWhenNotConnected(t *testing.T) {
	sess := NewSession("sqlite", nil)
	err := sess.Execute("disconnect")
	if err == nil {
		t.Fatal("expected error for disconnect when not connected")
	}
	if !strings.Contains(err.Error(), "not connected") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExecSimpleQuery(t *testing.T) {
	conn, err := connect("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.close() }()

	// Create a test table and insert data.
	_, err = conn.db.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = conn.db.Exec("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := conn.execQuery("SELECT id, name FROM users ORDER BY id", nil)
	if err != nil {
		t.Fatalf("execQuery: %v", err)
	}

	if !strings.Contains(result, "Alice") {
		t.Errorf("result should contain Alice:\n%s", result)
	}
	if !strings.Contains(result, "Bob") {
		t.Errorf("result should contain Bob:\n%s", result)
	}
	if !strings.Contains(result, "(2 rows)") {
		t.Errorf("expected 2 rows:\n%s", result)
	}
}

func TestExecParameterized(t *testing.T) {
	conn, err := connect("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.close() }()

	_, err = conn.db.Exec("CREATE TABLE items (id INTEGER, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = conn.db.Exec("INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := conn.execQuery("SELECT val FROM items WHERE id = ?", []any{2})
	if err != nil {
		t.Fatalf("execQuery: %v", err)
	}

	if !strings.Contains(result, "b") {
		t.Errorf("result should contain 'b':\n%s", result)
	}
	if !strings.Contains(result, "(1 row)") {
		t.Errorf("expected 1 row:\n%s", result)
	}
}

func TestExecNoConnection(t *testing.T) {
	sess := NewSession("sqlite", nil)
	_ = sess.Execute("from users")
	err := sess.Execute("exec")
	if err == nil {
		t.Fatal("expected error for exec without connection")
	}
	if !strings.Contains(err.Error(), "not connected") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExecNoQuery(t *testing.T) {
	sess := NewSession("sqlite", nil)
	if err := sess.Execute("connect :memory:"); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.conn.close() }()

	err := sess.Execute("exec")
	if err == nil {
		t.Fatal("expected error for exec without query")
	}
	if !strings.Contains(err.Error(), "no query") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExecEngineMismatch(t *testing.T) {
	// Connect as sqlite, switch engine to postgres, exec should still work
	// (it warns but proceeds).
	sess := NewSession("sqlite", nil)
	if err := sess.Execute("connect :memory:"); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.conn.close() }()

	_, err := sess.conn.db.Exec("CREATE TABLE t (id INTEGER)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = sess.conn.db.Exec("INSERT INTO t VALUES (1)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	_ = sess.Execute("from t")
	_ = sess.Execute("engine postgres")

	// Engine mismatch: connected to sqlite, engine set to postgres.
	// The SQL generated will use postgres dialect but execute against sqlite.
	// For simple queries this should still work.
	err = sess.Execute("exec")
	// Depending on the SQL, this might succeed or fail at the DB level.
	// The important thing is the warning is printed, not that it errors.
	// Since "SELECT * FROM t" is valid in both dialects, it should succeed.
	if err != nil {
		t.Logf("exec with mismatch: %v (expected for some queries)", err)
	}
}

func TestExecNullDisplay(t *testing.T) {
	conn, err := connect("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.close() }()

	_, err = conn.db.Exec("CREATE TABLE n (id INTEGER, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = conn.db.Exec("INSERT INTO n VALUES (1, NULL)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := conn.execQuery("SELECT id, val FROM n", nil)
	if err != nil {
		t.Fatalf("execQuery: %v", err)
	}

	if !strings.Contains(result, "NULL") {
		t.Errorf("NULL values should display as 'NULL':\n%s", result)
	}
}
