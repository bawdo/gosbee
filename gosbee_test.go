package gosbee_test

import (
	"strings"
	"testing"

	"github.com/bawdo/gosbee"
)

// TestSimpleImportStyle demonstrates using the convenience package
func TestSimpleImportStyle(t *testing.T) {
	users := gosbee.NewTable("users")

	query := gosbee.NewSelect(users).
		Select(users.Col("id"), users.Col("name")).
		Where(users.Col("active").Eq(gosbee.Literal(true))).
		Order(users.Col("name").Asc()).
		Limit(10)

	visitor := gosbee.NewPostgresVisitor(gosbee.WithoutParams())
	sql, _, err := query.ToSQL(visitor)
	if err != nil {
		t.Fatalf("ToSQL failed: %v", err)
	}

	expected := `SELECT "users"."id", "users"."name" FROM "users" WHERE "users"."active" = TRUE ORDER BY "users"."name" ASC LIMIT 10`
	if sql != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, sql)
	}
}

// TestParameterisedQuery demonstrates parameterised queries
func TestParameterisedQuery(t *testing.T) {
	users := gosbee.NewTable("users")

	query := gosbee.NewSelect(users).
		Select(users.Col("id"), users.Col("name")).
		Where(users.Col("name").Eq(gosbee.BindParam("Alice"))).
		Where(users.Col("age").Gt(gosbee.BindParam(18)))

	// Enable parameterisation mode
	visitor := gosbee.NewPostgresVisitor(gosbee.WithParams())
	sql, params, err := query.ToSQL(visitor)
	if err != nil {
		t.Fatalf("ToSQL failed: %v", err)
	}

	if !strings.Contains(sql, "$1") || !strings.Contains(sql, "$2") {
		t.Errorf("Expected parameterised SQL, got: %s", sql)
	}

	if len(params) != 2 {
		t.Errorf("Expected 2 params, got %d", len(params))
	}
	if params[0] != "Alice" {
		t.Errorf("Expected first param to be 'Alice', got %v", params[0])
	}
	if params[1] != 18 {
		t.Errorf("Expected second param to be 18, got %v", params[1])
	}
}

// TestAggregateFunctions demonstrates aggregate functions
func TestAggregateFunctions(t *testing.T) {
	users := gosbee.NewTable("users")

	query := gosbee.NewSelect(users).
		Select(
			users.Col("department"),
			gosbee.Count(gosbee.Star()).As("total"),
			gosbee.Avg(users.Col("salary")).As("avg_salary"),
		).
		Group(users.Col("department"))

	visitor := gosbee.NewPostgresVisitor(gosbee.WithoutParams())
	sql, _, err := query.ToSQL(visitor)
	if err != nil {
		t.Fatalf("ToSQL failed: %v", err)
	}

	if !strings.Contains(sql, "COUNT(*)") {
		t.Errorf("Expected COUNT(*), got: %s", sql)
	}
	if !strings.Contains(sql, "AVG(") {
		t.Errorf("Expected AVG, got: %s", sql)
	}
}

// TestMultipleDialects demonstrates using different SQL dialects
func TestMultipleDialects(t *testing.T) {
	users := gosbee.NewTable("users")

	tests := []struct {
		name     string
		expected string
	}{
		{
			name:     "PostgreSQL",
			expected: `SELECT "users"."name" FROM "users" WHERE "users"."active" = TRUE`,
		},
		{
			name:     "MySQL",
			expected: "SELECT `users`.`name` FROM `users` WHERE `users`.`active` = TRUE",
		},
		{
			name:     "SQLite",
			expected: `SELECT "users"."name" FROM "users" WHERE "users"."active" = TRUE`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := gosbee.NewSelect(users).
				Select(users.Col("name")).
				Where(users.Col("active").Eq(gosbee.Literal(true)))

			var sql string
			var err error

			switch tt.name {
			case "PostgreSQL":
				sql, _, err = query.ToSQL(gosbee.NewPostgresVisitor(gosbee.WithoutParams()))
			case "MySQL":
				sql, _, err = query.ToSQL(gosbee.NewMySQLVisitor(gosbee.WithoutParams()))
			case "SQLite":
				sql, _, err = query.ToSQL(gosbee.NewSQLiteVisitor(gosbee.WithoutParams()))
			}

			if err != nil {
				t.Fatalf("ToSQL failed: %v", err)
			}
			if sql != tt.expected {
				t.Errorf("Expected:\n%s\nGot:\n%s", tt.expected, sql)
			}
		})
	}
}

// TestDMLOperations demonstrates INSERT, UPDATE, DELETE
func TestDMLOperations(t *testing.T) {
	users := gosbee.NewTable("users")
	visitor := gosbee.NewPostgresVisitor(gosbee.WithoutParams())

	// INSERT
	insertQuery := gosbee.NewInsert(users).
		Columns(users.Col("name"), users.Col("email")).
		Values(gosbee.Literal("Alice"), gosbee.Literal("alice@example.com"))

	sql, _, err := insertQuery.ToSQL(visitor)
	if err != nil {
		t.Fatalf("INSERT ToSQL failed: %v", err)
	}
	if !strings.Contains(sql, "INSERT INTO") {
		t.Errorf("Expected INSERT query, got: %s", sql)
	}

	// UPDATE
	updateQuery := gosbee.NewUpdate(users).
		Set(users.Col("status"), gosbee.Literal("inactive")).
		Where(users.Col("id").Eq(gosbee.Literal(1)))

	sql, _, err = updateQuery.ToSQL(visitor)
	if err != nil {
		t.Fatalf("UPDATE ToSQL failed: %v", err)
	}
	if !strings.Contains(sql, "UPDATE") {
		t.Errorf("Expected UPDATE query, got: %s", sql)
	}

	// DELETE
	deleteQuery := gosbee.NewDelete(users).
		Where(users.Col("status").Eq(gosbee.Literal("deleted")))

	sql, _, err = deleteQuery.ToSQL(visitor)
	if err != nil {
		t.Fatalf("DELETE ToSQL failed: %v", err)
	}
	if !strings.Contains(sql, "DELETE FROM") {
		t.Errorf("Expected DELETE query, got: %s", sql)
	}
}
