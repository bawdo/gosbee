package nodes

import (
	"os/exec"
	"strings"
	"testing"
)

// TestNewSqlLiteralRejectsPlainString verifies that passing a plain string
// variable to NewSqlLiteral is a compile-time error.
//
// This is a TDD security test: it FAILS before the RawSQL type fix is applied
// (because the vulnerable code compiles successfully), and PASSES after the fix
// (because the compiler rejects the plain string variable with a type error).
//
// To see the pre-fix failure run: go test -run TestNewSqlLiteralRejectsPlainString ./nodes/
// Expected output before fix:
//
//	FAIL: SECURITY: NewSqlLiteral accepted a plain string variable — ...
func TestNewSqlLiteralRejectsPlainString(t *testing.T) {
	cmd := exec.Command("go", "build", "./testdata/string_to_sql_literal")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal(
			"SECURITY: NewSqlLiteral accepted a plain string variable — " +
				"the RawSQL type safety fix has not been applied.\n" +
				"Any engineer can write nodes.NewSqlLiteral(userInput) without a compile error.\n" +
				"See: docs/plans/2026-04-28-rawsql-type-safety.md",
		)
	}
	if !strings.Contains(string(out), "cannot use") {
		t.Fatalf("build failed but not with expected type error — check output:\n%s", out)
	}
}

// TestSqlLiteralRendersVerbatim documents the injection mechanism: SqlLiteral
// stores its content unchanged and the visitor renders it directly into SQL
// output with no escaping or parameterization.
//
// This is intentional by design — SqlLiteral is a deliberate raw-SQL escape
// hatch. The RawSQL type fix does not change this behaviour; it only ensures
// that reaching this code path requires an explicit nodes.RawSQL(...) cast,
// making the unsafe operation visible and auditable.
func TestSqlLiteralRendersVerbatim(t *testing.T) {
	malicious := RawSQL("1; DROP TABLE users; --")
	lit := NewSqlLiteral(malicious)
	if string(lit.Raw) != string(malicious) {
		t.Fatalf("expected verbatim storage %q, got %q", malicious, lit.Raw)
	}
}
