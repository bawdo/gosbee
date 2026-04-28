// Package main is a testdata fixture used by TestNewSqlLiteralRejectsPlainString
// in nodes/rawsql_security_test.go.
//
// It represents the pre-fix vulnerability: a plain string variable can be passed
// directly to NewSqlLiteral without any explicit cast, making it trivial to
// accidentally introduce SQL injection.
//
// After the RawSQL type fix this file must fail to compile with:
//
//	cannot use userInput (variable of type string) as type nodes.RawSQL
package main

import "github.com/bawdo/gosbee/nodes"

func main() {
	userInput := "' OR '1'='1; DROP TABLE users --"
	_ = nodes.NewSqlLiteral(userInput)
}
