package iru

import "strings"

// SeverityRank returns a fixed semantic ordering for severity strings:
// Critical (0) > High (1) > Medium (2) > Low (3) > Undefined (4) > anything
// else (5). Case-insensitive. Use as a sort key whenever severity needs to be
// rendered in priority order.
func SeverityRank(s string) int {
	switch strings.ToLower(s) {
	case "critical":
		return 0
	case "high":
		return 1
	case "medium":
		return 2
	case "low":
		return 3
	case "undefined":
		return 4
	default:
		return 5
	}
}
