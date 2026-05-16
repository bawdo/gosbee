package iru

import "testing"

func TestSeverityRank(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"Critical", 0},
		{"critical", 0},
		{"CRITICAL", 0},
		{"High", 1},
		{"high", 1},
		{"Medium", 2},
		{"medium", 2},
		{"Low", 3},
		{"low", 3},
		{"Undefined", 4},
		{"undefined", 4},
		{"", 5},
		{"bogus", 5},
	}
	for _, c := range cases {
		if got := SeverityRank(c.in); got != c.want {
			t.Errorf("SeverityRank(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestSeverityRankProducesSemanticOrder(t *testing.T) {
	// Critical < High < Medium < Low < Undefined < anything else.
	if !(SeverityRank("Critical") < SeverityRank("High") &&
		SeverityRank("High") < SeverityRank("Medium") &&
		SeverityRank("Medium") < SeverityRank("Low") &&
		SeverityRank("Low") < SeverityRank("Undefined") &&
		SeverityRank("Undefined") < SeverityRank("nonsense")) {
		t.Fatalf("severity rank ordering violated: crit=%d high=%d med=%d low=%d und=%d other=%d",
			SeverityRank("Critical"), SeverityRank("High"), SeverityRank("Medium"),
			SeverityRank("Low"), SeverityRank("Undefined"), SeverityRank("nonsense"))
	}
}
