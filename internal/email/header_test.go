package email

import (
	"math"
	"testing"
)

func TestValidateHexColourAccepts(t *testing.T) {
	cases := []string{"#7566FF", "#c6b8fe", "#000000", "#FFFFFF"}
	for _, c := range cases {
		if err := ValidateHexColour(c); err != nil {
			t.Errorf("ValidateHexColour(%q) unexpected error: %v", c, err)
		}
	}
}

func TestValidateHexColourRejects(t *testing.T) {
	cases := []string{"", "7566FF", "#7566F", "#7566FFA", "#ZZZZZZ", "purple"}
	for _, c := range cases {
		if err := ValidateHexColour(c); err == nil {
			t.Errorf("ValidateHexColour(%q): expected error, got nil", c)
		}
	}
}

func TestHexToRGB(t *testing.T) {
	r, g, b, err := hexToRGB("#7566FF")
	if err != nil {
		t.Fatalf("hexToRGB: %v", err)
	}
	if r != 0x75 || g != 0x66 || b != 0xFF {
		t.Errorf("got (%d,%d,%d) want (117,102,255)", r, g, b)
	}
}

func TestRelativeLuminanceBlackWhite(t *testing.T) {
	if got := relativeLuminance(0, 0, 0); math.Abs(got) > 1e-9 {
		t.Errorf("black: got %v want 0", got)
	}
	if got := relativeLuminance(255, 255, 255); math.Abs(got-1.0) > 1e-9 {
		t.Errorf("white: got %v want 1", got)
	}
}

func TestComputeHeaderStylePalette(t *testing.T) {
	cases := []struct {
		bg     string
		light  bool // true => expect dark text branch
	}{
		{"#7566FF", false},
		{"#C6B8FE", true},
		{"#6846D8", false},
		{"#FFFFFF", true},
		{"#000000", false},
	}
	for _, c := range cases {
		got := computeHeaderStyle(c.bg)
		if got.BG != c.bg {
			t.Errorf("%s: BG echoed %q", c.bg, got.BG)
		}
		darkText := got.TextFG == "#0f172a"
		if c.light && !darkText {
			t.Errorf("%s: expected dark text (light bg), got %+v", c.bg, got)
		}
		if !c.light && darkText {
			t.Errorf("%s: expected light text (dark bg), got %+v", c.bg, got)
		}
	}
}
