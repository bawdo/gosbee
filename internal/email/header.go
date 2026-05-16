package email

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
)

// headerStyle is the resolved set of colours the header partial renders with.
type headerStyle struct {
	BG      string
	TextFG  string
	BadgeBG string
	BadgeFG string
}

var hexColourRe = regexp.MustCompile(`^#[0-9A-Fa-f]{6}$`)

// ValidateHexColour returns an error iff value is not a #RRGGBB hex string.
// Called from config-load and flag-parse so bad input fails early.
func ValidateHexColour(value string) error {
	if !hexColourRe.MatchString(value) {
		return fmt.Errorf("invalid hex colour %q (want #RRGGBB)", value)
	}
	return nil
}

// hexToRGB parses #RRGGBB into 0..255 components.
func hexToRGB(value string) (uint8, uint8, uint8, error) {
	if err := ValidateHexColour(value); err != nil {
		return 0, 0, 0, err
	}
	r, _ := strconv.ParseUint(value[1:3], 16, 8)
	g, _ := strconv.ParseUint(value[3:5], 16, 8)
	b, _ := strconv.ParseUint(value[5:7], 16, 8)
	return uint8(r), uint8(g), uint8(b), nil
}

// relativeLuminance applies the WCAG 2.1 sRGB linearisation and weighted sum.
func relativeLuminance(r, g, b uint8) float64 {
	return 0.2126*linearise(r) + 0.7152*linearise(g) + 0.0722*linearise(b)
}

func linearise(c uint8) float64 {
	f := float64(c) / 255.0
	if f <= 0.03928 {
		return f / 12.92
	}
	return math.Pow((f+0.055)/1.055, 2.4)
}

// computeHeaderStyle picks dark or light text based on background luminance.
// Bad input (rejected by ValidateHexColour upstream) falls back to dark text.
func computeHeaderStyle(bg string) headerStyle {
	r, g, b, err := hexToRGB(bg)
	if err != nil {
		return headerStyle{BG: bg, TextFG: "#0f172a", BadgeBG: "rgba(15,23,42,0.10)", BadgeFG: "#0f172a"}
	}
	if relativeLuminance(r, g, b) > 0.5 {
		return headerStyle{BG: bg, TextFG: "#0f172a", BadgeBG: "rgba(15,23,42,0.10)", BadgeFG: "#0f172a"}
	}
	return headerStyle{BG: bg, TextFG: "#f8fafc", BadgeBG: "rgba(255,255,255,0.18)", BadgeFG: "#f8fafc"}
}

// Header is the data the shared header partial renders against. The
// per-renderer view struct must expose this on its top-level Header field.
type Header struct {
	BG       string
	TextFG   string
	BadgeBG  string
	BadgeFG  string
	Badge    string
	Title    string
	Subtitle string
	HasLogo  bool
}

// buildHeader composes a Header from per-command strings plus the chosen
// background colour and a hasLogo flag.
func buildHeader(badge, title, subtitle, bg string, hasLogo bool) Header {
	s := computeHeaderStyle(bg)
	return Header{
		BG: s.BG, TextFG: s.TextFG, BadgeBG: s.BadgeBG, BadgeFG: s.BadgeFG,
		Badge: badge, Title: title, Subtitle: subtitle, HasLogo: hasLogo,
	}
}
