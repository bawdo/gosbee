package email

import (
	"fmt"
	"strings"
	"time"
)

// Built-in defaults applied when an Options field is empty after the cmd
// layer's flag + config resolution.
const (
	DefaultCVELinkPrimary   = "https://nvd.nist.gov/vuln/detail/{cve}"
	DefaultCVELinkSecondary = "https://www.cve.org/CVERecord?id={cve}"
)

// Options carries everything an email renderer needs that isn't part of the
// rendered domain data. All fields are optional except as noted; missing
// optional fields fall back to built-in defaults at renderer construction.
type Options struct {
	To      string // header value; empty renders as "<unspecified>"
	From    string // required at the cmd layer; renderer errors if empty
	Subject string // empty triggers per-renderer default

	CVELinkPrimary   string // empty defaults to DefaultCVELinkPrimary
	CVELinkSecondary string // empty defaults to DefaultCVELinkSecondary

	GeneratedAt time.Time // pinned by tests; cmd layer passes time.Now()
	Tenant      string    // shown in masthead, sourced from config.Profile.Subdomain

	// Injected for tests; production leaves these zero so assembleMessage
	// pulls from crypto/rand.
	BoundaryOverride  string
	MessageIDOverride string
}

// withDefaults returns a copy of opts with empty optional fields filled in.
func (o Options) withDefaults() Options {
	if o.CVELinkPrimary == "" {
		o.CVELinkPrimary = DefaultCVELinkPrimary
	}
	if o.CVELinkSecondary == "" {
		o.CVELinkSecondary = DefaultCVELinkSecondary
	}
	if o.GeneratedAt.IsZero() {
		o.GeneratedAt = time.Now()
	}
	return o
}

// buildCVELink substitutes the literal {cve} token in template with cve.
// All occurrences are replaced.
func buildCVELink(tmpl, cve string) string {
	return strings.ReplaceAll(tmpl, "{cve}", cve)
}

// validateLinkTemplate ensures a CVE link template contains the {cve} token.
// label appears in the error to disambiguate primary vs secondary.
func validateLinkTemplate(label, tmpl string) error {
	if !strings.Contains(tmpl, "{cve}") {
		return fmt.Errorf("email %s CVE link template must contain {cve}: got %q", label, tmpl)
	}
	return nil
}
