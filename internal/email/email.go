package email

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"mime/quotedprintable"
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

// messageHeaders is the minimum set of headers assembleMessage writes.
type messageHeaders struct {
	From    string
	To      string
	Subject string
	Date    time.Time
}

// assembleMessage produces a full RFC 5322 multipart/alternative message
// from a plain-text body and an HTML body. boundary and messageID are
// caller-supplied for test determinism; production callers pass values
// from randomBoundary() and randomMessageID().
func assembleMessage(h messageHeaders, htmlBody, textBody, boundary, messageID string) ([]byte, error) {
	var sb strings.Builder
	writeHeader := func(name, value string) {
		sb.WriteString(name)
		sb.WriteString(": ")
		sb.WriteString(sanitiseHeaderValue(value))
		sb.WriteString("\r\n")
	}

	to := h.To
	if to == "" {
		to = "<unspecified>"
	}

	writeHeader("From", h.From)
	writeHeader("To", to)
	writeHeader("Subject", h.Subject)
	writeHeader("Date", h.Date.Format(time.RFC1123Z))
	writeHeader("Message-ID", messageID)
	writeHeader("MIME-Version", "1.0")
	writeHeader("Content-Type", fmt.Sprintf("multipart/alternative; boundary=%q", sanitiseHeaderValue(boundary)))
	sb.WriteString("\r\n")

	writePart := func(contentType, body string) error {
		sb.WriteString("--")
		sb.WriteString(boundary)
		sb.WriteString("\r\n")
		sb.WriteString("Content-Type: ")
		sb.WriteString(contentType)
		sb.WriteString("\r\n")
		sb.WriteString("Content-Transfer-Encoding: quoted-printable\r\n\r\n")

		encoded, err := quotedPrintableEncode(body)
		if err != nil {
			return err
		}
		sb.WriteString(encoded)
		if !strings.HasSuffix(encoded, "\r\n") {
			sb.WriteString("\r\n")
		}
		return nil
	}

	if err := writePart("text/plain; charset=UTF-8", textBody); err != nil {
		return nil, err
	}
	if err := writePart("text/html; charset=UTF-8", htmlBody); err != nil {
		return nil, err
	}

	sb.WriteString("--")
	sb.WriteString(boundary)
	sb.WriteString("--\r\n")

	return []byte(sb.String()), nil
}

// quotedPrintableEncode runs body through stdlib quoted-printable, then
// normalises line endings to CRLF (stdlib emits LF after soft-break inserts
// on some Go versions; RFC 5322 requires CRLF throughout the message).
func quotedPrintableEncode(body string) (string, error) {
	buf := &strings.Builder{}
	w := quotedprintable.NewWriter(buf)
	if _, err := io.WriteString(w, body); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}
	return strings.ReplaceAll(strings.ReplaceAll(buf.String(), "\r\n", "\n"), "\n", "\r\n"), nil
}

// randomBoundary returns "=_jf_" + 16 lowercase hex chars (8 random bytes).
func randomBoundary() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "=_jf_" + hex.EncodeToString(b[:]), nil
}

// randomMessageID returns "<nanos.<6 hex chars>@<domain>>".
func randomMessageID(domain string) (string, error) {
	var b [3]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("<%d.%s@%s>", time.Now().UnixNano(), hex.EncodeToString(b[:]), domain), nil
}

// sanitiseHeaderValue strips CR/LF so a caller-supplied header value cannot
// inject additional headers (RFC 5322 header injection).
func sanitiseHeaderValue(v string) string {
	return strings.NewReplacer("\r", "", "\n", "").Replace(v)
}
