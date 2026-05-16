package email

import (
	"crypto/rand"
	"encoding/base64"
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
	DefaultHeaderBG         = "#7566FF"
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

	HeaderBG string // hex #RRGGBB; renderer applies DefaultHeaderBG if empty
	LogoPath string // optional path to a PNG; empty disables the logo

	// Injected for tests; production leaves these zero so assembleMessage
	// pulls from crypto/rand.
	BoundaryOverride        string
	RelatedBoundaryOverride string
	MessageIDOverride       string
}

// withDefaults returns a copy of opts with empty optional fields filled in.
func (o Options) withDefaults() Options {
	if o.CVELinkPrimary == "" {
		o.CVELinkPrimary = DefaultCVELinkPrimary
	}
	if o.CVELinkSecondary == "" {
		o.CVELinkSecondary = DefaultCVELinkSecondary
	}
	if o.HeaderBG == "" {
		o.HeaderBG = DefaultHeaderBG
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

// assembleMessage produces a full RFC 5322 message. When logo is nil, the
// result is multipart/alternative carrying the text and HTML bodies. When
// logo is non-nil, that multipart/alternative is wrapped in a
// multipart/related envelope and the logo bytes are emitted as an inline
// image part referenced by Content-ID.
//
// innerBoundary and outerBoundary are caller-supplied for test determinism;
// outerBoundary is ignored when logo is nil.
func assembleMessage(
	h messageHeaders,
	htmlBody, textBody string,
	innerBoundary, messageID string,
	outerBoundary string,
	logo *logoPart,
) ([]byte, error) {
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
	if logo == nil {
		writeHeader("Content-Type", fmt.Sprintf("multipart/alternative; boundary=%q", sanitiseHeaderValue(innerBoundary)))
	} else {
		writeHeader("Content-Type", fmt.Sprintf("multipart/related; type=%q; boundary=%q",
			"multipart/alternative", sanitiseHeaderValue(outerBoundary)))
	}
	sb.WriteString("\r\n")

	if logo != nil {
		sb.WriteString("--")
		sb.WriteString(outerBoundary)
		sb.WriteString("\r\n")
		sb.WriteString("Content-Type: ")
		sb.WriteString(fmt.Sprintf("multipart/alternative; boundary=%q", sanitiseHeaderValue(innerBoundary)))
		sb.WriteString("\r\n\r\n")
	}

	writePart := func(contentType, body string) error {
		sb.WriteString("--")
		sb.WriteString(innerBoundary)
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
	sb.WriteString(innerBoundary)
	sb.WriteString("--\r\n")

	if logo != nil {
		sb.WriteString("--")
		sb.WriteString(outerBoundary)
		sb.WriteString("\r\n")
		sb.WriteString("Content-Type: image/png\r\n")
		sb.WriteString("Content-Transfer-Encoding: base64\r\n")
		sb.WriteString(fmt.Sprintf("Content-ID: <%s>\r\n", logo.CID))
		sb.WriteString(fmt.Sprintf("Content-Disposition: inline; filename=%q\r\n\r\n", logo.Name))
		sb.WriteString(base64Wrap(logo.Bytes, 76))
		sb.WriteString("\r\n--")
		sb.WriteString(outerBoundary)
		sb.WriteString("--\r\n")
	}

	return []byte(sb.String()), nil
}

// base64Wrap returns the base64 encoding of data with CRLF inserted every
// lineWidth output chars (RFC 2045 line-length).
func base64Wrap(data []byte, lineWidth int) string {
	enc := base64.StdEncoding.EncodeToString(data)
	var sb strings.Builder
	for i := 0; i < len(enc); i += lineWidth {
		end := i + lineWidth
		if end > len(enc) {
			end = len(enc)
		}
		sb.WriteString(enc[i:end])
		sb.WriteString("\r\n")
	}
	return strings.TrimSuffix(sb.String(), "\r\n")
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

func sevRowBG(class string) string {
	switch class {
	case "crit":
		return "#dc2626"
	case "high":
		return "#ea580c"
	case "med":
		return "#ca8a04"
	default:
		return "#64748b"
	}
}

func sevPillBG(class string) string {
	switch class {
	case "crit":
		return "#fee2e2"
	case "high":
		return "#ffedd5"
	case "med":
		return "#fef3c7"
	default:
		return "#f1f5f9"
	}
}

func sevPillFG(class string) string {
	switch class {
	case "crit":
		return "#991b1b"
	case "high":
		return "#9a3412"
	case "med":
		return "#854d0e"
	default:
		return "#334155"
	}
}
