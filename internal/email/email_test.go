package email

import (
	"bytes"
	"mime"
	"mime/multipart"
	"net/mail"
	"strings"
	"testing"
	"time"
)



func TestBuildCVELinkSubstitutes(t *testing.T) {
	got := buildCVELink("https://nvd.nist.gov/vuln/detail/{cve}", "CVE-2024-3094")
	want := "https://nvd.nist.gov/vuln/detail/CVE-2024-3094"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestBuildCVELinkMultipleTokens(t *testing.T) {
	got := buildCVELink("https://x.test/{cve}/info?id={cve}", "CVE-1")
	want := "https://x.test/CVE-1/info?id=CVE-1"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestValidateLinkTemplateAcceptsToken(t *testing.T) {
	if err := validateLinkTemplate("primary", "https://x.test/{cve}"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateLinkTemplateRejectsMissingToken(t *testing.T) {
	err := validateLinkTemplate("primary", "https://x.test/foo")
	if err == nil {
		t.Fatal("expected error for template without {cve}")
	}
}

func TestAssembleMessageHeadersAndStructure(t *testing.T) {
	hdr := messageHeaders{
		From:    "Jellyfish <alice@example.com>",
		To:      "secops@example.com",
		Subject: "Test subject",
		Date:    time.Date(2026, 5, 16, 10, 42, 0, 0, time.FixedZone("AEST", 10*3600)),
	}
	out, err := assembleMessage(hdr, "<html><body>hi</body></html>", "hello plain text\n",
		"=_jf_FIXEDBOUNDARY", "<fixed-id@example.com>",
		"", nil,
	)
	if err != nil {
		t.Fatalf("assembleMessage: %v", err)
	}

	msg, err := mail.ReadMessage(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("parse: %v\nraw:\n%s", err, out)
	}

	if got := msg.Header.Get("From"); got != hdr.From {
		t.Errorf("From: got %q want %q", got, hdr.From)
	}
	if got := msg.Header.Get("To"); got != hdr.To {
		t.Errorf("To: got %q want %q", got, hdr.To)
	}
	if got := msg.Header.Get("Subject"); got != hdr.Subject {
		t.Errorf("Subject: got %q want %q", got, hdr.Subject)
	}
	if got := msg.Header.Get("Message-ID"); got != "<fixed-id@example.com>" {
		t.Errorf("Message-ID: got %q", got)
	}
	if got := msg.Header.Get("MIME-Version"); got != "1.0" {
		t.Errorf("MIME-Version: got %q", got)
	}

	mediaType, params, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("Content-Type parse: %v", err)
	}
	if mediaType != "multipart/alternative" {
		t.Errorf("media type: got %q want multipart/alternative", mediaType)
	}
	if params["boundary"] != "=_jf_FIXEDBOUNDARY" {
		t.Errorf("boundary: got %q", params["boundary"])
	}

	mr := multipart.NewReader(msg.Body, params["boundary"])
	parts := map[string]string{}
	for {
		p, err := mr.NextPart()
		if err != nil {
			break
		}
		buf := &bytes.Buffer{}
		if _, copyErr := buf.ReadFrom(p); copyErr != nil {
			t.Fatalf("read part: %v", copyErr)
		}
		ct := p.Header.Get("Content-Type")
		parts[strings.Split(ct, ";")[0]] = buf.String()
	}
	if _, ok := parts["text/plain"]; !ok {
		t.Errorf("missing text/plain part; got %v", parts)
	}
	if _, ok := parts["text/html"]; !ok {
		t.Errorf("missing text/html part; got %v", parts)
	}
}

func TestAssembleMessageUsesCRLF(t *testing.T) {
	hdr := messageHeaders{
		From: "a@b.c", To: "d@e.f", Subject: "s",
		Date: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	out, err := assembleMessage(hdr, "<p>h</p>", "h\n", "=_jf_X", "<i@b.c>", "", nil)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if !bytes.Contains(out, []byte("\r\n")) {
		t.Fatalf("expected CRLF line endings in output")
	}
	for _, line := range bytes.Split(out, []byte("\r\n")) {
		if bytes.Contains(line, []byte("\n")) {
			t.Fatalf("found bare LF inside line: %q", line)
		}
	}
}

func TestRandomBoundaryShape(t *testing.T) {
	b, err := randomBoundary()
	if err != nil {
		t.Fatalf("randomBoundary: %v", err)
	}
	if !strings.HasPrefix(b, "=_jf_") {
		t.Errorf("boundary missing prefix: %q", b)
	}
	if len(b) != len("=_jf_")+16 {
		t.Errorf("boundary length: got %d want %d (%q)", len(b), len("=_jf_")+16, b)
	}
}

func TestRandomMessageIDShape(t *testing.T) {
	id, err := randomMessageID("example.com")
	if err != nil {
		t.Fatalf("randomMessageID: %v", err)
	}
	if !strings.HasPrefix(id, "<") || !strings.HasSuffix(id, "@example.com>") {
		t.Errorf("message-id shape unexpected: %q", id)
	}
}

func TestAssembleMessageStripsHeaderInjection(t *testing.T) {
	hdr := messageHeaders{
		From:    "alice@example.com",
		To:      "bob@example.com",
		Subject: "Report\r\nBcc: attacker@evil.com",
		Date:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	out, err := assembleMessage(hdr, "<p>h</p>", "h\n", "=_jf_X", "<i@b.c>", "", nil)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	msg, err := mail.ReadMessage(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if msg.Header.Get("Bcc") != "" {
		t.Fatalf("Bcc unexpectedly present after sanitisation: %q", msg.Header.Get("Bcc"))
	}
	if got := msg.Header.Get("Subject"); got != "ReportBcc: attacker@evil.com" {
		t.Fatalf("Subject after strip: got %q want %q", got, "ReportBcc: attacker@evil.com")
	}
}

func TestAssembleMessageWithLogoEmitsMultipartRelated(t *testing.T) {
	hdr := messageHeaders{
		From: "alice@example.com", To: "bob@example.com",
		Subject: "x", Date: time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC),
	}
	logo := &logoPart{
		Bytes: []byte("\x89PNG\r\n\x1a\n" + strings.Repeat("a", 100)), // not a real PNG, but assembleMessage doesn't decode
		Name:  "logo.png",
		CID:   "jf-logo",
	}
	out, err := assembleMessage(hdr,
		"<html>hi</html>", "hi plain",
		"=_jf_INNER", "<id@example.com>",
		"=_jfr_OUTER", logo,
	)
	if err != nil {
		t.Fatalf("assembleMessage: %v", err)
	}
	msg, err := mail.ReadMessage(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("parse: %v\nraw:\n%s", err, out)
	}
	mediaType, params, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("Content-Type parse: %v", err)
	}
	if mediaType != "multipart/related" {
		t.Fatalf("outer media type: got %q want multipart/related", mediaType)
	}
	if params["boundary"] != "=_jfr_OUTER" {
		t.Errorf("outer boundary: got %q", params["boundary"])
	}
	if params["type"] != "multipart/alternative" {
		t.Errorf("outer type param: got %q", params["type"])
	}

	mr := multipart.NewReader(msg.Body, params["boundary"])
	first, err := mr.NextPart()
	if err != nil {
		t.Fatalf("first part: %v", err)
	}
	if ct := first.Header.Get("Content-Type"); !strings.HasPrefix(ct, "multipart/alternative") {
		t.Errorf("first part type: got %q", ct)
	}
	second, err := mr.NextPart()
	if err != nil {
		t.Fatalf("second part: %v", err)
	}
	if ct := second.Header.Get("Content-Type"); ct != "image/png" {
		t.Errorf("second part type: got %q want image/png", ct)
	}
	if cid := second.Header.Get("Content-ID"); cid != "<jf-logo>" {
		t.Errorf("Content-ID: got %q want <jf-logo>", cid)
	}
	if cd := second.Header.Get("Content-Disposition"); !strings.Contains(cd, "inline") || !strings.Contains(cd, "logo.png") {
		t.Errorf("Content-Disposition: got %q", cd)
	}
	if cte := second.Header.Get("Content-Transfer-Encoding"); cte != "base64" {
		t.Errorf("Content-Transfer-Encoding: got %q want base64", cte)
	}
}

func TestAssembleMessageNoLogoEmitsMultipartAlternative(t *testing.T) {
	hdr := messageHeaders{
		From: "a@example.com", To: "b@example.com",
		Subject: "x", Date: time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
	}
	out, err := assembleMessage(hdr, "<html>x</html>", "x",
		"=_jf_FIXED", "<id@example.com>",
		"", nil,
	)
	if err != nil {
		t.Fatalf("assembleMessage: %v", err)
	}
	msg, err := mail.ReadMessage(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("parse: %v\nraw:\n%s", err, out)
	}
	mediaType, _, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("Content-Type parse: %v", err)
	}
	if mediaType != "multipart/alternative" {
		t.Errorf("no-logo path: got %q want multipart/alternative", mediaType)
	}
}
