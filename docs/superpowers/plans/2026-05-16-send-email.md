# `--send-email` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `--send-email` flag to `jellyfish vulns summary` and `jellyfish user show` that renders the existing email output and sends it via the Gmail API, using a Workspace service account with domain-wide delegation (DWD) stored in the macOS Keychain.

**Architecture:** A new `internal/gmail` package wraps the Google Gmail API SDK with a small `Sender` interface and error sentinels. The Keychain package grows three thin wrappers under a new account name (`gmail_default`). `jellyfish configure email` is extended with a third prompt that ingests a service-account JSON file from disk into the Keychain. The `cmd` layer adds one boolean flag, one `resolveSendOptions` helper, and a small "buffer the .eml, call Send, print confirmation to stderr" branch in `runUserShow` / `runVulnsSummary`. All long-running surfaces (Keychain reads, Gmail send) are injected via function fields on the existing opts structs, mirroring how `configureOpts.StoreToken` already works.

**Tech Stack:** Go 1.25, `github.com/spf13/cobra`, `google.golang.org/api/gmail/v1`, `google.golang.org/api/option`, `google.golang.org/api/googleapi`, `golang.org/x/oauth2/google`, macOS Keychain via existing `internal/keychain`.

**Spec:** `docs/superpowers/specs/2026-05-16-send-email-design.md`

---

## File map

**Create:**
- `internal/gmail/gmail.go` — Sender interface, NewSender, error sentinels, classifyAPIError, ValidateServiceAccountJSON
- `internal/gmail/gmail_test.go` — unit tests (no live calls)
- `internal/gmail/integration_test.go` — env-gated live-send test
- `internal/keychain/keychain_gmail.go` — `SetGmailServiceAccount` / `GetGmailServiceAccount` / `DeleteGmailServiceAccount`
- `cmd/send_email.go` — `gmailNewSender` type, `resolveSendOptions` helper, `runSendUserShow` and `runSendVulnsSummary` helpers
- `cmd/send_email_test.go` — `resolveSendOptions` table tests + integration of the send path in `runUserShow` / `runVulnsSummary`

**Modify:**
- `internal/config/config.go` — add `GmailConfigured bool` to `EmailConfig`
- `internal/keychain/keychain_darwin_test.go` — extend round-trip with Gmail account
- `cmd/email.go` — add `Send bool` to `emailFlagValues`, extend `readEmailFlags`
- `cmd/configure.go` — extend `configureEmailOpts` and `runConfigureEmail` with the third prompt
- `cmd/configure_test.go` — extend with Gmail-prompt cases
- `cmd/user.go` — add `--send-email` flag, wire `Send` + DI seams into `userShowOpts`, route to send path in `runUserShow`
- `cmd/vulns.go` — same as above for `vulnsSummaryOpts` / `runVulnsSummary`
- `cmd/root.go` — extend `classifyError` with Gmail sentinels
- `go.mod`, `go.sum` — `go get` the three new Gmail-related modules
- `README.md` — replace the "future `--send-email` flag" note with real docs; add Keychain account note

---

## Task 1: Add `GmailConfigured` to `EmailConfig`

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/config/config_test.go` (existing test file already imports `testing` and `path/filepath`):

```go
func TestSaveLoadPreservesGmailConfigured(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	in := File{"default": Profile{
		Subdomain: "acme",
		Region:    "us",
		BaseURL:   "https://acme.api.kandji.io/api/v1",
		Email:     EmailConfig{From: "alice@example.com", GmailConfigured: true},
	}}
	if err := Save(path, in); err != nil {
		t.Fatalf("save: %v", err)
	}
	out, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !out["default"].Email.GmailConfigured {
		t.Errorf("GmailConfigured did not round-trip; got %+v", out["default"].Email)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/... -run TestSaveLoadPreservesGmailConfigured -count=1`
Expected: compile error — `GmailConfigured` not a field on `EmailConfig`.

- [ ] **Step 3: Add the field**

Edit `internal/config/config.go`. Replace the `EmailConfig` struct definition with:

```go
type EmailConfig struct {
	From             string `yaml:"from,omitempty"`
	DefaultTo        string `yaml:"default_to,omitempty"`
	SubjectTemplate  string `yaml:"subject_template,omitempty"`
	CVELinkPrimary   string `yaml:"cve_link_primary,omitempty"`
	CVELinkSecondary string `yaml:"cve_link_secondary,omitempty"`
	GmailConfigured  bool   `yaml:"gmail_configured,omitempty"`
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/config/... -count=1`
Expected: PASS (all existing tests still green, plus the new one).

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go
git commit -m "feat(config): added gmail_configured flag to EmailConfig"
```

---

## Task 2: Keychain helpers for the Gmail service account

**Files:**
- Create: `internal/keychain/keychain_gmail.go`
- Modify: `internal/keychain/keychain_darwin_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/keychain/keychain_darwin_test.go`:

```go
func TestGmailServiceAccountRoundTrip(t *testing.T) {
	skipIfNoKeychain(t)
	t.Cleanup(func() { _ = DeleteGmailServiceAccount() })

	want := []byte(`{"type":"service_account","client_email":"x@y.iam.gserviceaccount.com"}`)
	if err := SetGmailServiceAccount(want); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, err := GetGmailServiceAccount()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("round-trip mismatch:\n got: %s\nwant: %s", got, want)
	}

	if err := DeleteGmailServiceAccount(); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := GetGmailServiceAccount(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete: want ErrNotFound, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JELLYFISH_KEYCHAIN_TESTS=1 go test ./internal/keychain/... -run TestGmailServiceAccountRoundTrip -count=1`
Expected: compile error — the three helpers don't exist.

(Without the env var the test is skipped, which still surfaces the compile error in the test file.)

- [ ] **Step 3: Create the helpers**

Create `internal/keychain/keychain_gmail.go`:

```go
package keychain

// accountGmailServiceAccount is the Keychain account name under the existing
// jellyfish service that stores the Gmail service-account JSON.
const accountGmailServiceAccount = "gmail_default"

// SetGmailServiceAccount writes (or replaces) the service-account JSON blob.
func SetGmailServiceAccount(jsonBytes []byte) error {
	return Set(accountGmailServiceAccount, string(jsonBytes))
}

// GetGmailServiceAccount returns the stored service-account JSON, or
// ErrNotFound if `jellyfish configure email` was never run.
func GetGmailServiceAccount() ([]byte, error) {
	s, err := Get(accountGmailServiceAccount)
	if err != nil {
		return nil, err
	}
	return []byte(s), nil
}

// DeleteGmailServiceAccount removes the stored credential. Nil if absent.
func DeleteGmailServiceAccount() error {
	return Delete(accountGmailServiceAccount)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `JELLYFISH_KEYCHAIN_TESTS=1 go test ./internal/keychain/... -count=1`
Expected: PASS (the macOS prompt may appear once on first run; approve and re-run if so).

Run without the env var: `go test ./internal/keychain/... -count=1`
Expected: PASS, with the integration test SKIPped.

- [ ] **Step 5: Commit**

```bash
git add internal/keychain/keychain_gmail.go internal/keychain/keychain_darwin_test.go
git commit -m "feat(keychain): added Gmail service-account helpers"
```

---

## Task 3: Add Google Gmail API SDK dependencies

**Files:**
- Modify: `go.mod`
- Modify: `go.sum`

- [ ] **Step 1: Pull the modules**

Run:

```bash
go get google.golang.org/api/gmail/v1
go get google.golang.org/api/option
go get google.golang.org/api/googleapi
go get golang.org/x/oauth2/google
go mod tidy
```

Expected: `go.mod` gains `google.golang.org/api` and `golang.org/x/oauth2` (plus transitive entries); `go.sum` updated.

- [ ] **Step 2: Verify build still passes**

Run: `go build ./...`
Expected: success, no compile errors.

Run: `go test ./...`
Expected: existing tests still PASS (we have not yet imported anything from the new modules).

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "build: added Google Gmail API SDK dependencies"
```

---

## Task 4: `internal/gmail` package — Sender + error sentinels + validators

**Files:**
- Create: `internal/gmail/gmail.go`
- Create: `internal/gmail/gmail_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/gmail/gmail_test.go`:

```go
package gmail

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"google.golang.org/api/googleapi"
)

// validServiceAccountJSON returns a minimal but well-formed service-account
// JSON sufficient for google.JWTConfigFromJSON to parse. The private key is
// throwaway; unit tests never invoke the TokenSource (which is what would
// actually parse and use the key).
func validServiceAccountJSON() []byte {
	return []byte(`{
		"type": "service_account",
		"project_id": "test",
		"private_key_id": "k1",
		"private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC=\n-----END PRIVATE KEY-----\n",
		"client_email": "tester@test.iam.gserviceaccount.com",
		"client_id": "1234567890",
		"auth_uri": "https://accounts.google.com/o/oauth2/auth",
		"token_uri": "https://oauth2.googleapis.com/token"
	}`)
}

func TestNewSenderRejectsMalformedJSON(t *testing.T) {
	_, err := NewSender(context.Background(), []byte("not json"), "alice@example.com")
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func TestNewSenderRejectsEmptySubject(t *testing.T) {
	_, err := NewSender(context.Background(), validServiceAccountJSON(), "")
	if err == nil {
		t.Fatal("expected error for empty subjectUser")
	}
}

func TestNewSenderAcceptsValidJSON(t *testing.T) {
	s, err := NewSender(context.Background(), validServiceAccountJSON(), "alice@example.com")
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	if s == nil {
		t.Fatal("Sender was nil")
	}
}

func TestValidateServiceAccountJSONOK(t *testing.T) {
	if err := ValidateServiceAccountJSON(validServiceAccountJSON()); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestValidateServiceAccountJSONRejectsMalformed(t *testing.T) {
	if err := ValidateServiceAccountJSON([]byte("not json")); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateServiceAccountJSONRejectsWrongType(t *testing.T) {
	j := []byte(`{"type":"authorized_user","client_email":"x@y"}`)
	err := ValidateServiceAccountJSON(j)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "service_account") {
		t.Errorf("error should mention service_account; got %v", err)
	}
}

func TestValidateServiceAccountJSONRejectsMissingClientEmail(t *testing.T) {
	j := []byte(`{"type":"service_account"}`)
	err := ValidateServiceAccountJSON(j)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "client_email") {
		t.Errorf("error should mention client_email; got %v", err)
	}
}

func TestClassifyAPIErrorMaps401(t *testing.T) {
	err := classifyAPIError(&googleapi.Error{Code: 401, Message: "bad creds"})
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized; got %v", err)
	}
}

func TestClassifyAPIErrorMaps403(t *testing.T) {
	err := classifyAPIError(&googleapi.Error{Code: 403})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected ErrForbidden; got %v", err)
	}
}

func TestClassifyAPIErrorMaps429(t *testing.T) {
	err := classifyAPIError(&googleapi.Error{Code: 429})
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited; got %v", err)
	}
}

func TestClassifyAPIErrorMaps5xx(t *testing.T) {
	err := classifyAPIError(&googleapi.Error{Code: 503})
	if !errors.Is(err, ErrUpstream) {
		t.Fatalf("expected ErrUpstream; got %v", err)
	}
}

func TestClassifyAPIErrorPreservesGoogleapiInChain(t *testing.T) {
	orig := &googleapi.Error{Code: 401, Message: "go away"}
	err := classifyAPIError(orig)
	var ge *googleapi.Error
	if !errors.As(err, &ge) {
		t.Fatalf("googleapi.Error lost from chain: %v", err)
	}
	if ge.Code != 401 || !strings.Contains(ge.Message, "go away") {
		t.Errorf("wrong googleapi.Error: %+v", ge)
	}
}

func TestClassifyAPIErrorPassesNonGoogleapiThrough(t *testing.T) {
	orig := errors.New("network down")
	got := classifyAPIError(orig)
	if !errors.Is(got, orig) {
		t.Errorf("expected pass-through; got %v", got)
	}
}

func TestRawEncodingIsURLSafeNoPadding(t *testing.T) {
	// All 256 byte values round-trip via the encoding NewSender uses for Raw.
	var src [256]byte
	for i := range src {
		src[i] = byte(i)
	}
	enc := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(src[:])
	if strings.ContainsAny(enc, "+/=") {
		t.Errorf("encoding contains non-url-safe chars: %q", enc)
	}
	dec, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(dec) != string(src[:]) {
		t.Fatal("round-trip mismatch")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/gmail/... -count=1`
Expected: compile error — `internal/gmail/gmail.go` does not exist.

- [ ] **Step 3: Create the implementation**

Create `internal/gmail/gmail.go`:

```go
package gmail

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"golang.org/x/oauth2/google"
	gmailapi "google.golang.org/api/gmail/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// Error sentinels for cmd.classifyError integration.
var (
	ErrUnauthorized = errors.New("gmail: unauthorized")
	ErrForbidden    = errors.New("gmail: forbidden")
	ErrRateLimited  = errors.New("gmail: rate limited")
	ErrUpstream     = errors.New("gmail: upstream")
)

// Sender sends a pre-built RFC 5322 message via Gmail API users.messages.send,
// using the service account in saJSON to impersonate subjectUser via DWD.
type Sender interface {
	Send(ctx context.Context, rfc822 []byte) (messageID string, err error)
}

type apiSender struct {
	svc *gmailapi.Service
}

// NewSender returns a Sender. saJSON is the raw service-account JSON read from
// Keychain. subjectUser is the Workspace user to impersonate (the "From"
// address). The OAuth scope is hard-coded to gmail.send.
func NewSender(ctx context.Context, saJSON []byte, subjectUser string) (Sender, error) {
	if subjectUser == "" {
		return nil, errors.New("gmail: subjectUser must not be empty")
	}
	cfg, err := google.JWTConfigFromJSON(saJSON, gmailapi.GmailSendScope)
	if err != nil {
		return nil, fmt.Errorf("gmail: parse service-account JSON: %w", err)
	}
	if cfg.Email == "" {
		return nil, errors.New("gmail: service-account JSON has no client_email")
	}
	cfg.Subject = subjectUser
	svc, err := gmailapi.NewService(ctx, option.WithTokenSource(cfg.TokenSource(ctx)))
	if err != nil {
		return nil, fmt.Errorf("gmail: build service: %w", err)
	}
	return &apiSender{svc: svc}, nil
}

func (s *apiSender) Send(ctx context.Context, rfc822 []byte) (string, error) {
	raw := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(rfc822)
	msg := &gmailapi.Message{Raw: raw}
	sent, err := s.svc.Users.Messages.Send("me", msg).Context(ctx).Do()
	if err != nil {
		return "", classifyAPIError(err)
	}
	return sent.Id, nil
}

// ValidateServiceAccountJSON checks structure-level invariants on a candidate
// service-account JSON blob: parseable JSON, type=="service_account",
// non-empty client_email. Used at configure time so bad input is rejected
// before it hits Keychain.
func ValidateServiceAccountJSON(b []byte) error {
	var probe struct {
		Type        string `json:"type"`
		ClientEmail string `json:"client_email"`
	}
	if err := json.Unmarshal(b, &probe); err != nil {
		return fmt.Errorf("not valid JSON: %w", err)
	}
	if probe.Type != "service_account" {
		return fmt.Errorf(`type is %q, want "service_account"`, probe.Type)
	}
	if probe.ClientEmail == "" {
		return errors.New("missing client_email field")
	}
	return nil
}

// classifyAPIError wraps a Gmail API error with one of the package sentinels
// based on the HTTP status code. Non-googleapi errors pass through unchanged.
func classifyAPIError(err error) error {
	var ge *googleapi.Error
	if !errors.As(err, &ge) {
		return err
	}
	switch {
	case ge.Code == 401:
		return fmt.Errorf("%w: %v", ErrUnauthorized, ge)
	case ge.Code == 403:
		return fmt.Errorf("%w: %v", ErrForbidden, ge)
	case ge.Code == 429:
		return fmt.Errorf("%w: %v", ErrRateLimited, ge)
	case ge.Code >= 500:
		return fmt.Errorf("%w: %v", ErrUpstream, ge)
	default:
		return err
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/gmail/... -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/gmail/gmail.go internal/gmail/gmail_test.go
git commit -m "feat(gmail): added Sender, error sentinels, and JSON validator"
```

---

## Task 5: `--send-email` flag on `emailFlagValues` + `readEmailFlags`

**Files:**
- Modify: `cmd/email.go`

- [ ] **Step 1: Update the type and reader**

Edit `cmd/email.go`. Replace the existing `emailFlagValues` struct and `readEmailFlags` function with:

```go
// emailFlagValues holds the literal flag inputs from cobra; empty string means
// the flag was not set. Send is true iff --send-email was passed.
type emailFlagValues struct {
	To      string
	From    string
	Subject string
	Send    bool
}

// readEmailFlags pulls --email-to / --email-from / --email-subject / --send-email
// off a cobra command. Missing flags return zero values (no error).
func readEmailFlags(cmd *cobra.Command) emailFlagValues {
	to, _ := cmd.Flags().GetString("email-to")
	from, _ := cmd.Flags().GetString("email-from")
	subject, _ := cmd.Flags().GetString("email-subject")
	send, _ := cmd.Flags().GetBool("send-email")
	return emailFlagValues{To: to, From: from, Subject: subject, Send: send}
}
```

- [ ] **Step 2: Verify the package still compiles**

Run: `go build ./...`
Expected: success. The new `Send` field is read everywhere `readEmailFlags` is called; no other call sites need changes yet. The cobra flag itself will be declared in Tasks 7 and 8 — `GetBool` on an unknown flag returns `false, nil`, which is the right default.

- [ ] **Step 3: Verify existing tests still pass**

Run: `go test ./cmd/... -count=1`
Expected: PASS (no behavioural change yet — `Send` is always false).

- [ ] **Step 4: Commit**

```bash
git add cmd/email.go
git commit -m "feat(cmd): added Send field to emailFlagValues"
```

---

## Task 6: `resolveSendOptions` helper

**Files:**
- Create: `cmd/send_email.go`
- Create: `cmd/send_email_test.go`

- [ ] **Step 1: Write the failing tests**

Create `cmd/send_email_test.go`:

```go
package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/email"
	"flipgroup.com.au/jellyfish/internal/gmail"
)

// fakeGmailSender captures the bytes passed to Send so cmd tests can assert
// on the rendered .eml without calling Google.
type fakeGmailSender struct {
	sent     []byte
	err      error
	returnID string
}

func (f *fakeGmailSender) Send(_ context.Context, raw []byte) (string, error) {
	f.sent = append([]byte(nil), raw...)
	if f.err != nil {
		return "", f.err
	}
	if f.returnID == "" {
		return "msg-fake", nil
	}
	return f.returnID, nil
}

func newFakeSenderFactory(s *fakeGmailSender) gmailNewSender {
	return func(_ context.Context, _ []byte, _ string) (gmail.Sender, error) {
		return s, nil
	}
}

func stubKeychain(blob string) func() ([]byte, error) {
	return func() ([]byte, error) { return []byte(blob), nil }
}

func TestResolveSendOptionsErrorsWhenGmailNotConfigured(t *testing.T) {
	_, _, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: "ops@example.com"},
		"",
		config.Profile{Email: config.EmailConfig{GmailConfigured: false}},
		"",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "configure email") {
		t.Errorf("error should mention 'configure email'; got %v", err)
	}
}

func TestResolveSendOptionsErrorsOnExplicitNonEmailOutput(t *testing.T) {
	_, _, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: "ops@example.com"},
		"csv",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		"",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "csv") || !strings.Contains(err.Error(), "--send-email") {
		t.Errorf("error wording: got %v", err)
	}
}

func TestResolveSendOptionsAllowsExplicitEmailOutput(t *testing.T) {
	_, to, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: "ops@example.com"},
		"email",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		"",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if to != "ops@example.com" {
		t.Errorf("to: got %q", to)
	}
}

func TestResolveSendOptionsUsesUserDefaultToWhenEmpty(t *testing.T) {
	_, to, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: ""},
		"",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		"alice@example.com",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if to != "alice@example.com" {
		t.Errorf("expected user fallback; got %q", to)
	}
}

func TestResolveSendOptionsConfigDefaultToBeatsUserDefault(t *testing.T) {
	// email.Options.To is already populated from email.default_to by
	// resolveEmailOptions, so simulate that here: To non-empty + userDefaultTo
	// non-empty -> To wins.
	_, to, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: "secops@example.com"},
		"",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true, DefaultTo: "secops@example.com"}},
		"alice@example.com",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if to != "secops@example.com" {
		t.Errorf("expected default_to to win; got %q", to)
	}
}

func TestResolveSendOptionsErrorsWithNoRecipientAnywhere(t *testing.T) {
	_, _, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: ""},
		"",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		"",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "recipient") {
		t.Errorf("error wording: got %v", err)
	}
}

func TestResolveSendOptionsExplicitToWins(t *testing.T) {
	_, to, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: "flag@example.com"},
		"",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true, DefaultTo: "ignored@example.com"}},
		"alice@example.com",
		stubKeychain(`{}`),
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if to != "flag@example.com" {
		t.Errorf("expected explicit To to win; got %q", to)
	}
}

func TestResolveSendOptionsPropagatesKeychainError(t *testing.T) {
	bogus := errors.New("keychain locked")
	_, _, err := resolveSendOptions(
		context.Background(),
		email.Options{From: "alice@example.com", To: "ops@example.com"},
		"",
		config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		"",
		func() ([]byte, error) { return nil, bogus },
		newFakeSenderFactory(&fakeGmailSender{}),
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, bogus) {
		t.Errorf("expected wrapped keychain error; got %v", err)
	}
}

// Sanity that the test helpers compile.
var _ = bytes.Buffer{}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/... -run TestResolveSendOptions -count=1`
Expected: compile error — `resolveSendOptions`, `gmailNewSender`, `fakeGmailSender` not defined.

- [ ] **Step 3: Create the helper**

Create `cmd/send_email.go`:

```go
package cmd

import (
	"context"
	"errors"
	"fmt"

	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/email"
	"flipgroup.com.au/jellyfish/internal/gmail"
)

// gmailNewSender mirrors gmail.NewSender's signature so tests can inject a
// fake without dragging real Google credentials through the cmd layer.
type gmailNewSender func(ctx context.Context, saJSON []byte, subject string) (gmail.Sender, error)

// resolveSendOptions runs after resolveEmailOptions when --send-email is set.
// Returns the constructed Sender and the final recipient address per the
// precedence rules:
//
//	--email-to (already folded into eo.To)
//	email.default_to (already folded into eo.To)
//	userDefaultTo  (only non-empty for `user show`)
//	-> error
//
// User-facing exit-1 errors are returned for:
//   - profile.Email.GmailConfigured is false
//   - explicitOutput is non-empty and not "email"
//   - no recipient can be resolved
func resolveSendOptions(
	ctx context.Context,
	eo email.Options,
	explicitOutput string,
	profile config.Profile,
	userDefaultTo string,
	keychainGet func() ([]byte, error),
	newSender gmailNewSender,
) (gmail.Sender, string, error) {
	if explicitOutput != "" && explicitOutput != "email" {
		return nil, "", fmt.Errorf("--send-email implies email output; remove -o %s or set -o email", explicitOutput)
	}
	if !profile.Email.GmailConfigured {
		return nil, "", errors.New(`--send-email requires Gmail credentials. Run "jellyfish configure email" to install a service-account JSON`)
	}

	to := eo.To
	if to == "" {
		to = userDefaultTo
	}
	if to == "" {
		return nil, "", errors.New(`--send-email requires a recipient: pass --email-to, set email.default_to in config, or (for user show) target a user with an email address`)
	}

	saJSON, err := keychainGet()
	if err != nil {
		return nil, "", fmt.Errorf(`read Gmail credentials from Keychain: %w. Run "jellyfish configure email" to reinstall`, err)
	}

	sender, err := newSender(ctx, saJSON, eo.From)
	if err != nil {
		return nil, "", err
	}
	return sender, to, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/... -run TestResolveSendOptions -count=1`
Expected: PASS.

Run: `go test ./cmd/... -count=1`
Expected: PASS (existing tests still green).

- [ ] **Step 5: Commit**

```bash
git add cmd/send_email.go cmd/send_email_test.go
git commit -m "feat(cmd): added resolveSendOptions helper for --send-email"
```

---

## Task 7: Wire `--send-email` into `user show`

**Files:**
- Modify: `cmd/user.go`
- Modify: `cmd/user_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `cmd/user_test.go`. The existing imports already cover `bytes`, `context`, `errors`, `io`, `net/mail`, `strings`, `testing`, `time`, and the `iru` package. Add `config` and `gmail`:

```go
import (
	// existing imports unchanged; add:
	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/gmail"
)

func TestUserShowSendEmailDefaultsToUserEmail(t *testing.T) {
	client := &fakeClient{
		users:   []iru.User{{ID: "u-1", Name: "Alice", Email: "alice@example.com"}},
		devices: []iru.Device{{DeviceID: "d-1", DeviceName: "MBP", SerialNumber: "SN1"}},
		detections: []iru.Detection{
			{DeviceID: "d-1", CVEID: "CVE-A", Severity: "Critical", CVSSScore: 9.5, Name: "x", Version: "1.0"},
		},
	}
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	sender := &fakeGmailSender{returnID: "msg-xyz"}
	opts := userShowOpts{
		Identifier: "u-1",
		NoCache:    true,
		EmailFlags: emailFlagValues{Send: true, From: "ops@flipgroup.com.au"},
		EmailNow:   time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		Profile:    config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		KeychainGet: func() ([]byte, error) { return []byte(`{"type":"service_account"}`), nil },
		NewSender:   func(_ context.Context, _ []byte, _ string) (gmail.Sender, error) { return sender, nil },
	}
	if err := runUserShow(context.Background(), client, stdout, stderr, opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	if stdout.Len() > 0 {
		t.Errorf("stdout should be empty when --send-email; got %q", stdout.String())
	}
	want := "sent: to=alice@example.com from=ops@flipgroup.com.au gmail-id=msg-xyz"
	if !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr confirmation:\n got %q\nwant substring %q", stderr.String(), want)
	}
	if sender.sent == nil {
		t.Fatal("sender was not called")
	}
	msg, err := mail.ReadMessage(bytes.NewReader(sender.sent))
	if err != nil {
		t.Fatalf("parse sent eml: %v\nraw:\n%s", err, sender.sent)
	}
	if got := msg.Header.Get("To"); got != "alice@example.com" {
		t.Errorf("To: got %q", got)
	}
}

func TestUserShowSendEmailExplicitToOverridesUser(t *testing.T) {
	client := &fakeClient{
		users:   []iru.User{{ID: "u-1", Name: "Alice", Email: "alice@example.com"}},
		devices: []iru.Device{{DeviceID: "d-1", DeviceName: "MBP", SerialNumber: "SN1"}},
		detections: []iru.Detection{
			{DeviceID: "d-1", CVEID: "CVE-A", Severity: "Critical", CVSSScore: 9.5, Name: "x", Version: "1.0"},
		},
	}
	stderr := &bytes.Buffer{}
	sender := &fakeGmailSender{}
	opts := userShowOpts{
		Identifier: "u-1",
		NoCache:    true,
		EmailFlags: emailFlagValues{Send: true, From: "ops@flipgroup.com.au", To: "other@example.com"},
		EmailNow:   time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		Profile:    config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		KeychainGet: func() ([]byte, error) { return []byte(`{"type":"service_account"}`), nil },
		NewSender:   func(_ context.Context, _ []byte, _ string) (gmail.Sender, error) { return sender, nil },
	}
	if err := runUserShow(context.Background(), client, &bytes.Buffer{}, stderr, opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(stderr.String(), "to=other@example.com") {
		t.Errorf("expected explicit To to win; stderr=%q", stderr.String())
	}
}

func TestUserShowSendEmailPropagatesSenderError(t *testing.T) {
	client := &fakeClient{
		users:   []iru.User{{ID: "u-1", Name: "Alice", Email: "alice@example.com"}},
		devices: []iru.Device{{DeviceID: "d-1", DeviceName: "MBP", SerialNumber: "SN1"}},
	}
	sender := &fakeGmailSender{err: gmail.ErrUnauthorized}
	opts := userShowOpts{
		Identifier: "u-1",
		NoCache:    true,
		EmailFlags: emailFlagValues{Send: true, From: "ops@flipgroup.com.au"},
		EmailNow:   time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		Profile:    config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		KeychainGet: func() ([]byte, error) { return []byte(`{"type":"service_account"}`), nil },
		NewSender:   func(_ context.Context, _ []byte, _ string) (gmail.Sender, error) { return sender, nil },
	}
	err := runUserShow(context.Background(), client, &bytes.Buffer{}, &bytes.Buffer{}, opts)
	if !errors.Is(err, gmail.ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized propagated; got %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/... -run TestUserShowSendEmail -count=1`
Expected: compile error — `userShowOpts.KeychainGet` / `.NewSender` not defined.

- [ ] **Step 3: Update `userShowOpts` and `runUserShow` plumbing**

Edit `cmd/user.go`. Replace the `userShowOpts` struct with:

```go
type userShowOpts struct {
	Identifier  string
	Output      string
	NoCache     bool
	EmailFlags  emailFlagValues
	EmailNow    time.Time
	Profile     config.Profile
	gitEmail    gitEmailLookup
	ExplicitOutput string
	KeychainGet func() ([]byte, error)
	NewSender   gmailNewSender
}
```

Then update `newUserShowCmd`'s `RunE` to register the new flag and wire production defaults. Replace the `RunE` body and the trailing flag declarations with:

```go
RunE: func(cmd *cobra.Command, args []string) error {
	outFmt, _ := cmd.Flags().GetString("output")
	client, err := buildClient(cmd)
	if err != nil {
		return err
	}
	opts.Identifier = args[0]
	opts.Output = outFmt
	opts.EmailFlags = readEmailFlags(cmd)
	if cmd.Flags().Changed("output") {
		opts.ExplicitOutput = outFmt
	}
	opts.EmailNow = time.Now()
	if outFmt == "email" || opts.EmailFlags.Send {
		prof, err := activeProfile(cmd)
		if err != nil {
			return err
		}
		opts.Profile = prof
	}
	if opts.KeychainGet == nil {
		opts.KeychainGet = keychain.GetGmailServiceAccount
	}
	if opts.NewSender == nil {
		opts.NewSender = gmail.NewSender
	}
	return runUserShow(cmd.Context(), client, cmd.OutOrStdout(), cmd.ErrOrStderr(), opts)
},
```

Add the new imports to `cmd/user.go`:

```go
import (
	// existing imports unchanged; add:
	"flipgroup.com.au/jellyfish/internal/gmail"
	"flipgroup.com.au/jellyfish/internal/keychain"
)
```

Add the new flag alongside the existing email flags inside `newUserShowCmd`:

```go
c.Flags().Bool("send-email", false, "Send the rendered email via Gmail (requires `jellyfish configure email` to be run first)")
```

Now wire the send path. Edit `runUserShow` in `cmd/user.go`. After the line that constructs `bundle`, but before `return renderUserBundle(...)`, insert:

```go
	if opts.EmailFlags.Send {
		return runSendUserShow(ctx, stderr, opts, bundle)
	}
```

Then add a new function below `renderUserBundle` (still inside `cmd/user.go`):

```go
func runSendUserShow(ctx context.Context, stderr io.Writer, opts userShowOpts, b UserBundle) error {
	now := opts.EmailNow
	if now.IsZero() {
		now = time.Now()
	}
	gitLookup := opts.gitEmail
	if gitLookup == nil {
		gitLookup = gitUserEmail
	}
	emailOpts, err := resolveEmailOptions(opts.EmailFlags, opts.Profile, gitLookup, now)
	if err != nil {
		return err
	}

	sender, to, err := resolveSendOptions(
		ctx,
		emailOpts,
		opts.ExplicitOutput,
		opts.Profile,
		b.User.Email,
		opts.KeychainGet,
		opts.NewSender,
	)
	if err != nil {
		return err
	}
	emailOpts.To = to

	var buf bytes.Buffer
	if err := email.NewUserShowRenderer(emailOpts).Render(&buf, bundleToEmailInput(b)); err != nil {
		return err
	}

	id, err := sender.Send(ctx, buf.Bytes())
	if err != nil {
		return err
	}
	fmt.Fprintf(stderr, "sent: to=%s from=%s gmail-id=%s\n", to, emailOpts.From, id)
	return nil
}
```

Add the missing `bytes` import to `cmd/user.go` (the existing import block lacks it):

```go
import (
	"bytes"
	// existing imports unchanged
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/... -count=1`
Expected: PASS (the three new tests plus all existing ones).

- [ ] **Step 5: Commit**

```bash
git add cmd/user.go cmd/user_test.go
git commit -m "feat(user): wired --send-email into user show"
```

---

## Task 8: Wire `--send-email` into `vulns summary`

**Files:**
- Modify: `cmd/vulns.go`
- Modify: `cmd/vulns_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `cmd/vulns_test.go`. Existing imports already cover what we need plus `time`. Add `config` and `gmail`:

```go
import (
	// existing imports unchanged; add:
	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/gmail"
)

func TestVulnsSummarySendEmailRequiresRecipient(t *testing.T) {
	client := &fakeClient{vulnerabilities: []iru.Vulnerability{{CVEID: "CVE-A", Severity: "Critical"}}}
	opts := vulnsSummaryOpts{
		EmailFlags: emailFlagValues{Send: true, From: "ops@flipgroup.com.au"},
		EmailNow:   time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		Profile:    config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		NoCache:    true,
		KeychainGet: func() ([]byte, error) { return []byte(`{"type":"service_account"}`), nil },
		NewSender:   func(_ context.Context, _ []byte, _ string) (gmail.Sender, error) { return &fakeGmailSender{}, nil },
	}
	err := runVulnsSummary(context.Background(), client, &bytes.Buffer{}, &bytes.Buffer{}, opts)
	if err == nil {
		t.Fatal("expected error for missing recipient")
	}
	if !strings.Contains(err.Error(), "recipient") {
		t.Errorf("error wording: got %v", err)
	}
}

func TestVulnsSummarySendEmailSends(t *testing.T) {
	client := &fakeClient{vulnerabilities: []iru.Vulnerability{
		{CVEID: "CVE-A", Severity: "Critical", CVSSScore: 9.5, KEVScore: 1, DeviceCount: 2, Status: "Active", Software: []string{"foo"}},
	}}
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	sender := &fakeGmailSender{returnID: "msg-abc"}
	opts := vulnsSummaryOpts{
		EmailFlags: emailFlagValues{Send: true, From: "ops@flipgroup.com.au", To: "secops@example.com"},
		EmailNow:   time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		Profile:    config.Profile{Email: config.EmailConfig{GmailConfigured: true}},
		NoCache:    true,
		KeychainGet: func() ([]byte, error) { return []byte(`{"type":"service_account"}`), nil },
		NewSender:   func(_ context.Context, _ []byte, _ string) (gmail.Sender, error) { return sender, nil },
	}
	if err := runVulnsSummary(context.Background(), client, stdout, stderr, opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	if stdout.Len() > 0 {
		t.Errorf("stdout should be empty; got %q", stdout.String())
	}
	want := "sent: to=secops@example.com from=ops@flipgroup.com.au gmail-id=msg-abc"
	if !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr confirmation:\n got %q\nwant %q", stderr.String(), want)
	}
	if sender.sent == nil {
		t.Fatal("sender was not called")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/... -run TestVulnsSummarySendEmail -count=1`
Expected: compile error — `vulnsSummaryOpts.KeychainGet` / `.NewSender` not defined.

- [ ] **Step 3: Update `vulnsSummaryOpts` and `newVulnsSummaryCmd`**

Edit `cmd/vulns.go`. Replace `vulnsSummaryOpts` with:

```go
type vulnsSummaryOpts struct {
	Status         string
	Severity       string
	Sort           string
	Limit          int
	Output         string
	NoCache        bool
	EmailFlags     emailFlagValues
	EmailNow       time.Time
	Profile        config.Profile
	gitEmail       gitEmailLookup
	ExplicitOutput string
	KeychainGet    func() ([]byte, error)
	NewSender      gmailNewSender
}
```

Inside `newVulnsSummaryCmd`, update the `RunE` to register the flag and DI defaults. Replace the `RunE` body with:

```go
RunE: func(cmd *cobra.Command, _ []string) error {
	outFmt, _ := cmd.Flags().GetString("output")
	opts.Output = outFmt
	opts.EmailFlags = readEmailFlags(cmd)
	if cmd.Flags().Changed("output") {
		opts.ExplicitOutput = outFmt
	}
	opts.EmailNow = time.Now()
	if outFmt == "email" || opts.EmailFlags.Send {
		prof, err := activeProfile(cmd)
		if err != nil {
			return err
		}
		opts.Profile = prof
	}
	if opts.KeychainGet == nil {
		opts.KeychainGet = keychain.GetGmailServiceAccount
	}
	if opts.NewSender == nil {
		opts.NewSender = gmail.NewSender
	}
	client, err := buildClient(cmd)
	if err != nil {
		return err
	}
	return runVulnsSummary(cmd.Context(), client, cmd.OutOrStdout(), cmd.ErrOrStderr(), opts)
},
```

Add the new flag inside `newVulnsSummaryCmd` alongside the existing email flags:

```go
c.Flags().Bool("send-email", false, "Send the rendered email via Gmail (requires `jellyfish configure email` to be run first)")
```

Add the missing imports to `cmd/vulns.go`:

```go
import (
	"bytes"
	// existing imports unchanged; add:
	"flipgroup.com.au/jellyfish/internal/gmail"
	"flipgroup.com.au/jellyfish/internal/keychain"
)
```

Edit `runVulnsSummary` (in `cmd/vulns.go`). After the line that produces `filtered` and applies the limit, but before `return renderVulns(...)`, insert:

```go
	if opts.EmailFlags.Send {
		return runSendVulnsSummary(ctx, stderr, opts, filtered)
	}
```

Add the new function below `renderVulns`:

```go
func runSendVulnsSummary(ctx context.Context, stderr io.Writer, opts vulnsSummaryOpts, vs []iru.Vulnerability) error {
	now := opts.EmailNow
	if now.IsZero() {
		now = time.Now()
	}
	gitLookup := opts.gitEmail
	if gitLookup == nil {
		gitLookup = gitUserEmail
	}
	emailOpts, err := resolveEmailOptions(opts.EmailFlags, opts.Profile, gitLookup, now)
	if err != nil {
		return err
	}

	sender, to, err := resolveSendOptions(
		ctx,
		emailOpts,
		opts.ExplicitOutput,
		opts.Profile,
		"",
		opts.KeychainGet,
		opts.NewSender,
	)
	if err != nil {
		return err
	}
	emailOpts.To = to

	var buf bytes.Buffer
	if err := email.NewVulnSummaryRenderer(emailOpts).Render(&buf, vs); err != nil {
		return err
	}

	id, err := sender.Send(ctx, buf.Bytes())
	if err != nil {
		return err
	}
	fmt.Fprintf(stderr, "sent: to=%s from=%s gmail-id=%s\n", to, emailOpts.From, id)
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/... -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/vulns.go cmd/vulns_test.go
git commit -m "feat(vulns): wired --send-email into vulns summary"
```

---

## Task 9: `classifyError` — exit-code mapping for Gmail errors

**Files:**
- Modify: `cmd/root.go`
- Modify: `cmd/exit_test.go`

- [ ] **Step 1: Write the failing test**

Append to `cmd/exit_test.go`:

```go
func TestClassifyErrorMapsGmailUnauthorizedToExit2(t *testing.T) {
	if got := classifyError(gmail.ErrUnauthorized); got != 2 {
		t.Errorf("got %d, want 2", got)
	}
}

func TestClassifyErrorMapsGmailForbiddenToExit2(t *testing.T) {
	if got := classifyError(gmail.ErrForbidden); got != 2 {
		t.Errorf("got %d, want 2", got)
	}
}

func TestClassifyErrorMapsGmailRateLimitedToExit4(t *testing.T) {
	if got := classifyError(gmail.ErrRateLimited); got != 4 {
		t.Errorf("got %d, want 4", got)
	}
}

func TestClassifyErrorMapsGmailUpstreamToExit4(t *testing.T) {
	if got := classifyError(gmail.ErrUpstream); got != 4 {
		t.Errorf("got %d, want 4", got)
	}
}
```

Add this import line at the top of `cmd/exit_test.go` if it isn't already present:

```go
import "flipgroup.com.au/jellyfish/internal/gmail"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/... -run TestClassifyErrorMapsGmail -count=1`
Expected: FAIL — `classifyError` doesn't recognise the new sentinels (everything maps to exit 1).

- [ ] **Step 3: Extend `classifyError`**

Edit `cmd/root.go`. Add the gmail import:

```go
import (
	// existing imports unchanged; add:
	"flipgroup.com.au/jellyfish/internal/gmail"
)
```

Add the two cases to the `switch` inside `classifyError`, before the trailing `apiErr` block:

```go
switch {
case errors.Is(err, iru.ErrUnauthorized), errors.Is(err, iru.ErrForbidden):
	return 2
case errors.Is(err, iru.ErrNotFound):
	return 3
case errors.Is(err, iru.ErrRateLimited):
	return 4
case errors.Is(err, gmail.ErrUnauthorized), errors.Is(err, gmail.ErrForbidden):
	return 2
case errors.Is(err, gmail.ErrRateLimited), errors.Is(err, gmail.ErrUpstream):
	return 4
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/... -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/root.go cmd/exit_test.go
git commit -m "feat(root): mapped Gmail error sentinels to exit codes"
```

---

## Task 10: Extend `jellyfish configure email` with the Gmail JSON prompt

**Files:**
- Modify: `cmd/configure.go`
- Modify: `cmd/configure_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `cmd/configure_test.go`. Existing imports cover what we need; add `os`:

```go
// gmailKeychainStubs captures Set/Delete calls so tests can assert what
// runConfigureEmail did to the Keychain.
type gmailKeychainStubs struct {
	stored  []byte
	deleted bool
	storeErr error
	delErr   error
}

func (g *gmailKeychainStubs) store(b []byte) error {
	if g.storeErr != nil { return g.storeErr }
	g.stored = append([]byte(nil), b...)
	return nil
}

func (g *gmailKeychainStubs) delete() error {
	if g.delErr != nil { return g.delErr }
	g.deleted = true
	return nil
}

// writeGmailJSON writes a syntactically valid service-account JSON to a temp
// file and returns the path. Used by tests that exercise the path-reading
// branch of the Gmail prompt.
func writeGmailJSON(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "sa.json")
	contents := `{"type":"service_account","client_email":"x@y.iam.gserviceaccount.com","private_key":"k"}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write sa.json: %v", err)
	}
	return path
}

func TestConfigureEmailGmailPromptStoresJSON(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := seedConfigFile(t, tmp, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
	}})
	jsonPath := writeGmailJSON(t, tmp)
	kc := &gmailKeychainStubs{}

	in := strings.NewReader("alice@example.com\nsecops@example.com\n" + jsonPath + "\n")
	out := &bytes.Buffer{}

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: in, Stdout: out, Stderr: out,
		StoreGmailJSON:  kc.store,
		DeleteGmailJSON: kc.delete,
	})
	if err != nil {
		t.Fatalf("runConfigureEmail: %v", err)
	}
	if string(kc.stored) == "" {
		t.Fatal("expected StoreGmailJSON to be called")
	}
	loaded, _ := config.Load(cfgPath)
	if !loaded["default"].Email.GmailConfigured {
		t.Errorf("GmailConfigured should be true after storing JSON")
	}
}

func TestConfigureEmailGmailPromptDashClears(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := seedConfigFile(t, tmp, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email:     config.EmailConfig{From: "alice@example.com", DefaultTo: "secops@example.com", GmailConfigured: true},
	}})
	kc := &gmailKeychainStubs{}

	in := strings.NewReader("\n\n-\n")
	out := &bytes.Buffer{}

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: in, Stdout: out, Stderr: out,
		StoreGmailJSON:  kc.store,
		DeleteGmailJSON: kc.delete,
	})
	if err != nil {
		t.Fatalf("runConfigureEmail: %v", err)
	}
	if !kc.deleted {
		t.Error("expected DeleteGmailJSON to be called")
	}
	loaded, _ := config.Load(cfgPath)
	if loaded["default"].Email.GmailConfigured {
		t.Errorf("GmailConfigured should be false after clearing")
	}
}

func TestConfigureEmailGmailPromptEnterKeepsExisting(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := seedConfigFile(t, tmp, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email:     config.EmailConfig{From: "alice@example.com", DefaultTo: "secops@example.com", GmailConfigured: true},
	}})
	kc := &gmailKeychainStubs{}

	in := strings.NewReader("\n\n\n")
	out := &bytes.Buffer{}

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: in, Stdout: out, Stderr: out,
		StoreGmailJSON:  kc.store,
		DeleteGmailJSON: kc.delete,
	})
	if err != nil {
		t.Fatalf("runConfigureEmail: %v", err)
	}
	if kc.stored != nil || kc.deleted {
		t.Errorf("Enter on Gmail prompt should not touch Keychain (stored=%v deleted=%v)", kc.stored, kc.deleted)
	}
	loaded, _ := config.Load(cfgPath)
	if !loaded["default"].Email.GmailConfigured {
		t.Errorf("GmailConfigured should remain true")
	}
}

func TestConfigureEmailGmailPromptRejectsMissingFile(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := seedConfigFile(t, tmp, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
	}})
	missing := filepath.Join(tmp, "does-not-exist.json")
	kc := &gmailKeychainStubs{}

	in := strings.NewReader("alice@example.com\nsecops@example.com\n" + missing + "\n")
	out := &bytes.Buffer{}

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: in, Stdout: out, Stderr: out,
		StoreGmailJSON:  kc.store,
		DeleteGmailJSON: kc.delete,
	})
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	if kc.stored != nil {
		t.Error("StoreGmailJSON should not have been called")
	}
	loaded, _ := config.Load(cfgPath)
	if loaded["default"].Email.GmailConfigured {
		t.Errorf("GmailConfigured should remain false on validation failure")
	}
}

func TestConfigureEmailGmailPromptRejectsWrongType(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := seedConfigFile(t, tmp, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
	}})
	badPath := filepath.Join(tmp, "user.json")
	if err := os.WriteFile(badPath, []byte(`{"type":"authorized_user","client_email":"x@y"}`), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	kc := &gmailKeychainStubs{}

	in := strings.NewReader("alice@example.com\nsecops@example.com\n" + badPath + "\n")
	out := &bytes.Buffer{}

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: in, Stdout: out, Stderr: out,
		StoreGmailJSON:  kc.store,
		DeleteGmailJSON: kc.delete,
	})
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
	if kc.stored != nil {
		t.Error("StoreGmailJSON should not have been called")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/... -run TestConfigureEmailGmailPrompt -count=1`
Expected: compile error — `configureEmailOpts.StoreGmailJSON` / `.DeleteGmailJSON` not defined.

- [ ] **Step 3: Extend `configureEmailOpts` and `runConfigureEmail`**

Edit `cmd/configure.go`. Replace `configureEmailOpts` with:

```go
type configureEmailOpts struct {
	ConfigPath      string
	Stdin           io.Reader
	Stdout          io.Writer
	Stderr          io.Writer
	StoreGmailJSON  func(jsonBytes []byte) error
	DeleteGmailJSON func() error
}
```

Update `newConfigureEmailCmd` to wire production defaults:

```go
func newConfigureEmailCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "email",
		Short: "Interactively configure email output defaults (From, default To, Gmail credentials)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfgPath, err := cmd.Flags().GetString("config")
			if err != nil {
				return err
			}
			if cfgPath == "" {
				cfgPath, err = config.DefaultPath()
				if err != nil {
					return err
				}
			}
			return runConfigureEmail(cmd.Context(), configureEmailOpts{
				ConfigPath:      cfgPath,
				Stdin:           cmd.InOrStdin(),
				Stdout:          cmd.OutOrStdout(),
				Stderr:          cmd.ErrOrStderr(),
				StoreGmailJSON:  keychain.SetGmailServiceAccount,
				DeleteGmailJSON: keychain.DeleteGmailServiceAccount,
			})
		},
	}
}
```

Add the `gmail` import to `cmd/configure.go`:

```go
import (
	// existing imports unchanged; add:
	"flipgroup.com.au/jellyfish/internal/gmail"
)
```

Extend `runConfigureEmail`. Replace the function body with:

```go
func runConfigureEmail(ctx context.Context, o configureEmailOpts) error {
	_ = ctx

	file, err := config.Load(o.ConfigPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf(`no config found at %s - run "jellyfish configure" first to set up tenant + token`, o.ConfigPath)
		}
		return fmt.Errorf("read config: %w", err)
	}
	prof, ok := file["default"]
	if !ok {
		return errors.New(`no "default" profile yet - run "jellyfish configure" first to set up tenant + token`)
	}

	r := bufio.NewReader(o.Stdin)

	from, err := promptValidated(o.Stdout, o.Stderr, r, "Email From", prof.Email.From, false)
	if err != nil {
		return err
	}
	defaultTo, err := promptValidated(o.Stdout, o.Stderr, r, "Email default To", prof.Email.DefaultTo, true)
	if err != nil {
		return err
	}

	prof.Email.From = from
	prof.Email.DefaultTo = defaultTo

	if err := promptGmailJSON(o, r, &prof); err != nil {
		return err
	}

	file["default"] = prof

	if err := config.Save(o.ConfigPath, file); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(o.Stdout, "Email config saved to %s\n", o.ConfigPath)
	return nil
}

const gmailPromptPlaceholderConfigured = "configured"

// promptGmailJSON drives the third prompt:
//   - "" (after Enter on "[configured]") -> keep
//   - "-" -> clear (DeleteGmailJSON + GmailConfigured=false)
//   - any other -> read file, ValidateServiceAccountJSON, StoreGmailJSON,
//     GmailConfigured=true
//
// Validation failures error out without touching Keychain or config.
func promptGmailJSON(o configureEmailOpts, r *bufio.Reader, prof *config.Profile) error {
	current := ""
	if prof.Email.GmailConfigured {
		current = gmailPromptPlaceholderConfigured
	}
	line, err := promptWithDefault(o.Stdout, r, "Gmail service-account JSON path", current)
	if err != nil {
		return err
	}
	switch {
	case line == gmailPromptPlaceholderConfigured:
		// User accepted the existing configured value; no change.
		return nil
	case line == "":
		// User typed "-" (promptWithDefault collapses dash to empty).
		if !prof.Email.GmailConfigured {
			return nil
		}
		if o.DeleteGmailJSON != nil {
			if err := o.DeleteGmailJSON(); err != nil {
				return fmt.Errorf("delete Gmail credentials from Keychain: %w", err)
			}
		}
		prof.Email.GmailConfigured = false
		return nil
	default:
		// Treat as a filesystem path.
		// #nosec G304 - path is the operator's own input at configure time
		bytes, err := os.ReadFile(line)
		if err != nil {
			return fmt.Errorf("read Gmail JSON %s: %w", line, err)
		}
		if err := gmail.ValidateServiceAccountJSON(bytes); err != nil {
			return fmt.Errorf("validate Gmail JSON %s: %w", line, err)
		}
		if o.StoreGmailJSON != nil {
			if err := o.StoreGmailJSON(bytes); err != nil {
				return fmt.Errorf("store Gmail JSON in Keychain: %w", err)
			}
		}
		prof.Email.GmailConfigured = true
		return nil
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/... -count=1`
Expected: PASS — the five new Gmail-prompt tests plus all existing configure tests (they pass `nil` for the new fields, which is now handled by the `if ... != nil` guards inside `promptGmailJSON`).

Wait — the existing configure tests don't pass `StoreGmailJSON` or `DeleteGmailJSON`, so the prompt reads from the test's `Stdin` and finds EOF after the two email prompts. Update the existing tests' input strings to include a third empty line so the Gmail prompt receives Enter.

Edit `cmd/configure_test.go`:

- `TestConfigureEmailPromptsAndSaves`: change `in := strings.NewReader("alice@example.com\nsecops@example.com\n")` to `in := strings.NewReader("alice@example.com\nsecops@example.com\n\n")`
- `TestConfigureEmailPreservesOtherEmailFields`: same change
- `TestConfigureEmailEnterKeepsExisting`: change `"\n\n"` to `"\n\n\n"`
- `TestConfigureEmailDashClearsField`: change `"-\n\n"` to `"-\n\n\n"`
- `TestConfigureEmailAllowsEmptyDefaultTo`: change `"alice@example.com\n\n"` to `"alice@example.com\n\n\n"`

Re-run:

Run: `go test ./cmd/... -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/configure.go cmd/configure_test.go
git commit -m "feat(configure): added Gmail JSON prompt to configure email"
```

---

## Task 11: `internal/gmail` integration test (env-gated, hard-coded recipient)

**Files:**
- Create: `internal/gmail/integration_test.go`

- [ ] **Step 1: Create the test**

Create `internal/gmail/integration_test.go`:

```go
package gmail

import (
	"context"
	"os"
	"testing"
	"time"
)

// integrationRecipient is the ONLY address this integration suite will ever
// send to. DO NOT make this configurable - it is an invariant. A developer
// running this suite should never be one typo away from emailing a real
// customer or teammate.
const integrationRecipient = "k@flip.com.au"

func TestIntegrationSend(t *testing.T) {
	if os.Getenv("JELLYFISH_GMAIL_TESTS") != "1" {
		t.Skip("set JELLYFISH_GMAIL_TESTS=1 to run the live Gmail send integration test")
	}
	jsonPath := os.Getenv("JELLYFISH_GMAIL_TEST_JSON")
	if jsonPath == "" {
		t.Fatal("JELLYFISH_GMAIL_TEST_JSON must be a path to a service-account JSON with DWD configured")
	}
	subject := os.Getenv("JELLYFISH_GMAIL_TEST_FROM")
	if subject == "" {
		t.Fatal("JELLYFISH_GMAIL_TEST_FROM must be the Workspace user the service account can impersonate")
	}

	saJSON, err := os.ReadFile(jsonPath) // #nosec G304 - test-only, operator-provided path
	if err != nil {
		t.Fatalf("read service-account JSON: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sender, err := NewSender(ctx, saJSON, subject)
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}

	body := "From: " + subject + "\r\n" +
		"To: " + integrationRecipient + "\r\n" +
		"Subject: jellyfish integration-test " + time.Now().UTC().Format(time.RFC3339) + "\r\n" +
		"\r\nIntegration-test message - safe to delete.\r\n"

	id, err := sender.Send(ctx, []byte(body))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if id == "" {
		t.Fatal("empty Gmail message id returned")
	}
}
```

- [ ] **Step 2: Verify it compiles and skips by default**

Run: `go test ./internal/gmail/... -count=1`
Expected: PASS, with `TestIntegrationSend` SKIPped (env var not set).

- [ ] **Step 3: Commit**

```bash
git add internal/gmail/integration_test.go
git commit -m "test(gmail): added env-gated live-send integration test"
```

---

## Task 12: README updates

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Replace the "future flag" note**

Edit `README.md`. The existing Email output section ends with this paragraph (around line 211-213):

```
`-o email` writes an RFC 5322 multipart/alternative message (.eml) to stdout.
It carries a styled HTML body (executive summary + per-CVE table with
clickable NVD/MITRE CVE links) and a plain-text alternative. Open the .eml
file in Mail, pipe it to your mail tooling, or feed it to a future
`--send-email` flag.
```

Replace the trailing sentence "or feed it to a future `--send-email` flag" with `or pass --send-email to send it via the Gmail API (see below).` Then, immediately after the existing "Recipient, sender, and subject default from the `email:` block…" table, add a new subsection (literally — paste the content between the outer `~~~` markers, NOT the markers themselves):

~~~markdown
#### Sending via Gmail (`--send-email`)

`--send-email` on `vulns summary` or `user show` renders the .eml internally
and sends it via the Gmail API instead of writing it to stdout. Combine with
any of the existing email/filter flags:

```bash
jellyfish vulns summary --severity critical --send-email --email-to secops@flipgroup.com.au
jellyfish user show keith@flipgroup.com.au --send-email
```

Recipient resolution for `user show --send-email`:

1. `--email-to <addr>` if provided
2. `email.default_to` from config if non-empty
3. The resolved user's own email address

For `vulns summary --send-email` the user fallback does not apply — pass
`--email-to` or set `email.default_to`.

On success, stdout stays empty and stderr gets one line:
`sent: to=<addr> from=<addr> gmail-id=<id>`.

Combining `--send-email` with an explicit `-o` other than `email` errors out
(exit 1). Use one or the other.

The Gmail send path uses a Workspace service account with domain-wide
delegation. The service-account JSON is stored in the macOS Keychain under
service `au.com.flipgroup.jellyfish`, account `gmail_default` — install it
via `jellyfish configure email` (third prompt). Inspect or remove it via:

```bash
security find-generic-password -s au.com.flipgroup.jellyfish -a gmail_default
security delete-generic-password -s au.com.flipgroup.jellyfish -a gmail_default
```

Gmail-side failures surface via exit codes 2 (auth/permissions: bad JWT,
DWD scope not granted, mailbox forbidden) and 4 (rate-limited or 5xx
upstream).
~~~

- [ ] **Step 2: Update the `configure email` section**

Find the "Configure email defaults" subsection (around line 81-90). Replace its body with this (paste the content between the outer `~~~` markers, NOT the markers themselves):

~~~markdown
### Configure email defaults

```bash
jellyfish configure email
```

Prompts for `From`, default `To`, and a path to a Gmail service-account
JSON file. The first two values are written to the `email:` block of
`~/.config/jellyfish/config.yml`. When a JSON path is provided, the file
is read, validated (must have `type: "service_account"` and `client_email`),
then stored in the macOS Keychain under account `gmail_default`. The path
itself is **not** persisted; the file can be moved or deleted afterwards.

For each prompt: Enter keeps the current value; type a literal `-` to
clear a field (and, for the Gmail prompt, remove the Keychain entry).
The subject template and CVE link templates can be customised by
hand-editing the YAML (see [Email output](#email-output)).
~~~

- [ ] **Step 3: Verify**

Open `README.md` in a viewer and skim the Email output + Configure sections. No broken markdown, all links still resolve.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: documented --send-email and Gmail credential configuration"
```

---

## Self-review

After completing all tasks, run the full suite once more:

```bash
go build ./...
go test ./...
JELLYFISH_KEYCHAIN_TESTS=1 go test ./internal/keychain/...
```

Expected: all green; integration tests skipped where env vars are absent.

### Spec coverage check

- `--send-email` flag on `vulns summary` + `user show`, not `vulns list` ✓ (Tasks 7, 8)
- Reuses `--email-to`/`--email-from`/`--email-subject` ✓ (Task 5)
- Recipient precedence for `user show`: flag > default_to > user's email ✓ (Task 6 helper, Task 7 wiring)
- Recipient precedence for `vulns summary`: flag > default_to > error ✓ (Task 6 helper, Task 8 wiring)
- Stdout empty on success, stderr confirmation line ✓ (Tasks 7, 8)
- Explicit `-o csv`/`-o table` with `--send-email` → exit 1 ✓ (Task 6 + ExplicitOutput plumbing in Tasks 7, 8)
- Impersonation subject = From ✓ (Task 4 + cmd-layer wiring)
- `GmailConfigured` field round-trips in YAML ✓ (Task 1)
- Keychain helpers under `gmail_default` ✓ (Task 2)
- `configure email` Gmail prompt with file → validate → Keychain ✓ (Task 10)
- `internal/gmail` package with Sender interface, NewSender, error sentinels, validator ✓ (Task 4)
- Integration test hard-coded to `k@flip.com.au` ✓ (Task 11)
- `classifyError` maps Gmail sentinels ✓ (Task 9)
- README updates ✓ (Task 12)

No gaps detected.
