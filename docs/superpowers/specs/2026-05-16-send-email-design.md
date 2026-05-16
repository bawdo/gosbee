# Send email via Gmail API (`--send-email`)

**Status:** Design approved, ready for implementation plan.
**Date:** 2026-05-16

## Goal

Extend jellyfish so that the two commands which already render `-o email`
(`vulns summary` and `user show`) can send the rendered .eml directly via the
Gmail API, instead of forcing the user to pipe the .eml into another tool.

The README has flagged this as future work since the email-output design landed
("feed it to a future `--send-email` flag"). This spec turns that hint into a
concrete shape.

## Out of scope

- Adding email output (and therefore `--send-email`) to `vulns list`. Emailing
  a multi-thousand-row flat per-device/CVE list is not useful and would need a
  new renderer. Deferred.
- OAuth2 user credentials, SMTP relay, or any send path other than Gmail API
  with a Workspace service account.
- Non-darwin support. The credential store remains the macOS Keychain.

## Background

The user has a Workspace service-account JSON
(`type: "service_account"`, `client_email: jellyfish-emailler@jellyfish-mail.iam.gserviceaccount.com`)
with domain-wide delegation (DWD) configured in admin.google.com for scope
`https://www.googleapis.com/auth/gmail.send`. With DWD set up, the service
account can impersonate any Workspace user (set via the JWT `Subject` field)
and send mail "as" them via `gmail.users.messages.send`.

## UX surface

A new boolean flag `--send-email` is added to:

- `jellyfish user show <user> --send-email [--email-to X]`
- `jellyfish vulns summary --send-email --email-to X [--severity ...] [--status ...] ...`

`vulns list` is **not** modified.

When `--send-email` is set:

1. The renderer always builds the .eml internally, regardless of `-o`.
2. Nothing is written to stdout.
3. On success, stderr gets one line:
   `sent: to=<addr> from=<addr> gmail-id=<id>`.
4. If `-o` is explicitly set to anything other than `email`, the command
   errors with exit 1
   (`--send-email implies email output; remove -o or set -o email`).
   Surfacing the conflict is friendlier than silently ignoring the user's
   explicit format choice.

The new flag reuses every existing email knob: `--email-from`,
`--email-subject`, `--email-to`, and the `email:` block in `config.yml`. No new
subcommand surface is added on `user` or `vulns`.

### `--email-to` resolution

For `user show --send-email`, in precedence order:

1. `--email-to <addr>` if provided
2. `email.default_to` from config if non-empty
3. The resolved user's own email (the `<user>` positional after
   `iru.FindUserByEmail` lookup, or `user.Email` if a user-id was supplied)
4. Otherwise: exit 1 (`--send-email requires a recipient`)

For `vulns summary --send-email`, in precedence order:

1. `--email-to <addr>` if provided
2. `email.default_to` from config if non-empty
3. Otherwise: exit 1
   (`--send-email requires --email-to (no email.default_to in config)`)

No user-fallback exists for `vulns summary` because there is no single "owner"
of a summary.

### Impersonation subject = From

Whatever resolves as the From address (flag > `email.from` > `git user.email`)
is also passed as the JWT `Subject` to trigger DWD impersonation. This means
From must be a real Workspace user with a mailbox - it is anyway, today, for
the existing email output flow.

## Config and Keychain

### Config

Add one field to `internal/config.EmailConfig`:

```go
type EmailConfig struct {
    From             string `yaml:"from,omitempty"`
    DefaultTo        string `yaml:"default_to,omitempty"`
    SubjectTemplate  string `yaml:"subject_template,omitempty"`
    CVELinkPrimary   string `yaml:"cve_link_primary,omitempty"`
    CVELinkSecondary string `yaml:"cve_link_secondary,omitempty"`
    GmailConfigured  bool   `yaml:"gmail_configured,omitempty"`  // NEW
}
```

`GmailConfigured` is a UI hint only. It is set to true once `configure email`
has stored a service-account JSON in Keychain, and back to false if the user
clears the credential. It lets `--send-email` produce a useful "run jellyfish
configure email first" error without doing a Keychain probe on every command.

The service-account JSON itself is **never** written to `config.yml`.

### Keychain

The existing `internal/keychain` package stores secrets under service
`au.com.flipgroup.jellyfish`, account `default` (the Iru API token). Add a
parallel triad under a new account so the API token and Gmail JSON do not
collide:

```go
const accountAPIToken     = "default"        // existing, made explicit
const accountGmailJSON    = "gmail_default"  // NEW

func SetGmailServiceAccount(jsonBytes []byte) error
func GetGmailServiceAccount() ([]byte, error)
func DeleteGmailServiceAccount() error
```

These reuse the underlying Keychain primitives - only the account name
differs. macOS Keychain item size for ~2.5 KB of JSON is well within limits.

### `jellyfish configure email` extension

Add a third prompt to the existing two:

```
Email From [keith@flipgroup.com.au]:
Email default To [ops@flipgroup.com.au]:
Gmail service-account JSON path [configured]:
```

Prompt rules match the existing `promptWithDefault` helper:

- Enter keeps the current state (no Keychain write).
- Literal `-` clears: calls `DeleteGmailServiceAccount` and sets
  `GmailConfigured = false`.
- Any other input is treated as a path:
  1. Read the file from disk (`os.ReadFile`).
  2. Parse as JSON and validate it has `type: "service_account"` and a
     non-empty `client_email`.
  3. On validation success, call `SetGmailServiceAccount(bytes)` and set
     `GmailConfigured = true`.
  4. On any failure (file missing, bad JSON, wrong type), error out; Keychain
     and config are left untouched.

The path itself is not persisted. After configure, the JSON file on disk can
be moved or deleted - the Keychain copy is authoritative.

## `internal/gmail` package

New package with one small public surface:

```go
package gmail

// Sender sends a pre-built RFC 5322 message via Gmail API
// users.messages.send, using the service account in saJSON to impersonate
// subjectUser via DWD.
type Sender interface {
    Send(ctx context.Context, rfc822 []byte) (messageID string, err error)
}

// NewSender returns a Sender. saJSON is the raw service-account JSON read
// from Keychain. subjectUser is the From address (a real Workspace user) to
// impersonate. The OAuth scope is hard-coded to gmail.send.
func NewSender(ctx context.Context, saJSON []byte, subjectUser string) (Sender, error)

// Error sentinels for cmd.classifyError integration.
var (
    ErrUnauthorized = errors.New("gmail: unauthorized")
    ErrForbidden    = errors.New("gmail: forbidden")
    ErrRateLimited  = errors.New("gmail: rate limited")
)
```

### Implementation

1. `cfg, err := google.JWTConfigFromJSON(saJSON, gmail.GmailSendScope)`
   to parse the service-account JSON and request the right OAuth scope.
2. `cfg.Subject = subjectUser` - DWD impersonation. Without this, the JWT
   represents the service-account principal itself, which has no Gmail
   mailbox and `Send` will 400.
3. `svc, err := gmail.NewService(ctx, option.WithTokenSource(cfg.TokenSource(ctx)))`
   to build the API client.
4. `Send(ctx, rfc822)` wraps the bytes in a
   `gmail.Message{Raw: base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(rfc822)}`
   and calls `svc.Users.Messages.Send("me", msg).Context(ctx).Do()`. Returns
   the resulting `*gmail.Message`'s `Id`.

### Error mapping

| Gmail API condition                                       | Sentinel returned       | cmd exit |
|-----------------------------------------------------------|--------------------------|----------|
| 401 (invalid JWT, bad audience, DWD not granted)          | `ErrUnauthorized`        | 2        |
| 403 (delegated scope not granted, mailbox forbidden)      | `ErrForbidden`           | 2        |
| 429                                                       | `ErrRateLimited`         | 4        |
| 5xx                                                       | wrapped `*googleapi.Error` | 4      |
| Subject missing / not a real Workspace user               | `ErrForbidden`           | 2        |

Wrapping happens inside `internal/gmail` so the cmd layer only needs
`errors.Is(err, gmail.ErrFoo)` plus an `errors.As(err, &googleapi.Error{})`
for the 5xx case.

## cmd-layer wiring

### `cmd/email.go`

Extend `emailFlagValues` with one field, and add a resolver:

```go
type emailFlagValues struct {
    To      string
    From    string
    Subject string
    Send    bool   // NEW: from --send-email
}

// resolveSendOptions runs after resolveEmailOptions when Send is true.
// Returns the sender and the recipient resolved per the precedence rules.
//
// userDefaultTo is non-empty only for `user show` (the resolved user's
// email). It is used as the last-resort fallback after --email-to and
// email.default_to.
//
// Errors with exit 1 when:
//   - GmailConfigured is false (run jellyfish configure email)
//   - --output is explicit and not "email"
//   - No recipient can be resolved
func resolveSendOptions(
    ctx context.Context,
    eo email.Options,
    explicitOutput string,
    profile config.Profile,
    userDefaultTo string,
    keychainGet func() ([]byte, error),
    newSender func(ctx context.Context, saJSON []byte, subject string) (gmail.Sender, error),
) (sender gmail.Sender, to string, err error)
```

`keychainGet` and `newSender` are DI seams for tests. Production wires them
to `keychain.GetGmailServiceAccount` and `gmail.NewSender`.

### `cmd/user.go`

- Add `c.Flags().Bool("send-email", false, "Send the rendered email via Gmail (uses configured service account)")` to `newUserShowCmd`.
- Read `Send` in `readEmailFlags` and stash it on `userShowOpts`.
- In `renderUserBundle`, when `opts.EmailFlags.Send` is true:
  1. Run `resolveEmailOptions` to get `email.Options` (with From etc.).
  2. Run `resolveSendOptions` with `userDefaultTo = bundle.User.Email`.
     This returns the `Sender` and the final `To`. Update `email.Options.To`
     accordingly.
  3. Render the .eml into a `bytes.Buffer` using the existing
     `email.NewUserShowRenderer(emailOpts).Render(buf, input)` call - just
     pass the buffer instead of stdout.
  4. Call `id, err := sender.Send(ctx, buf.Bytes())`.
  5. On success, write
     `fmt.Fprintf(stderr, "sent: to=%s from=%s gmail-id=%s\n", to, eo.From, id)`.
     Return nil. Stdout stays empty.

### `cmd/vulns.go`

Same shape on `newVulnsSummaryCmd`, but `resolveSendOptions` is called with
`userDefaultTo = ""` so the user-fallback rule does not apply. The helper
errors out if no recipient is resolved.

### `cmd/root.go`

`classifyError` grows two cases:

```go
case errors.Is(err, gmail.ErrUnauthorized), errors.Is(err, gmail.ErrForbidden):
    return 2
case errors.Is(err, gmail.ErrRateLimited):
    return 4
```

A 5xx that surfaces as a wrapped `*googleapi.Error` is mapped to exit 4 inside
the `internal/gmail` package's error helper, so the cmd layer does not need to
type-switch on Google SDK types directly.

## README updates

- Replace the "feed it to a future `--send-email` flag" note under the Email
  output section with concrete `--send-email` docs (the recipient precedence,
  the stderr confirmation line, the exit-code mapping for Gmail failures).
- Document the third prompt added to `jellyfish configure email`.
- Note the new Keychain account (`gmail_default`) in the Configure section
  for users who want to inspect or delete it via `security`.

## Testing

### `internal/gmail/gmail_test.go` (unit)

- `NewSender` rejects: malformed JSON, JSON without `type: "service_account"`,
  JSON without `client_email`, empty `subjectUser`.
- Base64url encoding of a sample RFC 5322 message round-trips
  (URL-safe alphabet, no padding).
- Error mapping: synthetic `*googleapi.Error` with codes 401, 403, 429, 500
  wrap to the right sentinels.

### `internal/gmail/integration_test.go` (env-gated)

- Gated on `JELLYFISH_GMAIL_TESTS=1`. JSON path comes from
  `JELLYFISH_GMAIL_TEST_JSON`.
- **The recipient is hard-coded** to `k@flip.com.au` as a string literal at
  the call site. No env var controls the recipient. A top-of-test comment
  spells out the rationale so a future contributor does not "improve" it
  into a configurable. This is an invariant, not a configurable: prevents
  accidental spam during integration runs.
- Sends a tiny .eml, asserts the returned Gmail message id is non-empty.

### `internal/keychain/keychain_darwin_test.go`

Extend the existing real-Keychain test (also `JELLYFISH_KEYCHAIN_TESTS=1`-gated)
with `SetGmailServiceAccount` / `GetGmailServiceAccount` / `DeleteGmailServiceAccount`
round-trip coverage.

### `cmd/email_test.go`

Add `resolveSendOptions` table tests:

| Case | Expected |
|---|---|
| `Send=true`, `GmailConfigured=false`                                   | exit 1, mentions `jellyfish configure email` |
| `Send=true`, explicit `-o csv`                                         | exit 1, "incompatible with --send-email" |
| `Send=true`, no `--email-to`, non-empty `userDefaultTo`, no default_to | uses `userDefaultTo` |
| `Send=true`, no `--email-to`, non-empty `userDefaultTo` + `default_to` | uses `default_to` (default_to beats user fallback) |
| `Send=true`, no `--email-to`, no `default_to`, no `userDefaultTo`      | exit 1, "no recipient" |
| `Send=true`, explicit `--email-to`                                     | uses `--email-to` |

### `cmd/user_test.go`

- `runUserShow` with a fake `gmail.Sender` captures the .eml bytes; assert
  `To:` is the resolved user's own email when no `--email-to` is set and no
  `email.default_to` is configured.
- Stdout is empty when `--send-email` succeeds; stderr contains the
  `sent: to=... from=... gmail-id=...` line.
- Fake sender returns `gmail.ErrUnauthorized` -> propagates ->
  `classifyError` -> exit 2.

### `cmd/vulns_summary_test.go`

- `runVulnsSummary` with `--send-email`, no `--email-to`, no
  `email.default_to` -> exit 1.
- Fake sender captures .eml; assert the summary subject default is used when
  no `--email-subject` flag is given.

### `cmd/configure_test.go`

Extend `runConfigureEmail` cases (injected `StoreGmailServiceAccount` /
`DeleteGmailServiceAccount` fakes, no real Keychain calls):

- Empty path at the Gmail prompt -> no Keychain write, `GmailConfigured`
  preserved.
- `-` at the Gmail prompt -> delete called, `GmailConfigured = false`.
- Valid JSON path -> Keychain bytes match file contents,
  `GmailConfigured = true`.
- Path to a file that isn't `type: "service_account"` -> error, no Keychain
  write.
- Path to a file that does not exist -> error, no Keychain write.

All cmd-layer tests use injected fakes for both Keychain and `gmail.Sender`.
No unit test ever touches real Keychain or Gmail.

## Dependency additions

- `google.golang.org/api/gmail/v1`
- `google.golang.org/api/option`
- `golang.org/x/oauth2/google`

Pulled into `go.mod` via the new `internal/gmail` import; the existing
`internal/iru` HTTP client is untouched.

## Migration / rollout notes

- Existing users who do not run `jellyfish configure email` and do not pass
  `--send-email` see no behavioural change.
- A user who runs `--send-email` without first configuring credentials gets
  the friendly exit-1 message naming the configure subcommand. No crash.
- The new `gmail_configured` config field is additive; older config files
  loaded by the new binary will simply read it as `false`.
