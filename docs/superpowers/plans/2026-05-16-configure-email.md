# `jellyfish configure email` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `jellyfish configure email` — an interactive subcommand that captures `email.from` and `email.default_to` into `~/.config/jellyfish/config.yml`, preserving every other field on the `default` profile.

**Architecture:** New cobra subcommand registered under the existing `configure` command. Two small helpers (`promptWithDefault`, `validateEmailish`) in `cmd/configure.go`. A `configureEmailOpts` DI struct mirrors the existing `configureOpts` so tests pipe fake stdin and a tempdir config path. Save logic loads the file, mutates only the two target fields on the `default` profile, calls `config.Save`. No new packages, no new dependencies.

**Tech Stack:** Go 1.25, `github.com/spf13/cobra`, `gopkg.in/yaml.v3` (via existing `internal/config`), stdlib only otherwise.

**Spec:** `docs/superpowers/specs/2026-05-16-configure-email-design.md`

---

## Task 1: `promptWithDefault` helper

**Files:**
- Modify: `cmd/configure.go`
- Modify: `cmd/configure_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `cmd/configure_test.go`. The existing import block already has `bufio`, `bytes`, `strings`, `testing` — no import changes needed.

```go
func TestPromptWithDefaultKeepsCurrentOnEnter(t *testing.T) {
	out := &bytes.Buffer{}
	r := bufio.NewReader(strings.NewReader("\n"))
	got, err := promptWithDefault(out, r, "Email From", "old@x")
	if err != nil {
		t.Fatalf("promptWithDefault: %v", err)
	}
	if got != "old@x" {
		t.Errorf("value: got %q want %q", got, "old@x")
	}
	if !strings.Contains(out.String(), "Email From [old@x]: ") {
		t.Errorf("prompt text: got %q", out.String())
	}
}

func TestPromptWithDefaultReplacesOnTypedValue(t *testing.T) {
	out := &bytes.Buffer{}
	r := bufio.NewReader(strings.NewReader("new@y\n"))
	got, err := promptWithDefault(out, r, "Email From", "old@x")
	if err != nil {
		t.Fatalf("promptWithDefault: %v", err)
	}
	if got != "new@y" {
		t.Errorf("value: got %q want %q", got, "new@y")
	}
}

func TestPromptWithDefaultDashClears(t *testing.T) {
	out := &bytes.Buffer{}
	r := bufio.NewReader(strings.NewReader("-\n"))
	got, err := promptWithDefault(out, r, "Email From", "old@x")
	if err != nil {
		t.Fatalf("promptWithDefault: %v", err)
	}
	if got != "" {
		t.Errorf("value: got %q want empty", got)
	}
}

func TestPromptWithDefaultOmitsBracketsWhenNoCurrent(t *testing.T) {
	out := &bytes.Buffer{}
	r := bufio.NewReader(strings.NewReader("new@y\n"))
	got, err := promptWithDefault(out, r, "Email From", "")
	if err != nil {
		t.Fatalf("promptWithDefault: %v", err)
	}
	if got != "new@y" {
		t.Errorf("value: got %q", got)
	}
	if strings.Contains(out.String(), "[") {
		t.Errorf("prompt should not show brackets when current is empty; got %q", out.String())
	}
	if !strings.Contains(out.String(), "Email From: ") {
		t.Errorf("prompt text: got %q", out.String())
	}
}

func TestPromptWithDefaultTrimsWhitespace(t *testing.T) {
	out := &bytes.Buffer{}
	r := bufio.NewReader(strings.NewReader("  alice@x  \n"))
	got, err := promptWithDefault(out, r, "Email From", "")
	if err != nil {
		t.Fatalf("promptWithDefault: %v", err)
	}
	if got != "alice@x" {
		t.Errorf("value: got %q", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/... -run TestPromptWithDefault -count=1`
Expected: compile error — `promptWithDefault` not defined.

- [ ] **Step 3: Implement the helper**

Edit `cmd/configure.go`. Append to the end of the file (after `readMaskedToken`):

```go
// promptWithDefault writes "<label>[ [<current>]]: " to w, reads a line from
// r, and applies the keep/clear/replace rules:
//
//   ""  -> current   (Enter keeps)
//   "-" -> ""        (literal dash clears)
//   x   -> x         (replace, trimmed)
//
// Returns the resulting value. Does not validate.
func promptWithDefault(w io.Writer, r *bufio.Reader, label, current string) (string, error) {
	if current == "" {
		_, _ = fmt.Fprintf(w, "%s: ", label)
	} else {
		_, _ = fmt.Fprintf(w, "%s [%s]: ", label, current)
	}
	line, err := readLine(r)
	if err != nil {
		return "", err
	}
	switch line {
	case "":
		return current, nil
	case "-":
		return "", nil
	default:
		return line, nil
	}
}
```

The function reuses the existing `readLine` helper in `configure.go` which trims whitespace.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/... -run TestPromptWithDefault -count=1`
Expected: PASS for all five tests.

- [ ] **Step 5: Commit**

```bash
git add cmd/configure.go cmd/configure_test.go
git commit -m "feat: added promptWithDefault helper for configure"
```

---

## Task 2: `validateEmailish` helper

**Files:**
- Modify: `cmd/configure.go`
- Modify: `cmd/configure_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `cmd/configure_test.go`:

```go
func TestValidateEmailishAcceptsWithAt(t *testing.T) {
	if err := validateEmailish("alice@example.com", false, "From"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateEmailishAcceptsEmptyWhenAllowed(t *testing.T) {
	if err := validateEmailish("", true, "DefaultTo"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateEmailishRejectsEmptyWhenNotAllowed(t *testing.T) {
	err := validateEmailish("", false, "From")
	if err == nil {
		t.Fatal("expected error for empty value")
	}
	if !strings.Contains(err.Error(), "From") {
		t.Errorf("error should mention field label; got %v", err)
	}
}

func TestValidateEmailishRejectsMissingAt(t *testing.T) {
	err := validateEmailish("no-at-sign", false, "From")
	if err == nil {
		t.Fatal("expected error for value without @")
	}
	if !strings.Contains(err.Error(), "@") {
		t.Errorf("error should mention @; got %v", err)
	}
}

func TestValidateEmailishAllowEmptyStillRejectsMalformed(t *testing.T) {
	err := validateEmailish("no-at-sign", true, "DefaultTo")
	if err == nil {
		t.Fatal("expected error for non-empty value missing @")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/... -run TestValidateEmailish -count=1`
Expected: compile error — `validateEmailish` not defined.

- [ ] **Step 3: Implement the helper**

Append to `cmd/configure.go`:

```go
// validateEmailish returns nil if value is empty (when allowEmpty) or contains
// an '@'. Otherwise returns an error mentioning fieldLabel. The check is
// deliberately loose; real validation happens when the message is sent.
func validateEmailish(value string, allowEmpty bool, fieldLabel string) error {
	if value == "" {
		if allowEmpty {
			return nil
		}
		return fmt.Errorf("%s must not be empty", fieldLabel)
	}
	if !strings.Contains(value, "@") {
		return fmt.Errorf("%s must look like an email address (contain @)", fieldLabel)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/... -run TestValidateEmailish -count=1`
Expected: PASS for all five tests.

- [ ] **Step 5: Commit**

```bash
git add cmd/configure.go cmd/configure_test.go
git commit -m "feat: added validateEmailish helper"
```

---

## Task 3: `configure email` subcommand + `runConfigureEmail` + integration tests

**Files:**
- Modify: `cmd/configure.go`
- Modify: `cmd/configure_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `cmd/configure_test.go`. The import block already has what's needed (`bytes`, `context`, `path/filepath`, `strings`, `testing`, `flipgroup.com.au/jellyfish/internal/config`). No new imports.

```go
// seedConfigFile writes a config.File to a temp config.yml in dir and returns
// the path. Test helper used by the integration tests below.
func seedConfigFile(t *testing.T, dir string, f config.File) string {
	t.Helper()
	path := filepath.Join(dir, "config.yml")
	if err := config.Save(path, f); err != nil {
		t.Fatalf("seed config: %v", err)
	}
	return path
}

func TestConfigureEmailPromptsAndSaves(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := seedConfigFile(t, tmp, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
	}})

	in := strings.NewReader("alice@example.com\nsecops@example.com\n")
	out := &bytes.Buffer{}

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: in, Stdout: out, Stderr: out,
	})
	if err != nil {
		t.Fatalf("runConfigureEmail: %v", err)
	}

	loaded, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("reload config: %v", err)
	}
	got := loaded["default"]
	if got.Email.From != "alice@example.com" {
		t.Errorf("Email.From: got %q", got.Email.From)
	}
	if got.Email.DefaultTo != "secops@example.com" {
		t.Errorf("Email.DefaultTo: got %q", got.Email.DefaultTo)
	}
	if got.Subdomain != "acme" || got.Region != "us" {
		t.Errorf("non-email fields lost: %+v", got)
	}
	if !strings.Contains(out.String(), "Email config saved to") {
		t.Errorf("expected confirmation in stdout; got %q", out.String())
	}
}

func TestConfigureEmailPreservesOtherEmailFields(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := seedConfigFile(t, tmp, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{
			SubjectTemplate: "Custom - {{.Date}}",
			CVELinkPrimary:  "https://mirror.example/{cve}",
		},
	}})

	in := strings.NewReader("alice@example.com\nsecops@example.com\n")
	out := &bytes.Buffer{}

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: in, Stdout: out, Stderr: out,
	})
	if err != nil {
		t.Fatalf("runConfigureEmail: %v", err)
	}

	loaded, _ := config.Load(cfgPath)
	got := loaded["default"].Email
	if got.From != "alice@example.com" || got.DefaultTo != "secops@example.com" {
		t.Errorf("updated fields wrong: %+v", got)
	}
	if got.SubjectTemplate != "Custom - {{.Date}}" {
		t.Errorf("SubjectTemplate lost: %q", got.SubjectTemplate)
	}
	if got.CVELinkPrimary != "https://mirror.example/{cve}" {
		t.Errorf("CVELinkPrimary lost: %q", got.CVELinkPrimary)
	}
}

func TestConfigureEmailEnterKeepsExisting(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := seedConfigFile(t, tmp, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{From: "old@x", DefaultTo: "def@x"},
	}})

	in := strings.NewReader("\n\n")
	out := &bytes.Buffer{}

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: in, Stdout: out, Stderr: out,
	})
	if err != nil {
		t.Fatalf("runConfigureEmail: %v", err)
	}

	loaded, _ := config.Load(cfgPath)
	got := loaded["default"].Email
	if got.From != "old@x" {
		t.Errorf("From: got %q want %q", got.From, "old@x")
	}
	if got.DefaultTo != "def@x" {
		t.Errorf("DefaultTo: got %q want %q", got.DefaultTo, "def@x")
	}
}

func TestConfigureEmailDashClearsField(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := seedConfigFile(t, tmp, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{From: "old@x", DefaultTo: "def@x"},
	}})

	in := strings.NewReader("-\n\n")
	out := &bytes.Buffer{}

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: in, Stdout: out, Stderr: out,
	})
	if err != nil {
		t.Fatalf("runConfigureEmail: %v", err)
	}

	loaded, _ := config.Load(cfgPath)
	got := loaded["default"].Email
	if got.From != "" {
		t.Errorf("From should have been cleared; got %q", got.From)
	}
	if got.DefaultTo != "def@x" {
		t.Errorf("DefaultTo: got %q want %q", got.DefaultTo, "def@x")
	}
}

func TestConfigureEmailRejectsInvalidFrom(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := seedConfigFile(t, tmp, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
	}})

	in := strings.NewReader("no-at\nstill-no-at\nnope\n")
	out := &bytes.Buffer{}

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: in, Stdout: out, Stderr: out,
	})
	if err == nil {
		t.Fatal("expected error after 3 invalid From attempts")
	}
	if !strings.Contains(err.Error(), "From") || !strings.Contains(err.Error(), "3 attempts") {
		t.Errorf("error wording: got %v", err)
	}
}

func TestConfigureEmailRejectsInvalidDefaultTo(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := seedConfigFile(t, tmp, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
	}})

	in := strings.NewReader("alice@example.com\nbad\nbad\nbad\n")
	out := &bytes.Buffer{}

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: in, Stdout: out, Stderr: out,
	})
	if err == nil {
		t.Fatal("expected error after 3 invalid DefaultTo attempts")
	}
	if !strings.Contains(err.Error(), "DefaultTo") {
		t.Errorf("error wording: got %v", err)
	}
}

func TestConfigureEmailAllowsEmptyDefaultTo(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := seedConfigFile(t, tmp, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
	}})

	in := strings.NewReader("alice@example.com\n\n")
	out := &bytes.Buffer{}

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: in, Stdout: out, Stderr: out,
	})
	if err != nil {
		t.Fatalf("runConfigureEmail: %v", err)
	}

	loaded, _ := config.Load(cfgPath)
	got := loaded["default"].Email
	if got.From != "alice@example.com" {
		t.Errorf("From: got %q", got.From)
	}
	if got.DefaultTo != "" {
		t.Errorf("DefaultTo should be empty; got %q", got.DefaultTo)
	}
}

func TestConfigureEmailErrorsWhenConfigMissing(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "does-not-exist.yml")

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: strings.NewReader(""), Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{},
	})
	if err == nil {
		t.Fatal("expected error for missing config")
	}
	if !strings.Contains(err.Error(), "no config found") {
		t.Errorf("error wording: got %v", err)
	}
}

func TestConfigureEmailErrorsWhenNoDefaultProfile(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := seedConfigFile(t, tmp, config.File{"other": config.Profile{Subdomain: "x", Region: "us"}})

	err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, Stdin: strings.NewReader(""), Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{},
	})
	if err == nil {
		t.Fatal("expected error for missing default profile")
	}
	if !strings.Contains(err.Error(), `"default" profile`) {
		t.Errorf("error wording: got %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/... -run TestConfigureEmail -count=1`
Expected: compile error — `runConfigureEmail` and `configureEmailOpts` not defined.

- [ ] **Step 3: Implement the opts struct, the subcommand, and the run function**

Edit `cmd/configure.go`.

(a) Append the opts struct (place it after the existing `configureOpts` struct definition):

```go
// configureEmailOpts is the DI surface for `configure email`.
type configureEmailOpts struct {
	ConfigPath string
	Stdin      io.Reader
	Stdout     io.Writer
	Stderr     io.Writer
}
```

(b) Modify `newConfigureCmd` so it registers the `email` subcommand. The current function is:

```go
func newConfigureCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "configure",
		Short: "Interactively configure jellyfish credentials",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// ...existing body...
		},
	}
}
```

Replace with:

```go
func newConfigureCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "configure",
		Short: "Interactively configure jellyfish credentials",
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
			return runConfigure(cmd.Context(), configureOpts{
				ConfigPath:    cfgPath,
				Stdin:         cmd.InOrStdin(),
				Stdout:        cmd.OutOrStdout(),
				Stderr:        cmd.ErrOrStderr(),
				StoreToken:    keychain.Set,
				ReadTokenLine: readMaskedToken,
			})
		},
	}
	c.AddCommand(newConfigureEmailCmd())
	return c
}
```

(c) Append the new subcommand factory:

```go
func newConfigureEmailCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "email",
		Short: "Interactively configure email output defaults (From, default To)",
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
				ConfigPath: cfgPath,
				Stdin:      cmd.InOrStdin(),
				Stdout:     cmd.OutOrStdout(),
				Stderr:     cmd.ErrOrStderr(),
			})
		},
	}
}
```

(d) Append the run function. `context.Context` is accepted for symmetry with `runConfigure` and to future-proof cancellation, even though the body does no async I/O.

```go
const configureEmailMaxAttempts = 3

func runConfigureEmail(ctx context.Context, o configureEmailOpts) error {
	_ = ctx // accepted for symmetry; no async I/O today

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
	file["default"] = prof

	if err := config.Save(o.ConfigPath, file); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(o.Stdout, "Email config saved to %s\n", o.ConfigPath)
	return nil
}

// promptValidated runs promptWithDefault + validateEmailish in a loop up to
// configureEmailMaxAttempts times. Returns the validated value or an error.
// Validation errors are printed to stderr; the loop re-prompts.
func promptValidated(stdout, stderr io.Writer, r *bufio.Reader, label, current string, allowEmpty bool) (string, error) {
	fieldName := label
	if idx := strings.Index(label, " "); idx > 0 {
		// e.g. "Email From" -> "From" for error wording
		fieldName = label[idx+1:]
	}
	if fieldName == "default To" {
		fieldName = "DefaultTo"
	}
	for attempt := 1; attempt <= configureEmailMaxAttempts; attempt++ {
		value, err := promptWithDefault(stdout, r, label, current)
		if err != nil {
			return "", err
		}
		if vErr := validateEmailish(value, allowEmpty, fieldName); vErr != nil {
			_, _ = fmt.Fprintln(stderr, vErr)
			continue
		}
		return value, nil
	}
	return "", fmt.Errorf("invalid %s address after %d attempts", fieldName, configureEmailMaxAttempts)
}
```

(e) The top-of-file import block currently is:

```go
import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/keychain"
)
```

No new imports needed — `os`, `errors`, `fmt`, `strings`, `bufio`, `io`, `context`, `config`, `cobra` are already imported.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/... -run TestConfigureEmail -count=1`
Expected: PASS for all nine new integration tests.

Run: `go test ./cmd/... -count=1`
Expected: PASS for every test (including pre-existing ones).

Run: `go test ./... -count=1`
Expected: PASS module-wide.

- [ ] **Step 5: Commit**

```bash
git add cmd/configure.go cmd/configure_test.go
git commit -m "feat: added configure email subcommand"
```

---

## Task 4: README — "Configure email defaults" subsection

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add the section**

Edit `README.md`. Find the existing `## Configure` heading (around line 59). The section currently ends with the `security delete-generic-password` example and the start of `## Usage`. Insert the new `### Configure email defaults` subsection IMMEDIATELY BEFORE the `## Usage` heading (so it lives at the end of the Configure section).

Use this exact content:

````markdown
### Configure email defaults

```bash
jellyfish configure email
```

Prompts for `From` and default `To`, then writes them to the `email:` block
of `~/.config/jellyfish/config.yml`. Enter keeps the current value; type a
literal `-` to clear a field. The subject template and CVE link templates
can be customised by hand-editing the YAML (see [Email output](#email-output)).

````

- [ ] **Step 2: Verify the section is in place**

Run: `sed -n '/^### Configure email defaults/,/^## Usage/p' README.md`
Expected: the new section followed by `## Usage` on its own line.

- [ ] **Step 3: Run the full suite for safety**

Run: `go test ./... -count=1`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: documented configure email subcommand"
```

---

## Self-Review

**Spec coverage:**

| Spec section | Implemented by |
|---|---|
| `jellyfish configure email` subcommand registered under `configure` | Task 3 (step 3b: `c.AddCommand(newConfigureEmailCmd())`) |
| Prompts for From and DefaultTo, in that order | Task 3 (step 3d: the two `promptValidated` calls in order) |
| Bracketed current value shown when set; omitted when empty | Task 1 (`promptWithDefault`); `TestPromptWithDefaultOmitsBracketsWhenNoCurrent` asserts |
| Enter keeps current value | Task 1; `TestPromptWithDefaultKeepsCurrentOnEnter`, `TestConfigureEmailEnterKeepsExisting` |
| Literal `-` clears the field | Task 1; `TestPromptWithDefaultDashClears`, `TestConfigureEmailDashClearsField` |
| From validation: must contain `@`; 3 attempts max | Task 2 + Task 3 (`validateEmailish` + `promptValidated` loop); `TestConfigureEmailRejectsInvalidFrom` |
| DefaultTo validation: empty allowed; non-empty must contain `@`; 3 attempts max | Task 2 + Task 3; `TestConfigureEmailAllowsEmptyDefaultTo`, `TestConfigureEmailRejectsInvalidDefaultTo` |
| Missing config file errors with "no config found" | Task 3 (step 3d: `os.ErrNotExist` branch); `TestConfigureEmailErrorsWhenConfigMissing` |
| Missing `default` profile errors | Task 3 (step 3d: `ok` check); `TestConfigureEmailErrorsWhenNoDefaultProfile` |
| Save preserves Subdomain/Region/BaseURL and other Email fields | Task 3 (step 3d: mutates only `From`/`DefaultTo` then puts the profile back); `TestConfigureEmailPreservesOtherEmailFields` |
| Saves at 0600 (via existing `config.Save`) | Inherited from existing `config.Save` — no test added (covered by existing `TestConfigureWritesConfigAndCallsKeychain`) |
| README `Configure email defaults` subsection | Task 4 |
| No new packages, no new deps | Confirmed — Task 3 (step 3e) lists existing imports |

**Placeholder scan:** No "TBD" / "TODO" / "implement later" / "handle edge cases" patterns in any task body. Every step has concrete code or commands.

**Type consistency:** `configureEmailOpts` fields (`ConfigPath`, `Stdin`, `Stdout`, `Stderr`) are used identically across all Task 3 tests and the production code in step 3d. `promptWithDefault(w, r, label, current)` signature in Task 1 matches every caller in Task 3. `validateEmailish(value, allowEmpty, fieldLabel)` signature in Task 2 matches the caller in `promptValidated` in Task 3. `runConfigureEmail(ctx, opts)` signature is consistent across the test calls and the subcommand `RunE`. The `configureEmailMaxAttempts` constant is defined once in Task 3 step 3d and referenced from the same step.

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-16-configure-email.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
