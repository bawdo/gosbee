# `jellyfish configure email`

## Goal

Add an interactive subcommand that captures email-output defaults (`email.from` and `email.default_to`) into `~/.config/jellyfish/config.yml`. Mirrors the existing `jellyfish configure` pattern: stdin prompts, dependency-injection-friendly opts struct, no new packages or deps.

## Non-goals

- Capturing `subject_template`, `cve_link_primary`, `cve_link_secondary`. Those have working built-in defaults; users who need to customise them can hand-edit the YAML. Out of scope to keep the wizard short.
- Per-profile email config beyond `default`. Multi-profile support is a separate, already-deferred item ("Known follow-ups" in README).
- Verifying the email From by sending mail. Not feasible without a configured transport; deferred to a future `--send-email` slice.

## CLI surface

`jellyfish configure email` — new cobra subcommand registered under the existing top-level `configure` command. Inherits `--config <path>` from the persistent flag on root (same as every other command).

## Interactive flow

```
$ jellyfish configure email
Email From [keith@flipgroup.com.au]: alice@example.com
Email default To [secops@flipgroup.com.au]:
Email config saved to ~/.config/jellyfish/config.yml
```

Per-prompt behaviour:

| Input | Meaning |
|---|---|
| Enter (empty line) | Keep the current value |
| Any non-empty string except `-` | Replace the current value with the typed string (trimmed) |
| `-` (literal single dash) | Clear that field |

Display rules:

- The bracketed default (`[current]`) is shown only when the field currently has a value. If `From` is unset, the prompt is `Email From: `.
- The "Email config saved to ..." line uses the resolved config path (after `--config` and `DefaultPath` fallback).

Validation:

- **From**: after applying the keep/clear/replace rule, the resulting value must contain `@`. If it does not, print `From must look like an email address (contain @)` to stderr and re-prompt. After 3 failed attempts, error with `invalid From address after 3 attempts` (exit 1).
- **DefaultTo**: empty is acceptable (the renderer prints `<unspecified>` in the To header so the user can fill in at send time). If non-empty, must contain `@`. Re-prompt logic identical to From. Error: `invalid DefaultTo address after 3 attempts`.

## Save semantics: merge, not overwrite

1. Load the existing `config.File` from the resolved path.
   - If the file cannot be read (missing or unreadable), error with `no config found at <path> - run "jellyfish configure" first to set up tenant + token` (exit 1).
   - If the file parses but has no `"default"` key, error with `no "default" profile yet - run "jellyfish configure" first to set up tenant + token` (exit 1).
2. Copy the existing `default` Profile into a local variable. Mutate only `Profile.Email.From` and `Profile.Email.DefaultTo`.
3. Reassign the profile back into the `config.File` map under `"default"`.
4. Call `config.Save(path, file)` (existing function: writes `0600`, creates parent dirs).

This preserves:

- `Subdomain`, `Region`, `BaseURL` on the profile
- `Email.SubjectTemplate`, `Email.CVELinkPrimary`, `Email.CVELinkSecondary` if hand-edited
- Any other profile keys in the file (forward-compat for multi-profile)

The existing `EmailConfig` already has `omitempty` on every field (from the email-output work), so a `From=""` after clear writes nothing to the YAML rather than `from: ""`.

## Code layout

```
cmd/configure.go        # MODIFIED
  - newConfigureCmd() adds the email subcommand
  - newConfigureEmailCmd() returns *cobra.Command
  - runConfigureEmail(ctx, configureEmailOpts) error
  - prompt helper(s) — see below

cmd/configure_test.go   # MODIFIED — append new tests
```

No new files, no new packages.

## DI-friendly opts

```go
type configureEmailOpts struct {
    ConfigPath string
    Stdin      io.Reader
    Stdout     io.Writer
    Stderr     io.Writer
}
```

Production `RunE` builds this from the cobra command; tests pass `bytes.Buffer` for I/O streams and a `t.TempDir()` path for `ConfigPath`.

## Prompt helper

Add one small helper to `cmd/configure.go`:

```go
// promptWithDefault reads a line from r, applying keep/clear/replace rules
// against current. Returns the resulting value. Does not validate.
//
//   ""  -> current (Enter keeps)
//   "-" -> ""      (dash clears)
//   x   -> x       (replace, trimmed)
func promptWithDefault(w io.Writer, r *bufio.Reader, label, current string) (string, error)
```

`label` is the prefix shown before `[current]:`. The function writes `<label>` then, if `current != ""`, ` [<current>]`, then `: ` to `w`, reads a line via the existing `readLine`, and applies the rules.

Validation is a separate helper:

```go
// validateEmailish returns nil if value is empty (when allowEmpty) or contains
// an '@'. Otherwise returns an error suitable for printing to stderr.
func validateEmailish(value string, allowEmpty bool, fieldLabel string) error
```

`runConfigureEmail` wraps `promptWithDefault` + `validateEmailish` in a small loop (max 3 attempts).

## Error handling

| Condition | Behaviour | Exit |
|---|---|---|
| `config.yml` cannot be read (missing or unreadable) | `no config found at <path> - run "jellyfish configure" first to set up tenant + token` | 1 |
| `config.yml` parses but has no `default` key | `no "default" profile yet - run "jellyfish configure" first to set up tenant + token` | 1 |
| `From` validation fails 3 times | `invalid From address after 3 attempts` | 1 |
| `DefaultTo` validation fails 3 times | `invalid DefaultTo address after 3 attempts` | 1 |
| `config.Save` fails | Propagate as-is | 1 |
| Ctrl-C during prompts | Cobra's signal handling cancels context | 130 |

`runConfigureEmail` returns errors via the standard `RunE` flow, so `classifyError` in `cmd/root.go` maps them to exit 1 ("user error") by default. None of these are upstream/network errors.

## README

Add a new "Configure email defaults" subsection immediately under "## Configure":

```markdown
### Configure email defaults

```bash
jellyfish configure email
```

Prompts for `From` and default `To`, then writes them to the `email:` block
of `~/.config/jellyfish/config.yml`. Enter keeps the current value; type a
literal `-` to clear a field. The subject template and CVE link templates
can be customised by hand-editing the YAML (see [Email output](#email-output)).
```

The existing "Email output" section already documents the underlying YAML keys; no further changes there.

## Testing

All tests live in `cmd/configure_test.go`. Each uses `t.TempDir()` for `ConfigPath` and `bytes.Buffer` for streams.

| Test | Setup | Stdin | Assertion |
|---|---|---|---|
| `TestConfigureEmailPromptsAndSaves` | Pre-seed `default` profile with no email block | `"alice@example.com\nsecops@example.com\n"` | Loaded config has both fields set; Subdomain/Region preserved |
| `TestConfigureEmailPreservesOtherEmailFields` | Pre-seed default profile with `Email.SubjectTemplate="X"`, `Email.CVELinkPrimary="Y"` | `"alice@example.com\nsecops@example.com\n"` | Loaded config has new From + DefaultTo AND original SubjectTemplate + CVELinkPrimary unchanged |
| `TestConfigureEmailEnterKeepsExisting` | Pre-seed `Email.From="old@x"`, `Email.DefaultTo="def@x"` | `"\n\n"` (two empty lines) | Loaded config still has `"old@x"` and `"def@x"` |
| `TestConfigureEmailDashClearsField` | Pre-seed `Email.From="old@x"` | `"-\n\n"` (clear From, keep DefaultTo) | Loaded config has empty From; DefaultTo unchanged |
| `TestConfigureEmailRejectsInvalidFrom` | Empty default profile | `"no-at\nstill-no-at\nnope\n"` | Error matches "invalid From address after 3 attempts" |
| `TestConfigureEmailRejectsInvalidDefaultTo` | Empty default profile | `"alice@example.com\nbad\nbad\nbad\n"` | Error matches "invalid DefaultTo address" |
| `TestConfigureEmailAllowsEmptyDefaultTo` | Empty default profile | `"alice@example.com\n\n"` | Loaded config has From set, DefaultTo empty |
| `TestConfigureEmailErrorsWhenConfigMissing` | No config file | (irrelevant) | Error contains "no config found" |
| `TestConfigureEmailErrorsWhenNoDefaultProfile` | Config file with a non-`default` key only | (irrelevant) | Error contains `"default" profile` |

No external dependencies, no network calls. Tests use the existing `TestMain`'s HOME redirect for safety.

## Dependencies

None new. Standard library + `github.com/spf13/cobra` (already on the module) + `gopkg.in/yaml.v3` (via `internal/config`, already on the module).

## Out of scope (deferred)

- Prompting for the three template fields. Future work; can layer on as `--advanced` if real demand emerges.
- Multi-profile support. The save logic deliberately only touches `default` — when multi-profile lands, add a `--profile` flag using the existing root-level flag for selection.
- Round-trip golden-file tests on the saved YAML. The existing per-field assertions are sufficient; a full round-trip test adds maintenance cost without catching a new class of bug.
