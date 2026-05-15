# Jellyfish v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a macOS-only Go CLI named `jellyfish` that queries the Iru (formerly Kandji) Endpoint Management API for vulnerability detections, device data, and user profiles, with secure credential storage in the macOS Keychain.

**Architecture:** Cobra-driven subcommands at the root, a thin `cmd/` layer that wires flags and renderers, and an internal Iru HTTP client with auto-pagination, retries, typed errors and a generic paginator. Token storage is split: secret in macOS Keychain, non-secret tenant config in a YAML file at `~/.config/jellyfish/config.yml`. Output is rendered through a small interface with four backends (table, json, yaml, csv).

**Tech Stack:** Go 1.21+, `github.com/spf13/cobra`, `gopkg.in/yaml.v3`, `github.com/keybase/go-keychain`, `github.com/jedib0t/go-pretty/v6`, `golang.org/x/term`, `golang.org/x/sync/errgroup`, `github.com/stretchr/testify` (tests only).

**Source spec:** [`../specs/2026-05-15-jellyfish-design.md`](../specs/2026-05-15-jellyfish-design.md)

---

## Working notes for the implementer

Before starting, please read:

1. The spec linked above. Every task here implements something in there.
2. Iru's live API docs at `https://api-docs.kandji.io/` to confirm exact JSON field names and query parameters for `/devices`, `/users` and `/vulnerability-management/detections`. The struct fields in this plan reflect the best evidence available at the time of writing; if a field name differs in the live API, prefer the live API and update the struct tag.

A clean cycle for every task:

1. Read the task's file list and steps.
2. Write the failing test first.
3. `go test ./...` to confirm it fails for the right reason.
4. Implement the minimum to make it pass.
5. `go test ./...` to confirm it now passes.
6. Stage + commit.

Conventional commit prefixes: `feat:`, `fix:`, `refactor:`, `test:`, `chore:`, `docs:`. **Commit messages must not mention Claude or Anthropic.**

---

## Phase 1: Project scaffold

### Task 1: Initialise module and base files

**Files:**

- Create: `go.mod`
- Create: `main.go`
- Create: `.gitignore`
- Create: `Makefile`

- [ ] **Step 1: Initialise the Go module**

Run:

```bash
go mod init flipgroup.com.au/jellyfish
```

Expected: `go.mod` is created with module line `module flipgroup.com.au/jellyfish` and a `go` directive at the toolchain's current version (1.21 or newer).

- [ ] **Step 2: Pin the minimum Go version**

Edit `go.mod` so the `go` directive reads:

```
go 1.21
```

- [ ] **Step 3: Create the `main.go` stub**

Write `main.go`:

```go
package main

import (
	"os"

	"flipgroup.com.au/jellyfish/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
```

This will not compile yet because `cmd` does not exist. That is expected and resolved in Task 3.

- [ ] **Step 4: Add `.gitignore`**

Write `.gitignore`:

```
# Build artefacts
/bin/
*.test
*.out

# OS junk
.DS_Store

# IDE
.idea/
.vscode/
```

- [ ] **Step 5: Add `Makefile`**

Write `Makefile`:

```makefile
VERSION := $(shell git describe --tags --always --dirty)
LDFLAGS := -X flipgroup.com.au/jellyfish/internal/version.Version=$(VERSION)

build:
	go build -ldflags "$(LDFLAGS)" -o bin/jellyfish .

install:
	go install -ldflags "$(LDFLAGS)" .

test:
	go test ./...

lint:
	golangci-lint run

.PHONY: build install test lint
```

- [ ] **Step 6: Stage and commit**

```bash
git add go.mod main.go .gitignore Makefile
git commit -m "chore: initialise module and base build files"
```

Note: a `go build` at this point will fail because `cmd/` does not exist yet. That is fixed in Task 3.

---

### Task 2: Version package

**Files:**

- Create: `internal/version/version.go`
- Test: `internal/version/version_test.go`

- [ ] **Step 1: Write the failing test**

Write `internal/version/version_test.go`:

```go
package version

import "testing"

func TestDefault(t *testing.T) {
	if Version == "" {
		t.Fatal("Version should not be empty")
	}
}

func TestDefaultIsDevWhenUnset(t *testing.T) {
	if Version != "dev" {
		t.Fatalf(`expected default Version to be "dev", got %q`, Version)
	}
}
```

- [ ] **Step 2: Run the test and confirm it fails**

Run:

```bash
go test ./internal/version/...
```

Expected: `package flipgroup.com.au/jellyfish/internal/version: cannot find package` or similar (the package does not exist yet).

- [ ] **Step 3: Implement the package**

Write `internal/version/version.go`:

```go
package version

// Version is the build version, overridden via -ldflags at build time.
// See the Makefile for the exact -X value.
var Version = "dev"
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run:

```bash
go test ./internal/version/...
```

Expected: `ok` for both tests.

- [ ] **Step 5: Stage and commit**

```bash
git add internal/version/version.go internal/version/version_test.go
git commit -m "feat: add version package with ldflags-injectable Version"
```

---

### Task 3: Cobra root command and `version` subcommand

**Files:**

- Create: `cmd/root.go`
- Create: `cmd/version.go`
- Test: `cmd/version_test.go`

- [ ] **Step 1: Add cobra as a dependency**

Run:

```bash
go get github.com/spf13/cobra@latest
```

Expected: `go.mod` and `go.sum` are updated. Stage them with the next commit.

- [ ] **Step 2: Write the failing test for `version`**

Write `cmd/version_test.go`:

```go
package cmd

import (
	"bytes"
	"strings"
	"testing"

	"flipgroup.com.au/jellyfish/internal/version"
)

func TestVersionCommandPrintsVersion(t *testing.T) {
	version.Version = "test-1.2.3"
	t.Cleanup(func() { version.Version = "dev" })

	buf := &bytes.Buffer{}
	root := newRootCmd()
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"version"})

	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "jellyfish test-1.2.3") {
		t.Fatalf("expected output to contain version, got %q", out)
	}
}
```

- [ ] **Step 3: Run the test to confirm it fails**

Run:

```bash
go test ./cmd/...
```

Expected: compile failure on `newRootCmd` not being defined.

- [ ] **Step 4: Implement the root command**

Write `cmd/root.go`:

```go
package cmd

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// Execute is the public entry point used by main.go.
func Execute() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	return newRootCmd().ExecuteContext(ctx)
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "jellyfish",
		Short:         "CLI for the Iru (Kandji) Endpoint Management API",
		SilenceUsage:  true,
		SilenceErrors: false,
	}

	root.PersistentFlags().StringP("output", "o", "table", "Output format: table, json, yaml, csv")
	root.PersistentFlags().BoolP("verbose", "v", false, "Verbose logging to stderr")
	root.PersistentFlags().String("config", "", "Override config file path (default ~/.config/jellyfish/config.yml)")
	root.PersistentFlags().String("profile", "default", "Profile name (reserved; only 'default' is honoured in v1)")

	root.AddCommand(newVersionCmd())
	return root
}
```

- [ ] **Step 5: Implement the version subcommand**

Write `cmd/version.go`:

```go
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"flipgroup.com.au/jellyfish/internal/version"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the jellyfish version",
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Fprintf(cmd.OutOrStdout(), "jellyfish %s\n", version.Version)
			return nil
		},
	}
}
```

- [ ] **Step 6: Run the tests to confirm they pass**

Run:

```bash
go test ./...
```

Expected: all tests pass.

- [ ] **Step 7: Verify the binary builds**

Run:

```bash
go build -o /tmp/jellyfish .
/tmp/jellyfish version
```

Expected: prints `jellyfish dev` (or similar, depending on ldflags).

- [ ] **Step 8: Stage and commit**

```bash
git add cmd/root.go cmd/version.go cmd/version_test.go go.mod go.sum
git commit -m "feat: add cobra root command and version subcommand"
```

---

## Phase 2: Config and Keychain

### Task 4: Config file load/save

**Files:**

- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go`

- [ ] **Step 1: Add yaml.v3 as a dependency**

Run:

```bash
go get gopkg.in/yaml.v3
```

- [ ] **Step 2: Write failing tests**

Write `internal/config/config_test.go`:

```go
package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveAndLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")

	in := File{
		"default": Profile{
			Subdomain: "acme",
			Region:    "us",
			BaseURL:   "https://acme.api.kandji.io/api/v1",
		},
	}

	if err := Save(path, in); err != nil {
		t.Fatalf("save: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected mode 0600, got %o", info.Mode().Perm())
	}

	out, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if out["default"].Subdomain != "acme" {
		t.Fatalf("got %+v", out)
	}
	if out["default"].BaseURL != "https://acme.api.kandji.io/api/v1" {
		t.Fatalf("got %+v", out)
	}
}

func TestSaveCreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "child", "config.yml")

	if err := Save(path, File{"default": Profile{Subdomain: "x", Region: "us"}}); err != nil {
		t.Fatalf("save: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file at %s: %v", path, err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.yml"))
	if !os.IsNotExist(err) {
		t.Fatalf("expected os.IsNotExist, got %v", err)
	}
}

func TestBuildBaseURL(t *testing.T) {
	cases := []struct {
		sub, region, want string
	}{
		{"acme", "us", "https://acme.api.kandji.io/api/v1"},
		{"acme", "eu", "https://acme.api.eu.kandji.io/api/v1"},
	}
	for _, c := range cases {
		got, err := BuildBaseURL(c.sub, c.region)
		if err != nil {
			t.Fatalf("BuildBaseURL(%q,%q): %v", c.sub, c.region, err)
		}
		if got != c.want {
			t.Fatalf("BuildBaseURL(%q,%q)=%q want %q", c.sub, c.region, got, c.want)
		}
	}
}

func TestBuildBaseURLRejectsBadInput(t *testing.T) {
	if _, err := BuildBaseURL("", "us"); err == nil {
		t.Fatal("expected error for empty subdomain")
	}
	if _, err := BuildBaseURL("acme", "apac"); err == nil {
		t.Fatal("expected error for invalid region")
	}
	if _, err := BuildBaseURL("Bad_Sub", "us"); err == nil {
		t.Fatal("expected error for invalid subdomain characters")
	}
}
```

- [ ] **Step 3: Run tests to confirm they fail**

Run:

```bash
go test ./internal/config/...
```

Expected: compile failure on the package not existing.

- [ ] **Step 4: Implement the package**

Write `internal/config/config.go`:

```go
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"gopkg.in/yaml.v3"
)

type Profile struct {
	Subdomain string `yaml:"subdomain"`
	Region    string `yaml:"region"`
	BaseURL   string `yaml:"base_url"`
}

// File maps profile name to its configuration. v1 only honours "default".
type File map[string]Profile

var subdomainRe = regexp.MustCompile(`^[a-z0-9-]+$`)

// DefaultPath returns ~/.config/jellyfish/config.yml.
func DefaultPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "jellyfish", "config.yml"), nil
}

// Load reads and parses the YAML file at path.
func Load(path string) (File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f File
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	return f, nil
}

// Save writes the file with mode 0600, creating parent directories with 0700 if needed.
func Save(path string, f File) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	data, err := yaml.Marshal(f)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(path, data, 0o600)
}

// BuildBaseURL derives the Iru API base URL from subdomain + region.
func BuildBaseURL(subdomain, region string) (string, error) {
	if !subdomainRe.MatchString(subdomain) {
		return "", errors.New("subdomain must match [a-z0-9-]+")
	}
	switch region {
	case "us":
		return fmt.Sprintf("https://%s.api.kandji.io/api/v1", subdomain), nil
	case "eu":
		return fmt.Sprintf("https://%s.api.eu.kandji.io/api/v1", subdomain), nil
	default:
		return "", fmt.Errorf("unsupported region %q (want us or eu)", region)
	}
}
```

- [ ] **Step 5: Run tests to confirm they pass**

Run:

```bash
go test ./internal/config/...
```

Expected: all tests pass.

- [ ] **Step 6: Stage and commit**

```bash
git add internal/config/ go.mod go.sum
git commit -m "feat: add config package with load, save and base URL derivation"
```

---

### Task 5: Keychain wrapper

**Files:**

- Create: `internal/keychain/keychain_darwin.go`
- Create: `internal/keychain/keychain_other.go`
- Create: `internal/keychain/keychain.go`
- Test: `internal/keychain/keychain_darwin_test.go`

- [ ] **Step 1: Add the keychain library**

Run:

```bash
go get github.com/keybase/go-keychain
```

- [ ] **Step 2: Write the shared declaration**

Write `internal/keychain/keychain.go`:

```go
package keychain

// Service is the macOS Keychain service identifier used by jellyfish.
const Service = "au.com.flipgroup.jellyfish"

// ErrNotFound is returned when no item exists for the given account.
type notFoundError struct{}

func (notFoundError) Error() string { return "keychain item not found" }

// ErrNotFound is the sentinel callers can compare against with errors.Is.
var ErrNotFound = notFoundError{}
```

- [ ] **Step 3: Write the darwin implementation**

Write `internal/keychain/keychain_darwin.go`:

```go
//go:build darwin

package keychain

import (
	"errors"

	kc "github.com/keybase/go-keychain"
)

// Get returns the stored token for account, or ErrNotFound.
func Get(account string) (string, error) {
	q := kc.NewItem()
	q.SetSecClass(kc.SecClassGenericPassword)
	q.SetService(Service)
	q.SetAccount(account)
	q.SetMatchLimit(kc.MatchLimitOne)
	q.SetReturnData(true)

	results, err := kc.QueryItem(q)
	if err != nil {
		return "", err
	}
	if len(results) == 0 {
		return "", ErrNotFound
	}
	return string(results[0].Data), nil
}

// Set writes (or replaces) the token for account.
func Set(account, token string) error {
	item := kc.NewItem()
	item.SetSecClass(kc.SecClassGenericPassword)
	item.SetService(Service)
	item.SetAccount(account)
	item.SetData([]byte(token))
	item.SetSynchronizable(kc.SynchronizableNo)
	item.SetAccessible(kc.AccessibleWhenUnlocked)

	err := kc.AddItem(item)
	if errors.Is(err, kc.ErrorDuplicateItem) {
		// Replace the existing item.
		query := kc.NewItem()
		query.SetSecClass(kc.SecClassGenericPassword)
		query.SetService(Service)
		query.SetAccount(account)
		if delErr := kc.DeleteItem(query); delErr != nil {
			return delErr
		}
		return kc.AddItem(item)
	}
	return err
}

// Delete removes the token for account. Returns nil if it did not exist.
func Delete(account string) error {
	q := kc.NewItem()
	q.SetSecClass(kc.SecClassGenericPassword)
	q.SetService(Service)
	q.SetAccount(account)
	err := kc.DeleteItem(q)
	if errors.Is(err, kc.ErrorItemNotFound) {
		return nil
	}
	return err
}
```

- [ ] **Step 4: Write the non-darwin stub so the package still type-checks elsewhere**

Write `internal/keychain/keychain_other.go`:

```go
//go:build !darwin

package keychain

import "errors"

var errUnsupported = errors.New("keychain is only supported on macOS")

func Get(account string) (string, error)      { return "", errUnsupported }
func Set(account, token string) error          { return errUnsupported }
func Delete(account string) error              { return errUnsupported }
```

- [ ] **Step 5: Write the darwin-only integration test**

Write `internal/keychain/keychain_darwin_test.go`:

```go
//go:build darwin

package keychain

import (
	"errors"
	"os"
	"testing"
	"time"
)

// Real-Keychain tests are gated. CI macOS runners can opt in.
func skipIfNoKeychain(t *testing.T) {
	if os.Getenv("JELLYFISH_KEYCHAIN_TESTS") != "1" {
		t.Skip("set JELLYFISH_KEYCHAIN_TESTS=1 to run macOS Keychain integration tests")
	}
}

func TestRoundTrip(t *testing.T) {
	skipIfNoKeychain(t)
	account := "jellyfish-test-" + time.Now().Format("150405.000000")
	t.Cleanup(func() { _ = Delete(account) })

	if err := Set(account, "secret-1"); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, err := Get(account)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != "secret-1" {
		t.Fatalf("got %q want %q", got, "secret-1")
	}

	if err := Set(account, "secret-2"); err != nil {
		t.Fatalf("replace: %v", err)
	}
	got2, err := Get(account)
	if err != nil {
		t.Fatalf("get after replace: %v", err)
	}
	if got2 != "secret-2" {
		t.Fatalf("got %q want %q", got2, "secret-2")
	}
}

func TestGetMissing(t *testing.T) {
	skipIfNoKeychain(t)
	_, err := Get("definitely-not-set-" + time.Now().Format("150405.000000"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
```

- [ ] **Step 6: Run the unit tests (the gated ones will skip)**

Run:

```bash
go test ./internal/keychain/...
```

Expected: passes with the integration tests skipped.

- [ ] **Step 7: Optionally run the real-Keychain tests once locally**

Run:

```bash
JELLYFISH_KEYCHAIN_TESTS=1 go test ./internal/keychain/... -count=1
```

Expected: passes (you may be prompted by macOS to allow the test binary access to the Keychain). Approve once and continue.

- [ ] **Step 8: Stage and commit**

```bash
git add internal/keychain/ go.mod go.sum
git commit -m "feat: add macOS Keychain wrapper with darwin/non-darwin split"
```

---

## Phase 3: Output renderers

### Task 6: Renderer interface and JSON renderer

**Files:**

- Create: `internal/output/output.go`
- Create: `internal/output/json.go`
- Test: `internal/output/json_test.go`

- [ ] **Step 1: Write the failing test**

Write `internal/output/json_test.go`:

```go
package output

import (
	"bytes"
	"strings"
	"testing"
)

func TestJSONRendererSimpleStruct(t *testing.T) {
	r := JSON()
	buf := &bytes.Buffer{}
	type item struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	if err := r.Render(buf, item{Name: "rover", Age: 4}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, `"name": "rover"`) {
		t.Fatalf("bad JSON output: %q", out)
	}
	if !strings.HasSuffix(out, "\n") {
		t.Fatalf("expected trailing newline, got %q", out)
	}
}
```

- [ ] **Step 2: Run to confirm failure**

Run:

```bash
go test ./internal/output/...
```

Expected: compile failure on package not existing.

- [ ] **Step 3: Define the interface**

Write `internal/output/output.go`:

```go
package output

import (
	"fmt"
	"io"
)

// Renderer writes a value v to w in a specific format.
type Renderer interface {
	Render(w io.Writer, v any) error
}

// For renders a value v to w using the renderer chosen by format.
// Supported formats: table, json, yaml, csv.
func For(format string) (Renderer, error) {
	switch format {
	case "json":
		return JSON(), nil
	case "yaml":
		return YAML(), nil
	case "csv":
		return CSV(), nil
	case "table", "":
		return Table(), nil
	default:
		return nil, fmt.Errorf("unsupported output format %q", format)
	}
}
```

Note: `Table()`, `YAML()` and `CSV()` will be added in later tasks. For this task, comment out those cases or temporarily return `JSON()` for them. The full switch is restored as each renderer is implemented.

To keep the compile clean now, replace the switch body with:

```go
	switch format {
	case "json", "":
		return JSON(), nil
	default:
		return nil, fmt.Errorf("unsupported output format %q", format)
	}
```

The other branches will be re-added in Tasks 7-9.

- [ ] **Step 4: Implement the JSON renderer**

Write `internal/output/json.go`:

```go
package output

import (
	"encoding/json"
	"io"
)

type jsonRenderer struct{}

func JSON() Renderer { return jsonRenderer{} }

func (jsonRenderer) Render(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
```

- [ ] **Step 5: Run to confirm pass**

Run:

```bash
go test ./internal/output/...
```

Expected: pass.

- [ ] **Step 6: Stage and commit**

```bash
git add internal/output/
git commit -m "feat: add output renderer interface and JSON renderer"
```

---

### Task 7: YAML renderer

**Files:**

- Create: `internal/output/yaml.go`
- Test: `internal/output/yaml_test.go`
- Modify: `internal/output/output.go` (uncomment yaml case)

- [ ] **Step 1: Write the failing test**

Write `internal/output/yaml_test.go`:

```go
package output

import (
	"bytes"
	"strings"
	"testing"
)

func TestYAMLRendererSimpleStruct(t *testing.T) {
	r := YAML()
	buf := &bytes.Buffer{}
	type item struct {
		Name string `yaml:"name"`
		Age  int    `yaml:"age"`
	}
	if err := r.Render(buf, item{Name: "rover", Age: 4}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "name: rover") {
		t.Fatalf("bad YAML output: %q", out)
	}
	if !strings.Contains(out, "age: 4") {
		t.Fatalf("bad YAML output: %q", out)
	}
}
```

- [ ] **Step 2: Run to confirm failure**

Run:

```bash
go test ./internal/output/...
```

Expected: compile failure on `YAML` undefined.

- [ ] **Step 3: Implement the YAML renderer**

Write `internal/output/yaml.go`:

```go
package output

import (
	"io"

	"gopkg.in/yaml.v3"
)

type yamlRenderer struct{}

func YAML() Renderer { return yamlRenderer{} }

func (yamlRenderer) Render(w io.Writer, v any) error {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	defer enc.Close()
	return enc.Encode(v)
}
```

- [ ] **Step 4: Re-add the yaml branch in `For`**

Edit `internal/output/output.go` switch:

```go
	switch format {
	case "json":
		return JSON(), nil
	case "yaml":
		return YAML(), nil
	case "":
		return JSON(), nil // temporary default until Table renderer lands
	default:
		return nil, fmt.Errorf("unsupported output format %q", format)
	}
```

- [ ] **Step 5: Run tests**

Run:

```bash
go test ./internal/output/...
```

Expected: pass.

- [ ] **Step 6: Stage and commit**

```bash
git add internal/output/
git commit -m "feat: add YAML output renderer"
```

---

### Task 8: Table renderer

**Files:**

- Create: `internal/output/table.go`
- Test: `internal/output/table_test.go`
- Modify: `internal/output/output.go` (add table case, restore default)

- [ ] **Step 1: Add the table library**

Run:

```bash
go get github.com/jedib0t/go-pretty/v6
```

- [ ] **Step 2: Write the failing test**

Write `internal/output/table_test.go`:

```go
package output

import (
	"bytes"
	"strings"
	"testing"
)

type tableItem struct {
	Name string
	Age  int
}

func TestTableRendererSliceWithColumns(t *testing.T) {
	rows := []tableItem{
		{Name: "rover", Age: 4},
		{Name: "spot", Age: 7},
	}

	r := Table().WithColumns([]Column{
		{Header: "NAME", Extract: func(v any) string { return v.(tableItem).Name }},
		{Header: "AGE", Extract: func(v any) string { return intStr(v.(tableItem).Age) }},
	})

	buf := &bytes.Buffer{}
	if err := r.Render(buf, rows); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"NAME", "AGE", "rover", "spot", "4", "7"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q in output:\n%s", want, out)
		}
	}
}

func TestTableRendererSingleStruct(t *testing.T) {
	r := Table().WithColumns([]Column{
		{Header: "NAME", Extract: func(v any) string { return v.(tableItem).Name }},
		{Header: "AGE", Extract: func(v any) string { return intStr(v.(tableItem).Age) }},
	})

	buf := &bytes.Buffer{}
	if err := r.Render(buf, tableItem{Name: "rover", Age: 4}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "rover") {
		t.Fatalf("expected name in output, got: %s", buf.String())
	}
}

func intStr(i int) string { return fmtItoa(i) }

func fmtItoa(i int) string {
	// avoid importing strconv at the top of this file
	return strconvItoa(i)
}
```

That last helper looks silly. Replace those three helpers with a single import of `strconv` at the top of the test file:

```go
import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func intStr(i int) string { return strconv.Itoa(i) }
```

and remove `fmtItoa` and `strconvItoa`.

- [ ] **Step 3: Run to confirm failure**

Run:

```bash
go test ./internal/output/...
```

Expected: compile failure on `Table`, `Column`, `WithColumns` undefined.

- [ ] **Step 4: Implement the table renderer**

Write `internal/output/table.go`:

```go
package output

import (
	"errors"
	"io"
	"reflect"

	"github.com/jedib0t/go-pretty/v6/table"
)

// Column declares one column in a table render. Extract is called per row.
type Column struct {
	Header  string
	Extract func(v any) string
}

type tableRenderer struct {
	columns []Column
}

// Table returns a renderer that needs WithColumns called before Render.
func Table() *tableRenderer { return &tableRenderer{} }

// WithColumns returns a renderer configured for the given columns.
func (r *tableRenderer) WithColumns(cols []Column) *tableRenderer {
	r.columns = cols
	return r
}

// Render writes v as a table. v must be a struct or a slice of structs.
func (r *tableRenderer) Render(w io.Writer, v any) error {
	if len(r.columns) == 0 {
		return errors.New("table renderer requires WithColumns before Render")
	}

	t := table.NewWriter()
	t.SetOutputMirror(w)

	header := make(table.Row, len(r.columns))
	for i, c := range r.columns {
		header[i] = c.Header
	}
	t.AppendHeader(header)

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			t.AppendRow(r.rowFor(rv.Index(i).Interface()))
		}
	default:
		t.AppendRow(r.rowFor(v))
	}

	t.Render()
	return nil
}

func (r *tableRenderer) rowFor(v any) table.Row {
	row := make(table.Row, len(r.columns))
	for i, c := range r.columns {
		row[i] = c.Extract(v)
	}
	return row
}
```

- [ ] **Step 5: Restore the table case in `For`**

Edit `internal/output/output.go`:

```go
func For(format string) (Renderer, error) {
	switch format {
	case "json":
		return JSON(), nil
	case "yaml":
		return YAML(), nil
	case "table", "":
		// Table requires columns per-command; callers should construct it directly.
		return Table(), nil
	default:
		return nil, fmt.Errorf("unsupported output format %q", format)
	}
}
```

Note: For the `table` default case, the command handler must call `WithColumns` before `Render`. The `For` helper exists for `vulns` and `user` to look up json/yaml/csv; callers wanting tables build a `Table().WithColumns(...)` directly. This keeps `For`'s API simple while keeping table rendering per-command typed.

- [ ] **Step 6: Run tests**

Run:

```bash
go test ./internal/output/...
```

Expected: pass.

- [ ] **Step 7: Stage and commit**

```bash
git add internal/output/ go.mod go.sum
git commit -m "feat: add table renderer with per-command Column projections"
```

---

### Task 9: CSV renderer

**Files:**

- Create: `internal/output/csv.go`
- Test: `internal/output/csv_test.go`
- Modify: `internal/output/output.go` (add csv case)

- [ ] **Step 1: Write the failing test**

Write `internal/output/csv_test.go`:

```go
package output

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func TestCSVRendererSliceWithColumns(t *testing.T) {
	rows := []tableItem{
		{Name: "rover", Age: 4},
		{Name: "spot", Age: 7},
	}

	r := CSV().WithColumns([]Column{
		{Header: "name", Extract: func(v any) string { return v.(tableItem).Name }},
		{Header: "age", Extract: func(v any) string { return strconv.Itoa(v.(tableItem).Age) }},
	})

	buf := &bytes.Buffer{}
	if err := r.Render(buf, rows); err != nil {
		t.Fatal(err)
	}

	want := "name,age\nrover,4\nspot,7\n"
	if buf.String() != want {
		t.Fatalf("got %q want %q", buf.String(), want)
	}
}

func TestCSVRendererEscapesCommas(t *testing.T) {
	r := CSV().WithColumns([]Column{
		{Header: "v", Extract: func(v any) string { return v.(string) }},
	})
	buf := &bytes.Buffer{}
	if err := r.Render(buf, []string{"a,b"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"a,b"`) {
		t.Fatalf("expected quoting, got %q", buf.String())
	}
}
```

- [ ] **Step 2: Run to confirm failure**

Run:

```bash
go test ./internal/output/...
```

Expected: compile failure on `CSV` undefined.

- [ ] **Step 3: Implement the CSV renderer**

Write `internal/output/csv.go`:

```go
package output

import (
	"encoding/csv"
	"errors"
	"io"
	"reflect"
)

type csvRenderer struct {
	columns []Column
}

func CSV() *csvRenderer { return &csvRenderer{} }

func (r *csvRenderer) WithColumns(cols []Column) *csvRenderer {
	r.columns = cols
	return r
}

func (r *csvRenderer) Render(w io.Writer, v any) error {
	if len(r.columns) == 0 {
		return errors.New("csv renderer requires WithColumns before Render")
	}

	cw := csv.NewWriter(w)
	defer cw.Flush()

	header := make([]string, len(r.columns))
	for i, c := range r.columns {
		header[i] = c.Header
	}
	if err := cw.Write(header); err != nil {
		return err
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			if err := r.writeRow(cw, rv.Index(i).Interface()); err != nil {
				return err
			}
		}
	default:
		if err := r.writeRow(cw, v); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func (r *csvRenderer) writeRow(cw *csv.Writer, v any) error {
	row := make([]string, len(r.columns))
	for i, c := range r.columns {
		row[i] = c.Extract(v)
	}
	return cw.Write(row)
}
```

- [ ] **Step 4: Add the csv case in `For`**

Edit `internal/output/output.go`:

```go
func For(format string) (Renderer, error) {
	switch format {
	case "json":
		return JSON(), nil
	case "yaml":
		return YAML(), nil
	case "csv":
		return CSV(), nil
	case "table", "":
		return Table(), nil
	default:
		return nil, fmt.Errorf("unsupported output format %q", format)
	}
}
```

- [ ] **Step 5: Run tests**

Run:

```bash
go test ./internal/output/...
```

Expected: pass.

- [ ] **Step 6: Stage and commit**

```bash
git add internal/output/
git commit -m "feat: add CSV output renderer"
```

---

## Phase 4: Iru API client

> **Reality check before starting Phase 4:** Hit `/api/v1/devices?limit=1` against the real Iru API with curl (using a real token) to confirm field names. Repeat for `/users` and `/vulnerability-management/detections`. If a field name differs from what is below, update the struct tag in that task and carry the correction forward.

### Task 10: Client constructor and transport

**Files:**

- Create: `internal/iru/client.go`
- Create: `internal/iru/transport.go`
- Test: `internal/iru/client_test.go`

- [ ] **Step 1: Write the failing test**

Write `internal/iru/client_test.go`:

```go
package iru

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientSetsAuthAndUserAgent(t *testing.T) {
	var got *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn-abc", WithUserAgent("jellyfish/test"))
	if err := c.do(context.Background(), http.MethodGet, "/ping", nil, nil); err != nil {
		t.Fatalf("do: %v", err)
	}

	if got == nil {
		t.Fatal("no request captured")
	}
	if h := got.Header.Get("Authorization"); h != "Bearer tkn-abc" {
		t.Fatalf("auth header %q", h)
	}
	if h := got.Header.Get("User-Agent"); !strings.HasPrefix(h, "jellyfish/") {
		t.Fatalf("user-agent header %q", h)
	}
}

func TestClientHonoursContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn", WithTimeout(50*time.Millisecond))
	err := c.do(context.Background(), http.MethodGet, "/slow", nil, nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}
```

- [ ] **Step 2: Run to confirm failure**

Run:

```bash
go test ./internal/iru/...
```

Expected: compile failure on package not existing.

- [ ] **Step 3: Implement the client**

Write `internal/iru/client.go`:

```go
package iru

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Client talks to the Iru/Kandji API.
type Client struct {
	baseURL    string
	httpClient *http.Client
	userAgent  string
}

// Option configures a Client at construction time.
type Option func(*Client)

func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.httpClient = h } }
func WithUserAgent(ua string) Option       { return func(c *Client) { c.userAgent = ua } }
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if c.httpClient == nil {
			c.httpClient = &http.Client{}
		}
		c.httpClient.Timeout = d
	}
}

// NewClient constructs a Client. baseURL must end with /api/v1 (no trailing slash).
func NewClient(baseURL, token string, opts ...Option) *Client {
	c := &Client{
		baseURL:   baseURL,
		userAgent: "jellyfish/dev",
		httpClient: &http.Client{
			Transport: &authTransport{token: token, base: http.DefaultTransport},
			Timeout:   30 * time.Second,
		},
	}
	for _, o := range opts {
		o(c)
	}
	// Preserve the auth transport when callers swap in their own http.Client.
	if c.httpClient.Transport == nil {
		c.httpClient.Transport = &authTransport{token: token, base: http.DefaultTransport}
	}
	return c
}

// do builds and sends a request. If out is non-nil and the response status is 2xx,
// the body is JSON-decoded into out.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, out any) error {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	var body io.Reader
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return decodeAPIError(resp)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(out); err != nil {
		raw, _ := io.ReadAll(io.MultiReader(bytes.NewReader([]byte{}), resp.Body))
		return fmt.Errorf("decode response: %w (raw=%q)", err, string(raw))
	}
	return nil
}
```

Write `internal/iru/transport.go`:

```go
package iru

import "net/http"

// authTransport adds Authorization: Bearer <token> to every request.
// It does not log the token.
type authTransport struct {
	token string
	base  http.RoundTripper
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(r)
}
```

Note: `decodeAPIError` is implemented in Task 11. For now, write a temporary stub so this compiles:

Append to `internal/iru/client.go` (will be replaced in Task 11):

```go
func decodeAPIError(resp *http.Response) error {
	return fmt.Errorf("api error: status %d", resp.StatusCode)
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
go test ./internal/iru/...
```

Expected: pass.

- [ ] **Step 5: Stage and commit**

```bash
git add internal/iru/
git commit -m "feat: add iru.Client constructor and auth transport"
```

---

### Task 11: Typed errors

**Files:**

- Create: `internal/iru/errors.go`
- Modify: `internal/iru/client.go` (remove stub `decodeAPIError`)
- Test: `internal/iru/errors_test.go`

- [ ] **Step 1: Write the failing tests**

Write `internal/iru/errors_test.go`:

```go
package iru

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestErrorMapping(t *testing.T) {
	cases := []struct {
		status int
		want   error
	}{
		{http.StatusUnauthorized, ErrUnauthorized},
		{http.StatusForbidden, ErrForbidden},
		{http.StatusNotFound, ErrNotFound},
		{http.StatusTooManyRequests, ErrRateLimited},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(c.status)
			_, _ = w.Write([]byte(`{"detail":"nope"}`))
		}))
		client := NewClient(srv.URL, "tkn")
		err := client.do(context.Background(), http.MethodGet, "/x", nil, nil)
		srv.Close()
		if !errors.Is(err, c.want) {
			t.Fatalf("status %d: expected %v, got %v", c.status, c.want, err)
		}
	}
}

func TestAPIErrorCarriesStatusAndMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"detail":"bad subdomain"}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	err := c.do(context.Background(), http.MethodGet, "/x", nil, nil)

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %v", err)
	}
	if apiErr.Status != 400 {
		t.Fatalf("status: %d", apiErr.Status)
	}
	if apiErr.Message == "" {
		t.Fatalf("expected non-empty message")
	}
}
```

- [ ] **Step 2: Run to confirm failure**

Run:

```bash
go test ./internal/iru/...
```

Expected: compile failures on `ErrUnauthorized`, `APIError` etc.

- [ ] **Step 3: Implement errors**

Write `internal/iru/errors.go`:

```go
package iru

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// Sentinel errors callers compare against with errors.Is.
var (
	ErrUnauthorized = errors.New("iru: unauthorized")
	ErrForbidden    = errors.New("iru: forbidden")
	ErrNotFound     = errors.New("iru: not found")
	ErrRateLimited  = errors.New("iru: rate limited")
)

// APIError is the typed error returned for non-2xx responses that do not map
// to a sentinel.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("iru: api error: status %d", e.Status)
	}
	return fmt.Sprintf("iru: api error: status %d: %s", e.Status, e.Message)
}

// Is lets callers use errors.Is to recover the sentinel for well-known statuses.
func (e *APIError) Is(target error) bool {
	switch e.Status {
	case http.StatusUnauthorized:
		return target == ErrUnauthorized
	case http.StatusForbidden:
		return target == ErrForbidden
	case http.StatusNotFound:
		return target == ErrNotFound
	case http.StatusTooManyRequests:
		return target == ErrRateLimited
	}
	return false
}

// decodeAPIError reads the response body and produces an APIError.
func decodeAPIError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	apiErr := &APIError{Status: resp.StatusCode}

	// Iru typically returns {"detail":"..."} or {"errors":[...]} - try both.
	var payload struct {
		Detail string `json:"detail"`
		Code   string `json:"code"`
	}
	if err := json.Unmarshal(body, &payload); err == nil {
		apiErr.Message = payload.Detail
		apiErr.Code = payload.Code
	}
	if apiErr.Message == "" && len(body) > 0 && len(body) < 512 {
		apiErr.Message = string(body)
	}
	return apiErr
}
```

- [ ] **Step 4: Remove the stub `decodeAPIError` from `client.go`**

Delete the temporary stub added in Task 10.

- [ ] **Step 5: Run tests**

Run:

```bash
go test ./internal/iru/...
```

Expected: pass.

- [ ] **Step 6: Stage and commit**

```bash
git add internal/iru/
git commit -m "feat: add typed APIError and sentinel errors for iru client"
```

---

### Task 12: Retry transport

**Files:**

- Create: `internal/iru/retry.go`
- Modify: `internal/iru/client.go` (wire retryTransport between authTransport and base)
- Test: `internal/iru/retry_test.go`

- [ ] **Step 1: Write the failing test**

Write `internal/iru/retry_test.go`:

```go
package iru

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestRetriesOn429ThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := calls.Add(1)
		if n < 3 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	if err := c.do(context.Background(), http.MethodGet, "/x", nil, nil); err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
}

func TestRetriesGiveUpAfterMaxAttempts(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	err := c.do(context.Background(), http.MethodGet, "/x", nil, nil)
	if err == nil {
		t.Fatal("expected failure after exhausting retries")
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
}
```

- [ ] **Step 2: Run to confirm failure**

Run:

```bash
go test ./internal/iru/...
```

Expected: tests fail because retries do not happen yet.

- [ ] **Step 3: Implement the retry transport**

Write `internal/iru/retry.go`:

```go
package iru

import (
	"net/http"
	"strconv"
	"time"
)

const maxAttempts = 3

// retryTransport retries idempotent requests on 429 and 5xx with backoff.
type retryTransport struct {
	base http.RoundTripper
	// sleep allows tests to inject a no-op sleeper.
	sleep func(time.Duration)
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.sleep == nil {
		t.sleep = time.Sleep
	}
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return t.base.RoundTrip(req)
	}

	var lastResp *http.Response
	var lastErr error
	backoff := 200 * time.Millisecond

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp, err := t.base.RoundTrip(req.Clone(req.Context()))
		if err != nil {
			lastErr = err
			t.sleep(backoff)
			backoff *= 2
			continue
		}
		if !shouldRetry(resp.StatusCode) {
			return resp, nil
		}
		// Drain and close so the conn can be reused.
		_ = resp.Body.Close()
		lastResp = resp

		wait := backoff
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil {
				wait = time.Duration(secs) * time.Second
			}
		}
		if attempt < maxAttempts {
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-time.After(wait):
			}
			backoff *= 2
		}
	}

	if lastResp != nil {
		return lastResp, nil
	}
	return nil, lastErr
}

func shouldRetry(status int) bool {
	if status == http.StatusTooManyRequests {
		return true
	}
	return status >= 500 && status <= 599
}
```

- [ ] **Step 4: Wire retryTransport into NewClient**

Edit `internal/iru/client.go`:

Change the construction of the `Transport` in `NewClient` to stack retry on top of auth:

```go
		httpClient: &http.Client{
			Transport: &authTransport{
				token: token,
				base:  &retryTransport{base: http.DefaultTransport},
			},
			Timeout: 30 * time.Second,
		},
```

And update the trailing default-transport branch the same way:

```go
	if c.httpClient.Transport == nil {
		c.httpClient.Transport = &authTransport{
			token: token,
			base:  &retryTransport{base: http.DefaultTransport},
		}
	}
```

Note: the retry test above uses `Retry-After: 0` which keeps the test fast; the real production sleeps are still in place.

- [ ] **Step 5: Run tests**

Run:

```bash
go test ./internal/iru/... -count=1
```

Expected: pass.

- [ ] **Step 6: Stage and commit**

```bash
git add internal/iru/
git commit -m "feat: add retry transport for 429 and 5xx with Retry-After support"
```

---

### Task 13: Generic paginator

**Files:**

- Create: `internal/iru/paginator.go`
- Test: `internal/iru/paginator_test.go`

- [ ] **Step 1: Write the failing test**

Write `internal/iru/paginator_test.go`:

```go
package iru

import (
	"context"
	"testing"
)

func TestWalkPaginates(t *testing.T) {
	pages := [][]int{
		{1, 2, 3, 4, 5},
		{6, 7, 8, 9, 10},
		{11, 12}, // short page signals end
	}

	var seenLimits []int
	var seenOffsets []int
	fetch := func(_ context.Context, limit, offset int) ([]int, error) {
		seenLimits = append(seenLimits, limit)
		seenOffsets = append(seenOffsets, offset)
		idx := offset / limit
		if idx >= len(pages) {
			return nil, nil
		}
		return pages[idx], nil
	}

	var collected []int
	err := Walk[int](context.Background(), 5, fetch, func(page []int) error {
		collected = append(collected, page...)
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	want := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	if len(collected) != len(want) {
		t.Fatalf("got %v want %v", collected, want)
	}
	for i, v := range want {
		if collected[i] != v {
			t.Fatalf("idx %d: got %d want %d", i, collected[i], v)
		}
	}
	if len(seenOffsets) != 3 {
		t.Fatalf("expected 3 fetches, got %d (offsets=%v)", len(seenOffsets), seenOffsets)
	}
}

func TestWalkStopsOnCallbackError(t *testing.T) {
	fetch := func(_ context.Context, limit, offset int) ([]int, error) {
		return []int{1, 2, 3, 4, 5}, nil
	}
	var calls int
	err := Walk[int](context.Background(), 5, fetch, func(page []int) error {
		calls++
		return context.Canceled
	})
	if err == nil {
		t.Fatal("expected error from callback to propagate")
	}
	if calls != 1 {
		t.Fatalf("expected 1 callback invocation, got %d", calls)
	}
}
```

- [ ] **Step 2: Run to confirm failure**

Run:

```bash
go test ./internal/iru/...
```

Expected: compile failure on `Walk` undefined.

- [ ] **Step 3: Implement Walk**

Write `internal/iru/paginator.go`:

```go
package iru

import "context"

// DefaultLimit is the page size used when callers do not specify one. Iru caps
// limit at 300 server-side; this matches that.
const DefaultLimit = 300

// Walk paginates limit/offset style until the fetch returns a short page.
//
// limit must be > 0. fetch is called once per page. cb is called with each page
// of results; if cb returns an error, the walk stops and that error is returned.
func Walk[T any](
	ctx context.Context,
	limit int,
	fetch func(ctx context.Context, limit, offset int) ([]T, error),
	cb func(page []T) error,
) error {
	if limit <= 0 {
		limit = DefaultLimit
	}
	for offset := 0; ; offset += limit {
		page, err := fetch(ctx, limit, offset)
		if err != nil {
			return err
		}
		if len(page) > 0 {
			if err := cb(page); err != nil {
				return err
			}
		}
		if len(page) < limit {
			return nil
		}
	}
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
go test ./internal/iru/...
```

Expected: pass.

- [ ] **Step 5: Stage and commit**

```bash
git add internal/iru/
git commit -m "feat: add generic limit/offset paginator"
```

---

### Task 14: Devices endpoints

**Files:**

- Create: `internal/iru/devices.go`
- Create: `internal/iru/types.go`
- Test: `internal/iru/devices_test.go`

- [ ] **Step 1: Write the failing test**

Write `internal/iru/devices_test.go`:

```go
package iru

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListDevicesPagePassesQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("limit") != "50" {
			t.Errorf("limit %q", q.Get("limit"))
		}
		if q.Get("offset") != "100" {
			t.Errorf("offset %q", q.Get("offset"))
		}
		if q.Get("user_id") != "u-1" {
			t.Errorf("user_id %q", q.Get("user_id"))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"device_id":"d-1","device_name":"Keith's Mac","serial_number":"SN1","user":{"id":"u-1","email":"k@x"}}]`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	got, err := c.ListDevicesPage(context.Background(), DeviceFilters{UserID: "u-1"}, 50, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].DeviceID != "d-1" {
		t.Fatalf("got %+v", got)
	}
}

func TestListDevicesAutoPaginates(t *testing.T) {
	var page int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page++
		w.WriteHeader(http.StatusOK)
		// Two full pages of 300 then a short page of 2.
		var n int
		switch page {
		case 1, 2:
			n = 300
		default:
			n = 2
		}
		devices := make([]map[string]string, n)
		for i := range devices {
			devices[i] = map[string]string{"device_id": fmt.Sprintf("d-%d-%d", page, i)}
		}
		_ = json.NewEncoder(w).Encode(devices)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	got, err := c.ListDevices(context.Background(), DeviceFilters{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 602 {
		t.Fatalf("expected 602 devices, got %d", len(got))
	}
}

func TestGetDeviceBySerial(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("serial_number") != "SN9" {
			t.Errorf("serial filter not propagated: %q", r.URL.Query().Get("serial_number"))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"device_id":"d-9","serial_number":"SN9"}]`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	d, err := c.GetDeviceBySerial(context.Background(), "SN9")
	if err != nil {
		t.Fatal(err)
	}
	if d.DeviceID != "d-9" {
		t.Fatalf("got %+v", d)
	}
}

func TestGetDeviceBySerialNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	_, err := c.GetDeviceBySerial(context.Background(), "SN-missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
```

- [ ] **Step 2: Run to confirm failure**

Run:

```bash
go test ./internal/iru/...
```

Expected: compile failures on `ListDevicesPage`, `ListDevices`, `GetDeviceBySerial`, `DeviceFilters`, `Device`.

- [ ] **Step 3: Implement types**

Write `internal/iru/types.go`:

```go
package iru

// Device is the subset of Iru's device record that jellyfish uses.
// Field names match Iru's JSON; extras are ignored by encoding/json.
type Device struct {
	DeviceID     string `json:"device_id" yaml:"device_id"`
	DeviceName   string `json:"device_name" yaml:"device_name"`
	SerialNumber string `json:"serial_number" yaml:"serial_number"`
	Model        string `json:"model" yaml:"model"`
	OSVersion    string `json:"os_version" yaml:"os_version"`
	Platform     string `json:"platform" yaml:"platform"`
	BlueprintID  string `json:"blueprint_id" yaml:"blueprint_id"`
	User         User   `json:"user" yaml:"user"`
}

// User is the subset of Iru's user record jellyfish uses.
type User struct {
	ID    string `json:"id" yaml:"id"`
	Name  string `json:"name" yaml:"name"`
	Email string `json:"email" yaml:"email"`
}

// Detection is one vulnerability detection on a device.
type Detection struct {
	DetectionID string `json:"detection_id" yaml:"detection_id"`
	DeviceID    string `json:"device_id" yaml:"device_id"`
	CVE         string `json:"cve" yaml:"cve"`
	Severity    string `json:"severity" yaml:"severity"`
	Status      string `json:"status" yaml:"status"`
	AppName     string `json:"app_name" yaml:"app_name"`
	AppVersion  string `json:"app_version" yaml:"app_version"`
	CreatedAt   string `json:"created_at" yaml:"created_at"`
	UpdatedAt   string `json:"updated_at" yaml:"updated_at"`
}

// DeviceFilters is the filter set for /devices queries.
type DeviceFilters struct {
	UserID       string
	SerialNumber string
}

// DetectionFilters is the filter set for /vulnerability-management/detections queries.
type DetectionFilters struct {
	DeviceID string
	Status   string // pass-through to Iru
}
```

Note: confirm exact field names against the live Iru API before merging Task 14. If `user_id` is a flat field rather than a nested `user` object, adjust `Device` and the test fixtures.

- [ ] **Step 4: Implement devices.go**

Write `internal/iru/devices.go`:

```go
package iru

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
)

// ListDevicesPage fetches one page of devices.
func (c *Client) ListDevicesPage(ctx context.Context, f DeviceFilters, limit, offset int) ([]Device, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	q.Set("offset", strconv.Itoa(offset))
	if f.UserID != "" {
		q.Set("user_id", f.UserID)
	}
	if f.SerialNumber != "" {
		q.Set("serial_number", f.SerialNumber)
	}
	var out []Device
	if err := c.do(ctx, http.MethodGet, "/devices", q, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ListDevices auto-paginates /devices using DefaultLimit.
func (c *Client) ListDevices(ctx context.Context, f DeviceFilters) ([]Device, error) {
	var all []Device
	err := Walk[Device](ctx, DefaultLimit,
		func(ctx context.Context, limit, offset int) ([]Device, error) {
			return c.ListDevicesPage(ctx, f, limit, offset)
		},
		func(page []Device) error {
			all = append(all, page...)
			return nil
		},
	)
	return all, err
}

// GetDeviceBySerial returns the device with the given serial number, or ErrNotFound.
func (c *Client) GetDeviceBySerial(ctx context.Context, serial string) (Device, error) {
	page, err := c.ListDevicesPage(ctx, DeviceFilters{SerialNumber: serial}, 1, 0)
	if err != nil {
		return Device{}, err
	}
	if len(page) == 0 {
		return Device{}, ErrNotFound
	}
	return page[0], nil
}
```

- [ ] **Step 5: Run tests**

Run:

```bash
go test ./internal/iru/...
```

Expected: pass.

- [ ] **Step 6: Stage and commit**

```bash
git add internal/iru/
git commit -m "feat: add devices endpoints with auto-pagination"
```

---

### Task 15: Users endpoints

**Files:**

- Create: `internal/iru/users.go`
- Test: `internal/iru/users_test.go`

- [ ] **Step 1: Write the failing tests**

Write `internal/iru/users_test.go`:

```go
package iru

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetUserByID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/users/u-1") {
			t.Errorf("path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"u-1","name":"Keith","email":"k@x"}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	u, err := c.GetUser(context.Background(), "u-1")
	if err != nil {
		t.Fatal(err)
	}
	if u.ID != "u-1" || u.Email != "k@x" {
		t.Fatalf("got %+v", u)
	}
}

func TestFindUserByEmailScansPages(t *testing.T) {
	var page int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page++
		switch page {
		case 1:
			users := make([]map[string]string, DefaultLimit)
			for i := range users {
				users[i] = map[string]string{"id": fmt.Sprintf("u-%d", i), "email": fmt.Sprintf("a%d@x", i)}
			}
			_ = json.NewEncoder(w).Encode(users)
		case 2:
			_ = json.NewEncoder(w).Encode([]map[string]string{
				{"id": "u-match", "email": "Keith@FlipGroup.com.au"},
			})
		}
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	u, err := c.FindUserByEmail(context.Background(), "keith@flipgroup.com.au")
	if err != nil {
		t.Fatal(err)
	}
	if u.ID != "u-match" {
		t.Fatalf("got %+v", u)
	}
}

func TestFindUserByEmailNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	_, err := c.FindUserByEmail(context.Background(), "nobody@x")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
```

- [ ] **Step 2: Run to confirm failure**

Run:

```bash
go test ./internal/iru/...
```

Expected: compile failures on `GetUser`, `FindUserByEmail`, `ListUsersPage`.

- [ ] **Step 3: Implement users.go**

Write `internal/iru/users.go`:

```go
package iru

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// GetUser fetches a single user by ID.
func (c *Client) GetUser(ctx context.Context, id string) (User, error) {
	var u User
	if err := c.do(ctx, http.MethodGet, "/users/"+url.PathEscape(id), nil, &u); err != nil {
		return User{}, err
	}
	return u, nil
}

// ListUsersPage fetches one page of users.
func (c *Client) ListUsersPage(ctx context.Context, limit, offset int) ([]User, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	q.Set("offset", strconv.Itoa(offset))
	var out []User
	if err := c.do(ctx, http.MethodGet, "/users", q, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// FindUserByEmail walks the user list and returns the first case-insensitive
// email match. Returns ErrNotFound if no user matches.
func (c *Client) FindUserByEmail(ctx context.Context, email string) (User, error) {
	target := strings.ToLower(email)
	var found User
	stop := errors.New("found")
	err := Walk[User](ctx, DefaultLimit,
		c.ListUsersPage,
		func(page []User) error {
			for _, u := range page {
				if strings.ToLower(u.Email) == target {
					found = u
					return stop
				}
			}
			return nil
		},
	)
	if errors.Is(err, stop) {
		return found, nil
	}
	if err != nil {
		return User{}, err
	}
	return User{}, ErrNotFound
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
go test ./internal/iru/...
```

Expected: pass.

- [ ] **Step 5: Stage and commit**

```bash
git add internal/iru/
git commit -m "feat: add user endpoints with case-insensitive email lookup"
```

---

### Task 16: Vulnerability detection endpoints

**Files:**

- Create: `internal/iru/vulnerabilities.go`
- Test: `internal/iru/vulnerabilities_test.go`

- [ ] **Step 1: Write the failing tests**

Write `internal/iru/vulnerabilities_test.go`:

```go
package iru

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListDetectionsPagePassesFilters(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("device_id") != "d-1" {
			t.Errorf("device_id %q", q.Get("device_id"))
		}
		if q.Get("status") != "active" {
			t.Errorf("status %q", q.Get("status"))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"detection_id":"x-1","device_id":"d-1","cve":"CVE-2025-0001","status":"active"}]`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	got, err := c.ListDetectionsPage(context.Background(), DetectionFilters{DeviceID: "d-1", Status: "active"}, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].DetectionID != "x-1" {
		t.Fatalf("got %+v", got)
	}
}

func TestListDetectionsAutoPaginates(t *testing.T) {
	var page int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		page++
		switch page {
		case 1:
			body := "["
			for i := 0; i < 300; i++ {
				if i > 0 {
					body += ","
				}
				body += `{"detection_id":"x"}`
			}
			body += "]"
			_, _ = w.Write([]byte(body))
		default:
			_, _ = w.Write([]byte(`[{"detection_id":"y"}]`))
		}
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "tkn")
	got, err := c.ListDetections(context.Background(), DetectionFilters{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 301 {
		t.Fatalf("expected 301 detections, got %d", len(got))
	}
}
```

- [ ] **Step 2: Run to confirm failure**

Run:

```bash
go test ./internal/iru/...
```

Expected: compile failures on `ListDetections`, `ListDetectionsPage`.

- [ ] **Step 3: Implement vulnerabilities.go**

Write `internal/iru/vulnerabilities.go`:

```go
package iru

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
)

const detectionsPath = "/vulnerability-management/detections"

// ListDetectionsPage fetches one page of detections.
func (c *Client) ListDetectionsPage(ctx context.Context, f DetectionFilters, limit, offset int) ([]Detection, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	q.Set("offset", strconv.Itoa(offset))
	if f.DeviceID != "" {
		q.Set("device_id", f.DeviceID)
	}
	if f.Status != "" {
		q.Set("status", f.Status)
	}
	var out []Detection
	if err := c.do(ctx, http.MethodGet, detectionsPath, q, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ListDetections auto-paginates /vulnerability-management/detections using DefaultLimit.
func (c *Client) ListDetections(ctx context.Context, f DetectionFilters) ([]Detection, error) {
	var all []Detection
	err := Walk[Detection](ctx, DefaultLimit,
		func(ctx context.Context, limit, offset int) ([]Detection, error) {
			return c.ListDetectionsPage(ctx, f, limit, offset)
		},
		func(page []Detection) error {
			all = append(all, page...)
			return nil
		},
	)
	return all, err
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
go test ./internal/iru/...
```

Expected: pass.

- [ ] **Step 5: Stage and commit**

```bash
git add internal/iru/
git commit -m "feat: add vulnerability detections endpoint"
```

---

## Phase 5: CLI commands

### Task 17: `jellyfish configure`

**Files:**

- Create: `cmd/configure.go`
- Create: `cmd/configure_test.go`
- Modify: `cmd/root.go` (register the subcommand)

- [ ] **Step 1: Add the term package**

Run:

```bash
go get golang.org/x/term
```

- [ ] **Step 2: Write the failing test**

Write `cmd/configure_test.go`:

```go
package cmd

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"flipgroup.com.au/jellyfish/internal/config"
)

func TestConfigureWritesConfigAndCallsKeychain(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tkn-123" {
			t.Errorf("auth header %q", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(srv.Close)

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "config.yml")

	var keychainCalls []string
	store := func(_, token string) error {
		keychainCalls = append(keychainCalls, token)
		return nil
	}

	in := strings.NewReader("acme\nus\ntkn-123\n")
	out := &bytes.Buffer{}

	err := runConfigure(context.Background(), configureOpts{
		ConfigPath:     cfgPath,
		Stdin:          in,
		Stdout:         out,
		Stderr:         out,
		StoreToken:     store,
		ReadTokenLine:  func() (string, error) { return "tkn-123", nil },
		VerifyBaseURL:  srv.URL,
	})
	if err != nil {
		t.Fatalf("runConfigure: %v", err)
	}

	if len(keychainCalls) != 1 || keychainCalls[0] != "tkn-123" {
		t.Fatalf("keychain calls: %v", keychainCalls)
	}

	loaded, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load saved config: %v", err)
	}
	got := loaded["default"]
	if got.Subdomain != "acme" || got.Region != "us" {
		t.Fatalf("saved profile %+v", got)
	}

	info, err := os.Stat(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode %o", info.Mode().Perm())
	}
}
```

- [ ] **Step 3: Run to confirm failure**

Run:

```bash
go test ./cmd/...
```

Expected: compile failure on `runConfigure`, `configureOpts`.

- [ ] **Step 4: Implement the configure subcommand**

Write `cmd/configure.go`:

```go
package cmd

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

// configureOpts is the dependency-injection surface for testing.
type configureOpts struct {
	ConfigPath    string
	Stdin         io.Reader
	Stdout        io.Writer
	Stderr        io.Writer
	StoreToken    func(account, token string) error
	ReadTokenLine func() (string, error) // masked read; injected for tests
	VerifyBaseURL string                  // override for tests, blank in production
}

func newConfigureCmd() *cobra.Command {
	return &cobra.Command{
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
}

func runConfigure(ctx context.Context, o configureOpts) error {
	r := bufio.NewReader(o.Stdin)

	fmt.Fprint(o.Stdout, "Tenant subdomain (lowercase, digits, dashes): ")
	subdomain, err := readLine(r)
	if err != nil {
		return err
	}

	fmt.Fprint(o.Stdout, "Region [us/eu]: ")
	region, err := readLine(r)
	if err != nil {
		return err
	}
	region = strings.ToLower(region)

	baseURL, err := config.BuildBaseURL(subdomain, region)
	if err != nil {
		return err
	}

	fmt.Fprint(o.Stdout, "API token: ")
	token, err := o.ReadTokenLine()
	if err != nil {
		return err
	}
	fmt.Fprintln(o.Stdout)
	if strings.TrimSpace(token) == "" {
		return errors.New("token must not be empty")
	}

	f := config.File{"default": config.Profile{
		Subdomain: subdomain,
		Region:    region,
		BaseURL:   baseURL,
	}}
	if err := config.Save(o.ConfigPath, f); err != nil {
		return err
	}
	if err := o.StoreToken("default", token); err != nil {
		return fmt.Errorf("store token in Keychain: %w", err)
	}

	// Verify the token works.
	verifyURL := baseURL
	if o.VerifyBaseURL != "" {
		verifyURL = o.VerifyBaseURL
	}
	c := iru.NewClient(verifyURL, token)
	if _, err := c.ListDevicesPage(ctx, iru.DeviceFilters{}, 1, 0); err != nil {
		if errors.Is(err, iru.ErrUnauthorized) || errors.Is(err, iru.ErrForbidden) {
			fmt.Fprintln(o.Stderr, "Warning: token did not authenticate against Iru. Config saved; re-run configure after rotating the token.")
			return nil
		}
		fmt.Fprintf(o.Stderr, "Warning: verification request failed: %v\n", err)
		return nil
	}
	fmt.Fprintln(o.Stdout, "Configured. Token verified.")
	return nil
}

func readLine(r *bufio.Reader) (string, error) {
	s, err := r.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return strings.TrimSpace(s), nil
}

func readMaskedToken() (string, error) {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		// Fallback: read line unmasked (e.g. when piped).
		r := bufio.NewReader(os.Stdin)
		s, err := r.ReadString('\n')
		return strings.TrimSpace(s), err
	}
	b, err := term.ReadPassword(fd)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
```

- [ ] **Step 5: Register the subcommand**

Edit `cmd/root.go` and add inside `newRootCmd()`:

```go
	root.AddCommand(newConfigureCmd())
```

- [ ] **Step 6: Run tests**

Run:

```bash
go test ./cmd/...
```

Expected: pass.

- [ ] **Step 7: Stage and commit**

```bash
git add cmd/ go.mod go.sum
git commit -m "feat: add jellyfish configure with Keychain storage and token verify"
```

---

### Task 18: `jellyfish vulns list`

**Files:**

- Create: `cmd/vulns.go`
- Create: `cmd/vulns_test.go`
- Create: `cmd/iru_iface.go`
- Modify: `cmd/root.go` (register `vulns`)

- [ ] **Step 1: Define the iru client interface so the command is testable**

Write `cmd/iru_iface.go`:

```go
package cmd

import (
	"context"

	"flipgroup.com.au/jellyfish/internal/iru"
)

// iruClient is the surface area the cmd package uses. Implemented by *iru.Client
// in production and a fake in tests.
type iruClient interface {
	ListDevicesPage(ctx context.Context, f iru.DeviceFilters, limit, offset int) ([]iru.Device, error)
	ListDevices(ctx context.Context, f iru.DeviceFilters) ([]iru.Device, error)
	GetDeviceBySerial(ctx context.Context, serial string) (iru.Device, error)
	GetUser(ctx context.Context, id string) (iru.User, error)
	FindUserByEmail(ctx context.Context, email string) (iru.User, error)
	ListDetections(ctx context.Context, f iru.DetectionFilters) ([]iru.Detection, error)
	ListDetectionsPage(ctx context.Context, f iru.DetectionFilters, limit, offset int) ([]iru.Detection, error)
}
```

- [ ] **Step 2: Write the failing test**

Write `cmd/vulns_test.go`:

```go
package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"flipgroup.com.au/jellyfish/internal/iru"
)

type fakeClient struct {
	detections []iru.Detection
	devices    []iru.Device
	users      []iru.User
	bySerial   func(string) (iru.Device, error)
}

func (f *fakeClient) ListDetections(ctx context.Context, _ iru.DetectionFilters) ([]iru.Detection, error) {
	return f.detections, nil
}
func (f *fakeClient) ListDetectionsPage(ctx context.Context, _ iru.DetectionFilters, _, _ int) ([]iru.Detection, error) {
	return f.detections, nil
}
func (f *fakeClient) ListDevices(_ context.Context, _ iru.DeviceFilters) ([]iru.Device, error) {
	return f.devices, nil
}
func (f *fakeClient) ListDevicesPage(_ context.Context, _ iru.DeviceFilters, _, _ int) ([]iru.Device, error) {
	return f.devices, nil
}
func (f *fakeClient) GetDeviceBySerial(_ context.Context, s string) (iru.Device, error) {
	if f.bySerial != nil {
		return f.bySerial(s)
	}
	return iru.Device{}, iru.ErrNotFound
}
func (f *fakeClient) GetUser(_ context.Context, id string) (iru.User, error) {
	for _, u := range f.users {
		if u.ID == id {
			return u, nil
		}
	}
	return iru.User{}, iru.ErrNotFound
}
func (f *fakeClient) FindUserByEmail(_ context.Context, e string) (iru.User, error) {
	for _, u := range f.users {
		if strings.EqualFold(u.Email, e) {
			return u, nil
		}
	}
	return iru.User{}, iru.ErrNotFound
}

func TestVulnsListJSON(t *testing.T) {
	client := &fakeClient{detections: []iru.Detection{
		{DetectionID: "x-1", DeviceID: "d-1", CVE: "CVE-2025-0001", Severity: "high", Status: "active"},
	}}
	buf := &bytes.Buffer{}
	err := runVulnsList(context.Background(), client, buf, vulnsListOpts{Output: "json"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(buf.String(), `"detection_id": "x-1"`) {
		t.Fatalf("output: %q", buf.String())
	}
}

func TestVulnsListSerialResolvesDeviceID(t *testing.T) {
	client := &fakeClient{
		bySerial: func(s string) (iru.Device, error) {
			if s != "SN1" {
				return iru.Device{}, iru.ErrNotFound
			}
			return iru.Device{DeviceID: "d-9"}, nil
		},
		detections: []iru.Detection{{DetectionID: "x-9", DeviceID: "d-9"}},
	}
	buf := &bytes.Buffer{}
	err := runVulnsList(context.Background(), client, buf, vulnsListOpts{
		Output: "json",
		Serial: "SN1",
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(buf.String(), "x-9") {
		t.Fatalf("output: %q", buf.String())
	}
}

func TestVulnsListSerialNotFoundExitsNotFound(t *testing.T) {
	client := &fakeClient{}
	err := runVulnsList(context.Background(), client, &bytes.Buffer{}, vulnsListOpts{
		Output: "json",
		Serial: "SN-missing",
	})
	if !errors.Is(err, iru.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestVulnsListRejectsMutuallyExclusiveFlags(t *testing.T) {
	client := &fakeClient{}
	err := runVulnsList(context.Background(), client, &bytes.Buffer{}, vulnsListOpts{
		Output:   "json",
		DeviceID: "d-1",
		Serial:   "SN1",
	})
	if err == nil {
		t.Fatal("expected error for both flags")
	}
}
```

- [ ] **Step 3: Run to confirm failure**

Run:

```bash
go test ./cmd/...
```

Expected: compile failure on `runVulnsList`, `vulnsListOpts`.

- [ ] **Step 4: Implement vulns.go**

Write `cmd/vulns.go`:

```go
package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/output"
)

type vulnsListOpts struct {
	DeviceID string
	Serial   string
	Status   string
	Limit    int
	Page     int
	Output   string
}

func newVulnsCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "vulns",
		Short: "Vulnerability detections",
	}
	c.AddCommand(newVulnsListCmd())
	return c
}

func newVulnsListCmd() *cobra.Command {
	var opts vulnsListOpts
	c := &cobra.Command{
		Use:   "list",
		Short: "List vulnerability detections",
		RunE: func(cmd *cobra.Command, _ []string) error {
			outFmt, _ := cmd.Flags().GetString("output")
			opts.Output = outFmt

			client, err := buildClient(cmd)
			if err != nil {
				return err
			}
			return runVulnsList(cmd.Context(), client, cmd.OutOrStdout(), opts)
		},
	}
	c.Flags().StringVar(&opts.DeviceID, "device-id", "", "Filter to a single device by ID")
	c.Flags().StringVar(&opts.Serial, "serial", "", "Filter to a single device by serial number")
	c.Flags().StringVar(&opts.Status, "status", "", "Filter by detection status (pass-through to Iru)")
	c.Flags().IntVar(&opts.Limit, "limit", 0, "Limit results to N (single page when set)")
	c.Flags().IntVar(&opts.Page, "page", 0, "Fetch a single page at this 1-indexed page number")
	return c
}

func runVulnsList(ctx context.Context, client iruClient, w io.Writer, opts vulnsListOpts) error {
	if opts.DeviceID != "" && opts.Serial != "" {
		return errors.New("--device-id and --serial are mutually exclusive")
	}

	// Resolve serial to device id, if provided.
	filters := iru.DetectionFilters{Status: opts.Status, DeviceID: opts.DeviceID}
	if opts.Serial != "" {
		d, err := client.GetDeviceBySerial(ctx, opts.Serial)
		if err != nil {
			return err
		}
		filters.DeviceID = d.DeviceID
	}

	var detections []iru.Detection
	switch {
	case opts.Limit > 0 || opts.Page > 0:
		limit := opts.Limit
		if limit <= 0 {
			limit = iru.DefaultLimit
		}
		if limit > iru.DefaultLimit {
			fmt.Fprintf(w, "warning: limit clamped to %d (Iru server-side max)\n", iru.DefaultLimit)
			limit = iru.DefaultLimit
		}
		page := opts.Page
		if page < 1 {
			page = 1
		}
		offset := (page - 1) * limit
		ds, err := client.ListDetectionsPage(ctx, filters, limit, offset)
		if err != nil {
			return err
		}
		detections = ds
	default:
		ds, err := client.ListDetections(ctx, filters)
		if err != nil {
			return err
		}
		detections = ds
	}

	return renderDetections(w, opts.Output, detections)
}

func renderDetections(w io.Writer, format string, detections []iru.Detection) error {
	if format == "table" || format == "" {
		t := output.Table().WithColumns(detectionColumns())
		return t.Render(w, detections)
	}
	if format == "csv" {
		c := output.CSV().WithColumns(detectionColumns())
		return c.Render(w, detections)
	}
	r, err := output.For(format)
	if err != nil {
		return err
	}
	return r.Render(w, detections)
}

func detectionColumns() []output.Column {
	return []output.Column{
		{Header: "DETECTION_ID", Extract: func(v any) string { return v.(iru.Detection).DetectionID }},
		{Header: "DEVICE_ID", Extract: func(v any) string { return v.(iru.Detection).DeviceID }},
		{Header: "CVE", Extract: func(v any) string { return v.(iru.Detection).CVE }},
		{Header: "SEVERITY", Extract: func(v any) string { return v.(iru.Detection).Severity }},
		{Header: "STATUS", Extract: func(v any) string { return v.(iru.Detection).Status }},
		{Header: "APP", Extract: func(v any) string { return v.(iru.Detection).AppName }},
	}
}
```

The `buildClient` helper builds an `iru.Client` from config + keychain. It is shared by `vulns` and `user`. Write it now in a new helper file:

Create `cmd/client.go`:

```go
package cmd

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/keychain"
	"flipgroup.com.au/jellyfish/internal/version"
)

func buildClient(cmd *cobra.Command) (iruClient, error) {
	cfgPath, _ := cmd.Flags().GetString("config")
	if cfgPath == "" {
		p, err := config.DefaultPath()
		if err != nil {
			return nil, err
		}
		cfgPath = p
	}
	f, err := config.Load(cfgPath)
	if err != nil {
		return nil, fmt.Errorf(`no credentials found at %s. Run "jellyfish configure" to set up`, cfgPath)
	}
	prof, ok := f["default"]
	if !ok {
		return nil, errors.New(`no "default" profile in config. Run "jellyfish configure" to set up`)
	}
	tok, err := keychain.Get("default")
	if err != nil {
		return nil, fmt.Errorf(`no token found in Keychain. Run "jellyfish configure" to set up`)
	}
	return iru.NewClient(prof.BaseURL, tok, iru.WithUserAgent("jellyfish/"+version.Version)), nil
}
```

- [ ] **Step 5: Register `vulns`**

Edit `cmd/root.go` and add inside `newRootCmd()`:

```go
	root.AddCommand(newVulnsCmd())
```

- [ ] **Step 6: Run tests**

Run:

```bash
go test ./...
```

Expected: pass.

- [ ] **Step 7: Stage and commit**

```bash
git add cmd/
git commit -m "feat: add jellyfish vulns list with status, serial and pagination flags"
```

---

### Task 19: `jellyfish user show`

**Files:**

- Create: `cmd/user.go`
- Create: `cmd/user_test.go`
- Modify: `cmd/root.go` (register `user`)

- [ ] **Step 1: Add errgroup**

Run:

```bash
go get golang.org/x/sync/errgroup
```

- [ ] **Step 2: Write the failing test**

Write `cmd/user_test.go`:

```go
package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"flipgroup.com.au/jellyfish/internal/iru"
)

func TestUserShowByEmailJSON(t *testing.T) {
	client := &fakeClient{
		users: []iru.User{{ID: "u-1", Name: "Keith", Email: "keith@flipgroup.com.au"}},
		devices: []iru.Device{
			{DeviceID: "d-1", DeviceName: "MBP", SerialNumber: "SN1", User: iru.User{ID: "u-1"}},
		},
		detections: []iru.Detection{
			{DetectionID: "x-1", DeviceID: "d-1", CVE: "CVE-2025-0001", Status: "active"},
		},
	}
	buf := &bytes.Buffer{}
	err := runUserShow(context.Background(), client, buf, userShowOpts{
		Identifier: "keith@flipgroup.com.au",
		Output:     "json",
	})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"keith@flipgroup.com.au", "d-1", "x-1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q in output:\n%s", want, out)
		}
	}
}

func TestUserShowByIDFallback(t *testing.T) {
	client := &fakeClient{
		users: []iru.User{{ID: "u-9", Name: "Test", Email: "t@x"}},
	}
	buf := &bytes.Buffer{}
	err := runUserShow(context.Background(), client, buf, userShowOpts{Identifier: "u-9", Output: "json"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "u-9") {
		t.Fatalf("output %q", buf.String())
	}
}

func TestUserShowUserNotFound(t *testing.T) {
	client := &fakeClient{}
	err := runUserShow(context.Background(), client, &bytes.Buffer{}, userShowOpts{Identifier: "u-x", Output: "json"})
	if !errors.Is(err, iru.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
```

- [ ] **Step 3: Run to confirm failure**

Run:

```bash
go test ./cmd/...
```

Expected: compile failure on `runUserShow`, `userShowOpts`.

- [ ] **Step 4: Implement user.go**

Write `cmd/user.go`:

```go
package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/output"
)

type userShowOpts struct {
	Identifier string
	Output     string
}

// UserBundle is the composite shape `user show` returns.
type UserBundle struct {
	User    iru.User                 `json:"user" yaml:"user"`
	Devices []DeviceWithDetections   `json:"devices" yaml:"devices"`
}

type DeviceWithDetections struct {
	Device     iru.Device      `json:"device" yaml:"device"`
	Detections []iru.Detection `json:"detections" yaml:"detections"`
}

func newUserCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "user",
		Short: "User-scoped queries",
	}
	c.AddCommand(newUserShowCmd())
	return c
}

func newUserShowCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "show <user-id-or-email>",
		Short: "Show a user, their devices, and active detections per device",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			outFmt, _ := cmd.Flags().GetString("output")
			client, err := buildClient(cmd)
			if err != nil {
				return err
			}
			return runUserShow(cmd.Context(), client, cmd.OutOrStdout(), userShowOpts{
				Identifier: args[0],
				Output:     outFmt,
			})
		},
	}
	return c
}

func runUserShow(ctx context.Context, client iruClient, w io.Writer, opts userShowOpts) error {
	user, err := resolveUser(ctx, client, opts.Identifier)
	if err != nil {
		return err
	}

	devices, err := client.ListDevices(ctx, iru.DeviceFilters{UserID: user.ID})
	if err != nil {
		return err
	}

	bundle := UserBundle{User: user, Devices: make([]DeviceWithDetections, len(devices))}
	for i := range devices {
		bundle.Devices[i] = DeviceWithDetections{Device: devices[i]}
	}

	// Concurrent fetch of active detections per device, bounded to 5 in-flight.
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(5)
	for i := range devices {
		i := i
		g.Go(func() error {
			ds, err := client.ListDetections(ctx, iru.DetectionFilters{
				DeviceID: devices[i].DeviceID,
				Status:   "active",
			})
			if err != nil {
				return err
			}
			bundle.Devices[i].Detections = ds
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return renderUserBundle(w, opts.Output, bundle)
}

func resolveUser(ctx context.Context, client iruClient, id string) (iru.User, error) {
	if strings.Contains(id, "@") {
		return client.FindUserByEmail(ctx, id)
	}
	return client.GetUser(ctx, id)
}

func renderUserBundle(w io.Writer, format string, b UserBundle) error {
	switch format {
	case "json", "yaml":
		r, err := output.For(format)
		if err != nil {
			return err
		}
		return r.Render(w, b)
	case "csv":
		return renderUserBundleCSV(w, b)
	case "table", "":
		return renderUserBundleTable(w, b)
	default:
		return fmt.Errorf("unsupported output format %q", format)
	}
}

func renderUserBundleTable(w io.Writer, b UserBundle) error {
	fmt.Fprintln(w, "USER")
	userTbl := output.Table().WithColumns([]output.Column{
		{Header: "ID", Extract: func(v any) string { return v.(iru.User).ID }},
		{Header: "NAME", Extract: func(v any) string { return v.(iru.User).Name }},
		{Header: "EMAIL", Extract: func(v any) string { return v.(iru.User).Email }},
	})
	if err := userTbl.Render(w, b.User); err != nil {
		return err
	}

	fmt.Fprintln(w, "\nDEVICES")
	devices := make([]iru.Device, len(b.Devices))
	for i := range b.Devices {
		devices[i] = b.Devices[i].Device
	}
	devTbl := output.Table().WithColumns([]output.Column{
		{Header: "DEVICE_ID", Extract: func(v any) string { return v.(iru.Device).DeviceID }},
		{Header: "NAME", Extract: func(v any) string { return v.(iru.Device).DeviceName }},
		{Header: "SERIAL", Extract: func(v any) string { return v.(iru.Device).SerialNumber }},
		{Header: "MODEL", Extract: func(v any) string { return v.(iru.Device).Model }},
		{Header: "OS", Extract: func(v any) string { return v.(iru.Device).OSVersion }},
	})
	if err := devTbl.Render(w, devices); err != nil {
		return err
	}

	for _, d := range b.Devices {
		fmt.Fprintf(w, "\nDETECTIONS for %s (%s)\n", d.Device.DeviceName, d.Device.SerialNumber)
		if len(d.Detections) == 0 {
			fmt.Fprintln(w, "  (none)")
			continue
		}
		detTbl := output.Table().WithColumns(detectionColumns())
		if err := detTbl.Render(w, d.Detections); err != nil {
			return err
		}
	}
	return nil
}

func renderUserBundleCSV(w io.Writer, b UserBundle) error {
	type row struct {
		userID, userEmail, userName string
		deviceID, deviceName, serial string
		detectionID, cve, severity, status string
	}
	var rows []row
	for _, d := range b.Devices {
		if len(d.Detections) == 0 {
			rows = append(rows, row{
				userID: b.User.ID, userEmail: b.User.Email, userName: b.User.Name,
				deviceID: d.Device.DeviceID, deviceName: d.Device.DeviceName, serial: d.Device.SerialNumber,
			})
			continue
		}
		for _, det := range d.Detections {
			rows = append(rows, row{
				userID: b.User.ID, userEmail: b.User.Email, userName: b.User.Name,
				deviceID: d.Device.DeviceID, deviceName: d.Device.DeviceName, serial: d.Device.SerialNumber,
				detectionID: det.DetectionID, cve: det.CVE, severity: det.Severity, status: det.Status,
			})
		}
	}
	c := output.CSV().WithColumns([]output.Column{
		{Header: "user_id", Extract: func(v any) string { return v.(row).userID }},
		{Header: "user_email", Extract: func(v any) string { return v.(row).userEmail }},
		{Header: "user_name", Extract: func(v any) string { return v.(row).userName }},
		{Header: "device_id", Extract: func(v any) string { return v.(row).deviceID }},
		{Header: "device_name", Extract: func(v any) string { return v.(row).deviceName }},
		{Header: "serial_number", Extract: func(v any) string { return v.(row).serial }},
		{Header: "detection_id", Extract: func(v any) string { return v.(row).detectionID }},
		{Header: "cve", Extract: func(v any) string { return v.(row).cve }},
		{Header: "severity", Extract: func(v any) string { return v.(row).severity }},
		{Header: "status", Extract: func(v any) string { return v.(row).status }},
	})
	return c.Render(w, rows)
}
```

- [ ] **Step 5: Register `user`**

Edit `cmd/root.go` and add inside `newRootCmd()`:

```go
	root.AddCommand(newUserCmd())
```

- [ ] **Step 6: Run tests**

Run:

```bash
go test ./...
```

Expected: pass.

- [ ] **Step 7: Stage and commit**

```bash
git add cmd/ go.mod go.sum
git commit -m "feat: add jellyfish user show with bounded concurrent detection fetch"
```

---

### Task 20: Map errors to exit codes

**Files:**

- Modify: `cmd/root.go`
- Modify: `main.go`
- Test: `cmd/exit_test.go`

- [ ] **Step 1: Write the failing test**

Write `cmd/exit_test.go`:

```go
package cmd

import (
	"errors"
	"testing"

	"flipgroup.com.au/jellyfish/internal/iru"
)

func TestClassifyError(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{nil, 0},
		{errors.New("oops"), 1},
		{iru.ErrUnauthorized, 2},
		{iru.ErrForbidden, 2},
		{iru.ErrNotFound, 3},
		{&iru.APIError{Status: 500}, 4},
	}
	for _, c := range cases {
		got := classifyError(c.err)
		if got != c.want {
			t.Fatalf("classify(%v)=%d want %d", c.err, got, c.want)
		}
	}
}
```

- [ ] **Step 2: Run to confirm failure**

Run:

```bash
go test ./cmd/...
```

Expected: compile failure on `classifyError`.

- [ ] **Step 3: Implement `classifyError`**

Append to `cmd/root.go`:

```go
import (
	// existing imports...
	"errors"

	"flipgroup.com.au/jellyfish/internal/iru"
)

// classifyError maps an error to the documented exit codes.
//
//	0 - success
//	1 - user error
//	2 - auth/permissions
//	3 - not found
//	4 - upstream / network
func classifyError(err error) int {
	if err == nil {
		return 0
	}
	switch {
	case errors.Is(err, iru.ErrUnauthorized), errors.Is(err, iru.ErrForbidden):
		return 2
	case errors.Is(err, iru.ErrNotFound):
		return 3
	}
	var apiErr *iru.APIError
	if errors.As(err, &apiErr) {
		if apiErr.Status >= 500 {
			return 4
		}
	}
	return 1
}
```

If a duplicate import block warning appears, merge the new `errors` and `iru` imports into the existing import block at the top of `cmd/root.go`.

- [ ] **Step 4: Wire the exit code in `Execute`**

Edit `cmd/root.go` so `Execute()` returns the right code via an exported helper. Replace the `Execute` body with:

```go
// Execute runs the CLI. The returned int is the process exit code.
func Execute() int {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	err := newRootCmd().ExecuteContext(ctx)
	return classifyError(err)
}
```

Update `main.go`:

```go
package main

import (
	"os"

	"flipgroup.com.au/jellyfish/cmd"
)

func main() {
	os.Exit(cmd.Execute())
}
```

- [ ] **Step 5: Run all tests**

Run:

```bash
go test ./...
```

Expected: pass.

- [ ] **Step 6: Stage and commit**

```bash
git add cmd/root.go cmd/exit_test.go main.go
git commit -m "feat: map iru errors to documented CLI exit codes"
```

---

## Phase 6: Polish

### Task 21: Lint config and clean build

**Files:**

- Create: `.golangci.yml`

- [ ] **Step 1: Write the lint config**

Write `.golangci.yml`:

```yaml
version: "2"
linters:
  enable:
    - errcheck
    - govet
    - staticcheck
    - ineffassign
    - gosec
issues:
  exclude-rules:
    - path: _test\.go
      linters:
        - gosec
```

- [ ] **Step 2: Run the linter**

Run:

```bash
golangci-lint run
```

Expected: clean. Fix anything it reports inline before continuing. Common fixes: ignore `_` returns from `Write`, capitalise initialisms (`URL`, `ID`) consistently, replace `interface{}` with `any`.

- [ ] **Step 3: Run all tests once more**

Run:

```bash
go test ./...
```

Expected: pass.

- [ ] **Step 4: Stage and commit**

```bash
git add .golangci.yml
git commit -m "chore: add golangci-lint config"
```

---

### Task 22: README

**Files:**

- Create: `README.md`

- [ ] **Step 1: Write the README**

Write `README.md`:

````markdown
# jellyfish

A macOS-only Go CLI for the Iru (formerly Kandji) Endpoint Management API.

## Features

- `jellyfish vulns list` - list vulnerability detections across the fleet, filter by device ID, serial number or status.
- `jellyfish user show <id-or-email>` - resolve a user, their devices, and the active detections per device.
- `jellyfish configure` - interactively store the tenant subdomain, region and API token (token in the macOS Keychain).

## Install

Requires Go 1.21+ on macOS.

```bash
go install flipgroup.com.au/jellyfish@latest
```

This places `jellyfish` in `$GOBIN` if set, otherwise `$GOPATH/bin`
(default `~/go/bin`). Add it to your `PATH` once:

```bash
echo 'export PATH="$HOME/go/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

To build locally instead:

```bash
make build       # ./bin/jellyfish
make install     # installs via `go install`
```

## Configure

```bash
jellyfish configure
```

Interactive prompts:

1. Tenant subdomain (the bit before `.api.kandji.io`).
2. Region: `us` or `eu`.
3. API token (input is masked).

Subdomain and region are written to `~/.config/jellyfish/config.yml` with mode
`0600`. The token is stored in the macOS Keychain under the service
`au.com.flipgroup.jellyfish` and account `default`. You can inspect or remove
it via Keychain Access or:

```bash
security find-generic-password -s au.com.flipgroup.jellyfish -a default
security delete-generic-password -s au.com.flipgroup.jellyfish -a default
```

## Usage

### Vulnerability detections

```bash
jellyfish vulns list                              # everything
jellyfish vulns list --status active              # active only
jellyfish vulns list --device-id d-123            # one device by ID
jellyfish vulns list --serial C02XL0RKDV4         # one device by serial
jellyfish vulns list --limit 50 --page 1          # single page of 50
jellyfish vulns list -o json                      # JSON for jq
jellyfish vulns list -o csv > vulns.csv           # CSV export
```

### Per-user view

```bash
jellyfish user show keith@flipgroup.com.au        # by email
jellyfish user show 1f5b...e4                     # by user ID
jellyfish user show keith@flipgroup.com.au -o json
```

### Output formats

`-o` accepts `table` (default), `json`, `yaml`, `csv`.

For `user show -o csv`, the output is flattened: one row per detection, with
user and device columns repeated. Column order:

```
user_id, user_email, user_name, device_id, device_name, serial_number,
detection_id, cve, severity, status
```

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | User error (bad flags, missing config) |
| 2 | Authentication or permissions failure |
| 3 | User, device or detection not found |
| 4 | Upstream error (5xx, network) |

## Development

```bash
make test
make lint
```

Real-Keychain integration tests:

```bash
JELLYFISH_KEYCHAIN_TESTS=1 go test ./internal/keychain/... -count=1
```

The first run will pop a macOS dialog asking to allow the test binary to read
your Keychain; approve it and re-run.
````

- [ ] **Step 2: Stage and commit**

```bash
git add README.md
git commit -m "docs: add README with install, configure and usage"
```

---

## Self-review checklist (run before declaring the plan done)

After implementing all tasks:

- [ ] `go test ./...` is green.
- [ ] `golangci-lint run` is clean.
- [ ] `go build -o /tmp/jellyfish . && /tmp/jellyfish --help` lists `configure`, `vulns`, `user`, `version`.
- [ ] Manual smoke test against a real Iru tenant:
  - [ ] `jellyfish configure` writes the config file at `~/.config/jellyfish/config.yml` with mode `0600` and stores a Keychain item under `au.com.flipgroup.jellyfish/default`.
  - [ ] `jellyfish vulns list -o json | jq '. | length'` returns a sensible count.
  - [ ] `jellyfish vulns list --serial <known-serial>` returns the device's detections only.
  - [ ] `jellyfish user show <your-email>` renders user, devices and detections in a table.
  - [ ] `jellyfish user show <bogus-email>` exits non-zero with exit code 3.

---

## Stretch / future work (out of scope for v1)

Captured here so they do not get lost.

- Promote `internal/iru` to a public package once a second consumer needs it.
- `JELLYFISH_API_TOKEN` env-var fallback for CI use.
- Multi-profile support: extend `--profile` to honour values other than `default`. Config format already supports it.
- Write operations: acknowledge or suppress detections, kick off remediation.
- Linux / Windows support: would require a different credential backend (libsecret, Windows Credential Manager).
- `-vv` extra-verbose mode that logs response bodies, with token and PII redaction.
