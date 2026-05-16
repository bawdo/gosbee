package cmd

import (
	"bufio"
	"bytes"
	"context"
	"errors"
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

	in := strings.NewReader("acme\nus\n")
	out := &bytes.Buffer{}

	err := runConfigure(context.Background(), configureOpts{
		ConfigPath:    cfgPath,
		Stdin:         in,
		Stdout:        out,
		Stderr:        out,
		StoreToken:    store,
		ReadTokenLine: func(_ *bufio.Reader) (string, error) { return "tkn-123", nil },
		VerifyBaseURL: srv.URL,
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

// gmailKeychainStubs captures Set/Delete calls so tests can assert what
// runConfigureEmail did to the Keychain.
type gmailKeychainStubs struct {
	stored   []byte
	deleted  bool
	storeErr error
	delErr   error
}

func (g *gmailKeychainStubs) store(b []byte) error {
	if g.storeErr != nil {
		return g.storeErr
	}
	g.stored = append([]byte(nil), b...)
	return nil
}

func (g *gmailKeychainStubs) delete() error {
	if g.delErr != nil {
		return g.delErr
	}
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
		Email: config.EmailConfig{From: "alice@example.com", DefaultTo: "secops@example.com", GmailConfigured: true},
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
		Email: config.EmailConfig{From: "alice@example.com", DefaultTo: "secops@example.com", GmailConfigured: true},
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

func TestConfigureEmailPromptHeaderBGValidAndClear(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	// Seed: requires existing default profile (configure email refuses otherwise).
	if err := config.Save(path, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{From: "a@example.com"},
	}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Inputs (one per prompt, in order): from kept, defaultTo kept, gmail kept (none),
	// header_bg = #C6B8FE.
	in := strings.NewReader("\n\n\n#C6B8FE\n")
	var out, errBuf bytes.Buffer
	if err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: path, Stdin: in, Stdout: &out, Stderr: &errBuf,
	}); err != nil {
		t.Fatalf("runConfigureEmail: %v\nstderr:\n%s", err, errBuf.String())
	}
	file, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if file["default"].Email.HeaderBG != "#C6B8FE" {
		t.Errorf("HeaderBG: got %q want #C6B8FE", file["default"].Email.HeaderBG)
	}
}

func TestConfigureEmailPromptHeaderBGRejectsInvalidThenAccepts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	_ = config.Save(path, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{From: "a@example.com"},
	}})
	// Inputs: from kept, defaultTo kept, gmail kept, bad colour twice, then valid.
	// Fifth prompt is logo - Enter keeps (empty current).
	in := strings.NewReader("\n\n\npurple\nnotahex\n#7566FF\n\n")
	var out, errBuf bytes.Buffer
	if err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: path, Stdin: in, Stdout: &out, Stderr: &errBuf,
	}); err != nil {
		t.Fatalf("runConfigureEmail: %v", err)
	}
	if !strings.Contains(errBuf.String(), "invalid hex colour") {
		t.Errorf("expected stderr to mention invalid hex; got:\n%s", errBuf.String())
	}
	file, _ := config.Load(path)
	if file["default"].Email.HeaderBG != "#7566FF" {
		t.Errorf("HeaderBG: got %q", file["default"].Email.HeaderBG)
	}
}

func TestConfigureEmailLogoCopiesIntoLogosDir(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yml")
	logosDir := filepath.Join(dir, "logos")
	srcLogo := filepath.Join(dir, "src", "flip.png")
	if err := os.MkdirAll(filepath.Dir(srcLogo), 0o700); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	// Reuse the fixture from internal/email/testdata as a real PNG.
	srcBytes, err := os.ReadFile("../internal/email/testdata/logo_small.png")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if err := os.WriteFile(srcLogo, srcBytes, 0o600); err != nil {
		t.Fatalf("write src: %v", err)
	}
	_ = config.Save(cfgPath, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{From: "a@example.com"},
	}})
	// Inputs (in order): from kept, defaultTo kept, gmail kept, header_bg blank kept, logo path supplied.
	in := strings.NewReader("\n\n\n\n" + srcLogo + "\n")
	var out, errBuf bytes.Buffer
	if err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, LogosDir: logosDir,
		Stdin: in, Stdout: &out, Stderr: &errBuf,
	}); err != nil {
		t.Fatalf("runConfigureEmail: %v\nstderr:\n%s", err, errBuf.String())
	}
	dst := filepath.Join(logosDir, "flip.png")
	gotBytes, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("expected logo at %s: %v", dst, err)
	}
	if !bytes.Equal(gotBytes, srcBytes) {
		t.Errorf("copied bytes differ from src")
	}
	file, _ := config.Load(cfgPath)
	if file["default"].Email.LogoPath != dst {
		t.Errorf("LogoPath: got %q want %q", file["default"].Email.LogoPath, dst)
	}
}

func TestConfigureEmailLogoClearDeletesManagedFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yml")
	logosDir := filepath.Join(dir, "logos")
	if err := os.MkdirAll(logosDir, 0o700); err != nil {
		t.Fatalf("mkdir logos: %v", err)
	}
	managed := filepath.Join(logosDir, "old.png")
	if err := os.WriteFile(managed, []byte("png-bytes"), 0o600); err != nil {
		t.Fatalf("seed managed file: %v", err)
	}
	_ = config.Save(cfgPath, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{From: "a@example.com", LogoPath: managed},
	}})
	in := strings.NewReader("\n\n\n\n-\n")
	var out, errBuf bytes.Buffer
	if err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, LogosDir: logosDir,
		Stdin: in, Stdout: &out, Stderr: &errBuf,
	}); err != nil {
		t.Fatalf("runConfigureEmail: %v\nstderr:\n%s", err, errBuf.String())
	}
	if _, err := os.Stat(managed); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected managed file deleted, stat err: %v", err)
	}
	file, _ := config.Load(cfgPath)
	if file["default"].Email.LogoPath != "" {
		t.Errorf("LogoPath: got %q want empty", file["default"].Email.LogoPath)
	}
}

func TestConfigureEmailLogoClearLeavesUnmanagedFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yml")
	logosDir := filepath.Join(dir, "logos")
	unmanaged := filepath.Join(dir, "elsewhere.png")
	if err := os.WriteFile(unmanaged, []byte("png"), 0o600); err != nil {
		t.Fatalf("seed unmanaged: %v", err)
	}
	_ = config.Save(cfgPath, config.File{"default": config.Profile{
		Subdomain: "acme", Region: "us", BaseURL: "https://acme.api.kandji.io/api/v1",
		Email: config.EmailConfig{From: "a@example.com", LogoPath: unmanaged},
	}})
	in := strings.NewReader("\n\n\n\n-\n")
	var out, errBuf bytes.Buffer
	if err := runConfigureEmail(context.Background(), configureEmailOpts{
		ConfigPath: cfgPath, LogosDir: logosDir,
		Stdin: in, Stdout: &out, Stderr: &errBuf,
	}); err != nil {
		t.Fatalf("runConfigureEmail: %v", err)
	}
	if _, err := os.Stat(unmanaged); err != nil {
		t.Errorf("unmanaged file should still exist: %v", err)
	}
}
