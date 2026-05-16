package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/email"
	"flipgroup.com.au/jellyfish/internal/gmail"
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
	ReadTokenLine func(r *bufio.Reader) (string, error) // masked read; injected for tests
	VerifyBaseURL string                                // override for tests, blank in production
}

// configureEmailOpts is the DI surface for `configure email`.
type configureEmailOpts struct {
	ConfigPath      string
	LogosDir        string // managed location for copied logos; defaults to <dirname(ConfigPath)>/logos
	Stdin           io.Reader
	Stdout          io.Writer
	Stderr          io.Writer
	StoreGmailJSON  func(jsonBytes []byte) error
	DeleteGmailJSON func() error
}

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

func runConfigure(ctx context.Context, o configureOpts) error {
	r := bufio.NewReader(o.Stdin)

	_, _ = fmt.Fprint(o.Stdout, "Tenant subdomain (lowercase, digits, dashes): ")
	subdomain, err := readLine(r)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprint(o.Stdout, "Region [us/eu]: ")
	region, err := readLine(r)
	if err != nil {
		return err
	}
	region = strings.ToLower(region)

	baseURL, err := config.BuildBaseURL(subdomain, region)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprint(o.Stdout, "API token: ")
	token, err := o.ReadTokenLine(r)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(o.Stdout)
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
			_, _ = fmt.Fprintln(o.Stderr, "Warning: token did not authenticate against Iru. Config saved; re-run configure after rotating the token.")
			return nil
		}
		_, _ = fmt.Fprintf(o.Stderr, "Warning: verification request failed: %v\n", err)
		return nil
	}
	_, _ = fmt.Fprintln(o.Stdout, "Configured. Token verified.")
	return nil
}

func readLine(r *bufio.Reader) (string, error) {
	s, err := r.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return strings.TrimSpace(s), nil
}

// readMaskedToken reads a token from in. When in is the real os.Stdin AND a
// terminal, it uses term.ReadPassword for masked entry. Otherwise it reads a
// line from in unmasked (the typical piped-input case for CI or scripts).
func readMaskedToken(in *bufio.Reader) (string, error) {
	if in == nil {
		in = bufio.NewReader(os.Stdin)
	}
	fd := int(os.Stdin.Fd())
	if term.IsTerminal(fd) {
		b, err := term.ReadPassword(fd)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	}
	s, err := in.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return strings.TrimSpace(s), nil
}

// promptWithDefault writes "<label>[ [<current>]]: " to w, reads a line from
// r, and applies the keep/clear/replace rules:
//
//   ""  -> current   (Enter keeps)
//   "-" -> ""        (literal dash clears)
//   x   -> x         (replace, trimmed)
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

const configureEmailMaxAttempts = 3

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

	headerBG, err := promptHeaderBG(o.Stdout, o.Stderr, r, prof.Email.HeaderBG)
	if err != nil {
		return err
	}
	prof.Email.HeaderBG = headerBG

	logosDir := o.LogosDir
	if logosDir == "" {
		logosDir = filepath.Join(filepath.Dir(o.ConfigPath), "logos")
	}
	newLogo, err := promptLogo(o.Stdout, o.Stderr, r, prof.Email.LogoPath, logosDir)
	if err != nil {
		return err
	}
	if newLogo != prof.Email.LogoPath {
		if prof.Email.LogoPath != "" {
			if rmErr := removeManagedLogo(prof.Email.LogoPath, logosDir); rmErr != nil {
				_, _ = fmt.Fprintf(o.Stderr, "warn: failed to remove previous logo %s: %v\n", prof.Email.LogoPath, rmErr)
			}
		}
	}
	prof.Email.LogoPath = newLogo

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
	switch line {
	case gmailPromptPlaceholderConfigured:
		// User accepted the existing configured value; no change.
		return nil
	case "":
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
		fileBytes, err := os.ReadFile(line)
		if err != nil {
			return fmt.Errorf("read Gmail JSON %s: %w", line, err)
		}
		if err := gmail.ValidateServiceAccountJSON(fileBytes); err != nil {
			return fmt.Errorf("validate Gmail JSON %s: %w", line, err)
		}
		if o.StoreGmailJSON != nil {
			if err := o.StoreGmailJSON(fileBytes); err != nil {
				return fmt.Errorf("store Gmail JSON in Keychain: %w", err)
			}
		}
		prof.Email.GmailConfigured = true
		return nil
	}
}

// promptValidated runs promptWithDefault + validateEmailish in a loop up to
// configureEmailMaxAttempts times. Validation errors print to stderr; the
// loop re-prompts. The fieldName used in error messages is derived from the
// label: "Email From" -> "From", "Email default To" -> "DefaultTo".
func promptValidated(stdout, stderr io.Writer, r *bufio.Reader, label, current string, allowEmpty bool) (string, error) {
	fieldName := label
	if idx := strings.Index(label, " "); idx > 0 {
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
		if value == "" {
			return value, nil
		}
		if vErr := validateEmailish(value, allowEmpty, fieldName); vErr != nil {
			_, _ = fmt.Fprintln(stderr, vErr)
			continue
		}
		return value, nil
	}
	return "", fmt.Errorf("invalid %s address after %d attempts", fieldName, configureEmailMaxAttempts)
}

func promptHeaderBG(stdout, stderr io.Writer, r *bufio.Reader, current string) (string, error) {
	for attempt := 1; attempt <= configureEmailMaxAttempts; attempt++ {
		value, err := promptWithDefault(stdout, r, "Header background colour", current)
		if err != nil {
			return "", err
		}
		if value == "" {
			return "", nil // user cleared
		}
		if vErr := email.ValidateHexColour(value); vErr != nil {
			_, _ = fmt.Fprintln(stderr, vErr)
			continue
		}
		return value, nil
	}
	return "", fmt.Errorf("invalid header background colour after %d attempts", configureEmailMaxAttempts)
}

func promptLogo(stdout, stderr io.Writer, r *bufio.Reader, current, logosDir string) (string, error) {
	for attempt := 1; attempt <= configureEmailMaxAttempts; attempt++ {
		value, err := promptWithDefault(stdout, r, "Logo PNG path", current)
		if err != nil {
			return "", err
		}
		switch value {
		case current:
			// Enter on existing value -> keep
			return current, nil
		case "":
			// dash collapsed -> clear; caller handles unlinking
			return "", nil
		default:
			dst, copyErr := copyLogoToManagedDir(value, logosDir)
			if copyErr != nil {
				_, _ = fmt.Fprintln(stderr, copyErr)
				continue
			}
			return dst, nil
		}
	}
	return "", fmt.Errorf("invalid logo after %d attempts", configureEmailMaxAttempts)
}

// copyLogoToManagedDir validates src as a PNG <= MaxLogoBytes and copies it
// to <logosDir>/<basename(src)> with mode 0o600. Returns the destination path.
func copyLogoToManagedDir(src, logosDir string) (string, error) {
	if _, err := email.ValidateLogoFile(src); err != nil {
		return "", err
	}
	if err := os.MkdirAll(logosDir, 0o700); err != nil {
		return "", fmt.Errorf("create logos dir %s: %w", logosDir, err)
	}
	// #nosec G304 - src is the operator's own input
	data, err := os.ReadFile(src)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", src, err)
	}
	dst := filepath.Join(logosDir, filepath.Base(src))
	// #nosec G304 G703 - dst is constructed from operator-controlled logosDir
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		return "", fmt.Errorf("write %s: %w", dst, err)
	}
	return dst, nil
}

// removeManagedLogo deletes the file at path iff it sits inside logosDir.
// Anything outside that directory is left alone.
func removeManagedLogo(path, logosDir string) error {
	if path == "" || logosDir == "" {
		return nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	absDir, err := filepath.Abs(logosDir)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(absDir, abs)
	if err != nil || strings.HasPrefix(rel, "..") || rel == "." {
		return nil // outside the managed dir
	}
	if err := os.Remove(abs); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
