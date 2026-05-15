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
	ReadTokenLine func(r *bufio.Reader) (string, error) // masked read; injected for tests
	VerifyBaseURL string                                // override for tests, blank in production
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
