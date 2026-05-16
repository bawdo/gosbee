package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"gopkg.in/yaml.v3"
)

// EmailConfig holds optional defaults for the -o email output. Every field
// is optional. Flags override these values; if both are empty the renderer
// falls back to built-in defaults (or, for "from", git user.email).
type EmailConfig struct {
	From             string `yaml:"from,omitempty"`
	DefaultTo        string `yaml:"default_to,omitempty"`
	SubjectTemplate  string `yaml:"subject_template,omitempty"`
	CVELinkPrimary   string `yaml:"cve_link_primary,omitempty"`
	CVELinkSecondary string `yaml:"cve_link_secondary,omitempty"`
	HeaderBG         string `yaml:"header_bg,omitempty"`
	LogoPath         string `yaml:"logo_path,omitempty"`
	GmailConfigured  bool   `yaml:"gmail_configured,omitempty"`
}

type Profile struct {
	Subdomain string      `yaml:"subdomain"`
	Region    string      `yaml:"region"`
	BaseURL   string      `yaml:"base_url"`
	Email     EmailConfig `yaml:"email,omitempty"`
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
	// #nosec G304 - path is controlled by user via --config flag or default location
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
