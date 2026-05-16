package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"text/template"
	"time"

	"github.com/spf13/cobra"

	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/email"
)

// emailFlagValues holds the literal flag inputs from cobra; empty string means
// the flag was not set.
type emailFlagValues struct {
	To      string
	From    string
	Subject string
}

// gitEmailLookup is the function signature for "find a from address by asking
// git". Production passes gitUserEmail; tests inject a fixture.
type gitEmailLookup func() (string, error)

// readEmailFlags pulls --email-to / --email-from / --email-subject off a cobra
// command. Missing flags return empty strings (no error).
func readEmailFlags(cmd *cobra.Command) emailFlagValues {
	to, _ := cmd.Flags().GetString("email-to")
	from, _ := cmd.Flags().GetString("email-from")
	subject, _ := cmd.Flags().GetString("email-subject")
	return emailFlagValues{To: to, From: from, Subject: subject}
}

// resolveEmailOptions applies the precedence:
//
//	From    : flag > config.Email.From > git user.email > error
//	To      : flag > config.Email.DefaultTo > "" (renderer prints <unspecified>)
//	Subject : flag > rendered config.Email.SubjectTemplate > "" (renderer's default)
func resolveEmailOptions(flags emailFlagValues, prof config.Profile, lookupGit gitEmailLookup, now time.Time) (email.Options, error) {
	opts := email.Options{
		Tenant:           prof.Subdomain,
		GeneratedAt:      now,
		To:               firstNonEmpty(flags.To, prof.Email.DefaultTo),
		CVELinkPrimary:   prof.Email.CVELinkPrimary,
		CVELinkSecondary: prof.Email.CVELinkSecondary,
	}

	opts.From = firstNonEmpty(flags.From, prof.Email.From)
	if opts.From == "" && lookupGit != nil {
		gitVal, err := lookupGit()
		if err == nil {
			opts.From = strings.TrimSpace(gitVal)
		}
	}
	if opts.From == "" {
		return email.Options{}, errors.New(`no email from address - set --email-from, email.from in config, or configure git user.email`)
	}

	switch {
	case flags.Subject != "":
		opts.Subject = flags.Subject
	case prof.Email.SubjectTemplate != "":
		rendered, err := renderSubjectTemplate(prof.Email.SubjectTemplate, now)
		if err != nil {
			return email.Options{}, fmt.Errorf("render email subject_template: %w", err)
		}
		opts.Subject = rendered
	}
	return opts, nil
}

func renderSubjectTemplate(tmplStr string, now time.Time) (string, error) {
	tmpl, err := template.New("subject").Parse(tmplStr)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	data := struct {
		Date string
		Time string
	}{
		Date: now.Format("2006-01-02"),
		Time: now.Format("15:04"),
	}
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// gitUserEmail shells out to `git config user.email`. Returns "" with nil
// error when the command runs but produces no value; returns an error only
// when git itself cannot be invoked (PATH miss, etc.).
func gitUserEmail() (string, error) {
	cmd := exec.Command("git", "config", "user.email")
	out, err := cmd.Output()
	if err != nil {
		if _, lookErr := exec.LookPath("git"); lookErr != nil {
			return "", fmt.Errorf("git not found on PATH: %w", lookErr)
		}
		return "", nil
	}
	return strings.TrimSpace(string(out)), nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
