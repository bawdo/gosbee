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
