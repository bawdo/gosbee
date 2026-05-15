package cmd

import (
	"context"
	"errors"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"flipgroup.com.au/jellyfish/internal/iru"
)

// Execute runs the CLI. The returned int is the process exit code.
func Execute() int {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	err := newRootCmd().ExecuteContext(ctx)
	return classifyError(err)
}

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
	root.AddCommand(newConfigureCmd())
	root.AddCommand(newVulnsCmd())
	root.AddCommand(newUserCmd())
	return root
}
