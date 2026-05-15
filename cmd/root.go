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
	root.AddCommand(newConfigureCmd())
	return root
}
