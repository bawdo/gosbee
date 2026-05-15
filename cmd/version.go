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
