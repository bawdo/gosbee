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
