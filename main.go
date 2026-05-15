package main

import (
	"os"

	"flipgroup.com.au/jellyfish/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
