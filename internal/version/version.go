package version

// Version is the build version, overridden via -ldflags at build time.
// See the Makefile for the exact -X value.
var Version = "dev"
