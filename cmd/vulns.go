package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/spf13/cobra"

	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/output"
)

type vulnsListOpts struct {
	DeviceID string
	Serial   string
	Limit    int
	Output   string
}

func newVulnsCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "vulns",
		Short: "Vulnerability detections",
	}
	c.AddCommand(newVulnsListCmd())
	return c
}

func newVulnsListCmd() *cobra.Command {
	var opts vulnsListOpts
	c := &cobra.Command{
		Use:   "list",
		Short: "List vulnerability detections",
		RunE: func(cmd *cobra.Command, _ []string) error {
			outFmt, _ := cmd.Flags().GetString("output")
			opts.Output = outFmt

			client, err := buildClient(cmd)
			if err != nil {
				return err
			}
			return runVulnsList(cmd.Context(), client, cmd.OutOrStdout(), cmd.ErrOrStderr(), opts)
		},
	}
	c.Flags().StringVar(&opts.DeviceID, "device-id", "", "Filter to a single device by ID")
	c.Flags().StringVar(&opts.Serial, "serial", "", "Filter to a single device by serial number")
	c.Flags().IntVar(&opts.Limit, "limit", 0, "Limit results to N (single page when set)")
	return c
}

func runVulnsList(ctx context.Context, client iruClient, w, stderr io.Writer, opts vulnsListOpts) error {
	if opts.DeviceID != "" && opts.Serial != "" {
		return errors.New("--device-id and --serial are mutually exclusive")
	}

	targetDeviceID := opts.DeviceID
	if opts.Serial != "" {
		d, err := client.GetDeviceBySerial(ctx, opts.Serial)
		if err != nil {
			return err
		}
		targetDeviceID = d.DeviceID
	}

	var detections []iru.Detection

	switch {
	case targetDeviceID != "":
		// Iru ignores per-device filters on /detections, so walk and filter.
		all, err := client.ListDetections(ctx, iru.DetectionFilters{})
		if err != nil {
			return err
		}
		for _, det := range all {
			if det.DeviceID == targetDeviceID {
				detections = append(detections, det)
			}
		}
	case opts.Limit > 0:
		limit := opts.Limit
		if limit > iru.DefaultLimit {
			_, _ = fmt.Fprintf(stderr, "warning: limit clamped to %d (Iru server-side max)\n", iru.DefaultLimit)
			limit = iru.DefaultLimit
		}
		ds, _, err := client.ListDetectionsPage(ctx, iru.DetectionFilters{}, limit, "")
		if err != nil {
			return err
		}
		detections = ds
	default:
		ds, err := client.ListDetections(ctx, iru.DetectionFilters{})
		if err != nil {
			return err
		}
		detections = ds
	}

	if opts.Limit > 0 && len(detections) > opts.Limit {
		detections = detections[:opts.Limit]
	}

	return renderDetections(w, opts.Output, detections)
}

func renderDetections(w io.Writer, format string, detections []iru.Detection) error {
	if format == "table" || format == "" {
		t := output.Table().WithColumns(detectionColumns())
		return t.Render(w, detections)
	}
	if format == "csv" {
		c := output.CSV().WithColumns(detectionColumns())
		return c.Render(w, detections)
	}
	r, err := output.For(format)
	if err != nil {
		return err
	}
	return r.Render(w, detections)
}

func detectionColumns() []output.Column {
	return []output.Column{
		{Header: "CVE", Extract: func(v any) string { return v.(iru.Detection).CVEID }},
		{Header: "SEVERITY", Extract: func(v any) string { return v.(iru.Detection).Severity }},
		{Header: "CVSS", Extract: func(v any) string {
			return strconv.FormatFloat(v.(iru.Detection).CVSSScore, 'f', 1, 64)
		}},
		{Header: "PACKAGE", Extract: func(v any) string { return v.(iru.Detection).Name }},
		{Header: "VERSION", Extract: func(v any) string { return v.(iru.Detection).Version }},
		{Header: "DEVICE", Extract: func(v any) string { return v.(iru.Detection).DeviceName }},
		{Header: "SERIAL", Extract: func(v any) string { return v.(iru.Detection).DeviceSerialNumber }},
	}
}
