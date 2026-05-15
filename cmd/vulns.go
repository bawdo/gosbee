package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/output"
)

type vulnsListOpts struct {
	DeviceID string
	Serial   string
	Status   string
	Limit    int
	Page     int
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
			return runVulnsList(cmd.Context(), client, cmd.OutOrStdout(), opts)
		},
	}
	c.Flags().StringVar(&opts.DeviceID, "device-id", "", "Filter to a single device by ID")
	c.Flags().StringVar(&opts.Serial, "serial", "", "Filter to a single device by serial number")
	c.Flags().StringVar(&opts.Status, "status", "", "Filter by detection status (pass-through to Iru)")
	c.Flags().IntVar(&opts.Limit, "limit", 0, "Limit results to N (single page when set)")
	c.Flags().IntVar(&opts.Page, "page", 0, "Fetch a single page at this 1-indexed page number")
	return c
}

func runVulnsList(ctx context.Context, client iruClient, w io.Writer, opts vulnsListOpts) error {
	if opts.DeviceID != "" && opts.Serial != "" {
		return errors.New("--device-id and --serial are mutually exclusive")
	}

	// Resolve serial to device id, if provided.
	filters := iru.DetectionFilters{Status: opts.Status, DeviceID: opts.DeviceID}
	if opts.Serial != "" {
		d, err := client.GetDeviceBySerial(ctx, opts.Serial)
		if err != nil {
			return err
		}
		filters.DeviceID = d.DeviceID
	}

	var detections []iru.Detection
	switch {
	case opts.Limit > 0 || opts.Page > 0:
		limit := opts.Limit
		if limit <= 0 {
			limit = iru.DefaultLimit
		}
		if limit > iru.DefaultLimit {
			fmt.Fprintf(w, "warning: limit clamped to %d (Iru server-side max)\n", iru.DefaultLimit)
			limit = iru.DefaultLimit
		}
		page := opts.Page
		if page < 1 {
			page = 1
		}
		offset := (page - 1) * limit
		ds, err := client.ListDetectionsPage(ctx, filters, limit, offset)
		if err != nil {
			return err
		}
		detections = ds
	default:
		ds, err := client.ListDetections(ctx, filters)
		if err != nil {
			return err
		}
		detections = ds
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
		{Header: "DETECTION_ID", Extract: func(v any) string { return v.(iru.Detection).DetectionID }},
		{Header: "DEVICE_ID", Extract: func(v any) string { return v.(iru.Detection).DeviceID }},
		{Header: "CVE", Extract: func(v any) string { return v.(iru.Detection).CVE }},
		{Header: "SEVERITY", Extract: func(v any) string { return v.(iru.Detection).Severity }},
		{Header: "STATUS", Extract: func(v any) string { return v.(iru.Detection).Status }},
		{Header: "APP", Extract: func(v any) string { return v.(iru.Detection).AppName }},
	}
}
