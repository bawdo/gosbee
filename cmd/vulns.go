package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"flipgroup.com.au/jellyfish/internal/config"
	"flipgroup.com.au/jellyfish/internal/email"
	"flipgroup.com.au/jellyfish/internal/gmail"
	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/keychain"
	"flipgroup.com.au/jellyfish/internal/output"
)

type vulnsListOpts struct {
	DeviceID string
	Serial   string
	Limit    int
	Output   string
	NoCache  bool
}

func newVulnsCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "vulns",
		Short: "Vulnerability detections and rollups",
	}
	c.AddCommand(newVulnsListCmd())
	c.AddCommand(newVulnsSummaryCmd())
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
	c.Flags().BoolVar(&opts.NoCache, "no-cache", false, "Skip the detection cache; always fetch fresh")
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
		all, err := fetchAllDetections(ctx, client, stderr, !opts.NoCache)
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
		ds, err := fetchAllDetections(ctx, client, stderr, !opts.NoCache)
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
	sortDetectionsBySeverity(detections)
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

// sortDetectionsBySeverity orders detections Critical first, with CVSS desc
// as the tiebreaker within a severity tier. Stable so callers can rely on
// original order for fully-equal records.
func sortDetectionsBySeverity(ds []iru.Detection) {
	sort.SliceStable(ds, func(i, j int) bool {
		ri, rj := iru.SeverityRank(ds[i].Severity), iru.SeverityRank(ds[j].Severity)
		if ri != rj {
			return ri < rj
		}
		return ds[i].CVSSScore > ds[j].CVSSScore
	})
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

type vulnsSummaryOpts struct {
	Status         string
	Severity       string
	Sort           string
	Limit          int
	Output         string
	NoCache        bool
	EmailFlags     emailFlagValues
	EmailNow       time.Time
	Profile        config.Profile
	gitEmail       gitEmailLookup
	ExplicitOutput string
	KeychainGet    func() ([]byte, error)
	NewSender      gmailNewSender
}

func newVulnsSummaryCmd() *cobra.Command {
	var opts vulnsSummaryOpts
	c := &cobra.Command{
		Use:   "summary",
		Short: "Per-CVE rollup view across the fleet",
		Long: `Per-CVE rollup view backed by Iru's /vulnerability-management/vulnerabilities
endpoint. Unlike "vulns list" (one row per device-CVE intersection), this is
one row per CVE with fleet-wide status, affected software, and device count.

Iru does not honour status or severity query params on this endpoint, so all
filtering happens client-side after a full walk. Results are cached for 15
minutes; pass --no-cache to force a fresh fetch.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			outFmt, _ := cmd.Flags().GetString("output")
			opts.Output = outFmt
			opts.EmailFlags = readEmailFlags(cmd)
			if cmd.Flags().Changed("output") {
				opts.ExplicitOutput = outFmt
			}
			opts.EmailNow = time.Now()
			if outFmt == "email" || opts.EmailFlags.Send {
				prof, err := activeProfile(cmd)
				if err != nil {
					return err
				}
				opts.Profile = prof
			}
			if opts.KeychainGet == nil {
				opts.KeychainGet = keychain.GetGmailServiceAccount
			}
			if opts.NewSender == nil {
				opts.NewSender = gmail.NewSender
			}
			client, err := buildClient(cmd)
			if err != nil {
				return err
			}
			return runVulnsSummary(cmd.Context(), client, cmd.OutOrStdout(), cmd.ErrOrStderr(), opts)
		},
	}
	c.Flags().StringVar(&opts.Status, "status", "", "Filter by status: active | remediated (default: all)")
	c.Flags().StringVar(&opts.Severity, "severity", "", "Filter by severity: critical | high | medium | low | undefined (default: all)")
	c.Flags().StringVar(&opts.Sort, "sort", "severity", "Sort by: severity (default) | cvss | kev | devices | cve")
	c.Flags().IntVar(&opts.Limit, "limit", 0, "Cap the rendered rows after sort")
	c.Flags().BoolVar(&opts.NoCache, "no-cache", false, "Skip the vulnerabilities cache; always fetch fresh")
	c.Flags().String("email-to", "", "Email To: header (default: email.default_to from config)")
	c.Flags().String("email-from", "", "Email From: header (default: email.from from config, then git user.email)")
	c.Flags().String("email-subject", "", "Email Subject: header (default: rendered email.subject_template or a per-command default)")
	c.Flags().Bool("send-email", false, "Send the rendered email via Gmail (requires `jellyfish configure email` to be run first)")
	return c
}

func runVulnsSummary(ctx context.Context, client iruClient, w, stderr io.Writer, opts vulnsSummaryOpts) error {
	all, err := fetchAllVulnerabilities(ctx, client, stderr, !opts.NoCache)
	if err != nil {
		return err
	}

	// Client-side filters.
	statusWant := strings.ToLower(opts.Status)
	sevWant := strings.ToLower(opts.Severity)
	filtered := make([]iru.Vulnerability, 0, len(all))
	for _, v := range all {
		if statusWant != "" && !strings.EqualFold(v.Status, statusWant) {
			continue
		}
		if sevWant != "" && !strings.EqualFold(v.Severity, sevWant) {
			continue
		}
		filtered = append(filtered, v)
	}

	sortVulns(filtered, opts.Sort)

	if opts.Limit > 0 && len(filtered) > opts.Limit {
		filtered = filtered[:opts.Limit]
	}

	if opts.EmailFlags.Send {
		return runSendVulnsSummary(ctx, stderr, opts, filtered)
	}

	return renderVulns(w, opts, filtered)
}

func sortVulns(vs []iru.Vulnerability, key string) {
	switch strings.ToLower(key) {
	case "cvss":
		sort.Slice(vs, func(i, j int) bool { return vs[i].CVSSScore > vs[j].CVSSScore })
	case "kev":
		sort.Slice(vs, func(i, j int) bool { return vs[i].KEVScore > vs[j].KEVScore })
	case "devices":
		sort.Slice(vs, func(i, j int) bool { return vs[i].DeviceCount > vs[j].DeviceCount })
	case "cve":
		sort.Slice(vs, func(i, j int) bool { return vs[i].CVEID < vs[j].CVEID })
	case "severity", "":
		sort.SliceStable(vs, func(i, j int) bool {
			ri, rj := iru.SeverityRank(vs[i].Severity), iru.SeverityRank(vs[j].Severity)
			if ri != rj {
				return ri < rj
			}
			return vs[i].CVSSScore > vs[j].CVSSScore
		})
	}
}

func renderVulns(w io.Writer, opts vulnsSummaryOpts, vs []iru.Vulnerability) error {
	switch opts.Output {
	case "table", "":
		t := output.Table().WithColumns(vulnColumns())
		return t.Render(w, vs)
	case "csv":
		c := output.CSV().WithColumns(vulnColumns())
		return c.Render(w, vs)
	case "email":
		now := opts.EmailNow
		if now.IsZero() {
			now = time.Now()
		}
		gitLookup := opts.gitEmail
		if gitLookup == nil {
			gitLookup = gitUserEmail
		}
		emailOpts, err := resolveEmailOptions(opts.EmailFlags, opts.Profile, gitLookup, now)
		if err != nil {
			return err
		}
		return email.NewVulnSummaryRenderer(emailOpts).Render(w, vs)
	}
	r, err := output.For(opts.Output)
	if err != nil {
		return err
	}
	return r.Render(w, vs)
}

func runSendVulnsSummary(ctx context.Context, stderr io.Writer, opts vulnsSummaryOpts, vs []iru.Vulnerability) error {
	now := opts.EmailNow
	if now.IsZero() {
		now = time.Now()
	}
	gitLookup := opts.gitEmail
	if gitLookup == nil {
		gitLookup = gitUserEmail
	}
	emailOpts, err := resolveEmailOptions(opts.EmailFlags, opts.Profile, gitLookup, now)
	if err != nil {
		return err
	}

	sender, to, err := resolveSendOptions(
		ctx,
		emailOpts,
		opts.ExplicitOutput,
		opts.Profile,
		"",
		opts.KeychainGet,
		opts.NewSender,
	)
	if err != nil {
		return err
	}
	emailOpts.To = to

	var buf bytes.Buffer
	if err := email.NewVulnSummaryRenderer(emailOpts).Render(&buf, vs); err != nil {
		return err
	}

	id, err := sender.Send(ctx, buf.Bytes())
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stderr, "sent: to=%s from=%s gmail-id=%s\n", to, emailOpts.From, id)
	return nil
}

func vulnColumns() []output.Column {
	return []output.Column{
		{Header: "CVE", Extract: func(v any) string { return v.(iru.Vulnerability).CVEID }},
		{Header: "SEVERITY", Extract: func(v any) string { return v.(iru.Vulnerability).Severity }},
		{Header: "CVSS", Extract: func(v any) string {
			return strconv.FormatFloat(v.(iru.Vulnerability).CVSSScore, 'f', 1, 64)
		}},
		{Header: "KEV", Extract: func(v any) string {
			return strconv.FormatFloat(v.(iru.Vulnerability).KEVScore, 'f', 1, 64)
		}},
		{Header: "DEVICES", Extract: func(v any) string {
			return strconv.Itoa(v.(iru.Vulnerability).DeviceCount)
		}},
		{Header: "STATUS", Extract: func(v any) string { return v.(iru.Vulnerability).Status }},
		{Header: "SOFTWARE", Extract: func(v any) string {
			return strings.Join(v.(iru.Vulnerability).Software, ",")
		}},
	}
}
