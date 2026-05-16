package cmd

import (
	"bytes"
	"context"
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

type userShowOpts struct {
	Identifier     string
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

// UserBundle is the composite shape `user show` returns.
type UserBundle struct {
	User    iru.User               `json:"user" yaml:"user"`
	Devices []DeviceWithDetections `json:"devices" yaml:"devices"`
}

type DeviceWithDetections struct {
	Device     iru.Device      `json:"device" yaml:"device"`
	Detections []iru.Detection `json:"detections" yaml:"detections"`
}

func newUserCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "user",
		Short: "User-scoped queries",
	}
	c.AddCommand(newUserShowCmd())
	return c
}

func newUserShowCmd() *cobra.Command {
	var opts userShowOpts
	c := &cobra.Command{
		Use:   "show <user-id-or-email>",
		Short: "Show a user, their devices, and active detections per device",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			outFmt, _ := cmd.Flags().GetString("output")
			client, err := buildClient(cmd)
			if err != nil {
				return err
			}
			opts.Identifier = args[0]
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
			return runUserShow(cmd.Context(), client, cmd.OutOrStdout(), cmd.ErrOrStderr(), opts)
		},
	}
	c.Flags().BoolVar(&opts.NoCache, "no-cache", false, "Skip the detection cache; always fetch fresh")
	c.Flags().String("email-to", "", "Email To: header (default: email.default_to from config)")
	c.Flags().String("email-from", "", "Email From: header (default: email.from from config, then git user.email)")
	c.Flags().String("email-subject", "", "Email Subject: header (default: rendered email.subject_template or a per-command default)")
	c.Flags().String("email-header-bg", "", "Email header background colour as #RRGGBB (default: email.header_bg or #7566FF)")
	c.Flags().String("email-logo", "", "Path to a PNG to show in the email header (default: email.logo_path)")
	c.Flags().Bool("send-email", false, "Send the rendered email via Gmail (requires `jellyfish configure email` to be run first)")
	return c
}

func runUserShow(ctx context.Context, client iruClient, w, stderr io.Writer, opts userShowOpts) error {
	user, err := resolveUser(ctx, client, opts.Identifier)
	if err != nil {
		return err
	}

	devices, err := client.ListDevices(ctx, iru.DeviceFilters{UserID: user.ID})
	if err != nil {
		return err
	}

	// Iru's /detections endpoint doesn't honour any per-device filter, so we
	// fetch all detections once and bucket them by device id. Single walk for
	// the whole user view, regardless of how many devices the user owns.
	deviceIDs := make(map[string]struct{}, len(devices))
	for _, d := range devices {
		deviceIDs[d.DeviceID] = struct{}{}
	}

	all, err := fetchAllDetections(ctx, client, stderr, !opts.NoCache)
	if err != nil {
		return err
	}

	byDevice := make(map[string][]iru.Detection, len(devices))
	for _, det := range all {
		if _, ok := deviceIDs[det.DeviceID]; ok {
			byDevice[det.DeviceID] = append(byDevice[det.DeviceID], det)
		}
	}

	bundle := UserBundle{User: user, Devices: make([]DeviceWithDetections, len(devices))}
	for i, d := range devices {
		bundle.Devices[i] = DeviceWithDetections{
			Device:     d,
			Detections: byDevice[d.DeviceID],
		}
	}

	if opts.EmailFlags.Send {
		return runSendUserShow(ctx, stderr, opts, bundle)
	}

	return renderUserBundle(w, opts, bundle)
}

func resolveUser(ctx context.Context, client iruClient, id string) (iru.User, error) {
	if strings.Contains(id, "@") {
		return client.FindUserByEmail(ctx, id)
	}
	return client.GetUser(ctx, id)
}

func renderUserBundle(w io.Writer, opts userShowOpts, b UserBundle) error {
	switch opts.Output {
	case "json", "yaml":
		r, err := output.For(opts.Output)
		if err != nil {
			return err
		}
		return r.Render(w, b)
	case "csv":
		return renderUserBundleCSV(w, b)
	case "table", "":
		return renderUserBundleTable(w, b)
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
		return email.NewUserShowRenderer(emailOpts).Render(w, bundleToEmailInput(b))
	default:
		return fmt.Errorf("unsupported output format %q", opts.Output)
	}
}

func runSendUserShow(ctx context.Context, stderr io.Writer, opts userShowOpts, b UserBundle) error {
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
		b.User.Email,
		opts.KeychainGet,
		opts.NewSender,
	)
	if err != nil {
		return err
	}
	emailOpts.To = to

	var buf bytes.Buffer
	if err := email.NewUserShowRenderer(emailOpts).Render(&buf, bundleToEmailInput(b)); err != nil {
		return err
	}

	id, err := sender.Send(ctx, buf.Bytes())
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stderr, "sent: to=%s from=%s gmail-id=%s\n", to, emailOpts.From, id)
	return nil
}

func bundleToEmailInput(b UserBundle) email.UserBundleInput {
	devs := make([]email.UserBundleDevice, len(b.Devices))
	for i, d := range b.Devices {
		devs[i] = email.UserBundleDevice{Device: d.Device, Detections: d.Detections}
	}
	return email.UserBundleInput{User: b.User, Devices: devs}
}

func renderUserBundleTable(w io.Writer, b UserBundle) error {
	_, _ = fmt.Fprintln(w, "USER")
	userTbl := output.Table().WithColumns([]output.Column{
		{Header: "ID", Extract: func(v any) string { return v.(iru.User).ID }},
		{Header: "NAME", Extract: func(v any) string { return v.(iru.User).Name }},
		{Header: "EMAIL", Extract: func(v any) string { return v.(iru.User).Email }},
	})
	if err := userTbl.Render(w, b.User); err != nil {
		return err
	}

	_, _ = fmt.Fprintln(w, "\nDEVICES")
	devices := make([]iru.Device, len(b.Devices))
	for i := range b.Devices {
		devices[i] = b.Devices[i].Device
	}
	devTbl := output.Table().WithColumns([]output.Column{
		{Header: "DEVICE_ID", Extract: func(v any) string { return v.(iru.Device).DeviceID }},
		{Header: "NAME", Extract: func(v any) string { return v.(iru.Device).DeviceName }},
		{Header: "SERIAL", Extract: func(v any) string { return v.(iru.Device).SerialNumber }},
		{Header: "MODEL", Extract: func(v any) string { return v.(iru.Device).Model }},
		{Header: "OS", Extract: func(v any) string { return v.(iru.Device).OSVersion }},
	})
	if err := devTbl.Render(w, devices); err != nil {
		return err
	}

	for _, d := range b.Devices {
		_, _ = fmt.Fprintf(w, "\nDETECTIONS for %s (%s)\n", d.Device.DeviceName, d.Device.SerialNumber)
		if len(d.Detections) == 0 {
			_, _ = fmt.Fprintln(w, "  (none)")
			continue
		}
		dets := append([]iru.Detection(nil), d.Detections...)
		sortDetectionsBySeverity(dets)
		detTbl := output.Table().WithColumns(detectionColumns())
		if err := detTbl.Render(w, dets); err != nil {
			return err
		}
	}
	return nil
}

func renderUserBundleCSV(w io.Writer, b UserBundle) error {
	type row struct {
		userID, userEmail, userName        string
		deviceID, deviceName, serial       string
		cveID, packageName, packageVersion string
		severity                           string
		cvssScore                          float64
		detectionDatetime                  string
	}
	var rows []row
	for _, d := range b.Devices {
		if len(d.Detections) == 0 {
			rows = append(rows, row{
				userID: b.User.ID, userEmail: b.User.Email, userName: b.User.Name,
				deviceID: d.Device.DeviceID, deviceName: d.Device.DeviceName, serial: d.Device.SerialNumber,
			})
			continue
		}
		for _, det := range d.Detections {
			rows = append(rows, row{
				userID: b.User.ID, userEmail: b.User.Email, userName: b.User.Name,
				deviceID: d.Device.DeviceID, deviceName: d.Device.DeviceName, serial: d.Device.SerialNumber,
				cveID: det.CVEID, packageName: det.Name, packageVersion: det.Version,
				severity: det.Severity, cvssScore: det.CVSSScore,
				detectionDatetime: det.DetectionDatetime,
			})
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		ri, rj := iru.SeverityRank(rows[i].severity), iru.SeverityRank(rows[j].severity)
		if ri != rj {
			return ri < rj
		}
		return rows[i].cvssScore > rows[j].cvssScore
	})
	c := output.CSV().WithColumns([]output.Column{
		{Header: "user_id", Extract: func(v any) string { return v.(row).userID }},
		{Header: "user_email", Extract: func(v any) string { return v.(row).userEmail }},
		{Header: "user_name", Extract: func(v any) string { return v.(row).userName }},
		{Header: "device_id", Extract: func(v any) string { return v.(row).deviceID }},
		{Header: "device_name", Extract: func(v any) string { return v.(row).deviceName }},
		{Header: "serial_number", Extract: func(v any) string { return v.(row).serial }},
		{Header: "cve_id", Extract: func(v any) string { return v.(row).cveID }},
		{Header: "package_name", Extract: func(v any) string { return v.(row).packageName }},
		{Header: "package_version", Extract: func(v any) string { return v.(row).packageVersion }},
		{Header: "severity", Extract: func(v any) string { return v.(row).severity }},
		{Header: "cvss_score", Extract: func(v any) string {
			return strconv.FormatFloat(v.(row).cvssScore, 'f', 1, 64)
		}},
		{Header: "detection_datetime", Extract: func(v any) string { return v.(row).detectionDatetime }},
	})
	return c.Render(w, rows)
}
