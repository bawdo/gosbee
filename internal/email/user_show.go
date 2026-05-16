package email

import (
	"embed"
	"fmt"
	htmltmpl "html/template"
	"strings"
	texttmpl "text/template"

	"flipgroup.com.au/jellyfish/internal/iru"
)

// UserBundleInput is the typed shape that NewUserShowRenderer expects. The
// cmd-layer UserBundle struct mirrors this; cmd translates before calling.
// Keeping a package-local type avoids a circular import.
type UserBundleInput struct {
	User    iru.User
	Devices []UserBundleDevice
}

type UserBundleDevice struct {
	Device     iru.Device
	Detections []iru.Detection
}

//go:embed templates/user_show.txt.tmpl templates/user_show.html.tmpl
var userShowFS embed.FS

type userShowView struct {
	User           iru.User
	Tenant         string
	GeneratedAtStr string
	GeneratedDate  string
	TotalCVEs      int
	CriticalCount  int
	HighCount      int
	MediumCount    int
	LowCount       int
	DeviceCount    int
	Devices        []userShowDeviceView
}

type userShowDeviceView struct {
	Device iru.Device
	Rows   []userShowRow
}

type userShowRow struct {
	CVEID         string
	Severity      string
	SeverityClass string
	CVSSStr       string
	Package       string
	NVDLink       string
	MITRELink     string
}

func buildUserShowView(b UserBundleInput, opts Options) userShowView {
	view := userShowView{
		User:           b.User,
		Tenant:         opts.Tenant,
		GeneratedAtStr: opts.GeneratedAt.Format("2 Jan 2006 - 15:04 MST"),
		GeneratedDate:  opts.GeneratedAt.Format("2006-01-02"),
		DeviceCount:    len(b.Devices),
		Devices:        make([]userShowDeviceView, len(b.Devices)),
	}
	for i, dev := range b.Devices {
		rows := make([]userShowRow, 0, len(dev.Detections))
		for _, det := range dev.Detections {
			switch strings.ToLower(det.Severity) {
			case "critical":
				view.CriticalCount++
			case "high":
				view.HighCount++
			case "medium":
				view.MediumCount++
			case "low":
				view.LowCount++
			}
			view.TotalCVEs++
			pkg := det.Name
			if det.Version != "" {
				pkg = det.Name + "@" + det.Version
			}
			rows = append(rows, userShowRow{
				CVEID:         det.CVEID,
				Severity:      det.Severity,
				SeverityClass: severityClass(det.Severity),
				CVSSStr:       fmt.Sprintf("%.1f", det.CVSSScore),
				Package:       pkg,
				NVDLink:       buildCVELink(opts.CVELinkPrimary, det.CVEID),
				MITRELink:     buildCVELink(opts.CVELinkSecondary, det.CVEID),
			})
		}
		view.Devices[i] = userShowDeviceView{Device: dev.Device, Rows: rows}
	}
	return view
}

func renderUserShowText(v userShowView) (string, error) {
	tmpl, err := texttmpl.New("user_show.txt.tmpl").Funcs(texttmpl.FuncMap{
		"cond": func(b bool, t, f string) string {
			if b {
				return t
			}
			return f
		},
	}).ParseFS(userShowFS, "templates/user_show.txt.tmpl")
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	if err := tmpl.Execute(&sb, v); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func renderUserShowHTML(v userShowView) (string, error) {
	tmpl, err := htmltmpl.New("user_show.html.tmpl").Funcs(htmltmpl.FuncMap{
		"sevRowBG":  sevRowBG,
		"sevPillBG": sevPillBG,
		"sevPillFG": sevPillFG,
	}).ParseFS(userShowFS, "templates/user_show.html.tmpl")
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	if err := tmpl.Execute(&sb, v); err != nil {
		return "", err
	}
	return sb.String(), nil
}
