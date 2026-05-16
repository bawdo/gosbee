package email

import (
	"embed"
	"fmt"
	"strings"
	"text/template"

	"flipgroup.com.au/jellyfish/internal/iru"
)

//go:embed templates/vulns_summary.txt.tmpl
var vulnSummaryFS embed.FS

// vulnSummaryView is the data shape vulns_summary templates render against.
// Every field is pre-formatted so templates contain no Go logic.
type vulnSummaryView struct {
	Tenant         string
	GeneratedAtStr string
	GeneratedDate  string
	TotalCVEs      int
	CriticalCount  int
	HighCount      int
	MediumCount    int
	LowCount       int
	KEVCount       int
	DeviceCount    int
	Rows           []vulnSummaryRow
}

type vulnSummaryRow struct {
	CVEID         string
	Severity      string
	SeverityClass string
	CVSS          float64
	CVSSStr       string
	IsKEV         bool
	Devices       int
	Software      string
	Status        string
	NVDLink       string
	MITRELink     string
}

func severityClass(sev string) string {
	switch strings.ToLower(sev) {
	case "critical":
		return "crit"
	case "high":
		return "high"
	case "medium":
		return "med"
	case "low":
		return "low"
	default:
		return "und"
	}
}

func buildVulnSummaryView(vs []iru.Vulnerability, opts Options) vulnSummaryView {
	view := vulnSummaryView{
		Tenant:         opts.Tenant,
		GeneratedAtStr: opts.GeneratedAt.Format("2 Jan 2006 - 15:04 MST"),
		GeneratedDate:  opts.GeneratedAt.Format("2006-01-02"),
		TotalCVEs:      len(vs),
		Rows:           make([]vulnSummaryRow, 0, len(vs)),
	}
	maxDevices := 0
	for _, v := range vs {
		isKEV := v.KEVScore > 0
		if isKEV {
			view.KEVCount++
		}
		switch strings.ToLower(v.Severity) {
		case "critical":
			view.CriticalCount++
		case "high":
			view.HighCount++
		case "medium":
			view.MediumCount++
		case "low":
			view.LowCount++
		}
		if v.DeviceCount > maxDevices {
			maxDevices = v.DeviceCount
		}
		view.Rows = append(view.Rows, vulnSummaryRow{
			CVEID:         v.CVEID,
			Severity:      v.Severity,
			SeverityClass: severityClass(v.Severity),
			CVSS:          v.CVSSScore,
			CVSSStr:       fmt.Sprintf("%.1f", v.CVSSScore),
			IsKEV:         isKEV,
			Devices:       v.DeviceCount,
			Software:      strings.Join(v.Software, ", "),
			Status:        v.Status,
			NVDLink:       buildCVELink(opts.CVELinkPrimary, v.CVEID),
			MITRELink:     buildCVELink(opts.CVELinkSecondary, v.CVEID),
		})
	}
	view.DeviceCount = maxDevices
	return view
}

func renderVulnSummaryText(v vulnSummaryView) (string, error) {
	tmpl, err := template.New("vulns_summary.txt.tmpl").Funcs(template.FuncMap{
		"cond": func(b bool, t, f string) string {
			if b {
				return t
			}
			return f
		},
	}).ParseFS(vulnSummaryFS, "templates/vulns_summary.txt.tmpl")
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	if err := tmpl.Execute(&sb, v); err != nil {
		return "", err
	}
	return sb.String(), nil
}
