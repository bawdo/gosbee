package email

import (
	"embed"
	"fmt"
	htmltmpl "html/template"
	"io"
	"strings"
	texttmpl "text/template"

	"flipgroup.com.au/jellyfish/internal/iru"
	"flipgroup.com.au/jellyfish/internal/output"
)

//go:embed templates/vulns_summary.txt.tmpl templates/vulns_summary.html.tmpl
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
	tmpl, err := texttmpl.New("vulns_summary.txt.tmpl").Funcs(texttmpl.FuncMap{
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

func renderVulnSummaryHTML(v vulnSummaryView) (string, error) {
	tmpl, err := htmltmpl.New("vulns_summary.html.tmpl").Funcs(htmltmpl.FuncMap{
		"sevRowBG":  sevRowBG,
		"sevPillBG": sevPillBG,
		"sevPillFG": sevPillFG,
	}).ParseFS(vulnSummaryFS, "templates/vulns_summary.html.tmpl")
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	if err := tmpl.Execute(&sb, v); err != nil {
		return "", err
	}
	return sb.String(), nil
}

type vulnSummaryRenderer struct {
	opts Options
}

// NewVulnSummaryRenderer returns an output.Renderer whose Render(w, v) expects
// v to be []iru.Vulnerability and writes a complete .eml message to w.
func NewVulnSummaryRenderer(opts Options) output.Renderer {
	return &vulnSummaryRenderer{opts: opts.withDefaults()}
}

func (r *vulnSummaryRenderer) Render(w io.Writer, v any) error {
	vs, ok := v.([]iru.Vulnerability)
	if !ok {
		return fmt.Errorf("email vulns summary renderer expected []iru.Vulnerability, got %T", v)
	}
	if err := validateLinkTemplate("primary", r.opts.CVELinkPrimary); err != nil {
		return err
	}
	if err := validateLinkTemplate("secondary", r.opts.CVELinkSecondary); err != nil {
		return err
	}
	if r.opts.From == "" {
		return fmt.Errorf("email renderer requires a non-empty From address")
	}

	view := buildVulnSummaryView(vs, r.opts)

	subject := r.opts.Subject
	if subject == "" {
		subject = "Jellyfish vulnerability summary - " + view.GeneratedDate
	}

	htmlBody, err := renderVulnSummaryHTML(view)
	if err != nil {
		return err
	}
	textBody, err := renderVulnSummaryText(view)
	if err != nil {
		return err
	}

	boundary := r.opts.BoundaryOverride
	if boundary == "" {
		boundary, err = randomBoundary()
		if err != nil {
			return err
		}
	}
	messageID := r.opts.MessageIDOverride
	if messageID == "" {
		messageID, err = randomMessageID(domainFromAddress(r.opts.From))
		if err != nil {
			return err
		}
	}

	bytesOut, err := assembleMessage(messageHeaders{
		From:    r.opts.From,
		To:      r.opts.To,
		Subject: subject,
		Date:    r.opts.GeneratedAt,
	}, htmlBody, textBody, boundary, messageID)
	if err != nil {
		return err
	}
	_, err = w.Write(bytesOut)
	return err
}

// domainFromAddress extracts the right-hand side of an email address used in
// a From header. Returns "localhost" if no '@' is present (defensive only).
func domainFromAddress(addr string) string {
	at := strings.LastIndex(addr, "@")
	if at < 0 {
		return "localhost"
	}
	rest := addr[at+1:]
	if end := strings.IndexAny(rest, "> "); end >= 0 {
		rest = rest[:end]
	}
	return rest
}
