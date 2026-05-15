package iru

// Device is the subset of Iru's device record that jellyfish uses.
// Field names match Iru's JSON; unknown extras are ignored by encoding/json.
type Device struct {
	DeviceID                 string   `json:"device_id" yaml:"device_id"`
	DeviceName               string   `json:"device_name" yaml:"device_name"`
	Model                    string   `json:"model" yaml:"model"`
	SerialNumber             string   `json:"serial_number" yaml:"serial_number"`
	UDID                     string   `json:"udid" yaml:"udid"`
	Platform                 string   `json:"platform" yaml:"platform"`
	OSVersion                string   `json:"os_version" yaml:"os_version"`
	SupplementalBuildVersion string   `json:"supplemental_build_version" yaml:"supplemental_build_version"`
	LastCheckIn              string   `json:"last_check_in" yaml:"last_check_in"`
	User                     User     `json:"user" yaml:"user"`
	AssetTag                 string   `json:"asset_tag" yaml:"asset_tag"`
	BlueprintID              string   `json:"blueprint_id" yaml:"blueprint_id"`
	BlueprintName            string   `json:"blueprint_name" yaml:"blueprint_name"`
	MDMEnabled               bool     `json:"mdm_enabled" yaml:"mdm_enabled"`
	AgentInstalled           bool     `json:"agent_installed" yaml:"agent_installed"`
	IsMissing                bool     `json:"is_missing" yaml:"is_missing"`
	IsRemoved                bool     `json:"is_removed" yaml:"is_removed"`
	AgentVersion             string   `json:"agent_version" yaml:"agent_version"`
	FirstEnrollment          string   `json:"first_enrollment" yaml:"first_enrollment"`
	LastEnrollment           string   `json:"last_enrollment" yaml:"last_enrollment"`
	FullSoftwareVersion      string   `json:"full_software_version" yaml:"full_software_version"`
	LostModeStatus           string   `json:"lost_mode_status" yaml:"lost_mode_status"`
	Tags                     []string `json:"tags" yaml:"tags"`
}

// User is Iru's user record. The standalone /users endpoint returns the full
// set of fields; when User is nested inside a Device, only id/name/email/
// active/is_archived are populated. Both `archived` (standalone) and
// `is_archived` (nested-in-device) are decoded — only one will ever be set
// per record.
type User struct {
	ID               string           `json:"id" yaml:"id"`
	Name             string           `json:"name" yaml:"name"`
	Email            string           `json:"email" yaml:"email"`
	Active           bool             `json:"active" yaml:"active"`
	Archived         bool             `json:"archived" yaml:"archived"`
	IsArchived       bool             `json:"is_archived" yaml:"is_archived"`
	Department       string           `json:"department,omitempty" yaml:"department,omitempty"`
	JobTitle         string           `json:"job_title,omitempty" yaml:"job_title,omitempty"`
	DeprecatedUserID string           `json:"deprecated_user_id,omitempty" yaml:"deprecated_user_id,omitempty"`
	DeviceCount      int              `json:"device_count,omitempty" yaml:"device_count,omitempty"`
	CreatedAt        string           `json:"created_at,omitempty" yaml:"created_at,omitempty"`
	UpdatedAt        string           `json:"updated_at,omitempty" yaml:"updated_at,omitempty"`
	Integration      *UserIntegration `json:"integration,omitempty" yaml:"integration,omitempty"`
}

// UserIntegration is the source-of-truth identity provider for a user (e.g.
// Entra ID, Google Workspace). Populated on standalone /users records.
type UserIntegration struct {
	ID   int    `json:"id" yaml:"id"`
	Name string `json:"name" yaml:"name"`
	UUID string `json:"uuid" yaml:"uuid"`
	Type string `json:"type" yaml:"type"`
}

// Detection is one vulnerability detection on a device. Iru's detection
// records do not include a detection_id or status field; a detection exists
// for as long as the underlying CVE remains on the device. When the package
// is patched, Iru drops the detection.
type Detection struct {
	DeviceID           string  `json:"device_id" yaml:"device_id"`
	DeviceName         string  `json:"device_name" yaml:"device_name"`
	DeviceSerialNumber string  `json:"device_serial_number" yaml:"device_serial_number"`
	DeviceModel        string  `json:"device_model" yaml:"device_model"`
	DeviceOSVersion    string  `json:"device_os_version" yaml:"device_os_version"`
	BlueprintID        string  `json:"blueprint_id" yaml:"blueprint_id"`
	BlueprintName      string  `json:"blueprint_name" yaml:"blueprint_name"`
	Name               string  `json:"name" yaml:"name"`
	Version            string  `json:"version" yaml:"version"`
	Path               string  `json:"path" yaml:"path"`
	BundleID           *string `json:"bundle_id" yaml:"bundle_id"`
	CVEID              string  `json:"cve_id" yaml:"cve_id"`
	Description        string  `json:"description" yaml:"description"`
	CVELink            string  `json:"cve_link" yaml:"cve_link"`
	CVSSScore          float64 `json:"cvss_score" yaml:"cvss_score"`
	Severity           string  `json:"severity" yaml:"severity"`
	DetectionDatetime  string  `json:"detection_datetime" yaml:"detection_datetime"`
	CVEPublishedAt     string  `json:"cve_published_at" yaml:"cve_published_at"`
	CVEModifiedAt      string  `json:"cve_modified_at" yaml:"cve_modified_at"`
}

// DeviceFilters is the filter set for /devices queries.
type DeviceFilters struct {
	UserID       string
	SerialNumber string
}

// DetectionFilters is the filter set for /vulnerability-management/detections
// queries. The Status field is currently ignored — Iru's detection records
// don't carry a status — but is left in place to be removed in a follow-up
// commit.
type DetectionFilters struct {
	DeviceID string
	Status   string
}
