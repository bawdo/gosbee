package iru

// Device is the subset of Iru's device record that jellyfish uses.
// Field names match Iru's JSON; extras are ignored by encoding/json.
type Device struct {
	DeviceID     string `json:"device_id" yaml:"device_id"`
	DeviceName   string `json:"device_name" yaml:"device_name"`
	SerialNumber string `json:"serial_number" yaml:"serial_number"`
	Model        string `json:"model" yaml:"model"`
	OSVersion    string `json:"os_version" yaml:"os_version"`
	Platform     string `json:"platform" yaml:"platform"`
	BlueprintID  string `json:"blueprint_id" yaml:"blueprint_id"`
	User         User   `json:"user" yaml:"user"`
}

// User is the subset of Iru's user record jellyfish uses.
type User struct {
	ID    string `json:"id" yaml:"id"`
	Name  string `json:"name" yaml:"name"`
	Email string `json:"email" yaml:"email"`
}

// Detection is one vulnerability detection on a device.
type Detection struct {
	DetectionID string `json:"detection_id" yaml:"detection_id"`
	DeviceID    string `json:"device_id" yaml:"device_id"`
	CVE         string `json:"cve" yaml:"cve"`
	Severity    string `json:"severity" yaml:"severity"`
	Status      string `json:"status" yaml:"status"`
	AppName     string `json:"app_name" yaml:"app_name"`
	AppVersion  string `json:"app_version" yaml:"app_version"`
	CreatedAt   string `json:"created_at" yaml:"created_at"`
	UpdatedAt   string `json:"updated_at" yaml:"updated_at"`
}

// DeviceFilters is the filter set for /devices queries.
type DeviceFilters struct {
	UserID       string
	SerialNumber string
}

// DetectionFilters is the filter set for /vulnerability-management/detections queries.
type DetectionFilters struct {
	DeviceID string
	Status   string // pass-through to Iru
}
