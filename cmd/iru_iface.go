package cmd

import (
	"context"

	"flipgroup.com.au/jellyfish/internal/iru"
)

// iruClient is the surface area the cmd package uses. Implemented by *iru.Client
// in production and a fake in tests.
type iruClient interface {
	ListDevicesPage(ctx context.Context, f iru.DeviceFilters, limit, offset int) ([]iru.Device, error)
	ListDevices(ctx context.Context, f iru.DeviceFilters) ([]iru.Device, error)
	GetDeviceBySerial(ctx context.Context, serial string) (iru.Device, error)
	GetUser(ctx context.Context, id string) (iru.User, error)
	FindUserByEmail(ctx context.Context, email string) (iru.User, error)
	ListDetections(ctx context.Context, f iru.DetectionFilters) ([]iru.Detection, error)
	ListDetectionsPage(ctx context.Context, f iru.DetectionFilters, limit int, cursor string) ([]iru.Detection, string, error)
	ListDetectionsStream(ctx context.Context, f iru.DetectionFilters, cb func(page []iru.Detection) error) error
	ListVulnerabilities(ctx context.Context, f iru.VulnerabilityFilters) ([]iru.Vulnerability, error)
	ListVulnerabilitiesStream(ctx context.Context, f iru.VulnerabilityFilters, cb func(page []iru.Vulnerability) error) error
	ListVulnerabilitiesPage(ctx context.Context, f iru.VulnerabilityFilters, page, size int) ([]iru.Vulnerability, int, error)
}
