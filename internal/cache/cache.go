// Package cache stores the latest detection walk on disk so subsequent
// invocations within a TTL can skip the (slow) walk.
package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"flipgroup.com.au/jellyfish/internal/iru"
)

const fileMode = 0o600
const dirMode = 0o700

// DefaultTTL is the maximum age for a cached detection list to be considered fresh.
const DefaultTTL = 15 * time.Minute

// DefaultPath returns ~/.cache/jellyfish/detections.json or the OS-appropriate
// equivalent (os.UserCacheDir).
func DefaultPath() (string, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "jellyfish", "detections.json"), nil
}

// File is what we write to disk.
type File struct {
	Version    int             `json:"version"`
	FetchedAt  time.Time       `json:"fetched_at"`
	Detections []iru.Detection `json:"detections"`
}

// Load reads the cache file. Returns (detections, true, nil) if a fresh
// (within ttl) entry exists. Returns (nil, false, nil) for a miss — file
// not present, corrupted, or expired. Returns (nil, false, err) for I/O
// errors other than NotExist.
func Load(path string, ttl time.Duration) ([]iru.Detection, bool, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is the OS cache dir + fixed filename
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var f File
	if err := json.Unmarshal(data, &f); err != nil {
		// Treat parse errors as a miss — corrupt cache shouldn't fail the command.
		return nil, false, nil
	}
	if f.Version != 1 {
		return nil, false, nil
	}
	if time.Since(f.FetchedAt) > ttl {
		return nil, false, nil
	}
	return f.Detections, true, nil
}

// Save writes the cache file with mode 0600, creating the parent dir with 0700.
func Save(path string, dets []iru.Detection) error {
	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}
	f := File{
		Version:    1,
		FetchedAt:  time.Now().UTC(),
		Detections: dets,
	}
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, fileMode)
}

// VulnFile is the on-disk representation of a vulnerability walk.
type VulnFile struct {
	Version         int                  `json:"version"`
	FetchedAt       time.Time            `json:"fetched_at"`
	Vulnerabilities []iru.Vulnerability  `json:"vulnerabilities"`
}

// DefaultVulnPath returns the cache file path for vulnerability rollups —
// distinct from the detections cache.
func DefaultVulnPath() (string, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "jellyfish", "vulnerabilities.json"), nil
}

// LoadVulnerabilities reads the vulnerability cache. Same hit/miss/error
// semantics as Load (detections).
func LoadVulnerabilities(path string, ttl time.Duration) ([]iru.Vulnerability, bool, error) {
	data, err := os.ReadFile(path) //nolint:gosec
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var f VulnFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, false, nil
	}
	if f.Version != 1 {
		return nil, false, nil
	}
	if time.Since(f.FetchedAt) > ttl {
		return nil, false, nil
	}
	return f.Vulnerabilities, true, nil
}

// SaveVulnerabilities writes the cache file with mode 0600.
func SaveVulnerabilities(path string, vulns []iru.Vulnerability) error {
	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}
	f := VulnFile{
		Version:         1,
		FetchedAt:       time.Now().UTC(),
		Vulnerabilities: vulns,
	}
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, fileMode)
}
