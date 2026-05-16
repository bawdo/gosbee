package email

import (
	"bytes"
	"errors"
	"fmt"
	"image/png"
	"os"
	"path/filepath"
)

// logoPart is the inline image attachment included in the multipart/related
// envelope when a logo is configured.
type logoPart struct {
	Bytes []byte
	Name  string
	CID   string
}

// MaxLogoBytes caps the on-disk PNG size accepted by loadLogo. 512 KiB is
// generous for a header logo and well under any mail-client size limit.
const MaxLogoBytes = 512 * 1024

// loadLogo reads path, validates it as a PNG no larger than MaxLogoBytes,
// and returns a *logoPart. An empty path returns (nil, nil): callers treat
// that as "no logo configured", not an error.
func loadLogo(path string) (*logoPart, error) {
	if path == "" {
		return nil, nil
	}
	// #nosec G304 - path is the operator's own config or flag input
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	if info.Size() > MaxLogoBytes {
		return nil, fmt.Errorf("logo %s is %d bytes (max %d)", path, info.Size(), MaxLogoBytes)
	}
	// #nosec G304 - see above
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if _, err := png.Decode(bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("decode PNG %s: %w", path, err)
	}
	return &logoPart{
		Bytes: data,
		Name:  filepath.Base(path),
		CID:   "jf-logo",
	}, nil
}

// errLogoNotConfigured is returned only when callers want to distinguish
// "no logo configured" from a hard load error. loadLogo itself returns
// (nil, nil) in that case; this sentinel is reserved for future use.
var errLogoNotConfigured = errors.New("no logo configured")
